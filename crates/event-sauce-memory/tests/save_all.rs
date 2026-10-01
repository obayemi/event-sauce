//! Integration test for `Repository::save_all` (M3-L1).
//!
//! `save_all` prepares several aggregates and persists them via a single
//! `EventStore::append_batch`. On the in-memory backend the default
//! `append_batch` loops `append` per stream, so this test asserts the API shape
//! and the happy path (all aggregates persisted on success). It deliberately
//! does NOT assert rollback: the in-memory default is per-stream, not atomic —
//! only the PostgreSQL override is transactional (see the postgres crate's
//! `test_append_batch_is_atomic` / `test_flush_multi_aggregate_is_atomic`).

use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, Entity, EntityId, EventStore,
    Repository,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
enum AccountError {}

impl AggregateError for AccountError {}

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    id: EntityId,
    balance: i64,
}

impl Entity for Account {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Account {
    type Event = AccountEvent;
    type Error = AccountError;
    type DeletedState = Self;
}

define_events! {
    enum AccountEvent for Account {
        Opened {
            opening_balance: i64,
        }
        @init
        => |id, event| {
            Account {
                id,
                balance: event.opening_balance,
            }
        },

        Adjusted {
            delta: i64,
        } => |account, event| {
            account.balance += event.delta;
        },
    }
}

command_handler! {
    impl Account {
        @clock @init fn open(opening_balance: i64) -> OpenedEvent { opening_balance };
        @clock fn adjust(delta: i64) -> AdjustedEvent { delta };
    }
}

/// `save_all` persists every aggregate in one call: a transfer that withdraws
/// from one account and deposits into another saves both, and a subsequent load
/// reflects both writes.
#[tokio::test]
async fn test_save_all_persists_all_aggregates() {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();

    let mut from = Account::open(1_000).unwrap();
    let mut to = Account::open(0).unwrap();

    // Both accounts must already exist before the transfer, so commit the init
    // events first.
    repo.save_all(&mut [&mut from, &mut to]).await.unwrap();

    let from_id = from.entity_id();
    let to_id = to.entity_id();

    // A two-aggregate "transfer" reaction: debit one, credit the other, saved
    // together in one call.
    from.adjust(-250).unwrap();
    to.adjust(250).unwrap();
    repo.save_all(&mut [&mut from, &mut to]).await.unwrap();

    let loaded_from = repo.load(from_id).await.unwrap();
    let loaded_to = repo.load(to_id).await.unwrap();

    assert_eq!(
        loaded_from.balance, 750,
        "debited account must reflect -250"
    );
    assert_eq!(loaded_to.balance, 250, "credited account must reflect +250");
}

/// `save_all` skips aggregates that have no pending events without erroring, so
/// a mixed batch (one dirty, one clean) persists only the dirty one.
#[tokio::test]
async fn test_save_all_skips_aggregates_without_pending_events() {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();

    let mut dirty = Account::open(10).unwrap();
    let mut clean = Account::open(20).unwrap();
    repo.save_all(&mut [&mut dirty, &mut clean]).await.unwrap();

    let dirty_id = dirty.entity_id();
    let clean_id = clean.entity_id();

    // Only `dirty` has a pending event; `clean` is untouched after its initial
    // save. The clean aggregate must be skipped (no spurious conflict).
    dirty.adjust(5).unwrap();
    repo.save_all(&mut [&mut dirty, &mut clean]).await.unwrap();

    assert_eq!(repo.load(dirty_id).await.unwrap().balance, 15);
    assert_eq!(repo.load(clean_id).await.unwrap().balance, 20);
}
