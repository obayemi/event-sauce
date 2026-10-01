//! Integration test for L9 — a failed `apply()` must POISON the `AggregateRoot`.
//!
//! `AggregateRoot::apply` runs validate -> apply -> post_validate. When
//! `post_validate` fails, the apply closure has ALREADY mutated the entity, but
//! the version is not bumped and the event is not pushed to pending. Today the
//! half-mutated aggregate stays usable: a subsequent `commit` silently succeeds,
//! persisting an aggregate whose entity state reflects a REJECTED event.
//!
//! The fix is to poison the `AggregateRoot` on any failed `apply*` and make the
//! commit-preparation path refuse a poisoned root with `Error::InvalidState`,
//! forcing the caller to discard and reload. These tests assert that contract.

use event_sauce_core::Repository;
use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, Entity, EntityId, Error, EventStore,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
enum AccountError {
    #[error("balance would go negative")]
    Overdrawn,
}

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

        // `apply` mutates the balance first; `post_validate` then rejects a
        // negative balance — so a rejected adjustment leaves `balance` mutated.
        Adjusted {
            delta: i64,
        }
        @post_validate |account, _event| {
            if account.balance < 0 {
                return Err(AccountError::Overdrawn);
            }
            Ok(())
        }
        => |account, event| {
            account.balance += event.delta;
        },

        // Refused BEFORE anything is touched. The counterpart of `Adjusted`: this
        // one leaves the account exactly as it was.
        Frozen {
            note: String,
        }
        @validate |account, _event| {
            if account.balance > 0 {
                return Err(AccountError::Overdrawn);
            }
            Ok(())
        }
        => |account, event| {
            account.balance -= event.note.len() as i64;
        },

        Closed {
            reason: String,
        }
        @delete
        => |account, _event| {
            account
        },
    }
}

command_handler! {
    impl Account {
        @clock @init fn open(opening_balance: i64) -> OpenedEvent { opening_balance };
        @clock fn adjust(delta: i64) -> AdjustedEvent { delta };
        @clock fn freeze(note: String) -> FrozenEvent { note };
        @clock @delete fn close(reason: String) -> ClosedEvent { reason };
    }
}

/// A rejected `apply()` (failing `post_validate`) must poison the aggregate so a
/// later `commit` refuses it with `Error::InvalidState` instead of silently
/// persisting an entity whose state reflects the rejected event.
#[tokio::test]
async fn test_failed_apply_poisons_and_blocks_commit() {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();

    let mut account = Account::open(0).unwrap();
    repo.save(&mut account).await.unwrap();

    // A valid adjustment succeeds.
    account.adjust(5).unwrap();

    // This adjustment drives the balance negative: `apply` mutates balance to
    // -95, then `post_validate` rejects it. `apply()` returns Err.
    let rejected = account.adjust(-100);
    assert!(
        rejected.is_err(),
        "adjustment that overdraws must be rejected by post_validate"
    );

    // BUG: the entity is now half-mutated (balance reflects the rejected -100),
    // yet the aggregate is still usable. Committing it must NOT silently succeed;
    // it must surface the poisoned state as Error::InvalidState so the caller
    // discards and reloads.
    let commit_result = repo.save(&mut account).await;

    assert!(
        commit_result.is_err(),
        "committing a poisoned aggregate must fail, not silently persist half-mutated state"
    );
    assert!(
        matches!(commit_result.as_ref().unwrap_err(), Error::InvalidState(_)),
        "expected Error::InvalidState for a poisoned aggregate, got {:?}",
        commit_result.unwrap_err()
    );
}

/// An aggregate with only successful applies is never poisoned and commits
/// normally — the poison guard must be zero-cost for the happy path.
#[tokio::test]
async fn test_clean_aggregate_is_not_poisoned() {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();

    let mut account = Account::open(100).unwrap();
    repo.save(&mut account).await.unwrap();

    let id = account.entity_id();

    account.adjust(-40).unwrap();
    account.adjust(10).unwrap();
    repo.save(&mut account).await.unwrap();

    assert_eq!(repo.load(id).await.unwrap().balance, 70);
}

/// Poison must also block the delete path: deleting a poisoned aggregate carries
/// the poison into the `DeletedAggregateRoot`, and committing it must fail with
/// `Error::InvalidState` rather than persist inconsistent state.
#[tokio::test]
async fn test_failed_apply_poisons_and_blocks_delete_commit() {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();

    let mut account = Account::open(0).unwrap();
    repo.save(&mut account).await.unwrap();

    // Poison the aggregate with a rejected adjustment.
    assert!(account.adjust(-100).is_err());

    // Deleting a poisoned aggregate yields a poisoned deleted root.
    let mut deleted = account.close("cleanup".to_string()).unwrap();

    let commit_result = repo.save_deleted(&mut deleted).await;

    assert!(
        commit_result.is_err(),
        "committing a poisoned deleted aggregate must fail"
    );
    assert!(
        matches!(commit_result.as_ref().unwrap_err(), Error::InvalidState(_)),
        "expected Error::InvalidState for a poisoned deleted aggregate, got {:?}",
        commit_result.unwrap_err()
    );
}

/// A clean aggregate can still be deleted and committed normally — the delete
/// poison guard must not interfere with the happy path.
#[tokio::test]
async fn test_clean_aggregate_can_be_deleted() {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();

    let mut account = Account::open(50).unwrap();
    repo.save(&mut account).await.unwrap();

    let id = account.entity_id();
    let mut deleted = account.close("done".to_string()).unwrap();
    repo.save_deleted(&mut deleted).await.unwrap();

    assert!(repo.load(id).await.is_err());
}

/// A REFUSAL is not a poisoning.
///
/// `validate` runs before anything is mutated, so an aggregate that refused a command
/// is exactly as it was and must stay usable. This is the ordinary shape of a caller
/// that EXPECTS a refusal — `let _ = incident.raise_severity(..)`, where a member that
/// does not raise the grade is a no-op — and it must still be able to save what it
/// did change.
///
/// Before this, `apply` poisoned on any error from `dispatch`, which cannot tell a
/// refusal apart from a failed apply. The next save then failed with "poisoned", and
/// the work the caller HAD done was lost.
#[tokio::test]
async fn a_refused_command_leaves_the_aggregate_usable() {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Account>();

    let mut account = Account::open(100).expect("opening is refused by nothing");
    repo.save(&mut account).await.expect("save the opening");

    // Refused by `validate`, which runs before `apply` touches anything.
    assert!(matches!(
        account.freeze("cold".to_owned()),
        Err(AccountError::Overdrawn)
    ));
    assert_eq!(account.entity().balance, 100, "a refusal changes nothing");

    // And the account still works: the refusal did not take the aggregate with it.
    account.adjust(-40).expect("a legal adjustment");
    assert_eq!(account.entity().balance, 60);
    repo.save(&mut account)
        .await
        .expect("a root that refused a command is still committable");
}
