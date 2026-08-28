//! Integration tests for `InMemoryStateStore` (Phase 3 of the state-store
//! split).
//!
//! The parity test is the acceptance criterion of the split: one aggregate +
//! command module, written once, runs unchanged against the event-sourced and
//! the state-stored repositories behind `R: Repository<A>`. The remaining
//! tests cover the store's own guarantees: optimistic concurrency, claim
//! uniqueness with release on delete, in-transaction projections with
//! rollback, `EventFilter` routing, and atomic `save_batch`.

use async_trait::async_trait;
use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateClaim, AggregateError, Entity, EntityId,
    Error, EventEnvelope, EventFilter, EventStore, Repository, Result, StateProjection, StateStore,
};
use event_sauce_memory::{InMemoryEventStore, InMemoryProjectionContext, InMemoryStateStore};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
enum AccountError {}

impl AggregateError for AccountError {}

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    id: EntityId,
    owner: String,
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

    fn claims(&self) -> Vec<AggregateClaim> {
        vec![AggregateClaim::new("Account.owner", json!(self.owner))]
    }
}

define_events! {
    enum AccountEvent for Account {
        Opened {
            owner: String,
            opening_balance: i64,
        }
        @init
        => |id, event| {
            Account {
                id,
                owner: event.owner.clone(),
                balance: event.opening_balance,
            }
        },

        Deposited {
            amount: i64,
        } => |account, event| {
            account.balance += event.amount;
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
        @clock @init fn open(owner: String, opening_balance: i64) -> OpenedEvent { owner, opening_balance };
        @clock fn deposit(amount: i64) -> DepositedEvent { amount };
        @clock @delete fn close(reason: String) -> ClosedEvent { reason };
    }
}

struct DepositTotals;

#[async_trait]
impl StateProjection<InMemoryProjectionContext> for DepositTotals {
    fn name(&self) -> &str {
        "deposit_totals"
    }

    fn filter(&self) -> EventFilter {
        EventFilter::by_event_type("Account.Deposited")
    }

    async fn project(
        &self,
        ctx: &mut InMemoryProjectionContext,
        events: &[EventEnvelope],
    ) -> Result<()> {
        let mut total = ctx
            .get("deposit_total")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let mut count = ctx
            .get("deposit_event_count")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        for event in events {
            total += event.event_data["amount"].as_i64().unwrap_or(0);
            count += 1;
        }
        ctx.insert("deposit_total", json!(total));
        ctx.insert("deposit_event_count", json!(count));
        Ok(())
    }
}

struct FailOnDeposit;

#[async_trait]
impl StateProjection<InMemoryProjectionContext> for FailOnDeposit {
    fn name(&self) -> &str {
        "fail_on_deposit"
    }

    fn filter(&self) -> EventFilter {
        EventFilter::by_event_type("Account.Deposited")
    }

    async fn project(
        &self,
        _ctx: &mut InMemoryProjectionContext,
        _events: &[EventEnvelope],
    ) -> Result<()> {
        Err(Error::custom("deposits are closed"))
    }
}

/// The generic scenario every repository must satisfy identically: create,
/// command, save, load, modify, exists, `get_version`, and the delete
/// lifecycle via `load_any`/`load_deleted`.
async fn run_scenario<R: Repository<Account>>(repo: &R, owner: &str) {
    let mut account = Account::open(owner.to_string(), 100).unwrap();
    let id = account.entity_id();

    assert!(!repo.exists(id).await.unwrap());
    repo.save(&mut account).await.unwrap();
    assert!(account.pending_events().is_empty());
    assert!(repo.exists(id).await.unwrap());
    assert_eq!(repo.get_version(id).await.unwrap().as_i64(), 1);

    account.deposit(50).unwrap();
    repo.save(&mut account).await.unwrap();

    let loaded = repo.load(id).await.unwrap();
    assert_eq!(loaded.balance, 150);
    assert_eq!(loaded.owner, owner);
    assert_eq!(loaded.version().as_i64(), 2);

    let balance = repo
        .modify(id, |account| {
            account.deposit(25)?;
            Ok(account.balance)
        })
        .await
        .unwrap();
    assert_eq!(balance, 175);
    assert_eq!(repo.get_version(id).await.unwrap().as_i64(), 3);

    let account = repo.load(id).await.unwrap();
    let mut closed = account.close("all done".to_string()).unwrap();
    repo.save_deleted(&mut closed).await.unwrap();
    assert!(closed.pending_events().is_empty());

    let err = repo.load(id).await.unwrap_err();
    assert!(err.is_aggregate_deleted());
    assert!(repo.load_any(id).await.unwrap().is_deleted());
    let tombstone = repo.load_deleted(id).await.unwrap();
    assert_eq!(tombstone.balance, 175);
    assert!(repo.exists(id).await.unwrap());
    assert_eq!(repo.get_version(id).await.unwrap().as_i64(), 4);
}

#[tokio::test]
async fn test_same_aggregate_code_runs_against_both_stores() {
    let event_store = Arc::new(InMemoryEventStore::new());
    run_scenario(&event_store.repository::<Account>(), "alice").await;

    let state_store = Arc::new(InMemoryStateStore::new());
    run_scenario(&state_store.repository::<Account>(), "bob").await;
}

#[tokio::test]
async fn test_stale_save_is_a_concurrency_conflict() {
    let store = Arc::new(InMemoryStateStore::new());
    let repo = store.repository::<Account>();

    let mut account = Account::open("carol".to_string(), 0).unwrap();
    let id = account.entity_id();
    repo.save(&mut account).await.unwrap();

    let mut winner = repo.load(id).await.unwrap();
    let mut loser = repo.load(id).await.unwrap();

    winner.deposit(10).unwrap();
    repo.save(&mut winner).await.unwrap();

    loser.deposit(20).unwrap();
    let err = repo.save(&mut loser).await.unwrap_err();
    assert!(err.is_concurrency_conflict());

    assert_eq!(repo.load(id).await.unwrap().balance, 10);
}

#[tokio::test]
async fn test_claim_conflict_across_aggregates() {
    let store = Arc::new(InMemoryStateStore::new());
    let repo = store.repository::<Account>();

    let mut holder = Account::open("dave".to_string(), 0).unwrap();
    repo.save(&mut holder).await.unwrap();

    let mut challenger = Account::open("dave".to_string(), 0).unwrap();
    let err = repo.save(&mut challenger).await.unwrap_err();
    match err {
        Error::ClaimConflict {
            claim_type,
            claim_key,
            held_by,
        } => {
            assert_eq!(claim_type, "Account.owner");
            assert_eq!(claim_key, json!("dave"));
            assert_eq!(held_by, Some(holder.entity_id().as_uuid()));
        }
        other => panic!("expected ClaimConflict, got {other:?}"),
    }
    assert!(!repo.exists(challenger.entity_id()).await.unwrap());
}

#[tokio::test]
async fn test_delete_releases_claims() {
    let store = Arc::new(InMemoryStateStore::new());
    let repo = store.repository::<Account>();

    let mut holder = Account::open("erin".to_string(), 0).unwrap();
    repo.save(&mut holder).await.unwrap();

    let mut closed = holder.close("moving on".to_string()).unwrap();
    repo.save_deleted(&mut closed).await.unwrap();

    let mut successor = Account::open("erin".to_string(), 0).unwrap();
    repo.save(&mut successor).await.unwrap();
    assert_eq!(
        repo.load(successor.entity_id()).await.unwrap().owner,
        "erin"
    );
}

#[tokio::test]
async fn test_projection_updates_context_on_save() {
    let store = Arc::new(
        InMemoryStateStore::builder()
            .with_projection(Arc::new(DepositTotals))
            .build(),
    );
    let repo = store.repository::<Account>();

    let mut account = Account::open("frank".to_string(), 0).unwrap();
    account.deposit(30).unwrap();
    account.deposit(12).unwrap();
    repo.save(&mut account).await.unwrap();

    assert_eq!(
        store.projection_state("deposit_total").await,
        Some(json!(42))
    );
    assert_eq!(
        store.projection_state("deposit_event_count").await,
        Some(json!(2))
    );
}

#[tokio::test]
async fn test_failing_projection_rolls_back_state_claims_and_context() {
    let store = Arc::new(
        InMemoryStateStore::builder()
            .with_projection(Arc::new(DepositTotals))
            .with_projection(Arc::new(FailOnDeposit))
            .build(),
    );
    let repo = store.repository::<Account>();

    let mut rejected = Account::open("grace".to_string(), 0).unwrap();
    rejected.deposit(13).unwrap();
    let err = repo.save(&mut rejected).await.unwrap_err();
    assert!(err.to_string().contains("deposits are closed"));

    assert!(!repo.exists(rejected.entity_id()).await.unwrap());
    assert!(store.projection_state("deposit_total").await.is_none());

    let mut successor = Account::open("grace".to_string(), 0).unwrap();
    repo.save(&mut successor).await.unwrap();
}

#[tokio::test]
async fn test_projection_filter_routes_only_matching_events() {
    let store = Arc::new(
        InMemoryStateStore::builder()
            .with_projection(Arc::new(DepositTotals))
            .build(),
    );
    let repo = store.repository::<Account>();

    let mut account = Account::open("heidi".to_string(), 500).unwrap();
    account.deposit(7).unwrap();
    repo.save(&mut account).await.unwrap();

    assert_eq!(
        store.projection_state("deposit_total").await,
        Some(json!(7))
    );
    assert_eq!(
        store.projection_state("deposit_event_count").await,
        Some(json!(1)),
        "the Opened event must not reach the deposit-filtered projection"
    );
}

#[tokio::test]
async fn test_save_batch_is_all_or_nothing() {
    let store = Arc::new(InMemoryStateStore::new());
    let repo = store.repository::<Account>();

    let mut stale = Account::open("judy".to_string(), 5).unwrap();
    let stale_id = stale.entity_id();
    repo.save(&mut stale).await.unwrap();

    let mut winner = repo.load(stale_id).await.unwrap();
    winner.deposit(1).unwrap();
    repo.save(&mut winner).await.unwrap();

    let mut fresh = Account::open("ivan".to_string(), 5).unwrap();
    stale.deposit(2).unwrap();
    let err = repo
        .save_all(&mut [&mut fresh, &mut stale])
        .await
        .unwrap_err();
    assert!(err.is_concurrency_conflict());

    assert!(
        !repo.exists(fresh.entity_id()).await.unwrap(),
        "the first commit must be unapplied when a later one conflicts"
    );
    assert!(!fresh.pending_events().is_empty());
    assert_eq!(repo.load(stale_id).await.unwrap().balance, 6);
}
