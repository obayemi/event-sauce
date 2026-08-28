//! Regression tests for loading nonexistent aggregates (F7a / H5).
//!
//! `load()`/`Repository::load`/`Repository::modify` must return a recoverable
//! `Error::NotFound` for an aggregate ID that was never created — for BOTH
//! init-pattern aggregates and legacy `DefaultEntity` aggregates.
//!
//! Before the fix, the empty-stream branch of `load_any` constructed a
//! synthetic default-state aggregate via `AggregateRoot::new_for_replay(id)`,
//! which calls `Entity::new(id)`. For `#[aggregate(init)]` aggregates
//! `Entity::new` is `panic!` by design (init aggregates do not implement
//! `DefaultEntity`), so loading any unknown ID PANICKED inside an async handler
//! instead of returning an error — reachable from untrusted input.

use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, AggregateRoot, DefaultEntity,
    DomainEvent, Entity, EntityId, EventApplicator, EventStore, Repository,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ============================================================================
// Init-pattern aggregate (NO DefaultEntity — Entity::new panics by design)
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("account error")]
struct AccountError;

impl AggregateError for AccountError {}

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    id: EntityId,
    owner: String,
    balance: i64,
}

// Note: NO `Entity::new` override and NO `DefaultEntity` impl — this aggregate
// is constructed exclusively through init events. `Entity::new` falls back to
// the default panic.
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
            owner: String,
        }
        @init
        => |id, event| {
            Account {
                id,
                owner: event.owner.clone(),
                balance: 0,
            }
        },

        Deposited {
            amount: i64,
        }
        => |account, event| {
            account.balance += event.amount;
        },
    }
}

command_handler! {
    impl Account {
        @clock @init fn open_account(owner: String) -> OpenedEvent { owner };
        @clock fn deposit(amount: i64) -> DepositedEvent { amount };
    }
}

// ============================================================================
// Legacy DefaultEntity aggregate (Entity::new is safe)
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("counter error")]
struct CounterError;

impl AggregateError for CounterError {}

#[derive(Debug, Serialize, Deserialize)]
struct Counter {
    id: EntityId,
    value: i64,
}

impl Entity for Counter {
    fn new(id: EntityId) -> Self {
        Self { id, value: 0 }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Counter {}

impl Aggregate for Counter {
    type Event = CounterEvent;
    type Error = CounterError;
    type DeletedState = Self;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CounterEvent {
    Incremented { amount: i64 },
}

impl DomainEvent for CounterEvent {
    type Aggregate = Counter;
    fn event_type(&self) -> &'static str {
        "Counter.Incremented"
    }
    fn event_version(&self) -> event_sauce_core::EventVersion {
        event_sauce_core::EventVersion::new(1)
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl EventApplicator<Counter> for CounterEvent {
    fn dispatch(&self, entity: &mut Counter) -> Result<(), CounterError> {
        match self {
            CounterEvent::Incremented { amount } => entity.value += amount,
        }
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut Counter) {
        match self {
            CounterEvent::Incremented { amount } => entity.value += amount,
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn create_store() -> Arc<InMemoryEventStore> {
    Arc::new(InMemoryEventStore::builder().build())
}

// ============================================================================
// Tests
// ============================================================================

/// Loading a never-created init-pattern aggregate must return `Error::NotFound`,
/// NOT panic inside `Entity::new`.
#[tokio::test]
async fn test_load_missing_init_aggregate_returns_not_found() {
    let store = create_store();
    let repo = store.repository::<Account>();

    let missing_id = EntityId::new();
    let result = repo.load(missing_id).await;

    let err = result.expect_err("loading a nonexistent init aggregate must be an error, not Ok");
    assert!(err.is_not_found(), "expected NotFound, got: {err:?}");
}

/// `Repository::modify` on a nonexistent init aggregate must surface a
/// `NotFound` load error (no panic).
#[tokio::test]
async fn test_modify_missing_init_aggregate_returns_not_found() {
    use AccountCommands;

    let store = create_store();
    let repo = store.repository::<Account>();

    let missing_id = EntityId::new();
    let result = repo
        .modify(missing_id, |account| account.deposit(100))
        .await;

    let err = result.expect_err("modify on a nonexistent init aggregate must error, not panic");
    let source: event_sauce_core::Error = err.into();
    assert!(
        source.is_not_found(),
        "expected modify load failure to be NotFound, got: {source:?}"
    );
}

/// Loading a never-created LEGACY `DefaultEntity` aggregate must ALSO return
/// `NotFound` (unified behavior — an empty stream means the aggregate does not
/// exist, regardless of whether `Entity::new` would have succeeded).
#[tokio::test]
async fn test_load_missing_legacy_aggregate_returns_not_found() {
    let store = create_store();
    let repo = store.repository::<Counter>();

    let missing_id = EntityId::new();
    let result = repo.load(missing_id).await;

    let err = result.expect_err("loading a nonexistent legacy aggregate must be an error");
    assert!(err.is_not_found(), "expected NotFound, got: {err:?}");
}

/// The non-empty path is unaffected: a legacy aggregate that has at least one
/// committed event still reconstructs correctly.
#[tokio::test]
async fn test_load_existing_legacy_aggregate_still_reconstructs() {
    let store = create_store();
    let repo = store.repository::<Counter>();

    let id = EntityId::new();
    let mut counter = AggregateRoot::<Counter>::new(id);
    counter
        .apply(CounterEvent::Incremented { amount: 7 })
        .unwrap();
    repo.save(&mut counter).await.unwrap();

    let loaded = repo
        .load(id)
        .await
        .expect("an existing aggregate must load successfully");
    assert_eq!(loaded.value, 7);
}

/// The non-empty path is unaffected for init aggregates too: after creating
/// and committing, the aggregate loads back correctly.
#[tokio::test]
async fn test_load_existing_init_aggregate_still_reconstructs() {
    use AccountCommands;

    let store = create_store();
    let repo = store.repository::<Account>();

    let mut account = Account::open_account("alice".to_string()).unwrap();
    account.deposit(250).unwrap();
    let id = account.entity_id();
    repo.save(&mut account).await.unwrap();

    let loaded = repo
        .load(id)
        .await
        .expect("an existing init aggregate must load successfully");
    assert_eq!(loaded.owner, "alice");
    assert_eq!(loaded.balance, 250);
}
