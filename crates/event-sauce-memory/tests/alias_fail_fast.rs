//! M6 fail-fast preservation guard (compiles today).
//!
//! The rename-safety alias work (M6) MUST keep the safe default for a
//! genuinely-unknown event_type: a key that matches no canonical name AND no
//! declared alias must still return `Err: Unknown event type` on load — NOT a
//! silent skip. Skipping unknown events would silently corrupt an aggregate's
//! reconstructed state, which is unacceptable for an event store.
//!
//! This lives in its own test target (separate from `alias.rs`) so it compiles
//! and runs today: it declares NO alias and exercises only existing behavior,
//! whereas `alias.rs` references the not-yet-existing `@aliases(...)` clause and
//! is a compile-fail RED until the GREEN stage lands it.

use event_sauce_core::Repository;
use event_sauce_core::{
    define_events, Aggregate, AggregateError, AggregateVersion, Entity, EntityId, EventEnvelope,
    EventStore, EventVersion, StreamId,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("ledger error")]
struct LedgerError;

impl AggregateError for LedgerError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ledger {
    id: EntityId,
    balance: i64,
}

impl Entity for Ledger {
    fn new(id: EntityId) -> Self {
        Self { id, balance: 0 }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Ledger {}

impl Aggregate for Ledger {
    type Event = LedgerEvent;
    type Error = LedgerError;
    type DeletedState = Self;
}

define_events! {
    enum LedgerEvent for Ledger {
        Opened {
            balance: i64,
        } => |ledger, event| {
            ledger.balance = event.balance;
        },
    }
}

/// An event_type that matches no canonical name AND no alias must fail fast with
/// `Unknown event type` (the safe default for an event store — no silent skip).
/// The M6 alias work must NOT weaken this.
#[tokio::test]
async fn test_unknown_event_type_without_alias_still_fails_fast() {
    let store: Arc<InMemoryEventStore> = Arc::new(InMemoryEventStore::builder().build());
    let id = EntityId::new();

    let stream_id = StreamId::new("Ledger", id.as_uuid());
    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        id.as_uuid(),
        "Ledger",
        "Ledger.NeverDeclared".to_string(),
        EventVersion::new(1),
        json!({ "balance": 10, "timestamp": chrono::Utc::now() }),
    );
    store
        .append(
            stream_id,
            vec![envelope],
            AggregateVersion::initial(),
            Vec::new(),
            false,
        )
        .await
        .expect("appending a raw historical envelope must succeed");

    let err = store
        .repository::<Ledger>()
        .load(id)
        .await
        .expect_err("an undeclared, un-aliased event_type must fail fast on load");

    let msg = format!("{err:?}");
    assert!(
        msg.contains("Unknown event type"),
        "fail-fast default must report `Unknown event type`, got: {msg}"
    );
}
