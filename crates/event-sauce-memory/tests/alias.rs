//! Regression tests for event-type rename-safety aliases (M6).
//!
//! The on-disk `event_type` key is `concat!(stringify!($aggregate), ".",
//! stringify!($variant))` for `define_events!` (and `format!("{prefix}.{variant}")`
//! for `#[derive(Event)]`). `from_envelope` matches `envelope.event_type ==
//! <Variant as EventType>::EVENT_TYPE` EXACTLY, with no alias/substitution
//! mechanism. So renaming an aggregate or a variant changes the persisted key
//! and orphans ALL historical events as `Error: Unknown event type` — there is
//! no way to keep the old wire string loadable short of a data migration.
//!
//! The fix adds an OPTIONAL per-variant `@aliases("Old.Name", ...)` clause to
//! `define_events!` (mirroring `@version`/`@encrypted_fields`/`@upcast`). The
//! generated `from_envelope` must match the canonical `EVENT_TYPE` OR any alias
//! for that variant, deserializing the SAME variant struct for an old alias.
//! The canonical `EVENT_TYPE` (current name) is unchanged — aliases are only
//! ADDITIONAL accepted-on-read names. The genuinely-unknown event_type case
//! still fails fast (no silent skip), which the second test guards.
//!
//! These mirror the H2 upcasting tests (`tests/upcasting.rs`): a raw historical
//! `EventEnvelope` is appended directly via the store, bypassing `to_envelope`,
//! so we can write a payload whose `event_type` is the PRE-RENAME wire string.

use event_sauce_core::{
    Aggregate, AggregateError, AggregateVersion, Entity, EntityId, EventEnvelope, EventStore,
    EventVersion, StreamId,
};
use event_sauce_memory::InMemoryEventStore;
use std::sync::Arc;
use uuid::Uuid;

/// Persist a raw `EventEnvelope` directly into the store, bypassing
/// `to_envelope` so we can write a historical payload whose `event_type` is an
/// arbitrary (possibly old / renamed) wire string.
async fn append_raw(
    store: &Arc<InMemoryEventStore>,
    aggregate_type: &'static str,
    aggregate_id: Uuid,
    event_type: &str,
    event_version: i64,
    event_data: serde_json::Value,
) {
    let stream_id = StreamId::new(aggregate_type, aggregate_id);
    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        aggregate_id,
        aggregate_type,
        event_type.to_string(),
        EventVersion::new(event_version),
        event_data,
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
}

// ============================================================================
// M6 MEANINGFUL test — `define_events!` `@aliases(...)` rename-safety.
//
// This references the not-yet-existing `@aliases("Old.Name")` clause, so it is a
// compile-fail RED until the GREEN stage teaches `define_events!` to parse and
// thread the per-variant aliases through `from_envelope`. This is the same
// documented-acceptable RED form the H2 upcasting tests used for the
// not-yet-existing trait hook.
// ============================================================================
mod aliased {
    use super::{append_raw, Aggregate, AggregateError, Entity, EntityId, InMemoryEventStore};
    use event_sauce_core::{define_events, DomainEvent, EventStore};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::Arc;

    #[derive(Debug, thiserror::Error)]
    #[error("account error")]
    struct AccountError;

    impl AggregateError for AccountError {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Account {
        id: EntityId,
        owner: String,
    }

    impl Entity for Account {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                owner: String::new(),
            }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl event_sauce_core::DefaultEntity for Account {}

    impl Aggregate for Account {
        type Event = AccountEvent;
        type Error = AccountError;
        type DeletedState = Self;
    }

    // The variant was RENAMED from `OldCreated` to `Created`. The canonical wire
    // string is now `Account.Created`, but historical events were persisted with
    // `Account.OldCreated`. The `@aliases(...)` clause declares the old wire name
    // so those historical events stay loadable into the renamed variant.
    define_events! {
        enum AccountEvent for Account {
            Created {
                owner: String,
            }
            @aliases("Account.OldCreated")
            => |account, event| {
                account.owner = event.owner.clone();
            },
        }
    }

    /// RED (compile-fail until GREEN): an event persisted under the PRE-RENAME
    /// wire string `Account.OldCreated` must still load into the renamed
    /// `Created` variant once the variant declares it as an alias. Without an
    /// alias mechanism the load fails with `Unknown event type`.
    #[tokio::test]
    async fn test_define_events_alias_loads_renamed_variant() {
        let store: Arc<InMemoryEventStore> = Arc::new(InMemoryEventStore::builder().build());
        let id = EntityId::new();

        // Historical payload persisted under the OLD wire name.
        append_raw(
            &store,
            "Account",
            id.as_uuid(),
            "Account.OldCreated",
            1,
            json!({ "owner": "alice", "timestamp": chrono::Utc::now() }),
        )
        .await;

        let loaded =
            store.repository::<Account>().load(id).await.expect(
                "an event under the old aliased wire name must load into the renamed variant",
            );

        assert_eq!(
            loaded.owner, "alice",
            "the aliased old-named event must deserialize into the renamed variant and apply"
        );
    }

    /// Sanity: the canonical (current) wire name still round-trips and the alias
    /// does NOT change `EVENT_TYPE`.
    #[tokio::test]
    async fn test_define_events_alias_keeps_canonical_name() {
        let store: Arc<InMemoryEventStore> = Arc::new(InMemoryEventStore::builder().build());
        let id = EntityId::new();

        append_raw(
            &store,
            "Account",
            id.as_uuid(),
            "Account.Created",
            1,
            json!({ "owner": "bob", "timestamp": chrono::Utc::now() }),
        )
        .await;

        let loaded = store
            .repository::<Account>()
            .load(id)
            .await
            .expect("the canonical wire name must keep loading after an alias is added");

        assert_eq!(loaded.owner, "bob");

        let event = AccountEvent::Created {
            owner: "carol".to_string(),
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(
            event.event_type(),
            "Account.Created",
            "the canonical EVENT_TYPE must be unchanged by adding an alias"
        );
    }
}
