//! Regression tests for the event upcasting hook (F4-H2).
//!
//! Every event carries an `event_version` (`DomainEvent::event_version`,
//! written into the envelope by `to_envelope`), and `EventVersion`'s docs claim
//! it enables "event evolution and migration". But neither deserialization path
//! reads it: the default trait `from_envelope` does `serde_json::from_value`
//! over the whole payload with no version inspection, and the
//! `define_events!`-generated `from_envelope` branches solely on the event type
//! then `from_value` directly. There is NO upcasting hook anywhere, so bumping
//! `@version` does nothing on load and any actual field-shape change serde-FAILS
//! on historical events with no migration path.
//!
//! These tests demonstrate the seam end-to-end on the in-memory backend:
//!
//! * `test_v1_payload_without_upcast_fails_to_deserialize` is the COMPILES-TODAY
//!   RED: it persists a raw v1 envelope whose payload is MISSING a field that
//!   the current struct requires (no serde default), then loads the aggregate
//!   and asserts the load is `Err` today. This proves the genuine deserialize
//!   failure that the upcasting seam must fix.
//!
//! * `test_upcast_migrates_old_payload_on_load` and
//!   `test_upcast_leaves_current_payload_untouched` exercise the actual hook:
//!   the event provides a `DomainEvent::upcast` override that inserts the
//!   missing field when `from_version == 1`. These reference the
//!   not-yet-existing `upcast` trait method, so they compile-fail until the
//!   GREEN stage adds the hook and wires it into the load path. After GREEN the
//!   migrating load SUCCEEDS and the field carries the upcast default, while a
//!   current-version (v2) payload passes through `upcast` UNTOUCHED.

use event_sauce_core::{
    Aggregate, AggregateError, AggregateVersion, DomainEvent, Entity, EntityId, EventApplicator,
    EventEnvelope, EventStore, EventVersion, StreamId,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

// ============================================================================
// Aggregate + event whose CURRENT shape gained a field at v2.
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("profile error")]
struct ProfileError;

impl AggregateError for ProfileError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Profile {
    id: EntityId,
    /// Display name set at registration (present since v1).
    name: String,
    /// Tier, introduced in event schema v2. NO serde default: a v1 payload that
    /// lacks this field cannot deserialize without migration.
    tier: String,
}

impl Entity for Profile {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            tier: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Profile {}

impl Aggregate for Profile {
    type Event = ProfileEvent;
    type Error = ProfileError;
    type DeletedState = Self;
}

/// Current (v2) event shape: `tier` is required and has NO serde default.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum ProfileEvent {
    Registered {
        name: String,
        /// Added in v2. Historical v1 payloads do not carry this field.
        tier: String,
    },
}

impl DomainEvent for ProfileEvent {
    type Aggregate = Profile;

    fn event_type(&self) -> &'static str {
        match self {
            ProfileEvent::Registered { .. } => "Profile.Registered",
        }
    }

    fn event_version(&self) -> EventVersion {
        // Current schema is version 2 (gained the `tier` field).
        EventVersion::new(2)
    }

    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    // ---- The hook under test (added by the GREEN stage) ----
    //
    // Called on load with the STORED `event_version` BEFORE deserialization.
    // For a v1 payload (which predates the `tier` field) inject a migrated
    // default so the current struct can deserialize. A current-version (v2)
    // payload already has `tier`, so it must pass through UNTOUCHED.
    //
    // This references `DomainEvent::upcast`, which does not exist on the trait
    // until the GREEN stage adds it. Until then this `impl` block fails to
    // compile (documented-acceptable RED form). The runtime RED that compiles
    // today is `test_v1_payload_without_upcast_fails_to_deserialize` below,
    // which uses a separate event type with no `upcast` override.
    fn upcast(event_type: &str, from_version: EventVersion, data: &mut serde_json::Value) {
        if event_type == "Profile.Registered" && from_version == EventVersion::new(1) {
            if let Some(obj) = data
                .get_mut("Registered")
                .and_then(serde_json::Value::as_object_mut)
            {
                obj.entry("tier")
                    .or_insert_with(|| json!("migrated-from-v1"));
            }
        }
    }
}

impl EventApplicator<Profile> for ProfileEvent {
    fn dispatch(&self, entity: &mut Profile) -> Result<(), ProfileError> {
        match self {
            ProfileEvent::Registered { name, tier } => {
                entity.name = name.clone();
                entity.tier = tier.clone();
            }
        }
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut Profile) {
        match self {
            ProfileEvent::Registered { name, tier } => {
                entity.name = name.clone();
                entity.tier = tier.clone();
            }
        }
    }
}

// ============================================================================
// Aggregate + event used for the COMPILES-TODAY runtime RED (no upcast hook
// referenced anywhere, so this part of the file always compiles).
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("legacy profile error")]
struct LegacyError;

impl AggregateError for LegacyError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyProfile {
    id: EntityId,
    name: String,
    /// Required field with NO serde default — same shape change as `Profile`.
    tier: String,
}

impl Entity for LegacyProfile {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            tier: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for LegacyProfile {}

impl Aggregate for LegacyProfile {
    type Event = LegacyEvent;
    type Error = LegacyError;
    type DeletedState = Self;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum LegacyEvent {
    Registered { name: String, tier: String },
}

impl DomainEvent for LegacyEvent {
    type Aggregate = LegacyProfile;

    fn event_type(&self) -> &'static str {
        match self {
            LegacyEvent::Registered { .. } => "Legacy.Registered",
        }
    }

    fn event_version(&self) -> EventVersion {
        EventVersion::new(2)
    }

    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    // Intentionally NO `upcast` override: relies entirely on the production
    // default `from_envelope`. With no upcasting seam, a stored v1 payload that
    // lacks `tier` cannot be migrated and the load fails.
}

impl EventApplicator<LegacyProfile> for LegacyEvent {
    fn dispatch(&self, entity: &mut LegacyProfile) -> Result<(), LegacyError> {
        match self {
            LegacyEvent::Registered { name, tier } => {
                entity.name = name.clone();
                entity.tier = tier.clone();
            }
        }
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut LegacyProfile) {
        match self {
            LegacyEvent::Registered { name, tier } => {
                entity.name = name.clone();
                entity.tier = tier.clone();
            }
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn create_store() -> Arc<InMemoryEventStore> {
    Arc::new(InMemoryEventStore::builder().build())
}

/// Persist a raw `EventEnvelope` directly into the store, bypassing
/// `to_envelope` so we can write a historical payload with `event_version=1`
/// and a field-shape that predates the current struct.
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
// COMPILES-TODAY runtime RED
// ============================================================================

/// RED (runtime, compiles today): a stored v1 payload missing the `tier` field
/// cannot deserialize on load, because nothing reads `event_version` to migrate
/// it. Once the upcasting seam exists AND a migration is supplied, this kind of
/// load succeeds — but with no hook, the load is `Err` today.
#[tokio::test]
async fn test_v1_payload_without_upcast_fails_to_deserialize() {
    let store = create_store();
    let id = EntityId::new();

    // Historical v1 payload: `tier` did not exist yet.
    append_raw(
        &store,
        "LegacyProfile",
        id.as_uuid(),
        "Legacy.Registered",
        1,
        json!({ "Registered": { "name": "alice" } }),
    )
    .await;

    let repo = store.repository::<LegacyProfile>();
    let result = repo.load(id).await;

    let err = result.expect_err(
        "loading a v1 payload that lacks the v2 `tier` field must fail today: there is no \
         upcasting hook to migrate it before deserialization",
    );
    let msg = format!("{err:?}");
    assert!(
        msg.contains("tier") || msg.to_lowercase().contains("missing field"),
        "expected a missing-field deserialize error mentioning `tier`, got: {msg}"
    );
}

// ============================================================================
// MEANINGFUL hook tests (compile-fail until GREEN adds `DomainEvent::upcast`)
// ============================================================================

/// GREEN behavior: with an `upcast` override that injects the migrated `tier`
/// when `from_version == 1`, loading a stored v1 payload SUCCEEDS and the field
/// carries the migrated value.
#[tokio::test]
async fn test_upcast_migrates_old_payload_on_load() {
    let store = create_store();
    let id = EntityId::new();

    append_raw(
        &store,
        "Profile",
        id.as_uuid(),
        "Profile.Registered",
        1,
        json!({ "Registered": { "name": "bob" } }),
    )
    .await;

    let repo = store.repository::<Profile>();
    let loaded = repo
        .load(id)
        .await
        .expect("a v1 payload must load after upcasting injects the missing `tier` field");

    assert_eq!(loaded.name, "bob");
    assert_eq!(
        loaded.tier, "migrated-from-v1",
        "upcast must populate the field that did not exist at v1"
    );
}

/// GREEN behavior: a CURRENT-version (v2) payload already carries `tier`, so
/// `upcast` must branch on `from_version` and leave it UNTOUCHED.
#[tokio::test]
async fn test_upcast_leaves_current_payload_untouched() {
    let store = create_store();
    let id = EntityId::new();

    append_raw(
        &store,
        "Profile",
        id.as_uuid(),
        "Profile.Registered",
        2,
        json!({ "Registered": { "name": "carol", "tier": "gold" } }),
    )
    .await;

    let repo = store.repository::<Profile>();
    let loaded = repo
        .load(id)
        .await
        .expect("a current-version payload must load unchanged");

    assert_eq!(loaded.name, "carol");
    assert_eq!(
        loaded.tier, "gold",
        "upcast must not rewrite a current-version payload"
    );
}

// ============================================================================
// `define_events!` + `@upcast` clause: the macro path of the same seam.
// ============================================================================
//
// The manual-impl tests above prove the trait hook and wired call sites. This
// module proves the OPTIONAL enum-level `@upcast |event_type, from_version,
// data| { ... }` clause: the macro emits a `DomainEvent::upcast` override
// running the supplied body, while omitting the clause keeps the trait default.
//
// `define_events!` serializes each variant as a FLAT struct (not enum-tagged),
// so `data` here is the variant's object directly (no wrapping key), unlike the
// manual `serde`-tagged enums above.
mod macro_path {
    use event_sauce_core::{
        define_events, Aggregate, AggregateError, AggregateVersion, DomainEvent, Entity, EntityId,
        EventStore, StreamId,
    };
    use event_sauce_memory::InMemoryEventStore;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    #[derive(Debug, thiserror::Error)]
    #[error("account error")]
    struct AccountError;

    impl AggregateError for AccountError {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Account {
        id: EntityId,
        plan: String,
    }

    impl Entity for Account {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                plan: String::new(),
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

    define_events! {
        enum AccountEvent for Account {
            // The seam under test: a v1 payload predates the `plan` field, so it
            // is injected with a migrated default; a current payload is left as-is.
            @upcast |event_type, from_version, data| {
                if event_type == "Account.Opened"
                    && from_version == event_sauce_core::EventVersion::new(1)
                {
                    if let Some(obj) = data.as_object_mut() {
                        obj.entry("plan")
                            .or_insert_with(|| json!("migrated-basic"));
                    }
                }
            }

            Opened {
                // Added in v2; historical v1 payloads lack it.
                plan: String,
            }
            @version(2)
            => |account, event| {
                account.plan = event.plan.clone();
            },
        }
    }

    async fn append_raw(
        store: &Arc<InMemoryEventStore>,
        aggregate_id: Uuid,
        event_version: i64,
        event_data: serde_json::Value,
    ) {
        let stream_id = StreamId::new("Account", aggregate_id);
        let envelope = event_sauce_core::EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            "Account",
            "Account.Opened".to_string(),
            event_sauce_core::EventVersion::new(event_version),
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

    /// A `define_events!` enum with an `@upcast` clause migrates a stored v1
    /// payload that lacks the v2 `plan` field.
    #[tokio::test]
    async fn test_define_events_upcast_migrates_old_payload() {
        let store = Arc::new(InMemoryEventStore::builder().build());
        let id = EntityId::new();

        append_raw(
            &store,
            id.as_uuid(),
            1,
            json!({ "timestamp": chrono::Utc::now() }),
        )
        .await;

        let loaded = store
            .repository::<Account>()
            .load(id)
            .await
            .expect("a v1 payload must load after the @upcast clause injects `plan`");

        assert_eq!(loaded.plan, "migrated-basic");
    }

    /// A current-version (v2) payload already carries `plan`, so the `@upcast`
    /// clause must branch on `from_version` and leave it untouched.
    #[tokio::test]
    async fn test_define_events_upcast_leaves_current_payload_untouched() {
        let store = Arc::new(InMemoryEventStore::builder().build());
        let id = EntityId::new();

        append_raw(
            &store,
            id.as_uuid(),
            2,
            json!({ "plan": "pro", "timestamp": chrono::Utc::now() }),
        )
        .await;

        let loaded = store
            .repository::<Account>()
            .load(id)
            .await
            .expect("a current-version payload must load unchanged");

        assert_eq!(loaded.plan, "pro");
    }

    /// Sanity check that a normal round-trip (no migration needed) still works
    /// for an enum that declares an `@upcast` clause — exercising the
    /// `event_version`/`event_type`/`occurred_at` generated arms.
    #[tokio::test]
    async fn test_define_events_upcast_roundtrip() {
        let opened = AccountEvent::Opened {
            plan: "enterprise".to_string(),
            timestamp: chrono::Utc::now(),
        };
        assert_eq!(opened.event_type(), "Account.Opened");
        assert_eq!(
            opened.event_version(),
            event_sauce_core::EventVersion::new(2)
        );

        let envelope = opened.to_envelope(Uuid::new_v4()).unwrap();
        let restored = AccountEvent::from_envelope(&envelope).unwrap();
        match restored {
            AccountEvent::Opened { plan, .. } => assert_eq!(plan, "enterprise"),
        }
    }
}
