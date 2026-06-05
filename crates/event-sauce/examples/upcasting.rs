//! # Event Upcasting Example - On-Load Schema Migration
//!
//! This example demonstrates the event upcasting seam: when an event's shape
//! changes (here, a `tier` field is added in schema v2), historical payloads
//! stored under the old `event_version` are migrated on load — before
//! deserialization — so the current struct can read them.
//!
//! ## Key Concepts
//!
//! - **`DomainEvent::upcast`**: a default-no-op hook called automatically by
//!   `from_envelope` with the STORED `event_version`, BEFORE deserialization.
//! - **`@upcast |event_type, from_version, data|`** in `define_events!`:
//!   overrides `upcast` with the supplied body. Mutate `data` (the variant's
//!   flat JSON object) in place to migrate an older payload.
//! - **Branch on `from_version`**: old payloads are migrated; current-version
//!   payloads pass through untouched.
//!
//! Run with:
//! ```bash
//! cargo run --example upcasting
//! ```

use std::sync::Arc;

use event_sauce_core::{
    define_events, Aggregate, AggregateError, AggregateVersion, DefaultEntity, Entity, EntityId,
    EventEnvelope, EventStore, EventVersion, StreamId,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use serde_json::json;

// ============================================================================
// Aggregate whose event shape gained a required `tier` field at schema v2.
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("profile error")]
struct ProfileError;

impl AggregateError for ProfileError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Profile {
    id: EntityId,
    name: String,
    /// Introduced in event schema v2 — has NO serde default, so a v1 payload
    /// that lacks it cannot deserialize without migration.
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

impl DefaultEntity for Profile {}

impl Aggregate for Profile {
    type Event = ProfileEvent;
    type Error = ProfileError;
    type DeletedState = Self;
}

define_events! {
    enum ProfileEvent for Profile {
        // Migrate payloads written before `tier` existed (v1 -> v2). The clause
        // runs on load, before deserialization, with the STORED event_version.
        @upcast |event_type, from_version, data| {
            if event_type == "Profile.Registered" && from_version == EventVersion::new(1) {
                if let Some(obj) = data.as_object_mut() {
                    obj.entry("tier").or_insert_with(|| json!("migrated-basic"));
                }
            }
        }

        Registered {
            name: String,
            /// Added in v2. Historical v1 payloads do not carry this field.
            tier: String,
        }
        @version(2)
        => |profile, event| {
            profile.name = event.name.clone();
            profile.tier = event.tier.clone();
        },
    }
}

/// Persist a raw historical envelope, bypassing `to_envelope`, so we can write
/// a v1 payload that predates the current struct's `tier` field.
async fn append_historical(
    store: &Arc<InMemoryEventStore>,
    aggregate_id: uuid::Uuid,
    event_version: i64,
    event_data: serde_json::Value,
) {
    let stream_id = StreamId::new("Profile", aggregate_id);
    let envelope = EventEnvelope::new(
        uuid::Uuid::new_v4(),
        aggregate_id,
        "Profile",
        "Profile.Registered".to_string(),
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
        .expect("appending a historical envelope must succeed");
}

#[tokio::main]
async fn main() {
    let store = Arc::new(InMemoryEventStore::builder().build());

    // --- A v1 payload (written before `tier` existed) is migrated on load ---
    let legacy_id = EntityId::new();
    append_historical(
        &store,
        legacy_id.as_uuid(),
        1,
        json!({ "name": "alice", "timestamp": chrono::Utc::now() }),
    )
    .await;

    let legacy = store
        .repository::<Profile>()
        .load(legacy_id)
        .await
        .expect("v1 payload loads after @upcast injects `tier`");
    println!(
        "loaded v1 profile: name={:?}, tier={:?} (migrated)",
        legacy.name, legacy.tier
    );
    assert_eq!(legacy.tier, "migrated-basic");

    // --- A current (v2) payload already has `tier`; upcast leaves it alone ---
    let current_id = EntityId::new();
    append_historical(
        &store,
        current_id.as_uuid(),
        2,
        json!({ "name": "bob", "tier": "gold", "timestamp": chrono::Utc::now() }),
    )
    .await;

    let current = store
        .repository::<Profile>()
        .load(current_id)
        .await
        .expect("v2 payload loads unchanged");
    println!(
        "loaded v2 profile: name={:?}, tier={:?} (untouched)",
        current.name, current.tier
    );
    assert_eq!(current.tier, "gold");

    println!("Event upcasting migrated the old payload on load.");
}
