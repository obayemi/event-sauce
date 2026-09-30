//! Shared test doubles for the policy context, runner, and macro-generated
//! handlers: an event envelope builder and a fresh checkpoint store.

use crate::test_fixtures::MockCheckpointStore;
use crate::{EventEnvelope, EventVersion};
use std::sync::Arc;
use uuid::Uuid;

pub(super) fn test_envelope(event_type: &str, aggregate_type: &str) -> EventEnvelope {
    EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        aggregate_type.to_string(),
        event_type.to_string(),
        EventVersion::new(1),
        serde_json::json!({}),
    )
}

pub(super) fn test_checkpoint_store() -> Arc<MockCheckpointStore> {
    Arc::new(MockCheckpointStore::new())
}
