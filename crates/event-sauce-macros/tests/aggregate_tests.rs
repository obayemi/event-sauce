//! Integration tests for #[aggregate(...)] attribute macro.
//!
//! These tests verify that the aggregate attribute macro correctly generates
//! the Entity and Aggregate trait implementations.

use chrono::{DateTime, Utc};
use event_sauce_core::{
    Aggregate, AggregateError, AggregateRoot, DomainEvent, Entity, EntityId, EventApplicator,
    Version,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// Define error type for testing
#[derive(Debug, Error)]
#[error("Test counter error")]
struct TestCounterError;

impl AggregateError for TestCounterError {}

// Define a simple event for testing
#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestCounterEvent {
    Incremented {
        amount: i32,
        timestamp: DateTime<Utc>,
    },
    Decremented {
        amount: i32,
        timestamp: DateTime<Utc>,
    },
}

impl DomainEvent for TestCounterEvent {
    type Aggregate = TestCounter;

    fn event_type(&self) -> &'static str {
        match self {
            TestCounterEvent::Incremented { .. } => "CounterIncremented",
            TestCounterEvent::Decremented { .. } => "CounterDecremented",
        }
    }

    fn event_version(&self) -> u64 {
        1
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        match self {
            TestCounterEvent::Incremented { timestamp, .. } => *timestamp,
            TestCounterEvent::Decremented { timestamp, .. } => *timestamp,
        }
    }
}

// EventApplicator dispatches to &mut TestCounter (entity) directly
impl EventApplicator<TestCounter> for TestCounterEvent {
    fn dispatch(&self, entity: &mut TestCounter) -> Result<(), TestCounterError> {
        match self {
            TestCounterEvent::Incremented { amount, .. } => {
                entity.value += amount;
            }
            TestCounterEvent::Decremented { amount, .. } => {
                entity.value -= amount;
            }
        }
        Ok(())
    }

    fn dispatch_unchecked(&self, entity: &mut TestCounter) {
        match self {
            TestCounterEvent::Incremented { amount, .. } => {
                entity.value += amount;
            }
            TestCounterEvent::Decremented { amount, .. } => {
                entity.value -= amount;
            }
        }
    }
}

// This is the aggregate struct using the new attribute macro
// The macro generates Entity + Aggregate impls (no wrapper or state struct)
#[event_sauce_macros::aggregate(event = "TestCounterEvent", error = "TestCounterError")]
#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestCounter {
    #[id]
    id: EntityId,
    value: i32,
}

#[test]
fn test_aggregate_derive_generates_entity_trait() {
    let id = EntityId::new();
    let counter = TestCounter::new(id);

    // The Entity trait should be implemented
    assert_eq!(counter.entity_id(), id);
    assert_eq!(counter.value, 0);
}

#[test]
fn test_aggregate_derive_entity_id() {
    let id = EntityId::new();
    let counter = TestCounter::new(id);

    assert_eq!(counter.entity_id(), id);
}

#[test]
fn test_aggregate_root_new() {
    let id = EntityId::new();
    let root = AggregateRoot::<TestCounter>::new(id);

    assert_eq!(root.entity_id(), id);
    assert_eq!(root.version(), Version::initial());
    assert_eq!(root.pending_events().len(), 0);
}

#[test]
fn test_aggregate_root_apply() {
    let id = EntityId::new();
    let mut root = AggregateRoot::<TestCounter>::new(id);

    let event = TestCounterEvent::Incremented {
        amount: 10,
        timestamp: Utc::now(),
    };

    root.apply(event).unwrap();

    // Version should be incremented
    assert_eq!(root.version(), Version::new(1));
    // Value should be updated - accessible via Deref
    assert_eq!(root.value, 10);
    // Pending events recorded
    assert_eq!(root.pending_events().len(), 1);
}

#[test]
fn test_aggregate_root_multiple_events() {
    let id = EntityId::new();
    let mut root = AggregateRoot::<TestCounter>::new(id);

    root.apply(TestCounterEvent::Incremented {
        amount: 5,
        timestamp: Utc::now(),
    })
    .unwrap();
    root.apply(TestCounterEvent::Incremented {
        amount: 3,
        timestamp: Utc::now(),
    })
    .unwrap();

    assert_eq!(root.pending_events().len(), 2);
    assert_eq!(root.value, 8);
    assert_eq!(root.version(), Version::new(2));
}

#[test]
fn test_aggregate_root_clear_pending_events() {
    let id = EntityId::new();
    let mut root = AggregateRoot::<TestCounter>::new(id);

    root.apply(TestCounterEvent::Incremented {
        amount: 5,
        timestamp: Utc::now(),
    })
    .unwrap();
    assert_eq!(root.pending_events().len(), 1);

    root.clear_pending_events();
    assert_eq!(root.pending_events().len(), 0);
}

#[test]
fn test_aggregate_derive_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<TestCounter>();
    assert_sync::<TestCounter>();
}

#[test]
fn test_aggregate_derive_aggregate_type() {
    let type_name = TestCounter::aggregate_type();
    assert_eq!(type_name, "TestCounter");
}

#[test]
fn test_no_state_struct_generated() {
    // In the new design, no separate State struct is generated.
    // The entity struct IS the domain type with direct field access.
    let counter = TestCounter::new(EntityId::new());
    assert_eq!(counter.value, 0);
}

#[test]
fn test_from_snapshot() {
    let id = EntityId::new();
    let entity = TestCounter { id, value: 100 };

    let root = AggregateRoot::<TestCounter>::from_snapshot(Version::new(5), entity);

    assert_eq!(root.value, 100);
    assert_eq!(root.version(), Version::new(5));
    assert_eq!(root.entity_id(), id);
}

#[test]
fn test_deref_read_access() {
    let id = EntityId::new();
    let mut root = AggregateRoot::<TestCounter>::new(id);

    root.apply(TestCounterEvent::Incremented {
        amount: 999,
        timestamp: Utc::now(),
    })
    .unwrap();

    // Can read state fields directly via Deref
    assert_eq!(root.value, 999);
}

#[test]
fn test_entity_ref_access() {
    let id = EntityId::new();
    let mut root = AggregateRoot::<TestCounter>::new(id);

    root.apply(TestCounterEvent::Incremented {
        amount: 5,
        timestamp: Utc::now(),
    })
    .unwrap();

    // Can get a reference to the entity
    let entity_ref = root.entity();
    assert_eq!(entity_ref.value, 5);
}
