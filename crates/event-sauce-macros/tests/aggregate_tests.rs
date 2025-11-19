//! Integration tests for #[aggregate(...)] attribute macro.
//!
//! These tests verify that the aggregate attribute macro correctly generates
//! the State struct and Aggregate trait implementation.

use chrono::{DateTime, Utc};
use event_sauce_core::{Aggregate, AggregateError, AggregateId, DomainEvent, Version};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

// Define error type for testing
#[derive(Debug, Error)]
#[error("Test counter error")]
struct TestCounterError;

impl AggregateError for TestCounterError {}

// Define a simple aggregate ID for testing
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
struct TestCounterId(String);

impl TestCounterId {
    fn new(id: String) -> Self {
        Self(id)
    }
}

impl fmt::Display for TestCounterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counter-{}", self.0)
    }
}

impl AggregateId for TestCounterId {
    fn to_uuid(&self) -> uuid::Uuid {
        // For testing purposes, create a deterministic UUID from the string
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        let hash = hasher.finish();

        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&hash.to_le_bytes());
        uuid::Uuid::from_bytes(bytes)
    }
}

// Define a simple event for testing
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
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

    fn event_version(&self) -> i32 {
        1
    }

    fn occurred_at(&self) -> DateTime<Utc> {
        match self {
            TestCounterEvent::Incremented { timestamp, .. } => *timestamp,
            TestCounterEvent::Decremented { timestamp, .. } => *timestamp,
        }
    }
}

// This is the aggregate struct using the new attribute macro
// The macro will generate TestCounterState and transform TestCounter into a wrapper
#[event_sauce_macros::aggregate(
    id = "TestCounterId",
    event = "TestCounterEvent",
    error = "TestCounterError"
)]
#[derive(Default)]
struct TestCounter {
    id: TestCounterId,
    value: i32,
}

// Business methods on the aggregate
impl TestCounter {
    fn increment(&mut self, amount: i32) {
        self.apply(TestCounterEvent::Incremented {
            amount,
            timestamp: Utc::now(),
        })
        .expect("increment should not fail");
    }

    fn apply_event(&mut self, event: &TestCounterEvent) -> Result<(), TestCounterError> {
        match event {
            TestCounterEvent::Incremented { amount, .. } => {
                self.value += amount;
            }
            TestCounterEvent::Decremented { amount, .. } => {
                self.value -= amount;
            }
        }
        Ok(())
    }
}

#[test]
fn test_aggregate_derive_implements_aggregate_trait() {
    let id = TestCounterId::new("test-1".to_string());
    let counter = TestCounter::new(id.clone());

    // The Aggregate trait should be implemented
    assert_eq!(counter.aggregate_id(), &id);
    assert_eq!(counter.version(), Version::initial());
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_derive_aggregate_id() {
    let id = TestCounterId::new("test-1".to_string());
    let counter = TestCounter::new(id.clone());

    assert_eq!(counter.aggregate_id(), &id);
}

#[test]
fn test_aggregate_derive_version() {
    let id = TestCounterId::new("test-1".to_string());
    let counter = TestCounter::new(id);

    assert_eq!(counter.version(), Version::initial());
}

#[test]
fn test_aggregate_derive_pending_events() {
    let id = TestCounterId::new("test-1".to_string());
    let mut counter = TestCounter::new(id);

    assert_eq!(counter.pending_events().len(), 0);

    counter.increment(5);

    assert_eq!(counter.pending_events().len(), 1);
}

#[test]
fn test_aggregate_derive_clear_pending_events() {
    let id = TestCounterId::new("test-1".to_string());
    let mut counter = TestCounter::new(id);

    counter.increment(5);
    assert_eq!(counter.pending_events().len(), 1);

    counter.clear_pending_events();
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_derive_apply() {
    let id = TestCounterId::new("test-1".to_string());
    let mut counter = TestCounter::new(id);

    let event = TestCounterEvent::Incremented {
        amount: 10,
        timestamp: Utc::now(),
    };

    let initial_version = counter.version();
    counter.apply(event).unwrap();

    // Version should be incremented
    assert_eq!(counter.version(), initial_version.next());
    // Value should be updated (via apply_event) - accessible via Deref
    assert_eq!(counter.value, 10);
}

#[test]
fn test_aggregate_derive_multiple_events() {
    let id = TestCounterId::new("test-1".to_string());
    let mut counter = TestCounter::new(id);

    counter.increment(5);
    counter.increment(3);

    assert_eq!(counter.pending_events().len(), 2);
    assert_eq!(counter.value, 8);
    assert_eq!(counter.version(), Version::new(2));
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
fn test_state_struct_generated() {
    // The macro should generate TestCounterState (business fields only, no id)
    let state = TestCounterState { value: 42 };

    assert_eq!(state.value, 42);
}

#[test]
fn test_from_snapshot_constructor() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState { value: 100 };

    let counter = TestCounter::from_snapshot(id, Version::new(5), state);

    assert_eq!(counter.value, 100);
    assert_eq!(counter.version(), Version::new(5));
}

#[test]
fn test_deref_transparent_access() {
    let id = TestCounterId::new("test-1".to_string());
    let mut counter = TestCounter::new(id);

    // Can access and modify state fields directly via Deref/DerefMut
    counter.value = 999;

    assert_eq!(counter.value, 999);
}
