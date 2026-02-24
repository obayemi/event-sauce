//! Integration tests for #[aggregate(...)] attribute macro.
//!
//! These tests verify that the aggregate attribute macro correctly generates
//! the State struct and Aggregate trait implementation.

use chrono::{DateTime, Utc};
use event_sauce_core::{
    Aggregate, AggregateError, AggregateId as _, DomainEvent, EventApplicator, Version,
};
use event_sauce_macros::AggregateId as DeriveAggregateId;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

// Define error type for testing
#[derive(Debug, Error)]
#[error("Test counter error")]
struct TestCounterError;

impl AggregateError for TestCounterError {}

// Define a simple aggregate ID for testing
#[derive(
    Default, DeriveAggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
struct TestCounterId(Uuid);

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

// EventApplicator must be defined before the aggregate macro expands,
// since the generated Aggregate impl requires it.
impl EventApplicator<TestCounter> for TestCounterEvent {
    fn dispatch(&self, aggregate: &mut TestCounter) -> Result<(), TestCounterError> {
        match self {
            TestCounterEvent::Incremented { amount, .. } => {
                aggregate.value += amount;
            }
            TestCounterEvent::Decremented { amount, .. } => {
                aggregate.value -= amount;
            }
        }
        Ok(())
    }

    fn dispatch_unchecked(&self, aggregate: &mut TestCounter) {
        match self {
            TestCounterEvent::Incremented { amount, .. } => {
                aggregate.value += amount;
            }
            TestCounterEvent::Decremented { amount, .. } => {
                aggregate.value -= amount;
            }
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
    #[aggregate_id]
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
}

#[test]
fn test_aggregate_derive_implements_aggregate_trait() {
    let id = TestCounterId::new();
    let counter = TestCounter::new(id);

    // The Aggregate trait should be implemented
    assert_eq!(counter.aggregate_id(), &id);
    assert_eq!(counter.version(), Version::initial());
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_derive_aggregate_id() {
    let id = TestCounterId::new();
    let counter = TestCounter::new(id);

    assert_eq!(counter.aggregate_id(), &id);
}

#[test]
fn test_aggregate_derive_version() {
    let id = TestCounterId::new();
    let counter = TestCounter::new(id);

    assert_eq!(counter.version(), Version::initial());
}

#[test]
fn test_aggregate_derive_pending_events() {
    let id = TestCounterId::new();
    let mut counter = TestCounter::new(id);

    assert_eq!(counter.pending_events().len(), 0);

    counter.increment(5);

    assert_eq!(counter.pending_events().len(), 1);
}

#[test]
fn test_aggregate_derive_clear_pending_events() {
    let id = TestCounterId::new();
    let mut counter = TestCounter::new(id);

    counter.increment(5);
    assert_eq!(counter.pending_events().len(), 1);

    counter.clear_pending_events();
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_derive_apply() {
    let id = TestCounterId::new();
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
    let id = TestCounterId::new();
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
    let id = TestCounterId::new();
    let state = TestCounterState { value: 100 };

    let counter = TestCounter::from_snapshot(id, Version::new(5), state);

    assert_eq!(counter.value, 100);
    assert_eq!(counter.version(), Version::new(5));
}

#[test]
fn test_deref_transparent_access() {
    let id = TestCounterId::new();
    let mut counter = TestCounter::new(id);

    // Can access and modify state fields directly via Deref/DerefMut
    counter.value = 999;

    assert_eq!(counter.value, 999);
}

#[test]
fn test_state_ref_and_mut() {
    let id = TestCounterId::new();
    let mut counter = TestCounter::new(id);

    counter.increment(5);

    // Can get a reference to the state
    let state_ref = counter.state_ref();
    assert_eq!(state_ref.value, 5);

    // Can get a mutable reference to the state
    let state_mut = counter.state_mut();
    state_mut.value = 100;

    assert_eq!(counter.value, 100);
}

#[test]
fn test_aggregate_state_method() {
    let id = TestCounterId::new();
    let mut counter = TestCounter::new(id);

    counter.increment(42);

    // The state() method from Aggregate trait
    let state = counter.state();
    assert_eq!(state.value, 42);
}
