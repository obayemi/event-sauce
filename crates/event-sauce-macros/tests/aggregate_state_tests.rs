//! Integration tests for #[derive(AggregateState)] macro.
//!
//! These tests verify that the AggregateState derive macro correctly generates
//! the wrapper aggregate struct and Aggregate trait implementation.

use event_sauce_core::{Aggregate, AggregateId, DomainEvent, Version};
use event_sauce_macros::AggregateError;
use chrono::{DateTime, Utc};
use std::fmt;
use thiserror::Error;

// Define error type for testing
#[derive(AggregateError, Debug, Error)]
#[error("Test counter error")]
pub struct TestCounterError;

// Define a simple aggregate ID for testing
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TestCounterId(String);

impl TestCounterId {
    pub fn new(id: String) -> Self {
        Self(id)
    }
}

impl fmt::Display for TestCounterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counter-{}", self.0)
    }
}

impl AggregateId for TestCounterId {}

// Define a simple event for testing
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum TestCounterEvent {
    Incremented { amount: i32, timestamp: DateTime<Utc> },
    Decremented { amount: i32, timestamp: DateTime<Utc> },
}

impl DomainEvent for TestCounterEvent {
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

// This is the state-only struct that uses the AggregateState derive macro
// The macro should generate a wrapper struct and Aggregate trait implementation
#[derive(event_sauce_macros::AggregateState, Debug, Clone, Default)]
#[aggregate(id = "TestCounterId", event = "TestCounterEvent", error = "TestCounterError")]
struct TestCounterState {
    #[aggregate_id]
    id: TestCounterId,
    value: i32,
}

impl Default for TestCounterId {
    fn default() -> Self {
        Self("default".to_string())
    }
}

impl TestCounterState {
    fn new(id: TestCounterId) -> Self {
        Self { id, value: 0 }
    }

    fn apply_event(&mut self, event: &TestCounterEvent) {
        match event {
            TestCounterEvent::Incremented { amount, .. } => {
                self.value += amount;
            }
            TestCounterEvent::Decremented { amount, .. } => {
                self.value -= amount;
            }
        }
    }
}

// The macro should generate TestCounterAggregate struct
// We can use it directly in tests

#[test]
fn test_aggregate_state_generates_wrapper_struct() {
    // The macro should have generated TestCounterAggregate
    let state = TestCounterState::new(TestCounterId::new("test-1".to_string()));
    let _counter = TestCounterAggregate::from_state(state);
}

#[test]
fn test_aggregate_state_implements_aggregate_trait() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState::new(id.clone());
    let counter = TestCounterAggregate::from_state(state);

    // The Aggregate trait should be implemented
    assert_eq!(counter.aggregate_id(), &id);
    assert_eq!(counter.version(), Version::initial());
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_state_new_with_default() {
    let counter = TestCounterAggregate::new();

    assert_eq!(counter.aggregate_id(), &TestCounterId::default());
    assert_eq!(counter.version(), Version::initial());
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_state_from_state() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState::new(id.clone());
    let counter = TestCounterAggregate::from_state(state);

    assert_eq!(counter.aggregate_id(), &id);
}

#[test]
fn test_aggregate_state_state_access() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState::new(id);
    let counter = TestCounterAggregate::from_state(state);

    // Should have state() method
    let _state_ref = counter.state();
    assert_eq!(_state_ref.value, 0);
}

#[test]
fn test_aggregate_state_state_mut_access() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState::new(id);
    let mut counter = TestCounterAggregate::from_state(state);

    // Should have state_mut() method
    let state_mut = counter.state_mut();
    state_mut.value = 42;

    assert_eq!(counter.state().value, 42);
}

#[test]
fn test_aggregate_state_deref() {
    let id = TestCounterId::new("test-1".to_string());
    let mut state = TestCounterState::new(id);
    state.value = 100;
    let counter = TestCounterAggregate::from_state(state);

    // Should be able to access state fields via Deref
    assert_eq!(counter.value, 100);
}

#[test]
fn test_aggregate_state_deref_mut() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState::new(id);
    let mut counter = TestCounterAggregate::from_state(state);

    // Should be able to mutate state fields via DerefMut
    counter.value = 42;
    assert_eq!(counter.value, 42);
}

#[test]
fn test_aggregate_state_apply_event() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState::new(id);
    let mut counter = TestCounterAggregate::from_state(state);

    let event = TestCounterEvent::Incremented {
        amount: 10,
        timestamp: Utc::now(),
    };

    let initial_version = counter.version();
    counter.apply(event);

    // Version should be incremented
    assert_eq!(counter.version(), initial_version.next());
    // Value should be updated (via apply_event delegation)
    assert_eq!(counter.value, 10);
    // Event should be in pending
    assert_eq!(counter.pending_events().len(), 1);
}

#[test]
fn test_aggregate_state_multiple_events() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState::new(id);
    let mut counter = TestCounterAggregate::from_state(state);

    counter.apply(TestCounterEvent::Incremented {
        amount: 5,
        timestamp: Utc::now(),
    });
    counter.apply(TestCounterEvent::Incremented {
        amount: 3,
        timestamp: Utc::now(),
    });

    assert_eq!(counter.pending_events().len(), 2);
    assert_eq!(counter.value, 8);
    assert_eq!(counter.version(), Version::new(2));
}

#[test]
fn test_aggregate_state_clear_pending_events() {
    let id = TestCounterId::new("test-1".to_string());
    let state = TestCounterState::new(id);
    let mut counter = TestCounterAggregate::from_state(state);

    counter.apply(TestCounterEvent::Incremented {
        amount: 5,
        timestamp: Utc::now(),
    });
    assert_eq!(counter.pending_events().len(), 1);

    counter.clear_pending_events();
    assert_eq!(counter.pending_events().len(), 0);
}

#[test]
fn test_aggregate_state_naming_convention() {
    // TestCounterState should generate TestCounterAggregate
    // This is verified by the code compiling
    let _counter: TestCounterAggregate = TestCounterAggregate::new();
}

#[test]
fn test_aggregate_state_implements_default() {
    let counter = TestCounterAggregate::default();
    assert_eq!(counter.version(), Version::initial());
}

// Test with custom name attribute
#[derive(event_sauce_macros::AggregateState, Debug, Clone, Default)]
#[aggregate(id = "TestCounterId", event = "TestCounterEvent", error = "TestCounterError", name = "CustomCounter")]
struct CustomCounterState {
    #[aggregate_id]
    id: TestCounterId,
    value: i32,
}

impl CustomCounterState {
    fn apply_event(&mut self, event: &TestCounterEvent) {
        match event {
            TestCounterEvent::Incremented { amount, .. } => {
                self.value += amount;
            }
            TestCounterEvent::Decremented { amount, .. } => {
                self.value -= amount;
            }
        }
    }
}

#[test]
fn test_aggregate_state_custom_name() {
    // Should generate CustomCounter instead of CustomCounterStateAggregate
    let _counter: CustomCounter = CustomCounter::new();
}

// Test with public visibility
#[derive(event_sauce_macros::AggregateState, Debug, Clone, Default)]
#[aggregate(id = "TestCounterId", event = "TestCounterEvent", error = "TestCounterError")]
pub struct PublicCounterState {
    #[aggregate_id]
    id: TestCounterId,
    value: i32,
}

impl PublicCounterState {
    fn apply_event(&mut self, event: &TestCounterEvent) {
        match event {
            TestCounterEvent::Incremented { amount, .. } => {
                self.value += amount;
            }
            TestCounterEvent::Decremented { amount, .. } => {
                self.value -= amount;
            }
        }
    }
}

#[test]
fn test_aggregate_state_public_visibility() {
    // Should generate public PublicCounterAggregate
    let _counter: PublicCounterAggregate = PublicCounterAggregate::new();
}

// Note: We don't test without error attribute because () doesn't implement AggregateError
// The error attribute is optional for backward compatibility with the Aggregate derive macro,
// but for AggregateState it's recommended to always specify an error type for proper error handling

#[test]
fn test_aggregate_state_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<TestCounterAggregate>();
    assert_sync::<TestCounterAggregate>();
}

#[test]
fn test_aggregate_state_aggregate_type() {
    let type_name = TestCounterAggregate::aggregate_type();
    assert_eq!(type_name, "TestCounterAggregate");
}
