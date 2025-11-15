//! Integration tests for #[derive(Aggregate)] macro.
//!
//! These tests verify that the Aggregate derive macro correctly generates
//! the Aggregate trait implementation.

use event_sauce_core::{Aggregate, AggregateId, DomainEvent, Version};
use chrono::{DateTime, Utc};
use std::fmt;

// Define a simple aggregate ID for testing
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

impl AggregateId for TestCounterId {}

// Define a simple event for testing
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum TestCounterEvent {
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

// This is the aggregate struct that uses the derive macro
// The macro should generate the Aggregate trait implementation
#[derive(event_sauce_macros::Aggregate, Debug, Clone)]
#[aggregate(id = "TestCounterId", event = "TestCounterEvent")]
struct TestCounter {
    #[aggregate_id]
    id: TestCounterId,
    value: i32,
    #[aggregate_version]
    version: Version,
    #[aggregate_events]
    pending_events: Vec<TestCounterEvent>,
}

impl TestCounter {
    fn new(id: TestCounterId) -> Self {
        Self {
            id,
            value: 0,
            version: Version::initial(),
            pending_events: Vec::new(),
        }
    }

    fn increment(&mut self, amount: i32) {
        let event = TestCounterEvent::Incremented {
            amount,
            timestamp: Utc::now(),
        };
        self.apply(&event);
        self.pending_events.push(event);
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
    counter.apply(&event);

    // Version should be incremented
    assert_eq!(counter.version(), initial_version.next());
    // Value should be updated (via apply_event)
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
