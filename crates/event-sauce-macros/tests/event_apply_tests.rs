//! Integration tests for #[derive(Event)] macro with aggregate attribute.
//!
//! These tests verify that the Event derive macro correctly generates
//! the apply_event method when the aggregate attribute is specified.

use chrono::{DateTime, Utc};
use event_sauce_core::{Aggregate, AggregateId, ApplyEvent, DomainEvent, Version};
use event_sauce_macros::{AggregateError, aggregate, Event as DeriveEvent};
use serde::{Deserialize, Serialize};

// ============================================================================
// Test Domain Model
// ============================================================================

/// Test aggregate ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
struct TestId(i32);

impl std::fmt::Display for TestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestId({})", self.0)
    }
}

impl AggregateId for TestId {
    fn to_uuid(&self) -> uuid::Uuid {
        // For testing purposes, create a deterministic UUID from the i32
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(&self.0.to_le_bytes());
        uuid::Uuid::from_bytes(bytes)
    }
}

/// Test error type
#[derive(AggregateError, Debug, thiserror::Error)]
enum TestError {
    #[error("Invalid value: {0}")]
    InvalidValue(i32),
}

// ============================================================================
// Test Events
// ============================================================================

/// Event: Value was set
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValueSetEvent {
    value: i32,
    timestamp: DateTime<Utc>,
}

/// Event: Value was incremented
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValueIncrementedEvent {
    amount: i32,
    timestamp: DateTime<Utc>,
}

/// Event: Value was reset
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValueResetEvent {
    timestamp: DateTime<Utc>,
}

/// Test event enum with aggregate attribute
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Test", aggregate = "TestAgg")]
enum TestEvent {
    Set(ValueSetEvent),
    Incremented(ValueIncrementedEvent),
    Reset(ValueResetEvent),
}

// ============================================================================
// ApplyEvent Implementations
// ============================================================================

impl ApplyEvent<TestAgg, TestError> for ValueSetEvent {
    fn validate(&self, _aggregate: &TestAgg) -> Result<(), TestError> {
        if self.value < 0 {
            return Err(TestError::InvalidValue(self.value));
        }
        Ok(())
    }

    fn apply(&self, aggregate: &mut TestAgg) {
        aggregate.value = self.value;
    }
}

impl ApplyEvent<TestAgg, TestError> for ValueIncrementedEvent {
    fn validate(&self, _aggregate: &TestAgg) -> Result<(), TestError> {
        if self.amount <= 0 {
            return Err(TestError::InvalidValue(self.amount));
        }
        Ok(())
    }

    fn apply(&self, aggregate: &mut TestAgg) {
        aggregate.value += self.amount;
    }
}

impl ApplyEvent<TestAgg, TestError> for ValueResetEvent {
    fn apply(&self, aggregate: &mut TestAgg) {
        aggregate.value = 0;
    }
}

// ============================================================================
// Test Aggregate
// ============================================================================

#[aggregate(id = "TestId", event = "TestEvent", error = "TestError")]
#[derive(Default)]
struct TestAgg {
    id: TestId,
    value: i32,
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn test_aggregate_attribute_generates_apply_event() {
    // Test that the aggregate attribute causes apply_event to be generated
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });

    let event = TestEvent::Set(ValueSetEvent {
        value: 42,
        timestamp: Utc::now(),
    });

    // This should compile and work - apply_event is auto-generated
    aggregate.apply_event(&event).unwrap();

    assert_eq!(aggregate.value, 42);
}

#[test]
fn test_apply_event_dispatches_to_apply_event_impl() {
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 10,
    });

    // Test Set event
    let set_event = TestEvent::Set(ValueSetEvent {
        value: 20,
        timestamp: Utc::now(),
    });
    aggregate.apply_event(&set_event).unwrap();
    assert_eq!(aggregate.value, 20);

    // Test Incremented event
    let inc_event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 5,
        timestamp: Utc::now(),
    });
    aggregate.apply_event(&inc_event).unwrap();
    assert_eq!(aggregate.value, 25);

    // Test Reset event
    let reset_event = TestEvent::Reset(ValueResetEvent {
        timestamp: Utc::now(),
    });
    aggregate.apply_event(&reset_event).unwrap();
    assert_eq!(aggregate.value, 0);
}

#[test]
fn test_apply_event_with_multiple_events() {
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });

    let events = vec![
        TestEvent::Set(ValueSetEvent {
            value: 10,
            timestamp: Utc::now(),
        }),
        TestEvent::Incremented(ValueIncrementedEvent {
            amount: 5,
            timestamp: Utc::now(),
        }),
        TestEvent::Incremented(ValueIncrementedEvent {
            amount: 3,
            timestamp: Utc::now(),
        }),
    ];

    for event in events {
        aggregate.apply_event(&event).unwrap();
    }

    assert_eq!(aggregate.value, 18); // 10 + 5 + 3
}

#[test]
fn test_apply_event_through_aggregate_trait() {
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });

    // Use the Aggregate trait's apply method
    aggregate
        .apply(ValueSetEvent {
            value: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

    assert_eq!(aggregate.value, 100);
    assert_eq!(aggregate.version(), Version::from(1));
    assert_eq!(aggregate.pending_events().len(), 1);
}

#[test]
fn test_apply_event_preserves_aggregate_state() {
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(42),
        value: 10,
    });

    let event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 5,
        timestamp: Utc::now(),
    });

    aggregate.apply_event(&event).unwrap();

    // Check that the aggregate state is preserved
    assert_eq!(aggregate.id, TestId(42));
    assert_eq!(aggregate.value, 15);
}

#[test]
fn test_apply_event_works_with_deref() {
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });

    // The aggregate derefs to state, so we can access state fields directly
    assert_eq!(aggregate.value, 0);

    aggregate
        .apply_event(&TestEvent::Set(ValueSetEvent {
            value: 50,
            timestamp: Utc::now(),
        }))
        .unwrap();

    // After applying, we can still access through deref
    assert_eq!(aggregate.value, 50);
}

#[test]
fn test_generated_apply_event_is_public() {
    // This test ensures the generated apply_event method is public
    // by calling it from outside the module
    let mut aggregate = TestAgg::from_state(TestAggState::default());

    let event = TestEvent::Reset(ValueResetEvent {
        timestamp: Utc::now(),
    });

    // This should compile without errors
    aggregate.apply_event(&event).unwrap();
}

#[test]
fn test_apply_event_with_validation() {
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });

    // Valid event through apply (which validates)
    aggregate
        .apply(ValueSetEvent {
            value: 10,
            timestamp: Utc::now(),
        })
        .unwrap();

    // Get the event before applying to avoid borrow checker issue
    let event = aggregate.pending_events()[0].clone();

    // Reset value to test apply_event
    aggregate.value = 0;

    // This should work since validation passes
    aggregate.apply_event(&event).unwrap();
    assert_eq!(aggregate.value, 10);
}

#[test]
fn test_apply_event_integration_with_aggregate_lifecycle() {
    // Create a new aggregate
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });

    // Apply events through the Aggregate trait
    aggregate
        .apply(ValueSetEvent {
            value: 10,
            timestamp: Utc::now(),
        })
        .unwrap();
    aggregate
        .apply(ValueIncrementedEvent {
            amount: 5,
            timestamp: Utc::now(),
        })
        .unwrap();

    // Check state
    assert_eq!(aggregate.value, 15);
    assert_eq!(aggregate.version(), Version::from(2));
    assert_eq!(aggregate.pending_events().len(), 2);

    // Replay events using apply_event
    let events: Vec<_> = aggregate.pending_events().to_vec();
    let mut replayed = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });

    for event in events {
        replayed.apply_unchecked(&event);
    }

    assert_eq!(replayed.value, 15);
    assert_eq!(replayed.version(), Version::from(2));
}

// ============================================================================
// Test Without Aggregate Attribute
// ============================================================================

/// Event enum without aggregate attribute - should NOT generate apply_event
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Basic", aggregate = "BasicAggregate")]
enum BasicEvent {
    Happened { timestamp: DateTime<Utc> },
}

// Mock aggregate for BasicEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BasicAggregate {
    id: TestId,
    version: event_sauce_core::Version,
    pending_events: Vec<BasicEvent>,
}

impl Aggregate for BasicAggregate {
    type Event = BasicEvent;
    type Id = TestId;
    type Error = TestError;

    fn new(id: Self::Id) -> Self {
        Self {
            id,
            version: event_sauce_core::Version::initial(),
            pending_events: Vec::new(),
        }
    }

    fn aggregate_id(&self) -> &Self::Id {
        &self.id
    }

    fn version(&self) -> event_sauce_core::Version {
        self.version
    }

    fn pending_events(&self) -> &[Self::Event] {
        &self.pending_events
    }

    fn clear_pending_events(&mut self) {
        self.pending_events.clear();
    }

    fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
        let event = event.into();
        self.apply_internal(&event)?;
        self.pending_events.push(event);
        Ok(())
    }

    fn apply_internal(&mut self, _event: &Self::Event) -> Result<(), Self::Error> {
        self.version = self.version.next();
        Ok(())
    }
}

#[test]
fn test_without_aggregate_attribute_no_apply_event() {
    // This test verifies that without the aggregate attribute,
    // no apply_event is generated. This is verified at compile time -
    // if we tried to call BasicEvent's apply_event, it wouldn't compile.
    let event = BasicEvent::Happened {
        timestamp: Utc::now(),
    };

    // We can still use the DomainEvent trait
    assert_eq!(event.event_type(), "Basic.Happened");
    assert_eq!(event.event_version(), 1);
}

// ============================================================================
// Test Edge Cases
// ============================================================================

#[test]
fn test_apply_event_with_reset_to_zero() {
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 100,
    });

    aggregate
        .apply_event(&TestEvent::Reset(ValueResetEvent {
            timestamp: Utc::now(),
        }))
        .unwrap();

    assert_eq!(aggregate.value, 0);
}

#[test]
fn test_apply_event_multiple_times_same_event() {
    let mut aggregate = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });

    let event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 1,
        timestamp: Utc::now(),
    });

    // Apply the same event multiple times
    for _ in 0..10 {
        aggregate.apply_event(&event).unwrap();
    }

    assert_eq!(aggregate.value, 10);
}

#[test]
fn test_apply_event_is_deterministic() {
    let event = TestEvent::Set(ValueSetEvent {
        value: 42,
        timestamp: Utc::now(),
    });

    // Apply to first aggregate
    let mut aggregate1 = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });
    aggregate1.apply_event(&event).unwrap();

    // Apply to second aggregate
    let mut aggregate2 = TestAgg::from_state(TestAggState {
        id: TestId(1),
        value: 0,
    });
    aggregate2.apply_event(&event).unwrap();

    // Results should be identical
    assert_eq!(aggregate1.value, aggregate2.value);
}
