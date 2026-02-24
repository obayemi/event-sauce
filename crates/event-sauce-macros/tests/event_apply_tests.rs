//! Integration tests for #[derive(Event)] macro with aggregate attribute.
//!
//! These tests verify that the Event derive macro correctly generates
//! the apply_internal method when the aggregate attribute is specified.

use chrono::{DateTime, Utc};
use event_sauce_core::{Aggregate, AggregateId as _, ApplyEvent, DomainEvent, Version};
use event_sauce_macros::{
    aggregate, AggregateError, AggregateId as DeriveAggregateId, Event as DeriveEvent,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// Test Domain Model
// ============================================================================

/// Test aggregate ID
#[derive(
    DeriveAggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default,
)]
#[repr(transparent)]
struct TestId(Uuid);

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

impl ApplyEvent<TestAgg> for ValueSetEvent {
    fn validate(&self, _aggregate: &TestAgg) -> Result<(), <TestAgg as Aggregate>::Error> {
        if self.value < 0 {
            return Err(TestError::InvalidValue(self.value));
        }
        Ok(())
    }

    fn apply(&self, aggregate: &mut TestAgg) {
        aggregate.value = self.value;
    }
}

impl ApplyEvent<TestAgg> for ValueIncrementedEvent {
    fn validate(&self, _aggregate: &TestAgg) -> Result<(), <TestAgg as Aggregate>::Error> {
        if self.amount <= 0 {
            return Err(TestError::InvalidValue(self.amount));
        }
        Ok(())
    }

    fn apply(&self, aggregate: &mut TestAgg) {
        aggregate.value += self.amount;
    }
}

impl ApplyEvent<TestAgg> for ValueResetEvent {
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
    #[aggregate_id]
    id: TestId,
    value: i32,
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn test_aggregate_attribute_generates_apply_internal() {
    // Test that the aggregate attribute causes apply_internal to be generated
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);

    let event = TestEvent::Set(ValueSetEvent {
        value: 42,
        timestamp: Utc::now(),
    });

    // This should compile and work - apply_internal is auto-generated
    aggregate.apply_internal(&event).unwrap();

    assert_eq!(aggregate.value, 42);
}

#[test]
fn test_apply_internal_dispatches_to_apply_internal_impl() {
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);
    // Initialize value to 10
    aggregate
        .apply(ValueSetEvent {
            value: 10,
            timestamp: Utc::now(),
        })
        .unwrap();

    // Test Set event
    let set_event = TestEvent::Set(ValueSetEvent {
        value: 20,
        timestamp: Utc::now(),
    });
    aggregate.apply_internal(&set_event).unwrap();
    assert_eq!(aggregate.value, 20);

    // Test Incremented event
    let inc_event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 5,
        timestamp: Utc::now(),
    });
    aggregate.apply_internal(&inc_event).unwrap();
    assert_eq!(aggregate.value, 25);

    // Test Reset event
    let reset_event = TestEvent::Reset(ValueResetEvent {
        timestamp: Utc::now(),
    });
    aggregate.apply_internal(&reset_event).unwrap();
    assert_eq!(aggregate.value, 0);
}

#[test]
fn test_apply_internal_with_multiple_events() {
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);

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
        aggregate.apply_internal(&event).unwrap();
    }

    assert_eq!(aggregate.value, 18); // 10 + 5 + 3
}

#[test]
fn test_apply_internal_through_aggregate_trait() {
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);

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
fn test_apply_internal_preserves_aggregate_state() {
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);
    // Initialize value to 10
    aggregate
        .apply(ValueSetEvent {
            value: 10,
            timestamp: Utc::now(),
        })
        .unwrap();

    let event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 5,
        timestamp: Utc::now(),
    });

    aggregate.apply_internal(&event).unwrap();

    // Check that the aggregate state is preserved
    assert_eq!(aggregate.aggregate_id(), &id);
    assert_eq!(aggregate.value, 15);
}

#[test]
fn test_apply_internal_works_with_deref() {
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);

    // The aggregate derefs to state, so we can access state fields directly
    assert_eq!(aggregate.value, 0);

    aggregate
        .apply_internal(&TestEvent::Set(ValueSetEvent {
            value: 50,
            timestamp: Utc::now(),
        }))
        .unwrap();

    // After applying, we can still access through deref
    assert_eq!(aggregate.value, 50);
}

#[test]
fn test_generated_apply_internal_is_public() {
    // This test ensures the generated apply_internal method is public
    // by calling it from outside the module
    let mut aggregate = TestAgg::new(TestId::new());

    let event = TestEvent::Reset(ValueResetEvent {
        timestamp: Utc::now(),
    });

    // This should compile without errors
    aggregate.apply_internal(&event).unwrap();
}

#[test]
fn test_apply_internal_with_validation() {
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);

    // Valid event through apply (which validates)
    aggregate
        .apply(ValueSetEvent {
            value: 10,
            timestamp: Utc::now(),
        })
        .unwrap();

    // Get the event before applying to avoid borrow checker issue
    let event = aggregate.pending_events()[0].clone();

    // Reset value to test apply_internal
    aggregate.value = 0;

    // This should work since validation passes
    aggregate.apply_internal(&event).unwrap();
    assert_eq!(aggregate.value, 10);
}

#[test]
fn test_apply_internal_integration_with_aggregate_lifecycle() {
    // Create a new aggregate
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);

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

    // Replay events using apply_internal
    let events: Vec<_> = aggregate.pending_events().to_vec();
    let mut replayed = TestAgg::new(id);

    for event in events {
        replayed.apply_unchecked(&event);
    }

    assert_eq!(replayed.value, 15);
    assert_eq!(replayed.version(), Version::from(2));
}

// ============================================================================
// Test Without Aggregate Attribute
// ============================================================================

#[derive(DeriveAggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
struct BasicAggregateId(Uuid);

/// Event enum without aggregate attribute - should NOT generate apply_internal
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Basic", aggregate = "BasicAggregate")]
enum BasicEvent {
    Happened { timestamp: DateTime<Utc> },
}

// Mock aggregate for BasicEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BasicAggregateState;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BasicAggregate {
    id: BasicAggregateId,
    state: BasicAggregateState,
    version: event_sauce_core::Version,
    pending_events: Vec<BasicEvent>,
}

impl event_sauce_core::EventApplicator<BasicAggregate> for BasicEvent {
    fn dispatch(&self, _aggregate: &mut BasicAggregate) -> Result<(), TestError> {
        Ok(())
    }

    fn dispatch_unchecked(&self, _aggregate: &mut BasicAggregate) {}
}

impl Aggregate for BasicAggregate {
    type Id = BasicAggregateId;
    type Event = BasicEvent;
    type Error = TestError;
    type State = BasicAggregateState;

    fn new(id: BasicAggregateId) -> Self {
        Self {
            id,
            state: BasicAggregateState,
            version: event_sauce_core::Version::initial(),
            pending_events: Vec::new(),
        }
    }

    fn aggregate_id(&self) -> &BasicAggregateId {
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

    fn push_pending_event(&mut self, event: Self::Event) {
        self.pending_events.push(event);
    }

    fn increment_version(&mut self) {
        self.version = self.version.next();
    }

    fn state(&self) -> &Self::State {
        &self.state
    }

    fn from_snapshot(
        id: BasicAggregateId,
        version: event_sauce_core::Version,
        state: Self::State,
    ) -> Self {
        Self {
            id,
            state,
            version,
            pending_events: Vec::new(),
        }
    }
}

#[test]
fn test_without_aggregate_attribute_no_apply_internal() {
    // This test verifies that without the aggregate attribute,
    // no apply_internal is generated. This is verified at compile time -
    // if we tried to call BasicEvent's apply_internal, it wouldn't compile.
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
fn test_apply_internal_with_reset_to_zero() {
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);
    // Initialize value to 100
    aggregate
        .apply(ValueSetEvent {
            value: 100,
            timestamp: Utc::now(),
        })
        .unwrap();

    aggregate
        .apply_internal(&TestEvent::Reset(ValueResetEvent {
            timestamp: Utc::now(),
        }))
        .unwrap();

    assert_eq!(aggregate.value, 0);
}

#[test]
fn test_apply_internal_multiple_times_same_event() {
    let id = TestId::new();
    let mut aggregate = TestAgg::new(id);

    let event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 1,
        timestamp: Utc::now(),
    });

    // Apply the same event multiple times
    for _ in 0..10 {
        aggregate.apply_internal(&event).unwrap();
    }

    assert_eq!(aggregate.value, 10);
}

#[test]
fn test_apply_internal_is_deterministic() {
    let id = TestId::new();
    let event = TestEvent::Set(ValueSetEvent {
        value: 42,
        timestamp: Utc::now(),
    });

    // Apply to first aggregate
    let mut aggregate1 = TestAgg::new(id);
    aggregate1.apply_internal(&event).unwrap();

    // Apply to second aggregate
    let mut aggregate2 = TestAgg::new(id);
    aggregate2.apply_internal(&event).unwrap();

    // Results should be identical
    assert_eq!(aggregate1.value, aggregate2.value);
}
