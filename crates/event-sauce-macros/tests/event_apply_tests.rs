//! Integration tests for #[derive(Event)] macro with aggregate attribute.
//!
//! These tests verify that the Event derive macro correctly generates
//! EventApplicator when the aggregate attribute is specified.

use chrono::{DateTime, Utc};
use event_sauce_core::{
    Aggregate, AggregateRoot, AggregateVersion, ApplyEvent, DomainEvent, Entity, EntityId,
};
use event_sauce_macros::{aggregate, AggregateError, Event as DeriveEvent};
use serde::{Deserialize, Serialize};

// ============================================================================
// Test Domain Model
// ============================================================================

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
    fn validate(&self, _entity: &TestAgg) -> Result<(), <TestAgg as Aggregate>::Error> {
        if self.value < 0 {
            return Err(TestError::InvalidValue(self.value));
        }
        Ok(())
    }

    fn apply(&self, entity: &mut TestAgg) {
        entity.value = self.value;
    }
}

impl ApplyEvent<TestAgg> for ValueIncrementedEvent {
    fn validate(&self, _entity: &TestAgg) -> Result<(), <TestAgg as Aggregate>::Error> {
        if self.amount <= 0 {
            return Err(TestError::InvalidValue(self.amount));
        }
        Ok(())
    }

    fn apply(&self, entity: &mut TestAgg) {
        entity.value += self.amount;
    }
}

impl ApplyEvent<TestAgg> for ValueResetEvent {
    fn apply(&self, entity: &mut TestAgg) {
        entity.value = 0;
    }
}

// ============================================================================
// Test Aggregate (uses #[aggregate] attribute macro)
// ============================================================================

#[aggregate(event = "TestEvent", error = "TestError")]
#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestAgg {
    #[id]
    id: EntityId,
    value: i32,
}
impl event_sauce_core::DefaultEntity for TestAgg {}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn test_event_applicator_dispatch() {
    // Test that the derive Event with aggregate attribute generates EventApplicator
    let mut entity = TestAgg::new(EntityId::new());

    let event = TestEvent::Set(ValueSetEvent {
        value: 42,
        timestamp: Utc::now(),
    });

    // EventApplicator::dispatch works on the entity directly
    event_sauce_core::EventApplicator::dispatch(&event, &mut entity).unwrap();

    assert_eq!(entity.value, 42);
}

#[test]
fn test_event_applicator_dispatches_all_variants() {
    let mut entity = TestAgg::new(EntityId::new());

    // Test Set event
    let set_event = TestEvent::Set(ValueSetEvent {
        value: 20,
        timestamp: Utc::now(),
    });
    event_sauce_core::EventApplicator::dispatch(&set_event, &mut entity).unwrap();
    assert_eq!(entity.value, 20);

    // Test Incremented event
    let inc_event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 5,
        timestamp: Utc::now(),
    });
    event_sauce_core::EventApplicator::dispatch(&inc_event, &mut entity).unwrap();
    assert_eq!(entity.value, 25);

    // Test Reset event
    let reset_event = TestEvent::Reset(ValueResetEvent {
        timestamp: Utc::now(),
    });
    event_sauce_core::EventApplicator::dispatch(&reset_event, &mut entity).unwrap();
    assert_eq!(entity.value, 0);
}

#[test]
fn test_apply_multiple_events() {
    let mut entity = TestAgg::new(EntityId::new());

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

    for event in &events {
        event_sauce_core::EventApplicator::dispatch(event, &mut entity).unwrap();
    }

    assert_eq!(entity.value, 18); // 10 + 5 + 3
}

#[test]
fn test_aggregate_root_apply_with_generated_event_applicator() {
    let id = EntityId::new();
    let mut root = AggregateRoot::<TestAgg>::new(id);

    // Use the AggregateRoot's apply method which delegates to EventApplicator
    root.apply(ValueSetEvent {
        value: 100,
        timestamp: Utc::now(),
    })
    .unwrap();

    assert_eq!(root.value, 100);
    assert_eq!(root.version(), AggregateVersion::new(1));
    assert_eq!(root.pending_events().len(), 1);
}

#[test]
fn test_apply_preserves_entity_state() {
    let id = EntityId::new();
    let mut entity = TestAgg::new(id);

    let event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 5,
        timestamp: Utc::now(),
    });

    event_sauce_core::EventApplicator::dispatch(&event, &mut entity).unwrap();

    // Check that the entity state is preserved
    assert_eq!(entity.entity_id(), id);
    assert_eq!(entity.value, 5);
}

#[test]
fn test_aggregate_root_integration_lifecycle() {
    // Create a new aggregate via AggregateRoot
    let id = EntityId::new();
    let mut root = AggregateRoot::<TestAgg>::new(id);

    // Apply events through AggregateRoot
    root.apply(ValueSetEvent {
        value: 10,
        timestamp: Utc::now(),
    })
    .unwrap();
    root.apply(ValueIncrementedEvent {
        amount: 5,
        timestamp: Utc::now(),
    })
    .unwrap();

    // Check state
    assert_eq!(root.value, 15);
    assert_eq!(root.version(), AggregateVersion::new(2));
    assert_eq!(root.pending_events().len(), 2);

    // Replay events using dispatch_unchecked on a fresh entity
    let events: Vec<_> = root.pending_events().to_vec();
    let mut replayed_entity = TestAgg::new(id);

    for event in &events {
        event_sauce_core::EventApplicator::dispatch_unchecked(event, &mut replayed_entity);
    }

    assert_eq!(replayed_entity.value, 15);
}

// ============================================================================
// Test Without Tuple Variants (no auto-generated EventApplicator)
// ============================================================================

/// Event enum without tuple variants - should NOT generate EventApplicator
#[derive(event_sauce_macros::Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Basic", aggregate = "BasicAggregate")]
enum BasicEvent {
    Happened { timestamp: DateTime<Utc> },
}

// Mock aggregate for BasicEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BasicAggregate {
    id: EntityId,
}

impl Entity for BasicAggregate {
    fn new(id: EntityId) -> Self {
        Self { id }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::EventApplicator<BasicAggregate> for BasicEvent {
    fn dispatch(&self, _aggregate: &mut BasicAggregate) -> Result<(), TestError> {
        Ok(())
    }

    fn dispatch_unchecked(&self, _aggregate: &mut BasicAggregate) {}
}

impl Aggregate for BasicAggregate {
    type Event = BasicEvent;
    type Error = TestError;
}

#[test]
fn test_without_tuple_variants_no_auto_event_applicator() {
    // This test verifies that without tuple variants,
    // no auto EventApplicator is generated.
    let event = BasicEvent::Happened {
        timestamp: Utc::now(),
    };

    // We can still use the DomainEvent trait
    assert_eq!(event.event_type(), "Basic.Happened");
    assert_eq!(
        event.event_version(),
        event_sauce_core::EventVersion::new(1)
    );
}

// ============================================================================
// Test Edge Cases
// ============================================================================

#[test]
fn test_apply_with_reset_to_zero() {
    let mut entity = TestAgg::new(EntityId::new());

    // Set to 100
    event_sauce_core::EventApplicator::dispatch(
        &TestEvent::Set(ValueSetEvent {
            value: 100,
            timestamp: Utc::now(),
        }),
        &mut entity,
    )
    .unwrap();

    // Reset
    event_sauce_core::EventApplicator::dispatch(
        &TestEvent::Reset(ValueResetEvent {
            timestamp: Utc::now(),
        }),
        &mut entity,
    )
    .unwrap();

    assert_eq!(entity.value, 0);
}

#[test]
fn test_apply_same_event_multiple_times() {
    let mut entity = TestAgg::new(EntityId::new());

    let event = TestEvent::Incremented(ValueIncrementedEvent {
        amount: 1,
        timestamp: Utc::now(),
    });

    // Apply the same event multiple times
    for _ in 0..10 {
        event_sauce_core::EventApplicator::dispatch(&event, &mut entity).unwrap();
    }

    assert_eq!(entity.value, 10);
}

#[test]
fn test_apply_is_deterministic() {
    let id = EntityId::new();
    let event = TestEvent::Set(ValueSetEvent {
        value: 42,
        timestamp: Utc::now(),
    });

    // Apply to first entity
    let mut entity1 = TestAgg::new(id);
    event_sauce_core::EventApplicator::dispatch(&event, &mut entity1).unwrap();

    // Apply to second entity
    let mut entity2 = TestAgg::new(id);
    event_sauce_core::EventApplicator::dispatch(&event, &mut entity2).unwrap();

    // Results should be identical
    assert_eq!(entity1.value, entity2.value);
}

#[test]
fn test_validation_failure_via_aggregate_root() {
    let mut root = AggregateRoot::<TestAgg>::new(EntityId::new());

    // Set with negative value should fail validation
    let result = root.apply(ValueSetEvent {
        value: -5,
        timestamp: Utc::now(),
    });

    assert!(result.is_err());
    assert_eq!(root.value, 0); // State unchanged
    assert_eq!(root.version(), AggregateVersion::initial()); // Version unchanged
    assert_eq!(root.pending_events().len(), 0); // No pending events
}
