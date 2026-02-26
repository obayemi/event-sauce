//! Integration tests for #[derive(Event)] macro.
//!
//! These tests verify that the Event derive macro correctly generates
//! the DomainEvent trait implementation.

use chrono::{DateTime, Utc};
use event_sauce_core::{DomainEvent, EventApplicator, EventEnvelope, EventVersion};
use serde::{Deserialize, Serialize};

// Note: serde_json is only used in TryFrom tests
#[allow(unused_imports)]
use serde_json::json;

// Test: Simple enum with timestamp field
// Note: The TestEvent needs the aggregate attribute for TryFrom tests
#[derive(event_sauce_macros::Event, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[event(version = 1, aggregate = "TestAggregate")]
#[allow(dead_code)]
enum TestEvent {
    Created {
        id: String,
        timestamp: DateTime<Utc>,
    },
    Updated {
        value: i32,
        timestamp: DateTime<Utc>,
    },
    Deleted {
        reason: String,
        timestamp: DateTime<Utc>,
    },
}

// Mock aggregate for TestEvent - now uses Entity + Aggregate pattern
use event_sauce_core::{Aggregate, AggregateError, Entity, EntityId};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("Test aggregate error")]
struct TestAggregateError;

impl AggregateError for TestAggregateError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestAggregate {
    id: EntityId,
}

impl Entity for TestAggregate {
    fn new(id: EntityId) -> Self {
        Self { id }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl EventApplicator<TestAggregate> for TestEvent {
    fn dispatch(&self, _aggregate: &mut TestAggregate) -> Result<(), TestAggregateError> {
        Ok(())
    }

    fn dispatch_unchecked(&self, _aggregate: &mut TestAggregate) {}
}

impl Aggregate for TestAggregate {
    type Event = TestEvent;
    type Error = TestAggregateError;
}

#[test]
fn test_event_derive_implements_domain_event() {
    let timestamp = Utc::now();
    let event = TestEvent::Created {
        id: "test-1".to_string(),
        timestamp,
    };

    // The DomainEvent trait should be implemented
    assert_eq!(event.event_type(), "Test.Created");
    assert_eq!(
        event.event_version(),
        event_sauce_core::EventVersion::new(1)
    );
    assert_eq!(event.occurred_at(), timestamp);
}

#[test]
fn test_event_derive_event_type() {
    let timestamp = Utc::now();

    let created = TestEvent::Created {
        id: "test-1".to_string(),
        timestamp,
    };
    let updated = TestEvent::Updated {
        value: 42,
        timestamp,
    };
    let deleted = TestEvent::Deleted {
        reason: "test".to_string(),
        timestamp,
    };

    assert_eq!(created.event_type(), "Test.Created");
    assert_eq!(updated.event_type(), "Test.Updated");
    assert_eq!(deleted.event_type(), "Test.Deleted");
}

#[test]
fn test_event_derive_event_version() {
    let timestamp = Utc::now();
    let event = TestEvent::Created {
        id: "test-1".to_string(),
        timestamp,
    };

    assert_eq!(
        event.event_version(),
        event_sauce_core::EventVersion::new(1)
    );
}

#[test]
fn test_event_derive_occurred_at() {
    let timestamp1 = Utc::now();
    let event1 = TestEvent::Created {
        id: "test-1".to_string(),
        timestamp: timestamp1,
    };

    assert_eq!(event1.occurred_at(), timestamp1);

    let timestamp2 = Utc::now();
    let event2 = TestEvent::Updated {
        value: 42,
        timestamp: timestamp2,
    };

    assert_eq!(event2.occurred_at(), timestamp2);
}

#[test]
fn test_event_derive_is_cloneable() {
    let timestamp = Utc::now();
    let event1 = TestEvent::Created {
        id: "test-1".to_string(),
        timestamp,
    };
    let event2 = event1.clone();

    assert_eq!(event1.event_type(), event2.event_type());
    assert_eq!(event1.occurred_at(), event2.occurred_at());
}

#[test]
fn test_event_derive_is_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<TestEvent>();
    assert_sync::<TestEvent>();
}

// Test: Versioned event (version 2) with its own aggregate
#[derive(event_sauce_macros::Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 2, aggregate = "TestAggregateV2")]
#[allow(dead_code)]
enum TestEventV2 {
    CreatedV2 {
        id: String,
        name: String,
        timestamp: DateTime<Utc>,
    },
}

// Mock aggregate for TestEventV2
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestAggregateV2 {
    id: EntityId,
}

impl Entity for TestAggregateV2 {
    fn new(id: EntityId) -> Self {
        Self { id }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl EventApplicator<TestAggregateV2> for TestEventV2 {
    fn dispatch(&self, _aggregate: &mut TestAggregateV2) -> Result<(), TestAggregateError> {
        Ok(())
    }

    fn dispatch_unchecked(&self, _aggregate: &mut TestAggregateV2) {}
}

impl Aggregate for TestAggregateV2 {
    type Event = TestEventV2;
    type Error = TestAggregateError;
}

#[test]
fn test_event_derive_different_version() {
    let timestamp = Utc::now();
    let event = TestEventV2::CreatedV2 {
        id: "test-1".to_string(),
        name: "Test".to_string(),
        timestamp,
    };

    assert_eq!(
        event.event_version(),
        event_sauce_core::EventVersion::new(2)
    );
}

// Test: Event with custom type name prefix
#[derive(event_sauce_macros::Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Order", aggregate = "OrderAggregate")]
#[allow(dead_code)]
enum OrderEvent {
    Placed {
        order_id: String,
        timestamp: DateTime<Utc>,
    },
    Shipped {
        order_id: String,
        timestamp: DateTime<Utc>,
    },
}

// Mock aggregate for OrderEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderAggregate {
    id: EntityId,
}

impl Entity for OrderAggregate {
    fn new(id: EntityId) -> Self {
        Self { id }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl EventApplicator<OrderAggregate> for OrderEvent {
    fn dispatch(&self, _aggregate: &mut OrderAggregate) -> Result<(), TestAggregateError> {
        Ok(())
    }

    fn dispatch_unchecked(&self, _aggregate: &mut OrderAggregate) {}
}

impl Aggregate for OrderAggregate {
    type Event = OrderEvent;
    type Error = TestAggregateError;
}

#[test]
fn test_event_derive_custom_type_prefix() {
    let timestamp = Utc::now();

    let placed = OrderEvent::Placed {
        order_id: "order-1".to_string(),
        timestamp,
    };
    let shipped = OrderEvent::Shipped {
        order_id: "order-1".to_string(),
        timestamp,
    };

    // With custom prefix "Order", variant "Placed" becomes "Order.Placed"
    assert_eq!(placed.event_type(), "Order.Placed");
    assert_eq!(shipped.event_type(), "Order.Shipped");
}

// Test: Single variant event
#[derive(event_sauce_macros::Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, aggregate = "SimpleAggregate")]
enum SimpleEvent {
    Occurred { timestamp: DateTime<Utc> },
}

// Mock aggregate for SimpleEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SimpleAggregate {
    id: EntityId,
}

impl Entity for SimpleAggregate {
    fn new(id: EntityId) -> Self {
        Self { id }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl EventApplicator<SimpleAggregate> for SimpleEvent {
    fn dispatch(&self, _aggregate: &mut SimpleAggregate) -> Result<(), TestAggregateError> {
        Ok(())
    }

    fn dispatch_unchecked(&self, _aggregate: &mut SimpleAggregate) {}
}

impl Aggregate for SimpleAggregate {
    type Event = SimpleEvent;
    type Error = TestAggregateError;
}

#[test]
fn test_event_derive_single_variant() {
    let timestamp = Utc::now();
    let event = SimpleEvent::Occurred { timestamp };

    assert_eq!(event.event_type(), "Simple.Occurred");
    assert_eq!(
        event.event_version(),
        event_sauce_core::EventVersion::new(1)
    );
    assert_eq!(event.occurred_at(), timestamp);
}

// ============================================================================
// TryFrom Tests
// ============================================================================

#[test]
fn test_try_from_event_envelope_owned() {
    let timestamp = Utc::now();
    let event_data = json!({
        "Created": {
            "id": "test-123",
            "timestamp": timestamp,
        }
    });

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "TestAggregate".to_string(),
        "Test.Created".to_string(),
        EventVersion::new(1),
        event_data,
    );

    // Test TryFrom<EventEnvelope> for owned conversion
    let event: TestEvent = envelope.try_into().unwrap();

    match event {
        TestEvent::Created { id, timestamp: ts } => {
            assert_eq!(id, "test-123");
            assert_eq!(ts, timestamp);
        }
        _ => panic!("Expected Created event"),
    }
}

#[test]
fn test_try_from_event_envelope_reference() {
    let timestamp = Utc::now();
    let event_data = json!({
        "Updated": {
            "value": 42,
            "timestamp": timestamp,
        }
    });

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "TestAggregate".to_string(),
        "Test.Updated".to_string(),
        EventVersion::new(1),
        event_data,
    );

    // Test TryFrom<&EventEnvelope> for reference conversion
    let event: TestEvent = (&envelope).try_into().unwrap();

    match event {
        TestEvent::Updated { value, .. } => {
            assert_eq!(value, 42);
        }
        _ => panic!("Expected Updated event"),
    }

    // Envelope should still be usable after reference conversion
    assert_eq!(envelope.event_type, "Test.Updated");
}

#[test]
fn test_try_from_with_invalid_data() {
    let event_data = json!({
        "invalid": "data",
    });

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "TestAggregate".to_string(),
        "Test.Created".to_string(),
        EventVersion::new(1),
        event_data,
    );

    // Test that TryFrom returns error for invalid data
    let result: Result<TestEvent, _> = envelope.try_into();
    assert!(result.is_err());
}

#[test]
fn test_try_from_in_function_with_question_mark() {
    fn process_envelope(envelope: &EventEnvelope) -> event_sauce_core::Result<String> {
        let event: TestEvent = envelope.try_into()?;
        Ok(match event {
            TestEvent::Created { id, .. } => format!("Created: {id}"),
            TestEvent::Updated { value, .. } => format!("Updated: {value}"),
            TestEvent::Deleted { reason, .. } => format!("Deleted: {reason}"),
        })
    }

    let timestamp = Utc::now();
    let event_data = json!({
        "Created": {
            "id": "test-456",
            "timestamp": timestamp,
        }
    });

    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "TestAggregate".to_string(),
        "Test.Created".to_string(),
        EventVersion::new(1),
        event_data,
    );

    let result = process_envelope(&envelope).unwrap();
    assert_eq!(result, "Created: test-456");
}

#[test]
fn test_try_from_roundtrip() {
    let timestamp = Utc::now();
    let original_event = TestEvent::Deleted {
        reason: "test reason".to_string(),
        timestamp,
    };

    let aggregate_id = Uuid::new_v4();
    let envelope = original_event.to_envelope(aggregate_id).unwrap();

    // Test roundtrip using TryFrom
    let deserialized_event: TestEvent = envelope.try_into().unwrap();

    assert_eq!(original_event, deserialized_event);
}

#[test]
fn test_try_from_works_with_all_variants() {
    let timestamp = Utc::now();
    let aggregate_id = Uuid::new_v4();

    // Test Created variant
    let created = TestEvent::Created {
        id: "test-1".to_string(),
        timestamp,
    };
    let envelope = created.to_envelope(aggregate_id).unwrap();
    let deserialized: TestEvent = envelope.try_into().unwrap();
    assert_eq!(created, deserialized);

    // Test Updated variant
    let updated = TestEvent::Updated {
        value: 99,
        timestamp,
    };
    let envelope = updated.to_envelope(aggregate_id).unwrap();
    let deserialized: TestEvent = envelope.try_into().unwrap();
    assert_eq!(updated, deserialized);

    // Test Deleted variant
    let deleted = TestEvent::Deleted {
        reason: "cleanup".to_string(),
        timestamp,
    };
    let envelope = deleted.to_envelope(aggregate_id).unwrap();
    let deserialized: TestEvent = envelope.try_into().unwrap();
    assert_eq!(deleted, deserialized);
}
