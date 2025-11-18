//! Integration tests for #[derive(Event)] macro.
//!
//! These tests verify that the Event derive macro correctly generates
//! the DomainEvent trait implementation.

use chrono::{DateTime, Utc};
use event_sauce_core::DomainEvent;

// Test: Simple enum with timestamp field
#[derive(event_sauce_macros::Event, Debug, Clone)]
#[event(version = 1)]
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

#[test]
fn test_event_derive_implements_domain_event() {
    let timestamp = Utc::now();
    let event = TestEvent::Created {
        id: "test-1".to_string(),
        timestamp,
    };

    // The DomainEvent trait should be implemented
    assert_eq!(event.event_type(), "TestCreated");
    assert_eq!(event.event_version(), 1);
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

    assert_eq!(created.event_type(), "TestCreated");
    assert_eq!(updated.event_type(), "TestUpdated");
    assert_eq!(deleted.event_type(), "TestDeleted");
}

#[test]
fn test_event_derive_event_version() {
    let timestamp = Utc::now();
    let event = TestEvent::Created {
        id: "test-1".to_string(),
        timestamp,
    };

    assert_eq!(event.event_version(), 1);
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

// Test: Versioned event (version 2)
#[derive(event_sauce_macros::Event, Debug, Clone)]
#[event(version = 2)]
#[allow(dead_code)]
enum TestEventV2 {
    CreatedV2 {
        id: String,
        name: String,
        timestamp: DateTime<Utc>,
    },
}

#[test]
fn test_event_derive_different_version() {
    let timestamp = Utc::now();
    let event = TestEventV2::CreatedV2 {
        id: "test-1".to_string(),
        name: "Test".to_string(),
        timestamp,
    };

    assert_eq!(event.event_version(), 2);
}

// Test: Event with custom type name prefix
#[derive(event_sauce_macros::Event, Debug, Clone)]
#[event(version = 1, type_prefix = "Order")]
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

    // With custom prefix "Order", variant "Placed" becomes "OrderPlaced"
    assert_eq!(placed.event_type(), "OrderPlaced");
    assert_eq!(shipped.event_type(), "OrderShipped");
}

// Test: Single variant event
#[derive(event_sauce_macros::Event, Debug, Clone)]
#[event(version = 1)]
enum SimpleEvent {
    Occurred { timestamp: DateTime<Utc> },
}

#[test]
fn test_event_derive_single_variant() {
    let timestamp = Utc::now();
    let event = SimpleEvent::Occurred { timestamp };

    assert_eq!(event.event_type(), "SimpleOccurred");
    assert_eq!(event.event_version(), 1);
    assert_eq!(event.occurred_at(), timestamp);
}
