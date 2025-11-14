//! Domain event trait.
//!
//! Defines the `DomainEvent` trait which all domain events must implement.

use chrono::{DateTime, Utc};
use std::fmt::Debug;

/// Trait for domain events.
///
/// All domain events must implement this trait. A domain event represents
/// something that happened in the domain that is of interest to the business.
///
/// # Requirements
///
/// - Must be `Clone`, `Debug`, and `Send + Sync` for async usage
/// - Must provide an event type name (used for deserialization)
/// - Must provide an event version (for schema evolution)
/// - Must provide a timestamp of when the event occurred
///
/// # Examples
///
/// ```
/// use event_sauce_core::DomainEvent;
/// use chrono::{DateTime, Utc};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum UserEvent {
///     Registered {
///         email: String,
///         timestamp: DateTime<Utc>,
///     },
///     EmailChanged {
///         new_email: String,
///         timestamp: DateTime<Utc>,
///     },
/// }
///
/// impl DomainEvent for UserEvent {
///     fn event_type(&self) -> &'static str {
///         match self {
///             UserEvent::Registered { .. } => "UserRegistered",
///             UserEvent::EmailChanged { .. } => "UserEmailChanged",
///         }
///     }
///
///     fn event_version(&self) -> i32 {
///         1 // Schema version
///     }
///
///     fn occurred_at(&self) -> DateTime<Utc> {
///         match self {
///             UserEvent::Registered { timestamp, .. } => *timestamp,
///             UserEvent::EmailChanged { timestamp, .. } => *timestamp,
///         }
///     }
/// }
/// ```
pub trait DomainEvent: Clone + Debug + Send + Sync {
    /// Returns the event type name.
    ///
    /// This is used for serialization/deserialization and event routing.
    /// Should be a stable identifier (e.g., `UserRegistered`, `OrderPlaced`).
    fn event_type(&self) -> &'static str;

    /// Returns the event schema version.
    ///
    /// Used for event versioning and schema evolution.
    /// Start at 1 and increment when the event structure changes.
    fn event_version(&self) -> i32;

    /// Returns when this event occurred.
    ///
    /// This should be the business timestamp, not when it was persisted.
    fn occurred_at(&self) -> DateTime<Utc>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    // Test event implementation
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestEvent {
        Created {
            id: String,
            timestamp: DateTime<Utc>,
        },
        Updated {
            id: String,
            value: i32,
            timestamp: DateTime<Utc>,
        },
        Deleted {
            id: String,
            timestamp: DateTime<Utc>,
        },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Created { .. } => "TestCreated",
                TestEvent::Updated { .. } => "TestUpdated",
                TestEvent::Deleted { .. } => "TestDeleted",
            }
        }

        fn event_version(&self) -> i32 {
            1
        }

        fn occurred_at(&self) -> DateTime<Utc> {
            match self {
                TestEvent::Created { timestamp, .. } => *timestamp,
                TestEvent::Updated { timestamp, .. } => *timestamp,
                TestEvent::Deleted { timestamp, .. } => *timestamp,
            }
        }
    }

    #[test]
    fn test_event_type() {
        let timestamp = Utc::now();
        let created = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };
        let updated = TestEvent::Updated {
            id: "test-1".to_string(),
            value: 42,
            timestamp,
        };
        let deleted = TestEvent::Deleted {
            id: "test-1".to_string(),
            timestamp,
        };

        assert_eq!(created.event_type(), "TestCreated");
        assert_eq!(updated.event_type(), "TestUpdated");
        assert_eq!(deleted.event_type(), "TestDeleted");
    }

    #[test]
    fn test_event_version() {
        let timestamp = Utc::now();
        let event = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };

        assert_eq!(event.event_version(), 1);
    }

    #[test]
    fn test_occurred_at() {
        let timestamp = Utc::now();
        let event = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };

        assert_eq!(event.occurred_at(), timestamp);
    }

    #[test]
    fn test_event_is_cloneable() {
        let timestamp = Utc::now();
        let event1 = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };
        let event2 = event1.clone();

        assert_eq!(event1, event2);
    }

    #[test]
    fn test_event_is_debuggable() {
        let timestamp = Utc::now();
        let event = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Created"));
        assert!(debug_str.contains("test-1"));
    }

    #[test]
    fn test_event_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<TestEvent>();
        assert_sync::<TestEvent>();
    }

    #[test]
    fn test_different_events_have_different_timestamps() {
        let time1 = Utc::now();
        let event1 = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp: time1,
        };

        // Ensure some time passes
        std::thread::sleep(std::time::Duration::from_millis(1));

        let time2 = Utc::now();
        let event2 = TestEvent::Updated {
            id: "test-1".to_string(),
            value: 42,
            timestamp: time2,
        };

        assert_ne!(event1.occurred_at(), event2.occurred_at());
        assert!(event2.occurred_at() > event1.occurred_at());
    }

    // Test with simple event (single variant)
    #[derive(Debug, Clone, PartialEq)]
    struct SimpleEvent {
        timestamp: DateTime<Utc>,
    }

    impl DomainEvent for SimpleEvent {
        fn event_type(&self) -> &'static str {
            "SimpleEvent"
        }

        fn event_version(&self) -> i32 {
            1
        }

        fn occurred_at(&self) -> DateTime<Utc> {
            self.timestamp
        }
    }

    #[test]
    fn test_simple_event() {
        let timestamp = Utc::now();
        let event = SimpleEvent { timestamp };

        assert_eq!(event.event_type(), "SimpleEvent");
        assert_eq!(event.event_version(), 1);
        assert_eq!(event.occurred_at(), timestamp);
    }

    // Test versioned event
    #[derive(Debug, Clone)]
    struct VersionedEvent {
        version: i32,
        timestamp: DateTime<Utc>,
    }

    impl DomainEvent for VersionedEvent {
        fn event_type(&self) -> &'static str {
            "VersionedEvent"
        }

        fn event_version(&self) -> i32 {
            self.version
        }

        fn occurred_at(&self) -> DateTime<Utc> {
            self.timestamp
        }
    }

    #[test]
    fn test_versioned_event() {
        let timestamp = Utc::now();
        let v1 = VersionedEvent {
            version: 1,
            timestamp,
        };
        let v2 = VersionedEvent {
            version: 2,
            timestamp,
        };

        assert_eq!(v1.event_version(), 1);
        assert_eq!(v2.event_version(), 2);
    }
}
