//! Domain event trait.
//!
//! Defines the `DomainEvent` trait which all domain events must implement.

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use uuid::Uuid;

use crate::{Aggregate, Error, EventEnvelope, EventVersion, Result};

/// Trait for domain events.
///
/// All domain events must implement this trait. A domain event represents
/// something that happened in the domain that is of interest to the business.
///
/// # Requirements
///
/// - Must be `Clone`, `Debug`, and `Send + Sync` for async usage
/// - Must be `Serialize` and `Deserialize` for event store persistence
/// - Must specify the aggregate type that produces this event
/// - Must provide an event type name (used for deserialization)
/// - Must provide an event version (for schema evolution)
/// - Must provide a timestamp of when the event occurred
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::DomainEvent;
/// use chrono::{DateTime, Utc};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum UserEvent {
///     Registered { email: String, timestamp: DateTime<Utc> },
///     EmailChanged { new_email: String, timestamp: DateTime<Utc> },
/// }
///
/// impl DomainEvent for UserEvent {
///     type Aggregate = User;
///
///     fn event_type(&self) -> &'static str {
///         match self {
///             UserEvent::Registered { .. } => "User.Registered",
///             UserEvent::EmailChanged { .. } => "User.EmailChanged",
///         }
///     }
///
///     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
///
///     fn occurred_at(&self) -> DateTime<Utc> {
///         match self {
///             UserEvent::Registered { timestamp, .. } => *timestamp,
///             UserEvent::EmailChanged { timestamp, .. } => *timestamp,
///         }
///     }
/// }
/// ```
pub trait DomainEvent: Clone + Debug + Send + Sync + Serialize + DeserializeOwned {
    /// The aggregate type that produces this event.
    type Aggregate: Aggregate<Event = Self>;

    /// Returns the event type name.
    fn event_type(&self) -> &'static str;

    /// Returns the event schema version.
    fn event_version(&self) -> EventVersion;

    /// Returns when this event occurred.
    fn occurred_at(&self) -> DateTime<Utc>;

    /// Converts this domain event into an `EventEnvelope`.
    ///
    /// Uses `EntityId.as_uuid()` for the aggregate ID and
    /// `Aggregate::aggregate_type()` for the aggregate type name.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be serialized to JSON.
    fn to_envelope(&self, aggregate_id: Uuid) -> Result<EventEnvelope> {
        let event_data = serde_json::to_value(self)
            .map_err(|e| Error::custom(format!("Failed to serialize event: {e}")))?;

        Ok(EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            Self::Aggregate::aggregate_type().to_string(),
            self.event_type().to_string(),
            self.event_version(),
            event_data,
        )
        .with_created_at(self.occurred_at()))
    }

    /// Creates a domain event from an `EventEnvelope`.
    ///
    /// Deserializes the event data from the envelope's JSON payload.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn from_envelope(envelope: &EventEnvelope) -> Result<Self> {
        serde_json::from_value(envelope.event_data.clone())
            .map_err(|e| Error::custom(format!("Failed to deserialize event: {e}")))
    }
}

/// Trait for providing compile-time event type strings.
///
/// This trait provides a const string that identifies the event type,
/// eliminating the need to specify string literals when working with
/// event handlers and projections.
///
/// # Examples
///
/// ```
/// use event_sauce_core::EventType;
///
/// #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// pub struct UserRegisteredEvent {
///     pub email: String,
///     pub timestamp: chrono::DateTime<chrono::Utc>,
/// }
///
/// impl EventType for UserRegisteredEvent {
///     const EVENT_TYPE: &'static str = "User.Registered";
/// }
/// ```
pub trait EventType {
    /// The event type identifier string.
    const EVENT_TYPE: &'static str;
}

#[cfg(test)]
#[allow(clippy::match_same_arms)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    use crate::{AggregateError, EntityId};
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test aggregate error")]
    struct TestAggregateError;

    impl AggregateError for TestAggregateError {}

    // Test aggregate
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestAggregate {
        id: EntityId,
        value: i32,
    }

    impl crate::Entity for TestAggregate {
        fn new(id: EntityId) -> Self {
            Self { id, value: 0 }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for TestAggregate {}

    impl crate::EventApplicator<TestAggregate> for TestEvent {
        fn dispatch(
            &self,
            aggregate: &mut TestAggregate,
        ) -> std::result::Result<(), TestAggregateError> {
            match self {
                TestEvent::Created { .. } => {}
                TestEvent::Updated { value, .. } => {
                    aggregate.value = *value;
                }
                TestEvent::Deleted { .. } => {
                    aggregate.value = 0;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, aggregate: &mut TestAggregate) {
            match self {
                TestEvent::Created { .. } => {}
                TestEvent::Updated { value, .. } => {
                    aggregate.value = *value;
                }
                TestEvent::Deleted { .. } => {
                    aggregate.value = 0;
                }
            }
        }
    }

    impl crate::Aggregate for TestAggregate {
        type Event = TestEvent;
        type Error = TestAggregateError;
    }

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
        type Aggregate = TestAggregate;

        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Created { .. } => "TestCreated",
                TestEvent::Updated { .. } => "TestUpdated",
                TestEvent::Deleted { .. } => "TestDeleted",
            }
        }

        fn event_version(&self) -> EventVersion {
            EventVersion::new(1)
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

        assert_eq!(event.event_version(), EventVersion::new(1));
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

    // Test with simple event
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SimpleEvent {
        timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SimpleEntity {
        id: EntityId,
    }

    impl crate::Entity for SimpleEntity {
        fn new(id: EntityId) -> Self {
            Self { id }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for SimpleEntity {}

    impl crate::EventApplicator<SimpleEntity> for SimpleEvent {
        fn dispatch(
            &self,
            _entity: &mut SimpleEntity,
        ) -> std::result::Result<(), TestAggregateError> {
            Ok(())
        }
        fn dispatch_unchecked(&self, _entity: &mut SimpleEntity) {}
    }

    impl crate::Aggregate for SimpleEntity {
        type Event = SimpleEvent;
        type Error = TestAggregateError;
    }

    impl DomainEvent for SimpleEvent {
        type Aggregate = SimpleEntity;

        fn event_type(&self) -> &'static str {
            "SimpleEvent"
        }

        fn event_version(&self) -> EventVersion {
            EventVersion::new(1)
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
        assert_eq!(event.event_version(), EventVersion::new(1));
        assert_eq!(event.occurred_at(), timestamp);
    }

    // Test versioned event
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct VersionedEvent {
        version: u64,
        timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct VersionedEntity {
        id: EntityId,
    }

    impl crate::Entity for VersionedEntity {
        fn new(id: EntityId) -> Self {
            Self { id }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for VersionedEntity {}

    impl crate::EventApplicator<VersionedEntity> for VersionedEvent {
        fn dispatch(
            &self,
            _entity: &mut VersionedEntity,
        ) -> std::result::Result<(), TestAggregateError> {
            Ok(())
        }
        fn dispatch_unchecked(&self, _entity: &mut VersionedEntity) {}
    }

    impl crate::Aggregate for VersionedEntity {
        type Event = VersionedEvent;
        type Error = TestAggregateError;
    }

    impl DomainEvent for VersionedEvent {
        type Aggregate = VersionedEntity;

        fn event_type(&self) -> &'static str {
            "VersionedEvent"
        }

        fn event_version(&self) -> EventVersion {
            EventVersion::new(self.version)
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

        assert_eq!(v1.event_version(), EventVersion::new(1));
        assert_eq!(v2.event_version(), EventVersion::new(2));
    }

    // Tests for to_envelope()
    #[test]
    fn test_to_envelope_basic() {
        let timestamp = Utc::now();
        let event = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };

        let aggregate_id = Uuid::new_v4();
        let envelope = event.to_envelope(aggregate_id).unwrap();

        assert_eq!(envelope.aggregate_id, aggregate_id);
        assert_eq!(envelope.aggregate_type, "TestAggregate");
        assert_eq!(envelope.event_type, "TestCreated");
        assert_eq!(envelope.event_version, crate::EventVersion::new(1));
        assert_eq!(envelope.created_at, timestamp);
    }

    #[test]
    fn test_to_envelope_with_complex_event() {
        let timestamp = Utc::now();
        let event = TestEvent::Updated {
            id: "test-1".to_string(),
            value: 42,
            timestamp,
        };

        let aggregate_id = Uuid::new_v4();
        let envelope = event.to_envelope(aggregate_id).unwrap();

        assert_eq!(envelope.event_type, "TestUpdated");
        assert_eq!(envelope.aggregate_type, "TestAggregate");
        assert!(envelope.event_data.get("Updated").is_some());
        let updated_data = &envelope.event_data["Updated"];
        assert!(updated_data.get("id").is_some());
        assert!(updated_data.get("value").is_some());
        assert_eq!(updated_data["value"], 42);
    }

    #[test]
    fn test_to_envelope_preserves_occurred_at() {
        let timestamp = Utc::now();
        let event = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };

        let envelope = event.to_envelope(Uuid::new_v4()).unwrap();

        assert_eq!(envelope.created_at, event.occurred_at());
    }

    #[test]
    fn test_to_envelope_uses_aggregate_type() {
        let timestamp = Utc::now();
        let event = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };

        let envelope = event.to_envelope(Uuid::new_v4()).unwrap();

        assert_eq!(envelope.aggregate_type, TestAggregate::aggregate_type());
        assert_eq!(envelope.aggregate_type, "TestAggregate");
    }

    // Tests for from_envelope()
    #[test]
    fn test_from_envelope_basic() {
        use crate::{EventEnvelope, EventVersion};
        use serde_json::json;

        let timestamp = Utc::now();
        let event_data = json!({
            "Created": {
                "id": "test-1",
                "timestamp": timestamp,
            }
        });

        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "TestAggregate".to_string(),
            "TestCreated".to_string(),
            EventVersion::new(1),
            event_data,
        );

        let event = TestEvent::from_envelope(&envelope).unwrap();

        match event {
            TestEvent::Created { id, timestamp: ts } => {
                assert_eq!(id, "test-1");
                assert_eq!(ts, timestamp);
            }
            _ => panic!("Expected Created event"),
        }
    }

    #[test]
    fn test_from_envelope_with_complex_event() {
        use crate::{EventEnvelope, EventVersion};
        use serde_json::json;

        let timestamp = Utc::now();
        let event_data = json!({
            "Updated": {
                "id": "test-1",
                "value": 42,
                "timestamp": timestamp,
            }
        });

        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "TestAggregate".to_string(),
            "TestUpdated".to_string(),
            EventVersion::new(1),
            event_data,
        );

        let event = TestEvent::from_envelope(&envelope).unwrap();

        match event {
            TestEvent::Updated { id, value, .. } => {
                assert_eq!(id, "test-1");
                assert_eq!(value, 42);
            }
            _ => panic!("Expected Updated event"),
        }
    }

    #[test]
    fn test_from_envelope_with_invalid_data() {
        use crate::{EventEnvelope, EventVersion};
        use serde_json::json;

        let event_data = json!({
            "invalid": "data",
        });

        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "TestAggregate".to_string(),
            "TestCreated".to_string(),
            EventVersion::new(1),
            event_data,
        );

        let result = TestEvent::from_envelope(&envelope);
        assert!(result.is_err());
    }

    #[test]
    fn test_roundtrip_serialization() {
        let timestamp = Utc::now();
        let original_event = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };

        let aggregate_id = Uuid::new_v4();
        let envelope = original_event.to_envelope(aggregate_id).unwrap();
        let deserialized_event = TestEvent::from_envelope(&envelope).unwrap();

        assert_eq!(original_event, deserialized_event);
    }

    #[test]
    fn test_roundtrip_with_all_variants() {
        let timestamp = Utc::now();
        let aggregate_id = Uuid::new_v4();

        let created = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };
        let envelope = created.to_envelope(aggregate_id).unwrap();
        let deserialized = TestEvent::from_envelope(&envelope).unwrap();
        assert_eq!(created, deserialized);

        let updated = TestEvent::Updated {
            id: "test-1".to_string(),
            value: 42,
            timestamp,
        };
        let envelope = updated.to_envelope(aggregate_id).unwrap();
        let deserialized = TestEvent::from_envelope(&envelope).unwrap();
        assert_eq!(updated, deserialized);

        let deleted = TestEvent::Deleted {
            id: "test-1".to_string(),
            timestamp,
        };
        let envelope = deleted.to_envelope(aggregate_id).unwrap();
        let deserialized = TestEvent::from_envelope(&envelope).unwrap();
        assert_eq!(deleted, deserialized);
    }

    // Tests for EventType trait
    use crate::EventType;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEventWithType {
        timestamp: DateTime<Utc>,
    }

    impl EventType for TestEventWithType {
        const EVENT_TYPE: &'static str = "Test.EventWithType";
    }

    #[test]
    fn test_event_type_const() {
        assert_eq!(TestEventWithType::EVENT_TYPE, "Test.EventWithType");
    }

    #[test]
    fn test_event_type_is_static() {
        const EVENT_TYPE: &str = TestEventWithType::EVENT_TYPE;
        assert_eq!(EVENT_TYPE, "Test.EventWithType");
    }

    #[test]
    fn test_event_type_accessible_without_instance() {
        let event_type = TestEventWithType::EVENT_TYPE;
        assert!(!event_type.is_empty());
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FirstEvent {
        value: i32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SecondEvent {
        value: String,
    }

    impl EventType for FirstEvent {
        const EVENT_TYPE: &'static str = "Domain.FirstEvent";
    }

    impl EventType for SecondEvent {
        const EVENT_TYPE: &'static str = "Domain.SecondEvent";
    }

    #[test]
    fn test_multiple_event_types_have_different_names() {
        assert_ne!(FirstEvent::EVENT_TYPE, SecondEvent::EVENT_TYPE);
        assert_eq!(FirstEvent::EVENT_TYPE, "Domain.FirstEvent");
        assert_eq!(SecondEvent::EVENT_TYPE, "Domain.SecondEvent");
    }

    #[test]
    fn test_event_type_follows_naming_convention() {
        assert!(TestEventWithType::EVENT_TYPE.contains('.'));

        let parts: Vec<&str> = TestEventWithType::EVENT_TYPE.split('.').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "Test");
        assert_eq!(parts[1], "EventWithType");
    }
}
