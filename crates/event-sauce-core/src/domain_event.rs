//! Domain event trait.
//!
//! Defines the `DomainEvent` trait which all domain events must implement.

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use uuid::Uuid;

use crate::{Aggregate, Error, EventEnvelope, Result, Version};

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
///     type Aggregate = UserAggregate; // Must specify the aggregate type
///
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
pub trait DomainEvent: Clone + Debug + Send + Sync + Serialize + DeserializeOwned {
    /// The aggregate type that produces this event.
    ///
    /// This is used to automatically determine the aggregate type name
    /// when converting events to envelopes.
    type Aggregate: Aggregate<Event = Self>;

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

    /// Converts this domain event into an `EventEnvelope`.
    ///
    /// This method serializes the event data to JSON and wraps it in an envelope
    /// with metadata needed for persistence and replay. The aggregate type name
    /// is automatically derived from the associated `Aggregate` type.
    ///
    /// # Arguments
    ///
    /// * `aggregate_id` - The ID of the aggregate that produced this event
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::{Aggregate, DomainEvent};
    /// use chrono::Utc;
    /// use serde::{Serialize, Deserialize};
    /// use uuid::Uuid;
    ///
    /// // Aggregate definition
    /// struct User {
    ///     id: UserId,
    ///     // ... other fields
    /// }
    ///
    /// impl Aggregate for User {
    ///     type Event = UserEvent;
    ///     // ... other implementations
    /// }
    ///
    /// #[derive(Debug, Clone, Serialize, Deserialize)]
    /// enum UserEvent {
    ///     Registered { email: String, timestamp: DateTime<Utc> },
    /// }
    ///
    /// impl DomainEvent for UserEvent {
    ///     type Aggregate = User;
    ///
    ///     fn event_type(&self) -> &'static str { "UserRegistered" }
    ///     fn event_version(&self) -> i32 { 1 }
    ///     fn occurred_at(&self) -> chrono::DateTime<Utc> {
    ///         match self {
    ///             UserEvent::Registered { timestamp, .. } => *timestamp,
    ///         }
    ///     }
    /// }
    ///
    /// let event = UserEvent::Registered {
    ///     email: "user@example.com".to_string(),
    ///     timestamp: Utc::now(),
    /// };
    ///
    /// let aggregate_id = Uuid::new_v4();
    /// // Aggregate type is automatically determined from UserEvent::Aggregate
    /// let envelope = event.to_envelope(aggregate_id).unwrap();
    ///
    /// assert_eq!(envelope.aggregate_type, "User");
    /// ```
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
            Version::new(self.event_version()),
            event_data,
        )
        .with_created_at(self.occurred_at()))
    }

    /// Creates a domain event from an `EventEnvelope`.
    ///
    /// This method deserializes the event data from the envelope's JSON payload
    /// back into the concrete event type.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::{DomainEvent, EventEnvelope, Version};
    /// use chrono::Utc;
    /// use serde::{Serialize, Deserialize};
    /// use serde_json::json;
    /// use uuid::Uuid;
    ///
    /// # struct UserAggregate;
    /// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    /// struct UserRegistered {
    ///     email: String,
    ///     timestamp: chrono::DateTime<Utc>,
    /// }
    ///
    /// impl DomainEvent for UserRegistered {
    ///     type Aggregate = UserAggregate;
    ///
    ///     fn event_type(&self) -> &'static str { "UserRegistered" }
    ///     fn event_version(&self) -> i32 { 1 }
    ///     fn occurred_at(&self) -> chrono::DateTime<Utc> { self.timestamp }
    /// }
    ///
    /// let timestamp = Utc::now();
    /// let event_data = json!({
    ///     "email": "user@example.com",
    ///     "timestamp": timestamp,
    /// });
    ///
    /// let envelope = EventEnvelope::new(
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    ///     "User".to_string(),
    ///     "UserRegistered".to_string(),
    ///     Version::new(1),
    ///     event_data,
    /// );
    ///
    /// let event = UserRegistered::from_envelope(&envelope).unwrap();
    /// assert_eq!(event.email, "user@example.com");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The envelope's event data is not valid JSON
    /// - The JSON cannot be deserialized into the expected event type
    /// - Required fields are missing from the event data
    fn from_envelope(envelope: &EventEnvelope) -> Result<Self> {
        serde_json::from_value(envelope.event_data.clone())
            .map_err(|e| Error::custom(format!("Failed to deserialize event: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    // Test aggregate for DomainEvent
    use crate::{AggregateError, DefaultAggregateId};
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test aggregate error")]
    struct TestAggregateError;

    impl AggregateError for TestAggregateError {}

    // Test aggregate implementation
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestAggregateState {
        value: i32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestAggregate {
        id: DefaultAggregateId,
        state: TestAggregateState,
        version: crate::Version,
        pending_events: Vec<TestEvent>,
    }

    impl crate::Aggregate for TestAggregate {
        type Id = DefaultAggregateId;
        type Event = TestEvent;
        type Error = TestAggregateError;
        type State = TestAggregateState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: TestAggregateState { value: 0 },
                version: crate::Version::initial(),
                pending_events: Vec::new(),
            }
        }

        fn aggregate_id(&self) -> &Self::Id {
            &self.id
        }

        fn version(&self) -> crate::Version {
            self.version
        }

        fn pending_events(&self) -> &[Self::Event] {
            &self.pending_events
        }

        fn clear_pending_events(&mut self) {
            self.pending_events.clear();
        }

        fn apply<E: Into<Self::Event>>(
            &mut self,
            event: E,
        ) -> std::result::Result<(), Self::Error> {
            let event = event.into();
            self.apply_internal(&event)
                .expect("apply should not fail in tests");
            self.pending_events.push(event);
            Ok(())
        }

        fn apply_internal(&mut self, event: &Self::Event) -> std::result::Result<(), Self::Error> {
            match event {
                TestEvent::Created { .. } => {
                    // Created doesn't have a value field
                }
                TestEvent::Updated { value, .. } => {
                    self.state.value = *value;
                }
                TestEvent::Deleted { .. } => {
                    self.state.value = 0;
                }
            }
            self.version = self.version.next();
            Ok(())
        }

        fn state(&self) -> &Self::State {
            &self.state
        }

        fn from_snapshot(id: Self::Id, version: crate::Version, state: Self::State) -> Self {
            Self {
                id,
                state,
                version,
                pending_events: Vec::new(),
            }
        }
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

        let debug_str = format!("{event:?}");
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

    // Test with simple event (single variant) - needs its own aggregate
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SimpleEvent {
        timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SimpleAggregateState;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SimpleAggregate {
        id: DefaultAggregateId,
        state: SimpleAggregateState,
        version: crate::Version,
        pending_events: Vec<SimpleEvent>,
    }

    impl crate::Aggregate for SimpleAggregate {
        type Id = DefaultAggregateId;
        type Event = SimpleEvent;
        type Error = TestAggregateError;
        type State = SimpleAggregateState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: SimpleAggregateState,
                version: crate::Version::initial(),
                pending_events: Vec::new(),
            }
        }

        fn aggregate_id(&self) -> &Self::Id {
            &self.id
        }

        fn version(&self) -> crate::Version {
            self.version
        }

        fn pending_events(&self) -> &[Self::Event] {
            &self.pending_events
        }

        fn clear_pending_events(&mut self) {
            self.pending_events.clear();
        }

        fn apply<E: Into<Self::Event>>(
            &mut self,
            event: E,
        ) -> std::result::Result<(), Self::Error> {
            let event = event.into();
            self.apply_internal(&event)
                .expect("apply should not fail in tests");
            self.pending_events.push(event);
            Ok(())
        }

        fn apply_internal(&mut self, _event: &Self::Event) -> std::result::Result<(), Self::Error> {
            self.version = self.version.next();
            Ok(())
        }

        fn state(&self) -> &Self::State {
            &self.state
        }

        fn from_snapshot(id: Self::Id, version: crate::Version, state: Self::State) -> Self {
            Self {
                id,
                state,
                version,
                pending_events: Vec::new(),
            }
        }
    }

    impl DomainEvent for SimpleEvent {
        type Aggregate = SimpleAggregate;

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

    // Test versioned event - needs its own aggregate
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct VersionedEvent {
        version: i32,
        timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct VersionedAggregateState;

    struct VersionedAggregate {
        id: DefaultAggregateId,
        state: VersionedAggregateState,
        version: crate::Version,
        pending_events: Vec<VersionedEvent>,
    }

    impl crate::Aggregate for VersionedAggregate {
        type Id = DefaultAggregateId;
        type Event = VersionedEvent;
        type Error = TestAggregateError;
        type State = VersionedAggregateState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: VersionedAggregateState,
                version: crate::Version::initial(),
                pending_events: Vec::new(),
            }
        }

        fn aggregate_id(&self) -> &Self::Id {
            &self.id
        }

        fn version(&self) -> crate::Version {
            self.version
        }

        fn pending_events(&self) -> &[Self::Event] {
            &self.pending_events
        }

        fn clear_pending_events(&mut self) {
            self.pending_events.clear();
        }

        fn apply<E: Into<Self::Event>>(
            &mut self,
            event: E,
        ) -> std::result::Result<(), Self::Error> {
            let event = event.into();
            self.apply_internal(&event)
                .expect("apply should not fail in tests");
            self.pending_events.push(event);
            Ok(())
        }

        fn apply_internal(&mut self, _event: &Self::Event) -> std::result::Result<(), Self::Error> {
            self.version = self.version.next();
            Ok(())
        }

        fn state(&self) -> &Self::State {
            &self.state
        }

        fn from_snapshot(id: Self::Id, version: crate::Version, state: Self::State) -> Self {
            Self {
                id,
                state,
                version,
                pending_events: Vec::new(),
            }
        }
    }

    impl DomainEvent for VersionedEvent {
        type Aggregate = VersionedAggregate;

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

    // Tests for to_envelope()
    #[test]
    fn test_to_envelope_basic() {
        use uuid::Uuid;

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
        assert_eq!(envelope.event_version, crate::Version::new(1));
        assert_eq!(envelope.created_at, timestamp);
    }

    #[test]
    fn test_to_envelope_with_complex_event() {
        use uuid::Uuid;

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
        // Serde serializes enums with externally tagged format: {"Updated": {...}}
        assert!(envelope.event_data.get("Updated").is_some());
        let updated_data = &envelope.event_data["Updated"];
        assert!(updated_data.get("id").is_some());
        assert!(updated_data.get("value").is_some());
        assert_eq!(updated_data["value"], 42);
    }

    #[test]
    fn test_to_envelope_preserves_occurred_at() {
        use uuid::Uuid;

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
        use uuid::Uuid;

        let timestamp = Utc::now();
        let event = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };

        let envelope = event.to_envelope(Uuid::new_v4()).unwrap();

        // Verify the aggregate type is automatically determined
        assert_eq!(envelope.aggregate_type, TestAggregate::aggregate_type());
        assert_eq!(envelope.aggregate_type, "TestAggregate");
    }

    // Tests for from_envelope()
    #[test]
    fn test_from_envelope_basic() {
        use crate::{EventEnvelope, Version};
        use serde_json::json;
        use uuid::Uuid;

        let timestamp = Utc::now();
        // Serde serializes enums with externally tagged format by default
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
            Version::new(1),
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
        use crate::{EventEnvelope, Version};
        use serde_json::json;
        use uuid::Uuid;

        let timestamp = Utc::now();
        // Serde serializes enums with externally tagged format by default
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
            Version::new(1),
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
        use crate::{EventEnvelope, Version};
        use serde_json::json;
        use uuid::Uuid;

        let event_data = json!({
            "invalid": "data",
        });

        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "TestAggregate".to_string(),
            "TestCreated".to_string(),
            Version::new(1),
            event_data,
        );

        let result = TestEvent::from_envelope(&envelope);
        assert!(result.is_err());
    }

    #[test]
    fn test_roundtrip_serialization() {
        use uuid::Uuid;

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
        use uuid::Uuid;

        let timestamp = Utc::now();
        let aggregate_id = Uuid::new_v4();

        // Test Created
        let created = TestEvent::Created {
            id: "test-1".to_string(),
            timestamp,
        };
        let envelope = created.to_envelope(aggregate_id).unwrap();
        let deserialized = TestEvent::from_envelope(&envelope).unwrap();
        assert_eq!(created, deserialized);

        // Test Updated
        let updated = TestEvent::Updated {
            id: "test-1".to_string(),
            value: 42,
            timestamp,
        };
        let envelope = updated.to_envelope(aggregate_id).unwrap();
        let deserialized = TestEvent::from_envelope(&envelope).unwrap();
        assert_eq!(updated, deserialized);

        // Test Deleted
        let deleted = TestEvent::Deleted {
            id: "test-1".to_string(),
            timestamp,
        };
        let envelope = deleted.to_envelope(aggregate_id).unwrap();
        let deserialized = TestEvent::from_envelope(&envelope).unwrap();
        assert_eq!(deleted, deserialized);
    }

    // Tests for try_into_event/into_event methods
    #[test]
    fn test_try_into_event_reference() {
        use crate::{EventEnvelope, Version};
        use serde_json::json;
        use uuid::Uuid;

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
            Version::new(1),
            event_data,
        );

        // Test try_into_event with reference
        let event: TestEvent = envelope.try_into_event().unwrap();

        match event {
            TestEvent::Created { id, timestamp: ts } => {
                assert_eq!(id, "test-1");
                assert_eq!(ts, timestamp);
            }
            _ => panic!("Expected Created event"),
        }
    }

    #[test]
    fn test_into_event_owned() {
        use crate::{EventEnvelope, Version};
        use serde_json::json;
        use uuid::Uuid;

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
            Version::new(1),
            event_data,
        );

        // Test into_event with owned value
        let event: TestEvent = envelope.into_event().unwrap();

        match event {
            TestEvent::Updated { id, value, .. } => {
                assert_eq!(id, "test-1");
                assert_eq!(value, 42);
            }
            _ => panic!("Expected Updated event"),
        }
    }

    #[test]
    fn test_try_into_event_with_invalid_data() {
        use crate::{EventEnvelope, Version};
        use serde_json::json;
        use uuid::Uuid;

        let event_data = json!({
            "invalid": "data",
        });

        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "TestAggregate".to_string(),
            "TestCreated".to_string(),
            Version::new(1),
            event_data,
        );

        // Test try_into_event with invalid data
        let result: Result<TestEvent> = envelope.try_into_event();
        assert!(result.is_err());
    }

    #[test]
    fn test_try_into_event_with_question_mark() {
        use crate::{EventEnvelope, Version};
        use serde_json::json;
        use uuid::Uuid;

        fn process_envelope(envelope: &EventEnvelope) -> Result<String> {
            let event: TestEvent = envelope.try_into_event()?;
            Ok(match event {
                TestEvent::Created { id, .. } => format!("Created: {id}"),
                TestEvent::Updated { id, .. } => format!("Updated: {id}"),
                TestEvent::Deleted { id, .. } => format!("Deleted: {id}"),
            })
        }

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
            "TestCreated".to_string(),
            Version::new(1),
            event_data,
        );

        let result = process_envelope(&envelope).unwrap();
        assert_eq!(result, "Created: test-123");
    }
}
