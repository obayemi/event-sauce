//! Event envelope and metadata types.
//!
//! Provides `EventEnvelope` and `EventMetadata` for wrapping domain events
//! with additional context and tracking information.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::EventVersion;

/// Optional metadata associated with an event.
///
/// `EventMetadata` is an **extension point** for attaching cross-cutting concerns
/// to events without modifying the domain event types themselves. It is stored
/// alongside the event in the [`EventEnvelope`] and is entirely optional — the
/// core event sourcing flow works without it.
///
/// # Built-in Fields
///
/// - **`correlation_id`** — Links related events across services for distributed tracing.
/// - **`causation_id`** — Identifies the command or event that caused this event.
/// - **`timestamp`** — When the metadata was created (set automatically).
/// - **`additional`** — Arbitrary JSON for application-specific data (e.g., user ID,
///   IP address, tenant ID).
///
/// # Examples
///
/// ```
/// use event_sauce_core::EventMetadata;
/// use uuid::Uuid;
///
/// let metadata = EventMetadata::new()
///     .with_correlation_id(Uuid::new_v4())
///     .with_causation_id(Uuid::new_v4());
///
/// assert!(metadata.correlation_id.is_some());
/// assert!(metadata.causation_id.is_some());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Correlation ID for tracing related events across the system.
    pub correlation_id: Option<Uuid>,

    /// Causation ID - the event or command that caused this event.
    pub causation_id: Option<Uuid>,

    /// When this metadata was created.
    pub timestamp: DateTime<Utc>,

    /// Additional metadata as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional: Option<serde_json::Value>,
}

impl EventMetadata {
    /// Creates new metadata with current timestamp.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    ///
    /// let metadata = EventMetadata::new();
    /// assert!(metadata.correlation_id.is_none());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            correlation_id: None,
            causation_id: None,
            timestamp: Utc::now(),
            additional: None,
        }
    }

    /// Sets the correlation ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    /// use uuid::Uuid;
    ///
    /// let correlation_id = Uuid::new_v4();
    /// let metadata = EventMetadata::new()
    ///     .with_correlation_id(correlation_id);
    ///
    /// assert_eq!(metadata.correlation_id, Some(correlation_id));
    /// ```
    #[must_use]
    pub fn with_correlation_id(mut self, id: Uuid) -> Self {
        self.correlation_id = Some(id);
        self
    }

    /// Sets the causation ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    /// use uuid::Uuid;
    ///
    /// let causation_id = Uuid::new_v4();
    /// let metadata = EventMetadata::new()
    ///     .with_causation_id(causation_id);
    ///
    /// assert_eq!(metadata.causation_id, Some(causation_id));
    /// ```
    #[must_use]
    pub fn with_causation_id(mut self, id: Uuid) -> Self {
        self.causation_id = Some(id);
        self
    }

    /// Sets additional metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    /// use serde_json::json;
    ///
    /// let metadata = EventMetadata::new()
    ///     .with_additional(json!({"user_id": "123", "ip": "192.168.1.1"}));
    ///
    /// assert!(metadata.additional.is_some());
    /// ```
    #[must_use]
    pub fn with_additional(mut self, additional: serde_json::Value) -> Self {
        self.additional = Some(additional);
        self
    }
}

impl Default for EventMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Event envelope wrapping a serialized event with metadata.
///
/// The envelope contains all information needed to store and replay events:
/// - Event ID (unique identifier)
/// - Aggregate information (ID, type)
/// - Event information (type, version, data)
/// - Metadata (who, when, why)
///
/// # Examples
///
/// ```
/// use event_sauce_core::{EventEnvelope, EventMetadata, EventVersion};
/// use uuid::Uuid;
/// use serde_json::json;
/// use chrono::Utc;
///
/// let envelope = EventEnvelope::new(
///     Uuid::new_v4(),
///     Uuid::new_v4(),
///     "User".to_string(),
///     "User.Registered".to_string(),
///     EventVersion::new(1),
///     json!({"email": "user@example.com"}),
/// );
///
/// assert_eq!(envelope.aggregate_type, "User");
/// assert_eq!(envelope.event_type, "User.Registered");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Unique identifier for this event.
    pub id: Uuid,

    /// Aggregate instance identifier.
    pub aggregate_id: Uuid,

    /// Aggregate type name.
    pub aggregate_type: String,

    /// Event type name.
    pub event_type: String,

    /// Event schema version.
    pub event_version: EventVersion,

    /// Serialized event data.
    pub event_data: serde_json::Value,

    /// Who created this event.
    pub created_by: Option<Uuid>,

    /// Optional metadata for cross-cutting concerns (tracing, audit, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<EventMetadata>,

    /// When this event was created.
    pub created_at: DateTime<Utc>,
}

impl EventEnvelope {
    /// Creates a new event envelope.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{EventEnvelope, EventVersion};
    /// use uuid::Uuid;
    /// use serde_json::json;
    ///
    /// let envelope = EventEnvelope::new(
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    ///     "Order".to_string(),
    ///     "Order.Placed".to_string(),
    ///     EventVersion::new(1),
    ///     json!({"total": 99.99}),
    /// );
    /// ```
    #[must_use]
    pub fn new(
        id: Uuid,
        aggregate_id: Uuid,
        aggregate_type: String,
        event_type: String,
        event_version: EventVersion,
        event_data: serde_json::Value,
    ) -> Self {
        Self {
            id,
            aggregate_id,
            aggregate_type,
            event_type,
            event_version,
            event_data,
            created_by: None,
            metadata: None,
            created_at: Utc::now(),
        }
    }

    /// Sets who created this event.
    #[must_use]
    pub fn with_created_by(mut self, user_id: Uuid) -> Self {
        self.created_by = Some(user_id);
        self
    }

    /// Sets the metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: EventMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Sets the creation timestamp.
    #[must_use]
    pub fn with_created_at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.created_at = timestamp;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_metadata_new() {
        let metadata = EventMetadata::new();

        assert!(metadata.correlation_id.is_none());
        assert!(metadata.causation_id.is_none());
        assert!(metadata.additional.is_none());
    }

    #[test]
    fn test_metadata_with_correlation_id() {
        let correlation_id = Uuid::new_v4();
        let metadata = EventMetadata::new().with_correlation_id(correlation_id);

        assert_eq!(metadata.correlation_id, Some(correlation_id));
    }

    #[test]
    fn test_metadata_with_causation_id() {
        let causation_id = Uuid::new_v4();
        let metadata = EventMetadata::new().with_causation_id(causation_id);

        assert_eq!(metadata.causation_id, Some(causation_id));
    }

    #[test]
    fn test_metadata_with_additional() {
        let additional = json!({"key": "value"});
        let metadata = EventMetadata::new().with_additional(additional.clone());

        assert_eq!(metadata.additional, Some(additional));
    }

    #[test]
    fn test_metadata_builder_chain() {
        let correlation_id = Uuid::new_v4();
        let causation_id = Uuid::new_v4();
        let additional = json!({"test": true});

        let metadata = EventMetadata::new()
            .with_correlation_id(correlation_id)
            .with_causation_id(causation_id)
            .with_additional(additional.clone());

        assert_eq!(metadata.correlation_id, Some(correlation_id));
        assert_eq!(metadata.causation_id, Some(causation_id));
        assert_eq!(metadata.additional, Some(additional));
    }

    #[test]
    fn test_metadata_default() {
        let metadata = EventMetadata::default();

        assert!(metadata.correlation_id.is_none());
        assert!(metadata.causation_id.is_none());
    }

    #[test]
    fn test_metadata_serialization() {
        let metadata = EventMetadata::new()
            .with_correlation_id(Uuid::nil())
            .with_causation_id(Uuid::nil());

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: EventMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(metadata.correlation_id, deserialized.correlation_id);
        assert_eq!(metadata.causation_id, deserialized.causation_id);
    }

    #[test]
    fn test_event_envelope_new() {
        let id = Uuid::new_v4();
        let aggregate_id = Uuid::new_v4();
        let event_data = json!({"value": 42});

        let envelope = EventEnvelope::new(
            id,
            aggregate_id,
            "TestAggregate".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            event_data.clone(),
        );

        assert_eq!(envelope.id, id);
        assert_eq!(envelope.aggregate_id, aggregate_id);
        assert_eq!(envelope.aggregate_type, "TestAggregate");
        assert_eq!(envelope.event_type, "TestEvent");
        assert_eq!(envelope.event_version, EventVersion::new(1));
        assert_eq!(envelope.event_data, event_data);
        assert!(envelope.created_by.is_none());
        assert!(envelope.metadata.is_none());
    }

    #[test]
    fn test_event_envelope_with_created_by() {
        let user_id = Uuid::new_v4();
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "Test".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            json!({}),
        )
        .with_created_by(user_id);

        assert_eq!(envelope.created_by, Some(user_id));
    }

    #[test]
    fn test_event_envelope_with_metadata() {
        let metadata = EventMetadata::new().with_correlation_id(Uuid::new_v4());

        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "Test".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            json!({}),
        )
        .with_metadata(metadata.clone());

        assert_eq!(envelope.metadata, Some(metadata));
    }

    #[test]
    fn test_event_envelope_with_created_at() {
        let timestamp = Utc::now();
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "Test".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            json!({}),
        )
        .with_created_at(timestamp);

        assert_eq!(envelope.created_at, timestamp);
    }

    #[test]
    fn test_event_envelope_builder_chain() {
        let user_id = Uuid::new_v4();
        let metadata = EventMetadata::new();
        let timestamp = Utc::now();

        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "Test".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            json!({}),
        )
        .with_created_by(user_id)
        .with_metadata(metadata.clone())
        .with_created_at(timestamp);

        assert_eq!(envelope.created_by, Some(user_id));
        assert_eq!(envelope.metadata, Some(metadata));
        assert_eq!(envelope.created_at, timestamp);
    }

    #[test]
    fn test_event_envelope_serialization() {
        let envelope = EventEnvelope::new(
            Uuid::nil(),
            Uuid::nil(),
            "Test".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            json!({"key": "value"}),
        );

        let json = serde_json::to_string(&envelope).unwrap();
        let deserialized: EventEnvelope = serde_json::from_str(&json).unwrap();

        assert_eq!(envelope.id, deserialized.id);
        assert_eq!(envelope.aggregate_id, deserialized.aggregate_id);
        assert_eq!(envelope.event_type, deserialized.event_type);
    }

    #[test]
    fn test_event_envelope_with_complex_data() {
        let complex_data = json!({
            "user": {
                "id": "123",
                "email": "test@example.com",
                "roles": ["admin", "user"]
            },
            "metadata": {
                "timestamp": "2024-01-01T00:00:00Z",
                "version": 2
            }
        });

        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "User".to_string(),
            "UserUpdated".to_string(),
            EventVersion::new(2),
            complex_data.clone(),
        );

        assert_eq!(envelope.event_data, complex_data);
    }

    #[test]
    fn test_version_in_envelope() {
        let v1_envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "Test".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            json!({}),
        );

        let v2_envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "Test".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(2),
            json!({}),
        );

        assert_eq!(v1_envelope.event_version, EventVersion::new(1));
        assert_eq!(v2_envelope.event_version, EventVersion::new(2));
        assert!(v2_envelope.event_version > v1_envelope.event_version);
    }
}
