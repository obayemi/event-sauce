//! Event envelope and metadata types.
//!
//! Provides `EventEnvelope` and `EventMetadata` for wrapping domain events
//! with additional context and tracking information.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{AggregateType, EventVersion};

mod metadata;

pub use metadata::{additional_from_value, EventMetadata};

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
/// use event_sauce_core::{AggregateType, EventEnvelope, EventMetadata, EventVersion};
/// use uuid::Uuid;
/// use serde_json::json;
/// use chrono::Utc;
///
/// let envelope = EventEnvelope::new(
///     Uuid::new_v4(),
///     Uuid::new_v4(),
///     "User",
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
    pub aggregate_type: AggregateType,

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
    ///     "Order",
    ///     "Order.Placed".to_string(),
    ///     EventVersion::new(1),
    ///     json!({"total": 99.99}),
    /// );
    /// ```
    #[must_use]
    pub fn new(
        id: Uuid,
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        event_type: String,
        event_version: EventVersion,
        event_data: serde_json::Value,
    ) -> Self {
        Self {
            id,
            aggregate_id,
            aggregate_type: aggregate_type.into(),
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
mod tests;
