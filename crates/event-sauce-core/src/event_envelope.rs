//! Event envelope and metadata types.
//!
//! Provides `EventEnvelope` and `EventMetadata` for wrapping domain events
//! with additional context and tracking information.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{AggregateType, EventVersion};

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
/// - **`causation_id`** — Identifies the service command or event that caused this event.
/// - **`tenant`** — The tenant the event belongs to.
/// - **`command`** — The name of the command that produced this event.
/// - **`correlation_path`** — The ordered labels of the scopes the event was
///   produced in, from the outermost to the innermost.
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
///     .with_correlation_id("req-123")
///     .with_causation_id(Uuid::new_v4());
///
/// assert!(metadata.correlation_id.is_some());
/// assert!(metadata.causation_id.is_some());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Correlation ID for tracing related events across the system.
    pub correlation_id: Option<String>,

    /// Causation ID - the event or service command that caused this event.
    pub causation_id: Option<Uuid>,

    /// Full causation chain from the root event to the direct parent.
    ///
    /// Each entry is the `id` of an ancestor event, ordered from the root
    /// (first element) to the direct parent (last element). An empty chain
    /// means this event was not produced by a policy.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub causation_chain: Vec<Uuid>,

    /// Tenant the event belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,

    /// Name of the command that produced this event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,

    /// Labels of the scopes the event was produced in, outermost first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub correlation_path: Vec<String>,

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
            causation_chain: Vec::new(),
            tenant: None,
            command: None,
            correlation_path: Vec::new(),
            timestamp: Utc::now(),
            additional: None,
        }
    }

    /// Sets the correlation ID.
    ///
    /// Correlation ids are free-form strings, so a request id, a trace id or
    /// an event id all fit.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    ///
    /// let metadata = EventMetadata::new()
    ///     .with_correlation_id("req-123");
    ///
    /// assert_eq!(metadata.correlation_id.as_deref(), Some("req-123"));
    /// ```
    #[must_use]
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Sets the tenant.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    ///
    /// let metadata = EventMetadata::new().with_tenant("acme");
    ///
    /// assert_eq!(metadata.tenant.as_deref(), Some("acme"));
    /// ```
    #[must_use]
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Sets the name of the command that produced the event.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    ///
    /// let metadata = EventMetadata::new().with_command("PlaceOrder");
    ///
    /// assert_eq!(metadata.command.as_deref(), Some("PlaceOrder"));
    /// ```
    #[must_use]
    pub fn with_command(mut self, command: impl Into<String>) -> Self {
        self.command = Some(command.into());
        self
    }

    /// Sets the correlation path.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    ///
    /// let metadata = EventMetadata::new()
    ///     .with_correlation_path(vec!["import".to_string(), "row-7".to_string()]);
    ///
    /// assert_eq!(metadata.correlation_path, vec!["import", "row-7"]);
    /// ```
    #[must_use]
    pub fn with_correlation_path(mut self, path: Vec<String>) -> Self {
        self.correlation_path = path;
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

    /// Sets the causation chain.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    /// use uuid::Uuid;
    ///
    /// let chain = vec![Uuid::new_v4(), Uuid::new_v4()];
    /// let metadata = EventMetadata::new()
    ///     .with_causation_chain(chain.clone());
    ///
    /// assert_eq!(metadata.causation_chain, chain);
    /// ```
    #[must_use]
    pub fn with_causation_chain(mut self, chain: Vec<Uuid>) -> Self {
        self.causation_chain = chain;
        self
    }

    /// Returns the cascade depth (number of ancestors in the chain).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    /// use uuid::Uuid;
    ///
    /// let metadata = EventMetadata::new();
    /// assert_eq!(metadata.cascade_depth(), 0);
    ///
    /// let metadata = metadata.with_causation_chain(vec![Uuid::new_v4()]);
    /// assert_eq!(metadata.cascade_depth(), 1);
    /// ```
    #[must_use]
    pub fn cascade_depth(&self) -> usize {
        self.causation_chain.len()
    }

    /// Creates child metadata for a reaction event.
    ///
    /// The child metadata inherits the correlation ID (or uses the parent's
    /// event ID, as a string, if none), sets the causation ID to the parent event ID,
    /// and extends the causation chain. The tenant and the correlation path
    /// carry over; the command and the additional data do not.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    /// use uuid::Uuid;
    ///
    /// let parent_event_id = Uuid::new_v4();
    /// let parent_metadata = EventMetadata::new()
    ///     .with_correlation_id("req-123");
    ///
    /// let child = parent_metadata.child_metadata(parent_event_id);
    ///
    /// assert_eq!(child.causation_id, Some(parent_event_id));
    /// assert_eq!(child.correlation_id, parent_metadata.correlation_id);
    /// assert_eq!(child.causation_chain.len(), 1);
    /// assert_eq!(child.causation_chain[0], parent_event_id);
    /// ```
    #[must_use]
    pub fn child_metadata(&self, parent_event_id: Uuid) -> Self {
        let correlation_id = self
            .correlation_id
            .clone()
            .unwrap_or_else(|| parent_event_id.to_string());
        let mut chain = self.causation_chain.clone();
        chain.push(parent_event_id);

        let mut child = Self::new()
            .with_correlation_id(correlation_id)
            .with_causation_id(parent_event_id)
            .with_causation_chain(chain)
            .with_correlation_path(self.correlation_path.clone());
        child.tenant.clone_from(&self.tenant);
        child
    }

    /// Fills every field this metadata leaves unset from `other`.
    ///
    /// The receiver wins field by field:
    ///
    /// - `correlation_id`, `tenant` and `command` come from `other` only when
    ///   unset.
    /// - `correlation_path` comes from `other` only when empty.
    /// - `causation_id` and `causation_chain` move together, from `other`,
    ///   only when the receiver has neither.
    /// - `additional` merges key by key when both are JSON objects, the
    ///   receiver's keys winning; it comes from `other` when unset and is
    ///   kept otherwise.
    /// - `timestamp` always stays the receiver's.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    ///
    /// let mut metadata = EventMetadata::new().with_tenant("acme");
    /// let other = EventMetadata::new()
    ///     .with_tenant("globex")
    ///     .with_command("PlaceOrder");
    ///
    /// metadata.fill_missing_from(&other);
    ///
    /// assert_eq!(metadata.tenant.as_deref(), Some("acme"));
    /// assert_eq!(metadata.command.as_deref(), Some("PlaceOrder"));
    /// ```
    pub fn fill_missing_from(&mut self, other: &EventMetadata) {
        fill_option(&mut self.correlation_id, other.correlation_id.as_ref());
        fill_option(&mut self.tenant, other.tenant.as_ref());
        fill_option(&mut self.command, other.command.as_ref());
        if self.correlation_path.is_empty() {
            self.correlation_path.clone_from(&other.correlation_path);
        }
        if self.causation_id.is_none() && self.causation_chain.is_empty() {
            self.causation_id = other.causation_id;
            self.causation_chain.clone_from(&other.causation_chain);
        }
        match (&mut self.additional, &other.additional) {
            (Some(serde_json::Value::Object(mine)), Some(serde_json::Value::Object(theirs))) => {
                for (key, value) in theirs {
                    mine.entry(key.clone()).or_insert_with(|| value.clone());
                }
            }
            (own @ None, theirs) => own.clone_from(theirs),
            _ => {}
        }
    }
}

fn fill_option<T: Clone>(own: &mut Option<T>, other: Option<&T>) {
    if own.is_none() {
        *own = other.cloned();
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

    /// Sets causation tracking fields on the metadata.
    ///
    /// Creates or updates the metadata with causation ID, correlation ID,
    /// and the full causation chain. Used by the policy system to propagate
    /// causation tracking through event reactions.
    #[must_use]
    pub fn with_causation(
        mut self,
        causation_id: Uuid,
        correlation_id: impl Into<String>,
        causation_chain: Vec<Uuid>,
    ) -> Self {
        let metadata = self
            .metadata
            .unwrap_or_default()
            .with_causation_id(causation_id)
            .with_correlation_id(correlation_id)
            .with_causation_chain(causation_chain);
        self.metadata = Some(metadata);
        self
    }
}

#[cfg(test)]
mod tests;
