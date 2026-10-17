//! Per-event correlation and causation metadata.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Optional metadata associated with an event.
///
/// `EventMetadata` is an **extension point** for attaching cross-cutting concerns
/// to events without modifying the domain event types themselves. It is stored
/// alongside the event in the [`EventEnvelope`](super::EventEnvelope) and is entirely optional — the
/// core event sourcing flow works without it.
///
/// # Built-in Fields
///
/// - **`correlation_id`** — Links related events across services for distributed tracing.
/// - **`causation_id`** — Identifies the service command or event that caused this event.
/// - **`tenant`** — The tenant the event belongs to.
/// - **`command`** — The name of the command that produced this event.
/// - **`correlation_path`** — Every correlation id of the flow that produced the
///   event, outermost first; `correlation_id` is its first entry. Filled from
///   the ambient `EventContext`.
/// - **`timestamp`** — When the metadata was created (set automatically).
/// - **`additional`** — Arbitrary JSON for application-specific data (e.g., user ID,
///   IP address, session id); the tenant has its own field.
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

    /// Every correlation id of the flow that produced the event, outermost
    /// first; `correlation_id` is its first entry. Filled from the ambient
    /// `EventContext`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub correlation_path: Vec<String>,

    /// When this metadata was created.
    pub timestamp: DateTime<Utc>,

    /// Additional metadata, always a JSON object at its root.
    ///
    /// The keys set on an event context land directly at this root. Data
    /// written by older code may hold any JSON value: on every read path a
    /// non-object loads as `{"value": <original>}` and `null` as `None`.
    #[serde(
        default,
        deserialize_with = "deserialize_additional",
        skip_serializing_if = "Option::is_none"
    )]
    pub additional: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Reads a stored JSON value as additional metadata, whose root is always an
/// object.
///
/// An object is kept as is and `null` reads as no metadata. Any other value,
/// written by code that allowed arbitrary JSON, is kept under the reserved key
/// `"value"`.
///
/// # Examples
///
/// ```
/// use event_sauce_core::additional_from_value;
/// use serde_json::json;
///
/// assert_eq!(additional_from_value(json!(null)), None);
/// assert_eq!(
///     additional_from_value(json!([1])).unwrap()["value"],
///     json!([1])
/// );
/// ```
#[must_use]
pub fn additional_from_value(
    value: serde_json::Value,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    match value {
        serde_json::Value::Object(map) => Some(map),
        serde_json::Value::Null => None,
        other => Some(serde_json::Map::from_iter([("value".to_owned(), other)])),
    }
}

fn deserialize_additional<'de, D>(
    deserializer: D,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde_json::Value::deserialize(deserializer).map(additional_from_value)
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

    /// Whether any field other than the timestamp holds data.
    ///
    /// Destructures every field, so adding one to [`EventMetadata`] fails to
    /// compile until it is accounted for here.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    ///
    /// assert!(!EventMetadata::new().carries_data());
    /// assert!(EventMetadata::new().with_tenant("acme").carries_data());
    /// ```
    #[must_use]
    pub fn carries_data(&self) -> bool {
        let Self {
            correlation_id,
            causation_id,
            causation_chain,
            tenant,
            command,
            correlation_path,
            additional,
            timestamp: _,
        } = self;
        correlation_id.is_some()
            || causation_id.is_some()
            || !causation_chain.is_empty()
            || tenant.is_some()
            || command.is_some()
            || !correlation_path.is_empty()
            || additional.is_some()
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

    /// Sets additional metadata, replacing any set before.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    /// use serde_json::{Map, json};
    ///
    /// let mut additional = Map::new();
    /// additional.insert("user_id".into(), json!("123"));
    /// let metadata = EventMetadata::new().with_additional(additional);
    ///
    /// assert!(metadata.additional.is_some());
    /// ```
    #[must_use]
    pub fn with_additional(
        mut self,
        additional: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        self.additional = Some(additional);
        self
    }

    /// Sets one additional metadata value under `key`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventMetadata;
    ///
    /// let metadata = EventMetadata::new()
    ///     .with_additional_entry("user_id", "123")
    ///     .with_additional_entry("attempt", 2);
    ///
    /// assert_eq!(metadata.additional.unwrap().len(), 2);
    /// ```
    #[must_use]
    pub fn with_additional_entry(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.additional
            .get_or_insert_default()
            .insert(key.into(), value.into());
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
    /// - `additional` merges key by key, the receiver's keys winning.
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
        let path_follows_id = self
            .correlation_id
            .as_ref()
            .is_none_or(|id| other.correlation_path.first() == Some(id));
        fill_option(&mut self.correlation_id, other.correlation_id.as_ref());
        fill_option(&mut self.tenant, other.tenant.as_ref());
        fill_option(&mut self.command, other.command.as_ref());
        if path_follows_id && self.correlation_path.is_empty() {
            self.correlation_path.clone_from(&other.correlation_path);
        }
        if self.causation_id.is_none() && self.causation_chain.is_empty() {
            self.causation_id = other.causation_id;
            self.causation_chain.clone_from(&other.causation_chain);
        }
        if let Some(theirs) = &other.additional {
            let mine = self.additional.get_or_insert_default();
            for (key, value) in theirs {
                mine.entry(key.clone()).or_insert_with(|| value.clone());
            }
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

#[cfg(test)]
mod tests;
