//! Event log query types and trait.
//!
//! Provides paginated, filtered access to the global event log.
//! This is useful for audit logs, admin dashboards, and debugging.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{EventEnvelope, Position};

/// Filter and pagination parameters for event log queries.
///
/// All filter fields are optional — when `None`, no filtering is applied
/// for that field. Multiple filters are combined with AND logic.
///
/// # Examples
///
/// ```
/// use event_sauce_core::EventLogParams;
///
/// // Query all events with defaults (page 0, 50 per page)
/// let params = EventLogParams::default();
/// assert_eq!(params.page, 0);
/// assert_eq!(params.per_page, 50);
///
/// // Query events for a specific aggregate type
/// let params = EventLogParams {
///     aggregate_type: Some("User".to_string()),
///     ..Default::default()
/// };
/// ```
pub struct EventLogParams {
    /// Filter by aggregate type (e.g., "User", "Order").
    pub aggregate_type: Option<String>,
    /// Filter by event type (e.g., "User.Registered").
    pub event_type: Option<String>,
    /// Filter by aggregate instance ID.
    pub aggregate_id: Option<Uuid>,
    /// Filter by who created the event.
    pub created_by: Option<Uuid>,
    /// Filter events created at or after this timestamp.
    pub from_date: Option<DateTime<Utc>>,
    /// Filter events created at or before this timestamp.
    pub to_date: Option<DateTime<Utc>>,
    /// Result ordering. Defaults to newest-first.
    pub order_by: EventLogOrder,
    /// Zero-indexed page number.
    pub page: u64,
    /// Number of entries per page.
    pub per_page: u64,
}

/// Ordering options for [`EventLogParams`]. All variants ultimately tiebreak
/// by event id (insertion order) so pagination is stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventLogOrder {
    /// `id DESC` — newest first. The historical default.
    #[default]
    CreatedAtDesc,
    /// `id ASC` — oldest first.
    CreatedAtAsc,
    /// `aggregate_type ASC, id DESC`.
    AggregateTypeAsc,
    /// `aggregate_type DESC, id DESC`.
    AggregateTypeDesc,
    /// `created_by ASC NULLS LAST, id DESC`.
    CreatedByAsc,
    /// `created_by DESC NULLS LAST, id DESC`.
    CreatedByDesc,
}

impl EventLogOrder {
    /// SQL `ORDER BY` body (everything after the keywords). Safe to interpolate
    /// — values come from this enum, never user input.
    #[must_use]
    pub fn order_sql(self) -> &'static str {
        match self {
            Self::CreatedAtDesc => "id DESC",
            Self::CreatedAtAsc => "id ASC",
            Self::AggregateTypeAsc => "aggregate_type ASC, id DESC",
            Self::AggregateTypeDesc => "aggregate_type DESC, id DESC",
            Self::CreatedByAsc => "created_by ASC NULLS LAST, id DESC",
            Self::CreatedByDesc => "created_by DESC NULLS LAST, id DESC",
        }
    }
}

impl Default for EventLogParams {
    fn default() -> Self {
        Self {
            aggregate_type: None,
            event_type: None,
            aggregate_id: None,
            created_by: None,
            from_date: None,
            to_date: None,
            order_by: EventLogOrder::default(),
            page: 0,
            per_page: 50,
        }
    }
}

/// A single entry in the event log.
///
/// Combines a global position (the BIGSERIAL `id` from the events table)
/// with the full event envelope.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{EventLogEntry, EventEnvelope, EventVersion, Position};
/// use uuid::Uuid;
/// use serde_json::json;
///
/// let entry = EventLogEntry {
///     position: Position::new(42),
///     envelope: EventEnvelope::new(
///         Uuid::new_v4(),
///         Uuid::new_v4(),
///         "User",
///         "User.Registered".to_string(),
///         EventVersion::new(1),
///         json!({"email": "user@example.com"}),
///     ),
/// };
///
/// assert_eq!(entry.position.as_i64(), 42);
/// ```
pub struct EventLogEntry {
    /// Global position in the event store (BIGSERIAL id).
    pub position: Position,
    /// The full event envelope.
    pub envelope: EventEnvelope,
}

/// A paginated page of event log entries.
///
/// # Examples
///
/// ```
/// use event_sauce_core::EventLogPage;
///
/// let page = EventLogPage {
///     entries: vec![],
///     total_count: 0,
///     page: 0,
///     per_page: 50,
/// };
///
/// assert_eq!(page.total_pages(), 0);
/// ```
pub struct EventLogPage {
    /// The entries on this page.
    pub entries: Vec<EventLogEntry>,
    /// Total number of matching entries across all pages.
    pub total_count: u64,
    /// Current page number (zero-indexed).
    pub page: u64,
    /// Number of entries per page.
    pub per_page: u64,
}

impl EventLogPage {
    /// Returns the total number of pages.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventLogPage;
    ///
    /// let page = EventLogPage {
    ///     entries: vec![],
    ///     total_count: 101,
    ///     page: 0,
    ///     per_page: 50,
    /// };
    /// assert_eq!(page.total_pages(), 3);
    /// ```
    #[must_use]
    pub fn total_pages(&self) -> u64 {
        if self.per_page == 0 {
            return 0;
        }
        self.total_count.div_ceil(self.per_page)
    }
}

/// Trait for querying the global event log.
///
/// Implementations provide paginated, filtered access to stored events.
/// This is a read-only query interface — it does not modify events.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::{EventLogQuery, EventLogParams};
///
/// async fn audit_log(query: &dyn EventLogQuery) {
///     let params = EventLogParams {
///         aggregate_type: Some("User".to_string()),
///         per_page: 25,
///         ..Default::default()
///     };
///     let page = query.query_events(params).await.unwrap();
///     println!("Found {} events", page.total_count);
/// }
/// ```
#[async_trait]
pub trait EventLogQuery: Send + Sync {
    /// Queries events with the given filter and pagination parameters.
    ///
    /// Results are ordered by position descending (newest first).
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails.
    async fn query_events(&self, params: EventLogParams) -> crate::Result<EventLogPage>;

    /// Returns all distinct aggregate types present in the event store.
    ///
    /// Useful for populating filter dropdowns in admin UIs.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails.
    async fn distinct_aggregate_types(&self) -> crate::Result<Vec<String>>;

    /// Returns all distinct event types present in the event store.
    ///
    /// Useful for populating filter dropdowns in admin UIs.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails.
    async fn distinct_event_types(&self) -> crate::Result<Vec<String>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_log_params_default() {
        let params = EventLogParams::default();
        assert!(params.aggregate_type.is_none());
        assert!(params.event_type.is_none());
        assert!(params.aggregate_id.is_none());
        assert!(params.created_by.is_none());
        assert!(params.from_date.is_none());
        assert!(params.to_date.is_none());
        assert_eq!(params.order_by, EventLogOrder::CreatedAtDesc);
        assert_eq!(params.page, 0);
        assert_eq!(params.per_page, 50);
    }

    #[test]
    fn test_event_log_order_default_is_newest_first() {
        assert_eq!(EventLogOrder::default(), EventLogOrder::CreatedAtDesc);
    }

    #[test]
    fn test_event_log_order_sql() {
        // Every variant tiebreaks by `id` so pagination stays stable.
        assert_eq!(EventLogOrder::CreatedAtDesc.order_sql(), "id DESC");
        assert_eq!(EventLogOrder::CreatedAtAsc.order_sql(), "id ASC");
        assert_eq!(
            EventLogOrder::AggregateTypeAsc.order_sql(),
            "aggregate_type ASC, id DESC"
        );
        assert_eq!(
            EventLogOrder::AggregateTypeDesc.order_sql(),
            "aggregate_type DESC, id DESC"
        );
        assert_eq!(
            EventLogOrder::CreatedByAsc.order_sql(),
            "created_by ASC NULLS LAST, id DESC"
        );
        assert_eq!(
            EventLogOrder::CreatedByDesc.order_sql(),
            "created_by DESC NULLS LAST, id DESC"
        );
    }

    #[test]
    fn test_event_log_page_total_pages_zero() {
        let page = EventLogPage {
            entries: vec![],
            total_count: 0,
            page: 0,
            per_page: 50,
        };
        assert_eq!(page.total_pages(), 0);
    }

    #[test]
    fn test_event_log_page_total_pages_exact() {
        let page = EventLogPage {
            entries: vec![],
            total_count: 100,
            page: 0,
            per_page: 50,
        };
        assert_eq!(page.total_pages(), 2);
    }

    #[test]
    fn test_event_log_page_total_pages_remainder() {
        let page = EventLogPage {
            entries: vec![],
            total_count: 101,
            page: 0,
            per_page: 50,
        };
        assert_eq!(page.total_pages(), 3);
    }

    #[test]
    fn test_event_log_page_total_pages_single() {
        let page = EventLogPage {
            entries: vec![],
            total_count: 1,
            page: 0,
            per_page: 50,
        };
        assert_eq!(page.total_pages(), 1);
    }

    #[test]
    fn test_event_log_page_total_pages_zero_per_page() {
        let page = EventLogPage {
            entries: vec![],
            total_count: 100,
            page: 0,
            per_page: 0,
        };
        assert_eq!(page.total_pages(), 0);
    }

    #[test]
    fn test_event_log_entry_fields() {
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "TestAggregate",
            "TestEvent".to_string(),
            crate::EventVersion::new(1),
            serde_json::json!({}),
        );
        let entry = EventLogEntry {
            position: Position::new(42),
            envelope,
        };
        assert_eq!(entry.position.as_i64(), 42);
        assert_eq!(entry.envelope.event_type, "TestEvent");
    }
}
