//! Event bus trait for event publishing and subscription.
//!
//! Defines the `EventBus` trait for publishing events and subscribing to event streams.

use async_trait::async_trait;
use futures::Stream;

use crate::{EventEnvelope, Result};

/// Event filter for selective event subscription.
///
/// Allows filtering events by type, aggregate type, or custom criteria.
///
/// # Examples
///
/// ```
/// use event_sauce_core::EventFilter;
///
/// // Filter by event type
/// let filter = EventFilter::by_event_type("UserRegistered");
///
/// // Filter by aggregate type
/// let filter = EventFilter::by_aggregate_type("User");
///
/// // Match all events
/// let filter = EventFilter::all();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventFilter {
    /// Match all events.
    All,
    /// Match events by event type.
    EventType(String),
    /// Match events by aggregate type.
    AggregateType(String),
    /// Match events by both event and aggregate type.
    Both {
        /// Event type to match.
        event_type: String,
        /// Aggregate type to match.
        aggregate_type: String,
    },
}

impl EventFilter {
    /// Creates a filter matching all events.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::all();
    /// ```
    #[must_use]
    pub const fn all() -> Self {
        Self::All
    }

    /// Creates a filter matching a specific event type.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::by_event_type("UserRegistered");
    /// ```
    #[must_use]
    pub fn by_event_type(event_type: impl Into<String>) -> Self {
        Self::EventType(event_type.into())
    }

    /// Creates a filter matching a specific aggregate type.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::by_aggregate_type("User");
    /// ```
    #[must_use]
    pub fn by_aggregate_type(aggregate_type: impl Into<String>) -> Self {
        Self::AggregateType(aggregate_type.into())
    }

    /// Creates a filter matching both event and aggregate type.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::both("UserRegistered", "User");
    /// ```
    #[must_use]
    pub fn both(event_type: impl Into<String>, aggregate_type: impl Into<String>) -> Self {
        Self::Both {
            event_type: event_type.into(),
            aggregate_type: aggregate_type.into(),
        }
    }

    /// Checks if an event matches this filter.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{EventFilter, EventEnvelope, Version};
    /// use uuid::Uuid;
    /// use serde_json::json;
    ///
    /// let filter = EventFilter::by_event_type("UserRegistered");
    /// let envelope = EventEnvelope::new(
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    ///     "User".to_string(),
    ///     "UserRegistered".to_string(),
    ///     Version::new(1),
    ///     json!({}),
    /// );
    ///
    /// assert!(filter.matches(&envelope));
    /// ```
    #[must_use]
    pub fn matches(&self, event: &EventEnvelope) -> bool {
        match self {
            Self::All => true,
            Self::EventType(event_type) => &event.event_type == event_type,
            Self::AggregateType(aggregate_type) => &event.aggregate_type == aggregate_type,
            Self::Both {
                event_type,
                aggregate_type,
            } => &event.event_type == event_type && &event.aggregate_type == aggregate_type,
        }
    }
}

/// Trait for event bus implementations.
///
/// The event bus is responsible for:
/// - Publishing events to interested subscribers
/// - Managing subscriptions
/// - Filtering events based on criteria
/// - Supporting async streaming for backpressure
///
/// # Streaming Support
///
/// Subscriptions return `Stream` for memory-efficient event processing
/// with automatic backpressure handling.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::{EventBus, EventFilter};
/// use futures::StreamExt;
///
/// async fn example(bus: impl EventBus) -> Result<(), Box<dyn std::error::Error>> {
///     // Subscribe to events
///     let filter = EventFilter::by_event_type("UserRegistered");
///     let mut subscription = bus.subscribe(filter).await?;
///
///     // Publish event
///     bus.publish(event).await?;
///
///     // Receive events
///     while let Some(event) = subscription.next().await {
///         println!("Received: {:?}", event);
///     }
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait EventBus: Send + Sync {
    /// Publishes a single event to all matching subscribers.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// bus.publish(event_envelope).await?;
    /// ```
    async fn publish(&self, event: EventEnvelope) -> Result<()>;

    /// Publishes multiple events efficiently.
    ///
    /// Implementations may batch or optimize multiple event publishing.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// bus.publish_batch(vec![event1, event2, event3]).await?;
    /// ```
    async fn publish_batch(&self, events: Vec<EventEnvelope>) -> Result<()> {
        // Default implementation: publish one by one
        for event in events {
            self.publish(event).await?;
        }
        Ok(())
    }

    /// Subscribes to events matching a filter.
    ///
    /// Returns a stream of events for processing with backpressure support.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let filter = EventFilter::by_event_type("UserRegistered");
    /// let mut subscription = bus.subscribe(filter).await?;
    ///
    /// while let Some(event) = subscription.next().await {
    ///     // Process event
    /// }
    /// ```
    async fn subscribe(
        &self,
        filter: EventFilter,
    ) -> Result<impl Stream<Item = EventEnvelope> + Send>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Version;
    use serde_json::json;
    use uuid::Uuid;

    fn create_test_envelope(event_type: &str, aggregate_type: &str) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            aggregate_type.to_string(),
            event_type.to_string(),
            Version::new(1),
            json!({}),
        )
    }

    #[test]
    fn test_event_filter_all() {
        let filter = EventFilter::all();
        let event1 = create_test_envelope("UserRegistered", "User");
        let event2 = create_test_envelope("OrderPlaced", "Order");

        assert!(filter.matches(&event1));
        assert!(filter.matches(&event2));
    }

    #[test]
    fn test_event_filter_by_event_type() {
        let filter = EventFilter::by_event_type("UserRegistered");
        let event1 = create_test_envelope("UserRegistered", "User");
        let event2 = create_test_envelope("UserLoggedIn", "User");
        let event3 = create_test_envelope("UserRegistered", "Admin");

        assert!(filter.matches(&event1));
        assert!(!filter.matches(&event2));
        assert!(filter.matches(&event3)); // Same event type, different aggregate
    }

    #[test]
    fn test_event_filter_by_aggregate_type() {
        let filter = EventFilter::by_aggregate_type("User");
        let event1 = create_test_envelope("UserRegistered", "User");
        let event2 = create_test_envelope("UserLoggedIn", "User");
        let event3 = create_test_envelope("OrderPlaced", "Order");

        assert!(filter.matches(&event1));
        assert!(filter.matches(&event2));
        assert!(!filter.matches(&event3));
    }

    #[test]
    fn test_event_filter_both() {
        let filter = EventFilter::both("UserRegistered", "User");
        let event1 = create_test_envelope("UserRegistered", "User");
        let event2 = create_test_envelope("UserLoggedIn", "User");
        let event3 = create_test_envelope("UserRegistered", "Admin");
        let event4 = create_test_envelope("OrderPlaced", "Order");

        assert!(filter.matches(&event1));
        assert!(!filter.matches(&event2)); // Wrong event type
        assert!(!filter.matches(&event3)); // Wrong aggregate type
        assert!(!filter.matches(&event4)); // Both wrong
    }

    #[test]
    fn test_event_filter_equality() {
        let filter1 = EventFilter::by_event_type("UserRegistered");
        let filter2 = EventFilter::by_event_type("UserRegistered");
        let filter3 = EventFilter::by_event_type("UserLoggedIn");

        assert_eq!(filter1, filter2);
        assert_ne!(filter1, filter3);
    }

    #[test]
    fn test_event_filter_clone() {
        let filter1 = EventFilter::by_event_type("UserRegistered");
        let filter2 = filter1.clone();

        assert_eq!(filter1, filter2);
    }

    #[test]
    fn test_event_filter_debug() {
        let filter = EventFilter::by_event_type("UserRegistered");
        let debug = format!("{:?}", filter);

        assert!(debug.contains("EventType"));
        assert!(debug.contains("UserRegistered"));
    }

    #[test]
    fn test_event_filter_all_matches_everything() {
        let filter = EventFilter::All;

        for i in 0..10 {
            let event = create_test_envelope(&format!("Event{}", i), &format!("Aggregate{}", i));
            assert!(filter.matches(&event));
        }
    }

    #[test]
    fn test_event_filter_case_sensitive() {
        let filter = EventFilter::by_event_type("UserRegistered");
        let event1 = create_test_envelope("UserRegistered", "User");
        let event2 = create_test_envelope("userregistered", "User");
        let event3 = create_test_envelope("USERREGISTERED", "User");

        assert!(filter.matches(&event1));
        assert!(!filter.matches(&event2));
        assert!(!filter.matches(&event3));
    }

    #[test]
    fn test_event_filter_empty_strings() {
        let filter = EventFilter::by_event_type("");
        let event1 = create_test_envelope("", "User");
        let event2 = create_test_envelope("UserRegistered", "User");

        assert!(filter.matches(&event1));
        assert!(!filter.matches(&event2));
    }

    #[test]
    fn test_event_filter_both_partial_match() {
        let filter = EventFilter::both("UserRegistered", "User");

        // Only event type matches
        let event1 = create_test_envelope("UserRegistered", "Admin");
        assert!(!filter.matches(&event1));

        // Only aggregate type matches
        let event2 = create_test_envelope("UserLoggedIn", "User");
        assert!(!filter.matches(&event2));

        // Both match
        let event3 = create_test_envelope("UserRegistered", "User");
        assert!(filter.matches(&event3));
    }

    #[test]
    fn test_event_filter_with_complex_names() {
        let filter = EventFilter::by_event_type("User::Events::Registered::V2");
        let event1 = create_test_envelope("User::Events::Registered::V2", "User");
        let event2 = create_test_envelope("User::Events::Registered::V1", "User");

        assert!(filter.matches(&event1));
        assert!(!filter.matches(&event2));
    }
}
