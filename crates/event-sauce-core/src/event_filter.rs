//! Event filtering for selective event consumption.
//!
//! [`EventFilter`] is the predicate used to select which events a consumer
//! cares about. It is load-bearing for the [`Policy`](crate::Policy) system
//! (via [`Policy::event_filter`](crate::Policy::event_filter)), the postgres
//! projection runner, and [`EventLogParams`](crate::EventLogParams).

use crate::EventEnvelope;

/// Event filter for selective event consumption.
///
/// Allows filtering events by type, aggregate type, or custom criteria.
///
/// # Examples
///
/// ```
/// use event_sauce_core::EventFilter;
///
/// // Filter by event type
/// let filter = EventFilter::by_event_type("User.Registered");
///
/// // Filter by aggregate type
/// let filter = EventFilter::by_aggregate_type("User");
///
/// // Match all events
/// let filter = EventFilter::all();
///
/// // Match any of several event types
/// let filter = EventFilter::any_of_event_types(vec!["UserRegistered", "UserActivated"]);
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
    /// Match any of the given event types.
    AnyOfEventTypes(Vec<String>),
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
    /// let filter = EventFilter::by_event_type("User.Registered");
    /// ```
    #[must_use]
    pub fn by_event_type(event_type: impl Into<String>) -> Self {
        Self::EventType(event_type.into())
    }

    /// Creates a filter matching a specific event type using the `EventType` trait.
    ///
    /// This provides type-safe event filtering using the `EventType::EVENT_TYPE` const.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::by_event::<UserRegisteredEvent>();
    /// ```
    #[must_use]
    pub fn by_event<T: crate::EventType>() -> Self {
        Self::EventType(T::EVENT_TYPE.to_string())
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
    /// let filter = EventFilter::both("User.Registered", "User");
    /// ```
    #[must_use]
    pub fn both(event_type: impl Into<String>, aggregate_type: impl Into<String>) -> Self {
        Self::Both {
            event_type: event_type.into(),
            aggregate_type: aggregate_type.into(),
        }
    }

    /// Creates a filter matching any of the given event type strings.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::any_of_event_types(["User.Created", "User.Updated"]);
    /// ```
    #[must_use]
    pub fn any_of_event_types(types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut types: Vec<String> = types.into_iter().map(Into::into).collect();
        if types.len() == 1 {
            Self::EventType(types.swap_remove(0))
        } else {
            Self::AnyOfEventTypes(types)
        }
    }

    /// Extends this filter to also match another event type.
    ///
    /// Promotes `EventType` to `AnyOfEventTypes` automatically.
    /// Only works on `EventType` and `AnyOfEventTypes` variants;
    /// other variants are returned unchanged.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::by_event::<UserCreatedEvent>()
    ///     .or_event::<UserUpdatedEvent>();
    /// ```
    #[must_use]
    pub fn or_event<T: crate::EventType>(self) -> Self {
        match self {
            Self::EventType(existing) => {
                Self::AnyOfEventTypes(vec![existing, T::EVENT_TYPE.to_string()])
            }
            Self::AnyOfEventTypes(mut types) => {
                types.push(T::EVENT_TYPE.to_string());
                Self::AnyOfEventTypes(types)
            }
            other => other,
        }
    }

    /// Checks if an event matches this filter.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{EventFilter, EventEnvelope, EventVersion};
    /// use uuid::Uuid;
    /// use serde_json::json;
    ///
    /// let filter = EventFilter::by_event_type("User.Registered");
    /// let envelope = EventEnvelope::new(
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    ///     "User".to_string(),
    ///     "User.Registered".to_string(),
    ///     EventVersion::new(1),
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
            Self::AnyOfEventTypes(types) => types.iter().any(|t| t == &event.event_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::create_test_envelope;

    #[test]
    fn test_event_filter_all() {
        let filter = EventFilter::all();
        assert_eq!(filter, EventFilter::All);

        let envelope = create_test_envelope("UserCreated", "User");
        assert!(filter.matches(&envelope));
    }

    #[test]
    fn test_event_filter_by_event_type() {
        let filter = EventFilter::by_event_type("UserCreated");

        let matching = create_test_envelope("UserCreated", "User");
        let non_matching = create_test_envelope("UserUpdated", "User");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    #[test]
    fn test_event_filter_by_aggregate_type() {
        let filter = EventFilter::by_aggregate_type("User");

        let matching = create_test_envelope("UserCreated", "User");
        let non_matching = create_test_envelope("OrderCreated", "Order");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    #[test]
    fn test_event_filter_any_of_event_types() {
        let filter = EventFilter::any_of_event_types(["UserCreated", "UserUpdated", "UserDeleted"]);

        let matching1 = create_test_envelope("UserCreated", "User");
        let matching2 = create_test_envelope("UserUpdated", "User");
        let matching3 = create_test_envelope("UserDeleted", "User");
        let non_matching = create_test_envelope("OrderCreated", "Order");

        assert!(filter.matches(&matching1));
        assert!(filter.matches(&matching2));
        assert!(filter.matches(&matching3));
        assert!(!filter.matches(&non_matching));
    }

    #[test]
    fn test_event_filter_any_of_event_types_single_element_optimization() {
        let filter = EventFilter::any_of_event_types(["UserCreated"]);
        // Single element should be optimized to EventType variant
        assert_eq!(filter, EventFilter::EventType("UserCreated".to_string()));
    }

    #[test]
    fn test_event_filter_any_of_event_types_empty() {
        let filter = EventFilter::any_of_event_types(std::iter::empty::<String>());
        // Empty matches nothing
        let envelope = create_test_envelope("UserCreated", "User");
        assert!(!filter.matches(&envelope));
    }

    #[test]
    fn test_event_filter_or_event_from_event_type() {
        // Helper types for type-safe or_event
        struct EventA;
        impl crate::EventType for EventA {
            const EVENT_TYPE: &'static str = "EventA";
        }
        struct EventB;
        impl crate::EventType for EventB {
            const EVENT_TYPE: &'static str = "EventB";
        }

        let filter = EventFilter::by_event::<EventA>().or_event::<EventB>();

        let matching_a = create_test_envelope("EventA", "Test");
        let matching_b = create_test_envelope("EventB", "Test");
        let non_matching = create_test_envelope("EventC", "Test");

        assert!(filter.matches(&matching_a));
        assert!(filter.matches(&matching_b));
        assert!(!filter.matches(&non_matching));
    }

    #[test]
    fn test_event_filter_or_event_chaining() {
        struct EventA;
        impl crate::EventType for EventA {
            const EVENT_TYPE: &'static str = "EventA";
        }
        struct EventB;
        impl crate::EventType for EventB {
            const EVENT_TYPE: &'static str = "EventB";
        }
        struct EventC;
        impl crate::EventType for EventC {
            const EVENT_TYPE: &'static str = "EventC";
        }

        let filter = EventFilter::by_event::<EventA>()
            .or_event::<EventB>()
            .or_event::<EventC>();

        assert!(filter.matches(&create_test_envelope("EventA", "Test")));
        assert!(filter.matches(&create_test_envelope("EventB", "Test")));
        assert!(filter.matches(&create_test_envelope("EventC", "Test")));
        assert!(!filter.matches(&create_test_envelope("EventD", "Test")));
    }

    #[test]
    fn test_event_filter_or_event_on_non_event_type_is_noop() {
        struct EventA;
        impl crate::EventType for EventA {
            const EVENT_TYPE: &'static str = "EventA";
        }

        // or_event on All should be a no-op
        let filter = EventFilter::all().or_event::<EventA>();
        assert_eq!(filter, EventFilter::All);

        // or_event on AggregateType should be a no-op
        let filter = EventFilter::by_aggregate_type("User").or_event::<EventA>();
        assert_eq!(filter, EventFilter::by_aggregate_type("User"));

        // or_event on Both should be a no-op
        let filter = EventFilter::both("Event", "Type").or_event::<EventA>();
        assert_eq!(filter, EventFilter::both("Event", "Type"));
    }

    #[test]
    fn test_event_filter_both() {
        let filter = EventFilter::both("UserCreated", "User");

        let matching = create_test_envelope("UserCreated", "User");
        let non_matching_event = create_test_envelope("UserUpdated", "User");
        let non_matching_aggregate = create_test_envelope("UserCreated", "Order");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching_event));
        assert!(!filter.matches(&non_matching_aggregate));
    }
}
