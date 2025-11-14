//! Aggregate trait for event-sourced aggregates.
//!
//! Defines the core `Aggregate` trait that all event-sourced aggregates must implement.

use crate::{AggregateId, DomainEvent, Version};

/// Trait for event-sourced aggregates.
///
/// An aggregate is the fundamental building block of event sourcing and DDD.
/// It represents a cluster of domain objects that can be treated as a single unit.
///
/// # Event Sourcing Pattern
///
/// 1. Business logic creates domain events
/// 2. Events are added to pending events list
/// 3. Events are applied to update aggregate state
/// 4. Repository persists events and publishes them
/// 5. Events are marked as committed
///
/// # Requirements
///
/// - Must have a unique identifier (`AggregateId`)
/// - Must have a version for optimistic concurrency control
/// - Must track pending (uncommitted) events
/// - Must be able to apply events to update state
/// - Must be `Send + Sync` for async usage
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Aggregate, AggregateId, DomainEvent, Version};
/// use chrono::{DateTime, Utc};
/// use std::fmt;
/// use uuid::Uuid;
///
/// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// struct CounterId(Uuid);
///
/// impl CounterId {
///     fn new() -> Self {
///         Self(Uuid::new_v4())
///     }
/// }
///
/// impl fmt::Display for CounterId {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "Counter-{}", self.0)
///     }
/// }
///
/// impl AggregateId for CounterId {}
///
/// #[derive(Debug, Clone)]
/// enum CounterEvent {
///     Incremented { amount: i32, timestamp: DateTime<Utc> },
/// }
///
/// impl DomainEvent for CounterEvent {
///     fn event_type(&self) -> &'static str {
///         "CounterIncremented"
///     }
///     fn event_version(&self) -> i32 {
///         1
///     }
///     fn occurred_at(&self) -> DateTime<Utc> {
///         match self {
///             CounterEvent::Incremented { timestamp, .. } => *timestamp,
///         }
///     }
/// }
///
/// struct Counter {
///     id: CounterId,
///     value: i32,
///     version: Version,
///     pending_events: Vec<CounterEvent>,
/// }
///
/// impl Counter {
///     fn new(id: CounterId) -> Self {
///         Self {
///             id,
///             value: 0,
///             version: Version::initial(),
///             pending_events: Vec::new(),
///         }
///     }
///
///     fn increment(&mut self, amount: i32) {
///         let event = CounterEvent::Incremented {
///             amount,
///             timestamp: Utc::now(),
///         };
///         self.apply_event(&event);
///         self.pending_events.push(event);
///     }
///
///     fn apply_event(&mut self, event: &CounterEvent) {
///         match event {
///             CounterEvent::Incremented { amount, .. } => {
///                 self.value += amount;
///             }
///         }
///     }
/// }
///
/// impl Aggregate for Counter {
///     type Event = CounterEvent;
///     type Id = CounterId;
///
///     fn aggregate_id(&self) -> &Self::Id {
///         &self.id
///     }
///
///     fn version(&self) -> Version {
///         self.version
///     }
///
///     fn pending_events(&self) -> &[Self::Event] {
///         &self.pending_events
///     }
///
///     fn clear_pending_events(&mut self) {
///         self.pending_events.clear();
///     }
///
///     fn apply(&mut self, event: &Self::Event) {
///         self.apply_event(event);
///         self.version = self.version.next();
///     }
/// }
/// ```
pub trait Aggregate: Send + Sync {
    /// The type of events this aggregate produces.
    type Event: DomainEvent;

    /// The type of the aggregate's identifier.
    type Id: AggregateId;

    /// Returns the aggregate's unique identifier.
    fn aggregate_id(&self) -> &Self::Id;

    /// Returns the current version of the aggregate.
    ///
    /// Used for optimistic concurrency control.
    fn version(&self) -> Version;

    /// Returns uncommitted events.
    ///
    /// These are events that have been applied to the aggregate's state
    /// but not yet persisted to the event store.
    fn pending_events(&self) -> &[Self::Event];

    /// Clears all pending events.
    ///
    /// Called after events have been successfully persisted.
    fn clear_pending_events(&mut self);

    /// Applies an event to update the aggregate's state.
    ///
    /// This method should:
    /// 1. Update the aggregate's internal state based on the event
    /// 2. Increment the version
    ///
    /// # Important
    ///
    /// This method does NOT add the event to pending events.
    /// That should be done by business logic methods.
    fn apply(&mut self, event: &Self::Event);

    /// Returns the aggregate type name.
    ///
    /// Used for event store organization and serialization.
    /// Defaults to the type name.
    #[must_use]
    fn aggregate_type() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>().split("::").last().unwrap_or("Unknown")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::fmt;
    use uuid::Uuid;

    // Test aggregate implementation
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestId(Uuid);

    impl TestId {
        fn new() -> Self {
            Self(Uuid::new_v4())
        }
    }

    impl fmt::Display for TestId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Test-{}", self.0)
        }
    }

    impl AggregateId for TestId {}

    #[derive(Debug, Clone)]
    enum TestEvent {
        Created { value: i32 },
        Updated { value: i32 },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Created { .. } => "TestCreated",
                TestEvent::Updated { .. } => "TestUpdated",
            }
        }

        fn event_version(&self) -> i32 {
            1
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }
    }

    struct TestAggregate {
        id: TestId,
        value: i32,
        version: Version,
        pending_events: Vec<TestEvent>,
    }

    impl TestAggregate {
        fn new(id: TestId) -> Self {
            Self {
                id,
                value: 0,
                version: Version::initial(),
                pending_events: Vec::new(),
            }
        }

        fn create(id: TestId, value: i32) -> Self {
            let mut aggregate = Self::new(id);
            let event = TestEvent::Created { value };
            aggregate.apply(&event);
            aggregate.pending_events.push(event);
            aggregate
        }

        fn update(&mut self, value: i32) {
            let event = TestEvent::Updated { value };
            self.apply(&event);
            self.pending_events.push(event);
        }
    }

    impl Aggregate for TestAggregate {
        type Event = TestEvent;
        type Id = TestId;

        fn aggregate_id(&self) -> &Self::Id {
            &self.id
        }

        fn version(&self) -> Version {
            self.version
        }

        fn pending_events(&self) -> &[Self::Event] {
            &self.pending_events
        }

        fn clear_pending_events(&mut self) {
            self.pending_events.clear();
        }

        fn apply(&mut self, event: &Self::Event) {
            match event {
                TestEvent::Created { value } | TestEvent::Updated { value } => {
                    self.value = *value;
                }
            }
            self.version = self.version.next();
        }
    }

    #[test]
    fn test_aggregate_has_id() {
        let id = TestId::new();
        let aggregate = TestAggregate::new(id.clone());

        assert_eq!(aggregate.aggregate_id(), &id);
    }

    #[test]
    fn test_aggregate_initial_version() {
        let aggregate = TestAggregate::new(TestId::new());

        assert_eq!(aggregate.version(), Version::initial());
    }

    #[test]
    fn test_aggregate_apply_increments_version() {
        let mut aggregate = TestAggregate::new(TestId::new());
        let event = TestEvent::Created { value: 42 };

        aggregate.apply(&event);

        assert_eq!(aggregate.version(), Version::new(1));
    }

    #[test]
    fn test_aggregate_apply_updates_state() {
        let mut aggregate = TestAggregate::new(TestId::new());
        let event = TestEvent::Created { value: 42 };

        aggregate.apply(&event);

        assert_eq!(aggregate.value, 42);
    }

    #[test]
    fn test_aggregate_pending_events() {
        let id = TestId::new();
        let aggregate = TestAggregate::create(id, 42);

        assert_eq!(aggregate.pending_events().len(), 1);
        assert!(matches!(
            aggregate.pending_events()[0],
            TestEvent::Created { value: 42 }
        ));
    }

    #[test]
    fn test_aggregate_clear_pending_events() {
        let id = TestId::new();
        let mut aggregate = TestAggregate::create(id, 42);

        assert_eq!(aggregate.pending_events().len(), 1);

        aggregate.clear_pending_events();

        assert_eq!(aggregate.pending_events().len(), 0);
    }

    #[test]
    fn test_aggregate_multiple_events() {
        let id = TestId::new();
        let mut aggregate = TestAggregate::create(id, 10);

        aggregate.update(20);
        aggregate.update(30);

        assert_eq!(aggregate.pending_events().len(), 3);
        assert_eq!(aggregate.value, 30);
        assert_eq!(aggregate.version(), Version::new(3));
    }

    #[test]
    fn test_aggregate_version_progression() {
        let mut aggregate = TestAggregate::new(TestId::new());

        assert_eq!(aggregate.version(), Version::new(0));

        aggregate.apply(&TestEvent::Created { value: 1 });
        assert_eq!(aggregate.version(), Version::new(1));

        aggregate.apply(&TestEvent::Updated { value: 2 });
        assert_eq!(aggregate.version(), Version::new(2));

        aggregate.apply(&TestEvent::Updated { value: 3 });
        assert_eq!(aggregate.version(), Version::new(3));
    }

    #[test]
    fn test_aggregate_type_name() {
        let type_name = TestAggregate::aggregate_type();
        assert_eq!(type_name, "TestAggregate");
    }

    #[test]
    fn test_aggregate_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<TestAggregate>();
        assert_sync::<TestAggregate>();
    }

    #[test]
    fn test_aggregate_event_replay() {
        let id = TestId::new();
        let mut aggregate = TestAggregate::new(id);

        // Simulate replaying events from event store
        let events = vec![
            TestEvent::Created { value: 10 },
            TestEvent::Updated { value: 20 },
            TestEvent::Updated { value: 30 },
        ];

        for event in &events {
            aggregate.apply(event);
        }

        assert_eq!(aggregate.value, 30);
        assert_eq!(aggregate.version(), Version::new(3));
        assert_eq!(aggregate.pending_events().len(), 0); // No pending events when replaying
    }

    #[test]
    fn test_aggregate_separate_apply_and_record() {
        let mut aggregate = TestAggregate::new(TestId::new());

        // Apply updates state and version, but doesn't add to pending
        let event = TestEvent::Created { value: 42 };
        aggregate.apply(&event);

        assert_eq!(aggregate.value, 42);
        assert_eq!(aggregate.version(), Version::new(1));
        assert_eq!(aggregate.pending_events().len(), 0);

        // Business logic must explicitly add to pending
        let event2 = TestEvent::Updated { value: 99 };
        aggregate.apply(&event2);
        aggregate.pending_events.push(event2);

        assert_eq!(aggregate.pending_events().len(), 1);
    }
}
