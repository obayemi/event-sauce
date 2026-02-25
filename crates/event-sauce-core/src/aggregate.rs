//! Aggregate trait for event-sourced entities.
//!
//! Defines the `Aggregate` trait that marks an entity as event-sourced,
//! specifying its event and error types. Infrastructure concerns (version,
//! pending events) are handled by `AggregateRoot<A>`.

use crate::{AggregateError, DomainEvent, Entity, EventApplicator};

/// Trait for event-sourced aggregates.
///
/// An aggregate is an entity whose state changes are captured as domain events.
/// This trait specifies only the domain-level concerns: what events the aggregate
/// produces and what errors it can return. All infrastructure (version tracking,
/// pending events, event application) is handled by [`AggregateRoot<A>`](crate::AggregateRoot).
///
/// # Relationship to Entity
///
/// `Aggregate` extends `Entity`, adding event sourcing capabilities:
/// - `Entity`: identity + construction
/// - `Aggregate`: entity + event types + error types
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Aggregate, AggregateError, Entity, EntityId, DomainEvent, EventApplicator};
/// use serde::{Serialize, Deserialize};
/// use thiserror::Error;
/// use chrono::Utc;
///
/// #[derive(Serialize, Deserialize)]
/// struct Counter {
///     id: EntityId,
///     value: i32,
/// }
///
/// impl Entity for Counter {
///     fn new(id: EntityId) -> Self {
///         Self { id, value: 0 }
///     }
///     fn entity_id(&self) -> EntityId { self.id }
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum CounterEvent {
///     Incremented { amount: i32, timestamp: chrono::DateTime<Utc> },
/// }
///
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///     fn event_type(&self) -> &'static str { "Incremented" }
///     fn event_version(&self) -> u64 { 1 }
///     fn occurred_at(&self) -> chrono::DateTime<Utc> {
///         match self {
///             CounterEvent::Incremented { timestamp, .. } => *timestamp,
///         }
///     }
/// }
///
/// impl EventApplicator<Counter> for CounterEvent {
///     fn dispatch(&self, counter: &mut Counter) -> Result<(), CounterError> {
///         match self {
///             CounterEvent::Incremented { amount, .. } => counter.value += amount,
///         }
///         Ok(())
///     }
///     fn dispatch_unchecked(&self, counter: &mut Counter) {
///         match self {
///             CounterEvent::Incremented { amount, .. } => counter.value += amount,
///         }
///     }
/// }
///
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// struct CounterError;
/// impl AggregateError for CounterError {}
///
/// impl Aggregate for Counter {
///     type Event = CounterEvent;
///     type Error = CounterError;
/// }
/// ```
pub trait Aggregate: Entity {
    /// The type of events this aggregate produces.
    ///
    /// Must implement `DomainEvent` (for serialization) and `EventApplicator<Self>`
    /// (for dispatching events to the entity).
    type Event: DomainEvent<Aggregate = Self> + EventApplicator<Self> + Clone;

    /// The type of errors that can occur during event application.
    type Error: AggregateError;

    /// Returns the aggregate type name.
    ///
    /// Defaults to the short type name (last segment of the full path).
    /// Used for stream ID construction and envelope metadata.
    ///
    /// # Coverage Note
    ///
    /// The `unwrap_or("Unknown")` fallback is defensive programming.
    /// Rust's `type_name()` always returns a non-empty string, so `.last()`
    /// will always return `Some(_)`. The fallback cannot be reached in practice.
    #[must_use]
    fn aggregate_type() -> &'static str {
        std::any::type_name::<Self>()
            .split("::")
            .last()
            .unwrap_or("Unknown")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntityId, Version};
    use chrono::Utc;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Test aggregate error")]
    struct TestAggregateError;

    impl AggregateError for TestAggregateError {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum TestEvent {
        Created { value: i32 },
        Updated { value: i32 },
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestAggregate;

        fn event_type(&self) -> &'static str {
            match self {
                TestEvent::Created { .. } => "TestCreated",
                TestEvent::Updated { .. } => "TestUpdated",
            }
        }

        fn event_version(&self) -> u64 {
            1
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct TestAggregate {
        id: EntityId,
        value: i32,
    }

    impl Entity for TestAggregate {
        fn new(id: EntityId) -> Self {
            Self { id, value: 0 }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl EventApplicator<TestAggregate> for TestEvent {
        fn dispatch(&self, aggregate: &mut TestAggregate) -> Result<(), TestAggregateError> {
            match self {
                TestEvent::Created { value } | TestEvent::Updated { value } => {
                    aggregate.value = *value;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, aggregate: &mut TestAggregate) {
            match self {
                TestEvent::Created { value } | TestEvent::Updated { value } => {
                    aggregate.value = *value;
                }
            }
        }
    }

    impl Aggregate for TestAggregate {
        type Event = TestEvent;
        type Error = TestAggregateError;
    }

    #[test]
    fn test_aggregate_type_name() {
        let type_name = TestAggregate::aggregate_type();
        assert_eq!(type_name, "TestAggregate");
    }

    #[test]
    fn test_aggregate_has_entity_identity() {
        let id = EntityId::new();
        let aggregate = TestAggregate::new(id);
        assert_eq!(aggregate.entity_id(), id);
    }

    #[test]
    fn test_aggregate_event_applicator_works() {
        let mut aggregate = TestAggregate::new(EntityId::new());
        let event = TestEvent::Created { value: 42 };

        EventApplicator::dispatch(&event, &mut aggregate).unwrap();
        assert_eq!(aggregate.value, 42);
    }

    #[test]
    fn test_aggregate_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<TestAggregate>();
        assert_sync::<TestAggregate>();
    }

    #[test]
    fn test_aggregate_root_integration() {
        // Test that AggregateRoot works with the new Aggregate trait
        let id = EntityId::new();
        let mut root = crate::AggregateRoot::<TestAggregate>::new(id);

        assert_eq!(root.entity_id(), id);
        assert_eq!(root.version(), Version::initial());
        assert_eq!(root.pending_events().len(), 0);

        root.apply(TestEvent::Created { value: 42 }).unwrap();

        assert_eq!(root.value, 42);
        assert_eq!(root.version(), Version::new(1));
        assert_eq!(root.pending_events().len(), 1);
    }
}
