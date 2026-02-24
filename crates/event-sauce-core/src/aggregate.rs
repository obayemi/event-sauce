//! Aggregate trait for event-sourced aggregates.
//!
//! Defines the core `Aggregate` trait that all event-sourced aggregates must implement.

use crate::{AggregateError, AggregateId, DomainEvent, EventApplicator, Version};

#[cfg(test)]
use crate::DefaultAggregateId;

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
/// ```ignore
/// use event_sauce_core::{Aggregate, AggregateError, AggregateId, DomainEvent, Version};
/// use chrono::{DateTime, Utc};
/// use serde::{Serialize, Deserialize};
/// use thiserror::Error;
///
/// # struct Counter {
/// #     id: AggregateId,
/// #     value: i32,
/// #     version: Version,
/// #     pending_events: Vec<CounterEvent>,
/// # }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum CounterEvent {
///     Incremented { amount: i32, timestamp: DateTime<Utc> },
/// }
///
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///
///     fn event_type(&self) -> &'static str {
///         "CounterIncremented"
///     }
///     fn event_version(&self) -> u64 {
///         1
///     }
///     fn occurred_at(&self) -> DateTime<Utc> {
///         match self {
///             CounterEvent::Incremented { timestamp, .. } => *timestamp,
///         }
///     }
/// }
///
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// struct CounterError;
///
/// impl AggregateError for CounterError {}
///
/// struct Counter {
///     id: AggregateId,
///     value: i32,
///     version: Version,
///     pending_events: Vec<CounterEvent>,
/// }
///
/// impl Counter {
///     fn new(id: AggregateId) -> Self {
///         Self {
///             id,
///             value: 0,
///             version: Version::initial(),
///             pending_events: Vec::new(),
///         }
///     }
///
///     fn increment(&mut self, amount: i32) {
///         self.apply(CounterEvent::Incremented {
///             amount,
///             timestamp: Utc::now(),
///         });
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
///     type Error = CounterError;
///
///     fn new(id: AggregateId) -> Self {
///         Self {
///             id,
///             value: 0,
///             version: Version::initial(),
///             pending_events: Vec::new(),
///         }
///     }
///
///     fn aggregate_id(&self) -> &AggregateId {
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
///     fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
///         let event = event.into();
///         self.apply_internal(&event)?;
///         self.pending_events.push(event);
///         Ok(())
///     }
///
///     fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error> {
///         self.apply_event(event);
///         self.version = self.version.next();
///         Ok(())
///     }
/// }
/// ```
pub trait Aggregate: Send + Sync + Sized {
    /// The type of the aggregate's unique identifier.
    type Id: AggregateId;

    /// The type of events this aggregate produces.
    ///
    /// Must implement `EventApplicator<Self>` so that the trait can provide
    /// default implementations for `apply`, `apply_internal`, and `apply_unchecked`.
    type Event: DomainEvent + EventApplicator<Self>;

    /// The type of errors that can occur when applying events.
    type Error: AggregateError;

    /// The type of the aggregate's state (business logic only, no infrastructure).
    ///
    /// This is used for snapshotting - only the pure business state is serialized,
    /// while infrastructure concerns (ID, version, pending events) are stored separately.
    type State: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;

    /// Creates a new aggregate with the given identifier.
    ///
    /// The aggregate should be initialized with version 0 and no pending events.
    fn new(id: Self::Id) -> Self;

    /// Returns the aggregate's unique identifier.
    fn aggregate_id(&self) -> &Self::Id;

    /// Returns the current version of the aggregate.
    fn version(&self) -> Version;

    /// Returns uncommitted events.
    fn pending_events(&self) -> &[Self::Event];

    /// Clears all pending events.
    ///
    /// Called after events have been successfully persisted.
    fn clear_pending_events(&mut self);

    /// Pushes an event onto the pending events list.
    ///
    /// Called by the default `apply` implementation after successful validation.
    fn push_pending_event(&mut self, event: Self::Event);

    /// Increments the aggregate version by one.
    ///
    /// Called by the default `apply_internal` and `apply_unchecked` implementations
    /// after applying state changes.
    fn increment_version(&mut self);

    /// Applies an event to update the aggregate's state and records it.
    ///
    /// This method:
    /// 1. Converts the event into the aggregate's event type (via Into)
    /// 2. Dispatches through `EventApplicator` (validate + apply + `post_validate`)
    /// 3. Increments the version
    /// 4. Adds the event to pending events
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails.
    fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
        let event = event.into();
        self.apply_internal(&event)?;
        self.push_pending_event(event);
        Ok(())
    }

    /// Applies an event to update the aggregate's state (internal use).
    ///
    /// Used by `apply()` and for event replay with validation.
    /// Updates state via `EventApplicator::dispatch` and increments version.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error> {
        EventApplicator::dispatch(event, self)?;
        self.increment_version();
        Ok(())
    }

    /// Applies an event without validation (for event replay).
    ///
    /// Uses `EventApplicator::dispatch_unchecked` which skips validation,
    /// then increments the version. Used when replaying historical events.
    fn apply_unchecked(&mut self, event: &Self::Event) {
        EventApplicator::dispatch_unchecked(event, self);
        self.increment_version();
    }

    /// Returns the aggregate type name.
    ///
    /// Defaults to the short type name (last segment of the full path).
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

    /// Returns a reference to the aggregate's state (business logic only).
    fn state(&self) -> &Self::State;

    /// Reconstructs an aggregate from a snapshot.
    fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use thiserror::Error;

    // Test error type
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

    #[derive(serde::Serialize, serde::Deserialize, Clone)]
    struct TestAggregateState {
        value: i32,
    }

    struct TestAggregate {
        id: DefaultAggregateId,
        state: TestAggregateState,
        version: Version,
        pending_events: Vec<TestEvent>,
    }

    impl TestAggregate {
        fn create(id: DefaultAggregateId, value: i32) -> Self {
            let mut aggregate = Self::new(id);
            aggregate.apply(TestEvent::Created { value }).unwrap();
            aggregate
        }

        fn update(&mut self, value: i32) {
            self.apply(TestEvent::Updated { value }).unwrap();
        }
    }

    impl crate::EventApplicator<TestAggregate> for TestEvent {
        fn dispatch(&self, aggregate: &mut TestAggregate) -> Result<(), TestAggregateError> {
            match self {
                TestEvent::Created { value } | TestEvent::Updated { value } => {
                    aggregate.state.value = *value;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, aggregate: &mut TestAggregate) {
            match self {
                TestEvent::Created { value } | TestEvent::Updated { value } => {
                    aggregate.state.value = *value;
                }
            }
        }
    }

    impl Aggregate for TestAggregate {
        type Id = DefaultAggregateId;
        type Event = TestEvent;
        type Error = TestAggregateError;
        type State = TestAggregateState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: TestAggregateState { value: 0 },
                version: Version::initial(),
                pending_events: Vec::new(),
            }
        }

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

        fn push_pending_event(&mut self, event: Self::Event) {
            self.pending_events.push(event);
        }

        fn increment_version(&mut self) {
            self.version = self.version.next();
        }

        fn state(&self) -> &Self::State {
            &self.state
        }

        fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
            Self {
                id,
                state,
                version,
                pending_events: Vec::new(),
            }
        }
    }

    #[test]
    fn test_aggregate_has_id() {
        let id = DefaultAggregateId::new();
        let aggregate = TestAggregate::new(id);

        assert_eq!(aggregate.aggregate_id(), &id);
    }

    #[test]
    fn test_aggregate_initial_version() {
        let aggregate = TestAggregate::new(DefaultAggregateId::new());

        assert_eq!(aggregate.version(), Version::initial());
    }

    #[test]
    fn test_aggregate_apply_increments_version() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        aggregate.apply(TestEvent::Created { value: 42 }).unwrap();

        assert_eq!(aggregate.version(), Version::new(1));
    }

    #[test]
    fn test_aggregate_apply_updates_state() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        aggregate.apply(TestEvent::Created { value: 42 }).unwrap();

        assert_eq!(aggregate.state.value, 42);
    }

    #[test]
    fn test_aggregate_pending_events() {
        let id = DefaultAggregateId::new();
        let aggregate = TestAggregate::create(id, 42);

        assert_eq!(aggregate.pending_events().len(), 1);
        assert!(matches!(
            aggregate.pending_events()[0],
            TestEvent::Created { value: 42 }
        ));
    }

    #[test]
    fn test_aggregate_clear_pending_events() {
        let id = DefaultAggregateId::new();
        let mut aggregate = TestAggregate::create(id, 42);

        assert_eq!(aggregate.pending_events().len(), 1);

        aggregate.clear_pending_events();

        assert_eq!(aggregate.pending_events().len(), 0);
    }

    #[test]
    fn test_aggregate_multiple_events() {
        let id = DefaultAggregateId::new();
        let mut aggregate = TestAggregate::create(id, 10);

        aggregate.update(20);
        aggregate.update(30);

        assert_eq!(aggregate.pending_events().len(), 3);
        assert_eq!(aggregate.state.value, 30);
        assert_eq!(aggregate.version(), Version::new(3));
    }

    #[test]
    fn test_aggregate_version_progression() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        assert_eq!(aggregate.version(), Version::new(0));

        aggregate.apply(TestEvent::Created { value: 1 }).unwrap();
        assert_eq!(aggregate.version(), Version::new(1));

        aggregate.apply(TestEvent::Updated { value: 2 }).unwrap();
        assert_eq!(aggregate.version(), Version::new(2));

        aggregate.apply(TestEvent::Updated { value: 3 }).unwrap();
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
        let id = DefaultAggregateId::new();
        let mut aggregate = TestAggregate::new(id);

        // Simulate replaying events from event store
        let events = vec![
            TestEvent::Created { value: 10 },
            TestEvent::Updated { value: 20 },
            TestEvent::Updated { value: 30 },
        ];

        for event in &events {
            aggregate.apply_unchecked(event);
        }

        assert_eq!(aggregate.state.value, 30);
        assert_eq!(aggregate.version(), Version::new(3));
        assert_eq!(aggregate.pending_events().len(), 0); // No pending events when replaying
    }

    #[test]
    fn test_aggregate_apply_records_event() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        // Apply updates state, version, AND adds to pending
        aggregate.apply(TestEvent::Created { value: 42 }).unwrap();

        assert_eq!(aggregate.state.value, 42);
        assert_eq!(aggregate.version(), Version::new(1));
        assert_eq!(aggregate.pending_events().len(), 1);

        // Applying another event adds to pending
        aggregate.apply(TestEvent::Updated { value: 99 }).unwrap();

        assert_eq!(aggregate.pending_events().len(), 2);
    }

    #[test]
    fn test_aggregate_apply_unchecked() {
        let mut aggregate = TestAggregate::new(DefaultAggregateId::new());

        // apply_unchecked should work the same as apply for basic aggregates
        let event = TestEvent::Created { value: 42 };
        aggregate.apply_unchecked(&event);

        assert_eq!(aggregate.state.value, 42);
        assert_eq!(aggregate.version(), Version::new(1));
    }

    #[test]
    fn test_aggregate_apply_unchecked_replay() {
        let id = DefaultAggregateId::new();
        let mut aggregate = TestAggregate::new(id);

        // Simulate event replay using apply_unchecked
        let events = vec![
            TestEvent::Created { value: 10 },
            TestEvent::Updated { value: 20 },
            TestEvent::Updated { value: 30 },
        ];

        for event in &events {
            aggregate.apply_unchecked(event);
        }

        assert_eq!(aggregate.state.value, 30);
        assert_eq!(aggregate.version(), Version::new(3));
        assert_eq!(aggregate.pending_events().len(), 0); // No pending events when replaying
    }
}
