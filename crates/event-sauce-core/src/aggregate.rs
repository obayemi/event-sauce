//! Aggregate trait for event-sourced aggregates.
//!
//! Defines the core `Aggregate` trait that all event-sourced aggregates must implement.

use crate::{AggregateError, AggregateId, DomainEvent, Version};

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
/// use std::fmt;
/// use thiserror::Error;
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
/// impl AggregateId for CounterId {
///     fn to_uuid(&self) -> Uuid {
///         self.0
///     }
/// }
///
/// # struct Counter {
/// #     id: CounterId,
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
/// #[derive(Debug, Error)]
/// #[error("Counter error")]
/// struct CounterError;
///
/// impl AggregateError for CounterError {}
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
///     type Id = CounterId;
///     type Error = CounterError;
///
///     fn new(id: Self::Id) -> Self {
///         Self {
///             id,
///             value: 0,
///             version: Version::initial(),
///             pending_events: Vec::new(),
///         }
///     }
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
pub trait Aggregate: Send + Sync {
    /// The type of events this aggregate produces.
    type Event: DomainEvent;

    /// The type of the aggregate's identifier.
    type Id: AggregateId;

    /// The type of errors that can occur when applying events.
    ///
    /// This is used by events that implement `ApplyEvent` trait
    /// to return validation errors.
    type Error: AggregateError;

    /// The type of the aggregate's state (business logic only, no infrastructure).
    ///
    /// This is used for snapshotting - only the pure business state is serialized,
    /// while infrastructure concerns (ID, version, pending events) are stored separately.
    type State: serde::Serialize + serde::de::DeserializeOwned + Send + Sync;

    /// Creates a new aggregate with the given identifier.
    ///
    /// This is the canonical way to create a new aggregate instance.
    /// The aggregate should be initialized with version 0 and no pending events.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let id = UserId::new();
    /// let user = User::new(id);
    /// ```
    fn new(id: Self::Id) -> Self;

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

    /// Applies an event to update the aggregate's state and records it.
    ///
    /// This method:
    /// 1. Converts the event into the aggregate's event type (via Into)
    /// 2. Validates the event can be applied (pre-validation)
    /// 3. Updates the aggregate's internal state based on the event
    /// 4. Validates the aggregate state after applying (post-validation)
    /// 5. Adds the event to pending events if all validations pass
    /// 6. Increments the version
    ///
    /// This method is typically called by business logic methods to apply
    /// and record new events.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Pre-validation fails (event cannot be applied)
    /// - Post-validation fails (aggregate state invalid after applying)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Instead of:
    /// let event = AccountEvent::Withdrawn(AccountWithdrawnEvent { amount: 100, timestamp: Utc::now() });
    /// self.apply(&event)?;
    /// self.pending_events.push(event);
    ///
    /// // You can now write:
    /// self.apply(AccountWithdrawnEvent { amount: 100, timestamp: Utc::now() })?;
    /// ```
    fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error>;

    /// Applies an event to update the aggregate's state (internal use).
    ///
    /// This method is used internally by `apply()` and for event replay.
    /// It only updates state and increments version, without recording
    /// the event in pending events.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error>;

    /// Applies an event without validation (for event replay).
    ///
    /// This method is used when replaying events from the event store,
    /// where events are historical facts that should not be re-validated.
    ///
    /// The default implementation simply calls `apply()`, which is
    /// appropriate for aggregates that don't use the validation pattern.
    ///
    /// When using events that implement `ApplyEvent` with validation,
    /// this method should skip validation and directly apply the state changes.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to apply
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::{Aggregate, AggregateId, AggregateError, DomainEvent, Version};
    /// use chrono::{DateTime, Utc};
    /// use std::fmt;
    /// use uuid::Uuid;
    /// # use thiserror::Error;
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
    /// impl AggregateId for CounterId {
    ///     fn to_uuid(&self) -> Uuid {
    ///         self.0
    ///     }
    /// }
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
    /// # #[derive(Debug, Error)]
    /// # #[error("Counter error")]
    /// # struct CounterError;
    /// # impl AggregateError for CounterError {}
    ///
    /// struct Counter {
    ///     id: CounterId,
    ///     value: i32,
    ///     version: Version,
    ///     pending_events: Vec<CounterEvent>,
    /// }
    ///
    /// impl Aggregate for Counter {
    ///     type Event = CounterEvent;
    ///     type Id = CounterId;
    ///     type Error = CounterError;
    ///
    ///     fn new(id: Self::Id) -> Self {
    ///         Self {
    ///             id,
    ///             value: 0,
    ///             version: Version::initial(),
    ///             pending_events: Vec::new(),
    ///         }
    ///     }
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
    ///     fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
    ///         let event = event.into();
    ///         self.apply_internal(&event)?;
    ///         self.pending_events.push(event);
    ///         Ok(())
    ///     }
    ///
    ///     fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error> {
    ///         match event {
    ///             CounterEvent::Incremented { amount, .. } => {
    ///                 self.value += amount;
    ///             }
    ///         }
    ///         self.version = self.version.next();
    ///         Ok(())
    ///     }
    /// }
    ///
    /// // Replaying events from event store
    /// let mut counter = Counter {
    ///     id: CounterId::new(),
    ///     value: 0,
    ///     version: Version::initial(),
    ///     pending_events: Vec::new(),
    /// };
    ///
    /// let events = vec![
    ///     CounterEvent::Incremented { amount: 5, timestamp: Utc::now() },
    ///     CounterEvent::Incremented { amount: 3, timestamp: Utc::now() },
    /// ];
    ///
    /// for event in &events {
    ///     counter.apply_unchecked(event);
    /// }
    ///
    /// assert_eq!(counter.value, 8);
    /// ```
    fn apply_unchecked(&mut self, event: &Self::Event) {
        // For event replay, we don't expect validation to fail since
        // events are historical facts. If validation fails, it indicates
        // a bug in the event application logic.
        self.apply_internal(event)
            .expect("Event replay should not fail validation");
    }

    /// Returns the aggregate type name.
    ///
    /// Used for event store organization and serialization.
    /// Defaults to the type name.
    ///
    /// # Coverage Note
    ///
    /// The `unwrap_or("Unknown")` fallback is defensive programming.
    /// Rust's `type_name()` always returns a non-empty string, so `.last()`
    /// will always return `Some(_)`. The fallback cannot be reached in practice.
    #[must_use]
    fn aggregate_type() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
            .split("::")
            .last()
            .unwrap_or("Unknown")
    }

    /// Returns a reference to the aggregate's state (business logic only).
    ///
    /// This is used for snapshotting - only the pure business state is serialized,
    /// while infrastructure concerns (ID, version, pending events) are stored separately.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let counter = Counter::new(id);
    /// let state = counter.state();
    /// // state contains only business fields, no infrastructure
    /// ```
    fn state(&self) -> &Self::State;

    /// Reconstructs an aggregate from a snapshot.
    ///
    /// This method creates an aggregate instance from its constituent parts:
    /// - `id`: The aggregate's unique identifier
    /// - `version`: The version at which the snapshot was taken
    /// - `state`: The business state at that version
    ///
    /// The aggregate is initialized with no pending events, as snapshots represent
    /// a consistent state after all events have been applied.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let state = CounterState { value: 42 };
    /// let counter = Counter::from_snapshot(id, Version::new(10), state);
    /// assert_eq!(counter.version(), Version::new(10));
    /// assert_eq!(counter.state().value, 42);
    /// ```
    fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::fmt;
    use thiserror::Error;
    use uuid::Uuid;

    // Test error type
    #[derive(Debug, Error)]
    #[error("Test aggregate error")]
    struct TestAggregateError;

    impl AggregateError for TestAggregateError {}

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

    impl AggregateId for TestId {
        fn to_uuid(&self) -> Uuid {
            self.0
        }
    }

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

        fn event_version(&self) -> i32 {
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
        id: TestId,
        state: TestAggregateState,
        version: Version,
        pending_events: Vec<TestEvent>,
    }

    impl TestAggregate {
        fn new(id: TestId) -> Self {
            Self {
                id,
                state: TestAggregateState { value: 0 },
                version: Version::initial(),
                pending_events: Vec::new(),
            }
        }

        fn create(id: TestId, value: i32) -> Self {
            let mut aggregate = Self::new(id);
            aggregate.apply(TestEvent::Created { value }).unwrap();
            aggregate
        }

        fn update(&mut self, value: i32) {
            self.apply(TestEvent::Updated { value }).unwrap();
        }

        fn apply_event(&mut self, event: &TestEvent) {
            match event {
                TestEvent::Created { value } | TestEvent::Updated { value } => {
                    self.state.value = *value;
                }
            }
        }
    }

    impl Aggregate for TestAggregate {
        type Event = TestEvent;
        type Id = TestId;
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

        fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
            let event = event.into();
            self.apply_internal(&event)?;
            self.pending_events.push(event);
            Ok(())
        }

        fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error> {
            self.apply_event(event);
            self.version = self.version.next();
            Ok(())
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

        aggregate.apply(TestEvent::Created { value: 42 }).unwrap();

        assert_eq!(aggregate.version(), Version::new(1));
    }

    #[test]
    fn test_aggregate_apply_updates_state() {
        let mut aggregate = TestAggregate::new(TestId::new());

        aggregate.apply(TestEvent::Created { value: 42 }).unwrap();

        assert_eq!(aggregate.state.value, 42);
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
        assert_eq!(aggregate.state.value, 30);
        assert_eq!(aggregate.version(), Version::new(3));
    }

    #[test]
    fn test_aggregate_version_progression() {
        let mut aggregate = TestAggregate::new(TestId::new());

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
        let id = TestId::new();
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
        let mut aggregate = TestAggregate::new(TestId::new());

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
        let mut aggregate = TestAggregate::new(TestId::new());

        // apply_unchecked should work the same as apply for basic aggregates
        let event = TestEvent::Created { value: 42 };
        aggregate.apply_unchecked(&event);

        assert_eq!(aggregate.state.value, 42);
        assert_eq!(aggregate.version(), Version::new(1));
    }

    #[test]
    fn test_aggregate_apply_unchecked_replay() {
        let id = TestId::new();
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
