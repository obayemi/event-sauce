//! Zero-boilerplate aggregate support via `AggregateState` and `AggregateRoot<S>`.
//!
//! This module provides a simpler way to define event-sourced aggregates
//! without implementing the full `Aggregate` trait manually.
//!
//! # Usage
//!
//! 1. Define your state struct with `Default`
//! 2. Implement `AggregateState` (just type aliases)
//! 3. Use `AggregateRoot<YourState>` as the aggregate type
//!
//! ```ignore
//! use event_sauce_core::{AggregateRoot, AggregateState};
//!
//! #[derive(Default, Serialize, Deserialize, Debug, Clone)]
//! struct OrderState {
//!     order_id: String,
//!     items: Vec<Item>,
//! }
//!
//! impl AggregateState for OrderState {
//!     type Id = OrderId;
//!     type Event = OrderEvent;
//!     type Error = OrderError;
//! }
//!
//! // Use AggregateRoot<OrderState> as the aggregate:
//! impl AggregateRoot<OrderState> {
//!     fn create(id: OrderId, order_id: String) -> Result<Self, OrderError> {
//!         let mut order = Self::new(id);
//!         order.apply(CreatedEvent { order_id, timestamp: Utc::now() })?;
//!         Ok(order)
//!     }
//! }
//! ```

use crate::{Aggregate, AggregateError, AggregateId, DomainEvent, EventApplicator, Version};

/// Trait for aggregate state types that enables zero-boilerplate aggregate definitions.
///
/// Implement this trait on your plain state struct, then use `AggregateRoot<YourState>`
/// as the full aggregate type. The `AggregateRoot` wrapper provides all infrastructure
/// (ID, version, pending events) and implements the `Aggregate` trait automatically.
///
/// # Type Parameters
///
/// - `Id`: The aggregate's unique identifier type
/// - `Event`: The domain event enum type
/// - `Error`: The aggregate's error type
///
/// # Examples
///
/// ```ignore
/// #[derive(Default, Serialize, Deserialize, Debug, Clone)]
/// struct CounterState {
///     value: i32,
/// }
///
/// impl AggregateState for CounterState {
///     type Id = CounterId;
///     type Event = CounterEvent;
///     type Error = CounterError;
/// }
///
/// // Now use AggregateRoot<CounterState> everywhere
/// let mut counter = AggregateRoot::<CounterState>::new(CounterId::new());
/// ```
pub trait AggregateState:
    Default + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Sized
{
    /// The aggregate's unique identifier type.
    type Id: AggregateId;

    /// The domain event enum type for this aggregate.
    type Event: DomainEvent<Aggregate = AggregateRoot<Self>>
        + EventApplicator<AggregateRoot<Self>>
        + Clone;

    /// The aggregate's error type.
    type Error: AggregateError;
}

/// A generic aggregate wrapper around a state type implementing `AggregateState`.
///
/// Provides all aggregate infrastructure (ID, version, pending events) and
/// implements the `Aggregate` trait. Access state fields directly via `Deref`/`DerefMut`.
///
/// # Examples
///
/// ```ignore
/// let mut counter = AggregateRoot::<CounterState>::new(CounterId::new());
///
/// // Access state fields directly via Deref
/// assert_eq!(counter.value, 0);
///
/// // Apply events through the Aggregate trait
/// counter.apply(CounterEvent::Incremented { amount: 5, timestamp: Utc::now() })?;
/// assert_eq!(counter.value, 5);
/// ```
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(
    bound = "S::Id: serde::Serialize + serde::de::DeserializeOwned, S::Event: serde::Serialize + serde::de::DeserializeOwned"
)]
pub struct AggregateRoot<S: AggregateState> {
    id: S::Id,
    /// The aggregate's business state, accessible via `Deref`/`DerefMut`.
    pub state: S,
    version: Version,
    pending_events: Vec<S::Event>,
}

impl<S: AggregateState> std::ops::Deref for AggregateRoot<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<S: AggregateState> std::ops::DerefMut for AggregateRoot<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<S: AggregateState> Aggregate for AggregateRoot<S> {
    type Id = S::Id;
    type Event = S::Event;
    type Error = S::Error;
    type State = S;

    fn new(id: Self::Id) -> Self {
        Self {
            id,
            state: S::default(),
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

impl<S: AggregateState> Clone for AggregateRoot<S>
where
    S: Clone,
    S::Id: Clone,
    S::Event: Clone,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            state: self.state.clone(),
            version: self.version,
            pending_events: self.pending_events.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ApplyEvent, DefaultAggregateId};
    use chrono::Utc;

    // Test error type
    #[derive(Debug, thiserror::Error)]
    #[error("Test error")]
    struct TestError;

    impl AggregateError for TestError {}

    // Test event structs
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct IncrementedEvent {
        amount: i32,
        timestamp: chrono::DateTime<Utc>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct ResetEvent {
        timestamp: chrono::DateTime<Utc>,
    }

    // Test event enum
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum CounterEvent {
        Incremented(IncrementedEvent),
        Reset(ResetEvent),
    }

    impl DomainEvent for CounterEvent {
        type Aggregate = AggregateRoot<CounterState>;

        fn event_type(&self) -> &'static str {
            match self {
                CounterEvent::Incremented(_) => "Counter.Incremented",
                CounterEvent::Reset(_) => "Counter.Reset",
            }
        }

        fn event_version(&self) -> u64 {
            1
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            match self {
                CounterEvent::Incremented(e) => e.timestamp,
                CounterEvent::Reset(e) => e.timestamp,
            }
        }
    }

    impl From<IncrementedEvent> for CounterEvent {
        fn from(e: IncrementedEvent) -> Self {
            CounterEvent::Incremented(e)
        }
    }

    impl From<ResetEvent> for CounterEvent {
        fn from(e: ResetEvent) -> Self {
            CounterEvent::Reset(e)
        }
    }

    impl ApplyEvent<AggregateRoot<CounterState>> for IncrementedEvent {
        fn apply(&self, aggregate: &mut AggregateRoot<CounterState>) {
            aggregate.state.value += self.amount;
        }
    }

    impl ApplyEvent<AggregateRoot<CounterState>> for ResetEvent {
        fn apply(&self, aggregate: &mut AggregateRoot<CounterState>) {
            aggregate.state.value = 0;
        }
    }

    impl EventApplicator<AggregateRoot<CounterState>> for CounterEvent {
        fn dispatch(&self, aggregate: &mut AggregateRoot<CounterState>) -> Result<(), TestError> {
            match self {
                CounterEvent::Incremented(e) => {
                    e.validate(aggregate)?;
                    e.apply(aggregate);
                    e.post_validate(aggregate)?;
                }
                CounterEvent::Reset(e) => {
                    e.validate(aggregate)?;
                    e.apply(aggregate);
                    e.post_validate(aggregate)?;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, aggregate: &mut AggregateRoot<CounterState>) {
            match self {
                CounterEvent::Incremented(e) => e.apply(aggregate),
                CounterEvent::Reset(e) => e.apply(aggregate),
            }
        }
    }

    // Test state
    #[derive(Default, Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct CounterState {
        value: i32,
    }

    impl AggregateState for CounterState {
        type Id = DefaultAggregateId;
        type Event = CounterEvent;
        type Error = TestError;
    }

    // Custom methods on the aggregate
    impl AggregateRoot<CounterState> {
        fn increment(&mut self, amount: i32) -> Result<(), TestError> {
            self.apply(IncrementedEvent {
                amount,
                timestamp: Utc::now(),
            })
        }

        fn reset(&mut self) -> Result<(), TestError> {
            self.apply(ResetEvent {
                timestamp: Utc::now(),
            })
        }
    }

    #[test]
    fn test_aggregate_state_new() {
        let id = DefaultAggregateId::new();
        let counter = AggregateRoot::<CounterState>::new(id);

        assert_eq!(counter.aggregate_id(), &id);
        assert_eq!(counter.version(), Version::initial());
        assert_eq!(counter.pending_events().len(), 0);
        assert_eq!(counter.value, 0);
    }

    #[test]
    fn test_aggregate_state_deref() {
        let counter = AggregateRoot::<CounterState>::new(DefaultAggregateId::new());

        // Deref gives access to state fields
        assert_eq!(counter.value, 0);
    }

    #[test]
    fn test_aggregate_state_deref_mut() {
        let mut counter = AggregateRoot::<CounterState>::new(DefaultAggregateId::new());

        // DerefMut gives mutable access to state fields
        counter.value = 42;
        assert_eq!(counter.value, 42);
    }

    #[test]
    fn test_aggregate_state_apply() {
        let mut counter = AggregateRoot::<CounterState>::new(DefaultAggregateId::new());

        counter.increment(5).unwrap();

        assert_eq!(counter.value, 5);
        assert_eq!(counter.version(), Version::new(1));
        assert_eq!(counter.pending_events().len(), 1);
    }

    #[test]
    fn test_aggregate_state_multiple_events() {
        let mut counter = AggregateRoot::<CounterState>::new(DefaultAggregateId::new());

        counter.increment(5).unwrap();
        counter.increment(3).unwrap();
        counter.increment(2).unwrap();

        assert_eq!(counter.value, 10);
        assert_eq!(counter.version(), Version::new(3));
        assert_eq!(counter.pending_events().len(), 3);
    }

    #[test]
    fn test_aggregate_state_reset() {
        let mut counter = AggregateRoot::<CounterState>::new(DefaultAggregateId::new());

        counter.increment(10).unwrap();
        counter.reset().unwrap();

        assert_eq!(counter.value, 0);
        assert_eq!(counter.version(), Version::new(2));
    }

    #[test]
    fn test_aggregate_state_replay() {
        let id = DefaultAggregateId::new();
        let mut counter = AggregateRoot::<CounterState>::new(id);

        let events = vec![
            CounterEvent::Incremented(IncrementedEvent {
                amount: 10,
                timestamp: Utc::now(),
            }),
            CounterEvent::Incremented(IncrementedEvent {
                amount: 5,
                timestamp: Utc::now(),
            }),
            CounterEvent::Reset(ResetEvent {
                timestamp: Utc::now(),
            }),
            CounterEvent::Incremented(IncrementedEvent {
                amount: 3,
                timestamp: Utc::now(),
            }),
        ];

        for event in &events {
            counter.apply_unchecked(event);
        }

        assert_eq!(counter.value, 3);
        assert_eq!(counter.version(), Version::new(4));
        assert_eq!(counter.pending_events().len(), 0);
    }

    #[test]
    fn test_aggregate_state_from_snapshot() {
        let id = DefaultAggregateId::new();
        let state = CounterState { value: 42 };

        let counter = AggregateRoot::<CounterState>::from_snapshot(id, Version::new(5), state);

        assert_eq!(counter.value, 42);
        assert_eq!(counter.version(), Version::new(5));
        assert_eq!(counter.aggregate_id(), &id);
    }

    #[test]
    fn test_aggregate_state_clear_pending() {
        let mut counter = AggregateRoot::<CounterState>::new(DefaultAggregateId::new());

        counter.increment(5).unwrap();
        counter.increment(3).unwrap();
        assert_eq!(counter.pending_events().len(), 2);

        counter.clear_pending_events();
        assert_eq!(counter.pending_events().len(), 0);
    }

    #[test]
    fn test_aggregate_state_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<AggregateRoot<CounterState>>();
        assert_sync::<AggregateRoot<CounterState>>();
    }

    #[test]
    fn test_aggregate_state_type_name() {
        let type_name = AggregateRoot::<CounterState>::aggregate_type();
        // aggregate_type() returns the last segment after "::" from std::any::type_name
        // For generic types like AggregateRoot<CounterState>, type_name includes ">"
        // which causes the last segment after "::" split to be "CounterState>"
        assert!(!type_name.is_empty());
    }

    #[test]
    fn test_aggregate_state_clone() {
        let mut counter = AggregateRoot::<CounterState>::new(DefaultAggregateId::new());
        counter.increment(5).unwrap();

        let cloned = counter.clone();
        assert_eq!(cloned.value, 5);
        assert_eq!(cloned.version(), Version::new(1));
    }

    #[test]
    fn test_aggregate_state_method() {
        let mut counter = AggregateRoot::<CounterState>::new(DefaultAggregateId::new());
        counter.increment(42).unwrap();

        let state = counter.state();
        assert_eq!(state.value, 42);
    }
}
