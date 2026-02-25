//! `AggregateRoot<A>` — infrastructure wrapper for event-sourced aggregates.
//!
//! Provides version tracking, pending event management, and event application
//! for any type implementing `Aggregate`. Access entity fields via `Deref`
//! (read-only); state changes must go through `apply()`.

use crate::{Aggregate, EntityId, EventApplicator, Version};

/// Infrastructure wrapper for event-sourced aggregates.
///
/// Wraps an entity that implements `Aggregate`, providing all infrastructure
/// concerns: version tracking, pending event management, and event application
/// lifecycle (validate → apply → `post_validate`).
///
/// # Read-Only Access via Deref
///
/// `AggregateRoot<A>` implements `Deref<Target = A>` for read-only access
/// to entity fields. It does **not** implement `DerefMut`—all state mutations
/// must go through `apply()`, ensuring events are the only mutation path.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Aggregate, AggregateRoot, AggregateError, Entity, EntityId, DomainEvent, EventApplicator, Version};
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
///     fn new(id: EntityId) -> Self { Self { id, value: 0 } }
///     fn entity_id(&self) -> EntityId { self.id }
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum CounterEvent {
///     Incremented { amount: i32 },
/// }
///
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///     fn event_type(&self) -> &'static str { "Incremented" }
///     fn event_version(&self) -> u64 { 1 }
///     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
/// }
///
/// impl EventApplicator<Counter> for CounterEvent {
///     fn dispatch(&self, c: &mut Counter) -> Result<(), CounterError> {
///         match self { CounterEvent::Incremented { amount } => c.value += amount }
///         Ok(())
///     }
///     fn dispatch_unchecked(&self, c: &mut Counter) {
///         match self { CounterEvent::Incremented { amount } => c.value += amount }
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
///
/// let mut counter = AggregateRoot::<Counter>::new(EntityId::new());
/// counter.apply(CounterEvent::Incremented { amount: 5 }).unwrap();
/// assert_eq!(counter.value, 5); // Read via Deref
/// ```
#[derive(Debug)]
pub struct AggregateRoot<A: Aggregate> {
    entity: A,
    version: Version,
    pending_events: Vec<A::Event>,
}

impl<A: Aggregate> std::ops::Deref for AggregateRoot<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

// No DerefMut — state changes must go through apply()

impl<A: Aggregate> AggregateRoot<A> {
    /// Creates a new aggregate root with the given ID.
    ///
    /// The entity is initialized via `Entity::new(id)`, version starts at 0,
    /// and there are no pending events.
    ///
    /// # Examples
    ///
    /// ```
    /// # use event_sauce_core::{AggregateRoot, EntityId};
    /// # use event_sauce_core::test_fixtures::TestCounter;
    /// let counter = AggregateRoot::<TestCounter>::new(EntityId::new());
    /// ```
    #[must_use]
    pub fn new(id: EntityId) -> Self {
        Self {
            entity: A::new(id),
            version: Version::initial(),
            pending_events: Vec::new(),
        }
    }

    /// Returns the entity's unique identifier.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        self.entity.entity_id()
    }

    /// Returns the current version of the aggregate.
    #[must_use]
    pub fn version(&self) -> Version {
        self.version
    }

    /// Returns uncommitted events.
    #[must_use]
    pub fn pending_events(&self) -> &[A::Event] {
        &self.pending_events
    }

    /// Clears all pending events.
    ///
    /// Called after events have been successfully persisted.
    pub fn clear_pending_events(&mut self) {
        self.pending_events.clear();
    }

    /// Returns a reference to the inner entity.
    #[must_use]
    pub fn entity(&self) -> &A {
        &self.entity
    }

    /// Applies an event to update the entity's state and records it.
    ///
    /// This method:
    /// 1. Converts the event into the aggregate's event type (via `Into`)
    /// 2. Dispatches through `EventApplicator` (validate → apply → `post_validate`)
    /// 3. Increments the version
    /// 4. Adds the event to pending events
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails.
    pub fn apply<E: Into<A::Event>>(&mut self, event: E) -> Result<(), A::Error> {
        let event = event.into();
        EventApplicator::dispatch(&event, &mut self.entity)?;
        self.version = self.version.next();
        self.pending_events.push(event);
        Ok(())
    }

    /// Applies an event without validation (for event replay).
    ///
    /// Uses `EventApplicator::dispatch_unchecked` which skips validation,
    /// then increments the version. Used when replaying historical events.
    pub fn apply_unchecked(&mut self, event: &A::Event) {
        EventApplicator::dispatch_unchecked(event, &mut self.entity);
        self.version = self.version.next();
    }

    /// Reconstructs an aggregate root from a snapshot.
    ///
    /// Used when loading from the event store with snapshot support.
    #[must_use]
    pub fn from_snapshot(version: Version, entity: A) -> Self {
        Self {
            entity,
            version,
            pending_events: Vec::new(),
        }
    }

    /// Returns the aggregate type name.
    #[must_use]
    pub fn aggregate_type() -> &'static str {
        A::aggregate_type()
    }
}

impl<A: Aggregate> serde::Serialize for AggregateRoot<A>
where
    A: serde::Serialize,
    A::Event: serde::Serialize,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("AggregateRoot", 3)?;
        state.serialize_field("entity", &self.entity)?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("pending_events", &self.pending_events)?;
        state.end()
    }
}

impl<A: Aggregate> Clone for AggregateRoot<A>
where
    A: Clone,
    A::Event: Clone,
{
    fn clone(&self) -> Self {
        Self {
            entity: self.entity.clone(),
            version: self.version,
            pending_events: self.pending_events.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateError, ApplyEvent, DomainEvent, EntityId};
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
        type Aggregate = CounterEntity;

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

    impl ApplyEvent<CounterEntity> for IncrementedEvent {
        fn apply(&self, entity: &mut CounterEntity) {
            entity.value += self.amount;
        }
    }

    impl ApplyEvent<CounterEntity> for ResetEvent {
        fn apply(&self, entity: &mut CounterEntity) {
            entity.value = 0;
        }
    }

    impl EventApplicator<CounterEntity> for CounterEvent {
        fn dispatch(&self, entity: &mut CounterEntity) -> Result<(), TestError> {
            match self {
                CounterEvent::Incremented(e) => {
                    e.validate(entity)?;
                    e.apply(entity);
                    e.post_validate(entity)?;
                }
                CounterEvent::Reset(e) => {
                    e.validate(entity)?;
                    e.apply(entity);
                    e.post_validate(entity)?;
                }
            }
            Ok(())
        }

        fn dispatch_unchecked(&self, entity: &mut CounterEntity) {
            match self {
                CounterEvent::Incremented(e) => e.apply(entity),
                CounterEvent::Reset(e) => e.apply(entity),
            }
        }
    }

    // Test entity
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct CounterEntity {
        id: EntityId,
        value: i32,
    }

    impl crate::Entity for CounterEntity {
        fn new(id: EntityId) -> Self {
            Self { id, value: 0 }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl Aggregate for CounterEntity {
        type Event = CounterEvent;
        type Error = TestError;
    }

    // Custom methods on the aggregate root
    impl AggregateRoot<CounterEntity> {
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
    fn test_aggregate_root_new() {
        let id = EntityId::new();
        let counter = AggregateRoot::<CounterEntity>::new(id);

        assert_eq!(counter.entity_id(), id);
        assert_eq!(counter.version(), Version::initial());
        assert_eq!(counter.pending_events().len(), 0);
        assert_eq!(counter.value, 0);
    }

    #[test]
    fn test_aggregate_root_deref() {
        let counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        // Deref gives read-only access to entity fields
        assert_eq!(counter.value, 0);
    }

    // NOTE: DerefMut is intentionally not implemented for AggregateRoot.
    // This is a compile-time guarantee enforced by the type system.
    // See trybuild tests for compile-fail verification if needed.

    #[test]
    fn test_aggregate_root_apply() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        counter.increment(5).unwrap();

        assert_eq!(counter.value, 5);
        assert_eq!(counter.version(), Version::new(1));
        assert_eq!(counter.pending_events().len(), 1);
    }

    #[test]
    fn test_aggregate_root_multiple_events() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        counter.increment(5).unwrap();
        counter.increment(3).unwrap();
        counter.increment(2).unwrap();

        assert_eq!(counter.value, 10);
        assert_eq!(counter.version(), Version::new(3));
        assert_eq!(counter.pending_events().len(), 3);
    }

    #[test]
    fn test_aggregate_root_reset() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        counter.increment(10).unwrap();
        counter.reset().unwrap();

        assert_eq!(counter.value, 0);
        assert_eq!(counter.version(), Version::new(2));
    }

    #[test]
    fn test_aggregate_root_replay() {
        let id = EntityId::new();
        let mut counter = AggregateRoot::<CounterEntity>::new(id);

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
    fn test_aggregate_root_from_snapshot() {
        let entity = CounterEntity {
            id: EntityId::new(),
            value: 42,
        };

        let counter = AggregateRoot::<CounterEntity>::from_snapshot(Version::new(5), entity);

        assert_eq!(counter.value, 42);
        assert_eq!(counter.version(), Version::new(5));
    }

    #[test]
    fn test_aggregate_root_clear_pending() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        counter.increment(5).unwrap();
        counter.increment(3).unwrap();
        assert_eq!(counter.pending_events().len(), 2);

        counter.clear_pending_events();
        assert_eq!(counter.pending_events().len(), 0);
    }

    #[test]
    fn test_aggregate_root_type_name() {
        let type_name = AggregateRoot::<CounterEntity>::aggregate_type();
        assert_eq!(type_name, "CounterEntity");
    }

    #[test]
    fn test_aggregate_root_entity_method() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter.increment(42).unwrap();

        let entity = counter.entity();
        assert_eq!(entity.value, 42);
    }

    #[test]
    fn test_aggregate_root_serialize() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter.increment(5).unwrap();
        counter.increment(3).unwrap();

        let json = serde_json::to_value(&counter).unwrap();

        assert!(
            json.get("entity").is_some(),
            "JSON should have 'entity' field"
        );
        assert!(
            json.get("version").is_some(),
            "JSON should have 'version' field"
        );
        assert!(
            json.get("pending_events").is_some(),
            "JSON should have 'pending_events' field"
        );

        assert_eq!(json["entity"]["value"], 8);
        assert_eq!(json["version"], 2);
        assert_eq!(json["pending_events"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_aggregate_root_clone_preserves_all_fields() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter.increment(7).unwrap();
        counter.increment(3).unwrap();

        let cloned = counter.clone();

        assert_eq!(cloned.value, counter.value);
        assert_eq!(cloned.entity_id(), counter.entity_id());
        assert_eq!(cloned.version(), counter.version());
        assert_eq!(
            cloned.pending_events().len(),
            counter.pending_events().len()
        );
    }
}
