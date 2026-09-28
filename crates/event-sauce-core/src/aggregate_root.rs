//! `AggregateRoot<A>` — infrastructure wrapper for event-sourced aggregates.
//!
//! Provides version tracking, pending event management, and event application
//! for any type implementing `Aggregate`. Access entity fields via `Deref`
//! (read-only); state changes must go through `apply()`.

use crate::{
    Aggregate, AggregateVersion, DefaultEntity, DeleteEvent, DeletedAggregateRoot, EntityId,
    EventApplicator, EventMetadata,
};

/// A pending event with optional actor and metadata information.
///
/// Wraps an event with the entity ID of the actor who caused it
/// and optional metadata for causation tracking. At commit time,
/// `actor_id` flows to `EventEnvelope::created_by` and `metadata`
/// merges into `EventEnvelope::metadata`.
#[derive(Debug)]
pub(crate) struct PendingEvent<E> {
    pub event: E,
    pub actor_id: Option<EntityId>,
    pub metadata: Option<EventMetadata>,
}

/// Builds event envelopes from pending events, attaching actor and metadata.
///
/// Shared by the event-sourced commit path and the state-stored save path.
#[cfg(any(feature = "event-sourcing", feature = "state-store"))]
pub(crate) fn envelopes_from_pending<E: crate::DomainEvent>(
    pending: &[PendingEvent<E>],
    aggregate_id: uuid::Uuid,
) -> crate::Result<Vec<crate::EventEnvelope>> {
    pending
        .iter()
        .map(|pe| {
            let mut envelope = pe.event.to_envelope(aggregate_id)?;
            if let Some(actor_id) = pe.actor_id {
                envelope = envelope.with_created_by(actor_id.as_uuid());
            }
            if let Some(metadata) = &pe.metadata {
                envelope = envelope.with_metadata(metadata.clone());
            }
            Ok(envelope)
        })
        .collect()
}

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
/// use event_sauce_core::{Aggregate, AggregateRoot, AggregateError, DefaultEntity, Entity, EntityId, DomainEvent, EventApplicator, AggregateVersion, EventVersion};
/// use serde::{Serialize, Deserialize};
/// use thiserror::Error;
/// use chrono::Utc;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct Counter {
///     id: EntityId,
///     value: i32,
/// }
///
/// impl Entity for Counter {
///     fn new(id: EntityId) -> Self { Self { id, value: 0 } }
///     fn entity_id(&self) -> EntityId { self.id }
/// }
/// impl DefaultEntity for Counter {}
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum CounterEvent {
///     Incremented { amount: i32 },
/// }
///
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///     fn event_type(&self) -> &'static str { "Incremented" }
///     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
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
///     type DeletedState = Self;
/// }
///
/// let mut counter = AggregateRoot::<Counter>::new(EntityId::new());
/// counter.apply(CounterEvent::Incremented { amount: 5 }).unwrap();
/// assert_eq!(counter.value, 5); // Read via Deref
/// ```
#[derive(Debug)]
pub struct AggregateRoot<A: Aggregate> {
    entity: A,
    version: AggregateVersion,
    pending_events: Vec<PendingEvent<A::Event>>,
    /// Set to `true` when an [`apply`](Self::apply) call fails *after*
    /// mutating the entity.
    ///
    /// A pre-validation refusal (caught by `EventApplicator::validate_only`
    /// before anything runs) never sets this: the entity is untouched, so the
    /// aggregate stays exactly as usable as it was. A failure that strikes
    /// once the apply closure has already run (`post_validate`, or a
    /// hand-written `EventApplicator` that does not split pre-validation out
    /// as `validate_only`) does: the entity is mutated, but the version is
    /// not bumped and the event is not recorded. There is no `Clone` bound to
    /// roll the entity back, so the aggregate is left in an inconsistent
    /// state. Rather than let that state be reused or committed silently, the
    /// root is *poisoned*: further `apply*`/`apply_delete*` calls and the
    /// commit-preparation path refuse it with [`crate::Error::InvalidState`],
    /// forcing the caller to discard and reload the aggregate.
    poisoned: bool,
}

impl<A: Aggregate> std::ops::Deref for AggregateRoot<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

// No DerefMut — state changes must go through apply()

impl<A: Aggregate + DefaultEntity> AggregateRoot<A> {
    /// Creates a new aggregate root with the given ID.
    ///
    /// The entity is initialized via `Entity::new(id)`, version starts at 0,
    /// and there are no pending events.
    ///
    /// Requires `DefaultEntity` to guarantee that `Entity::new(id)` is safe.
    /// For aggregates using init events, use
    /// [`UninitAggregateRoot::apply_init()`](crate::UninitAggregateRoot::apply_init) instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use event_sauce_core::{
    /// #     Aggregate, AggregateError, AggregateRoot, DefaultEntity, DomainEvent, Entity,
    /// #     EntityId, EventApplicator, EventVersion,
    /// # };
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # struct Counter {
    /// #     id: EntityId,
    /// #     value: i32,
    /// # }
    /// # impl Entity for Counter {
    /// #     fn new(id: EntityId) -> Self { Self { id, value: 0 } }
    /// #     fn entity_id(&self) -> EntityId { self.id }
    /// # }
    /// # impl DefaultEntity for Counter {}
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # enum CounterEvent {
    /// #     Incremented { amount: i32 },
    /// # }
    /// # impl DomainEvent for CounterEvent {
    /// #     type Aggregate = Counter;
    /// #     fn event_type(&self) -> &'static str { "Incremented" }
    /// #     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
    /// #     fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }
    /// # }
    /// # impl EventApplicator<Counter> for CounterEvent {
    /// #     fn dispatch(&self, c: &mut Counter) -> Result<(), CounterError> {
    /// #         match self { CounterEvent::Incremented { amount } => c.value += amount }
    /// #         Ok(())
    /// #     }
    /// #     fn dispatch_unchecked(&self, c: &mut Counter) {
    /// #         match self { CounterEvent::Incremented { amount } => c.value += amount }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, thiserror::Error)]
    /// # #[error("counter error")]
    /// # struct CounterError;
    /// # impl AggregateError for CounterError {}
    /// #
    /// # impl Aggregate for Counter {
    /// #     type Event = CounterEvent;
    /// #     type Error = CounterError;
    /// #     type DeletedState = Self;
    /// # }
    /// let counter = AggregateRoot::<Counter>::new(EntityId::new());
    /// ```
    #[must_use]
    pub fn new(id: EntityId) -> Self {
        Self {
            entity: A::new(id),
            version: AggregateVersion::initial(),
            pending_events: Vec::new(),
            poisoned: false,
        }
    }
}

impl<A: Aggregate> AggregateRoot<A> {
    /// Returns the entity's unique identifier.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        self.entity.entity_id()
    }

    /// Returns the current version of the aggregate.
    #[must_use]
    pub fn version(&self) -> AggregateVersion {
        self.version
    }

    /// Returns `true` if a previous [`apply`](Self::apply) call failed and
    /// poisoned this aggregate.
    ///
    /// A poisoned aggregate holds inconsistent state (its entity reflects a
    /// rejected event, but its version and pending events do not). Any further
    /// `apply*` call and the commit path refuse it with
    /// [`crate::Error::InvalidState`]. The aggregate must be discarded and
    /// reloaded from the event store.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Returns uncommitted events (without actor information).
    #[must_use]
    pub fn pending_events(&self) -> Vec<&A::Event> {
        self.pending_events.iter().map(|pe| &pe.event).collect()
    }

    /// Returns uncommitted events with actor information (for commit).
    #[cfg(any(feature = "event-sourcing", feature = "state-store"))]
    pub(crate) fn pending_events_with_actors(&self) -> &[PendingEvent<A::Event>] {
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

    /// Consumes the root and returns the entity it holds.
    ///
    /// For a caller that wanted the aggregate and not the bookkeeping — a read model
    /// assembling a payload, say. Any pending events are dropped with the root, so
    /// call it only where nothing is owed: after a save, or on a freshly loaded root.
    #[must_use]
    pub fn into_entity(self) -> A {
        self.entity
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
    /// Returns an error if validation (pre or post) fails. A pre-validation
    /// **refusal** mutates nothing and leaves the aggregate exactly as it
    /// was — it is not a poisoning, and the caller may keep using and saving
    /// it. A failure that strikes once the apply closure has already run
    /// (`post_validate`, or any error from a hand-written `EventApplicator`
    /// whose `dispatch` does not split pre-validation out as `validate_only`)
    /// **poisons** the aggregate instead (see
    /// [`is_poisoned`](Self::is_poisoned)): the entity was mutated, but the
    /// version is not bumped and the event is not recorded, leaving
    /// inconsistent state that cannot be rolled back. A poisoned aggregate is
    /// refused by the commit path with [`crate::Error::InvalidState`];
    /// discard it and reload.
    ///
    /// The apply closure must be **pure and deterministic** — it must never
    /// read the clock, an RNG, or any external state. Generate any such value
    /// once in the command and pass it in as an event field, so that replaying
    /// the stored event always reproduces the same state.
    pub fn apply<E: Into<A::Event>>(&mut self, event: E) -> Result<(), A::Error> {
        let event = event.into();
        // A REFUSAL is not a poisoning. Pre-validation runs before anything is
        // mutated, so an aggregate that refused a command is exactly as it was and
        // stays usable — which is the whole point of a typed refusal: a caller that
        // expects one (`let _ = incident.raise_severity(..)`) must still be able to
        // save what it did change.
        EventApplicator::validate_only(&event, &self.entity)?;
        if let Err(error) = EventApplicator::dispatch(&event, &mut self.entity) {
            self.poisoned = true;
            return Err(error);
        }
        self.version = self.version.next();
        self.pending_events.push(PendingEvent {
            event,
            actor_id: None,
            metadata: None,
        });
        Ok(())
    }

    /// Applies an event with actor tracking.
    ///
    /// Like [`apply()`](Self::apply), but records the actor's entity ID
    /// alongside the event. At commit time, the actor ID flows into
    /// `EventEnvelope::created_by`.
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails. Like
    /// [`apply()`](Self::apply), a failed call **poisons** the aggregate.
    pub fn apply_with_actor<E: Into<A::Event>>(
        &mut self,
        event: E,
        actor_id: EntityId,
    ) -> Result<(), A::Error> {
        let event = event.into();
        EventApplicator::validate_only(&event, &self.entity)?;
        if let Err(error) = EventApplicator::dispatch(&event, &mut self.entity) {
            self.poisoned = true;
            return Err(error);
        }
        self.version = self.version.next();
        self.pending_events.push(PendingEvent {
            event,
            actor_id: Some(actor_id),
            metadata: None,
        });
        Ok(())
    }

    /// Applies an event with causation metadata.
    ///
    /// Like [`apply()`](Self::apply), but attaches metadata to the pending
    /// event. At commit time, the metadata merges into `EventEnvelope::metadata`.
    /// Used by the policy system to propagate causation tracking.
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails. Like
    /// [`apply()`](Self::apply), a failed call **poisons** the aggregate.
    pub fn apply_with_metadata<E: Into<A::Event>>(
        &mut self,
        event: E,
        metadata: EventMetadata,
    ) -> Result<(), A::Error> {
        let event = event.into();
        if let Err(error) = EventApplicator::dispatch(&event, &mut self.entity) {
            self.poisoned = true;
            return Err(error);
        }
        self.version = self.version.next();
        self.pending_events.push(PendingEvent {
            event,
            actor_id: None,
            metadata: Some(metadata),
        });
        Ok(())
    }

    /// Sets metadata on all pending events that don't already have metadata.
    ///
    /// Used by `PolicyContext::commit()` to inject causation tracking
    /// into pending events before delegating to the event store.
    #[cfg(feature = "event-sourcing")]
    pub(crate) fn set_pending_metadata(&mut self, metadata: &EventMetadata) {
        for pe in &mut self.pending_events {
            if pe.metadata.is_none() {
                pe.metadata = Some(metadata.clone());
            }
        }
    }

    /// Applies an event without validation (for event replay).
    ///
    /// Uses `EventApplicator::dispatch_unchecked` which skips validation,
    /// then increments the version. Used when replaying historical events.
    pub fn apply_unchecked(&mut self, event: &A::Event) {
        EventApplicator::dispatch_unchecked(event, &mut self.entity);
        self.version = self.version.next();
    }

    /// Wraps an entity a store already rebuilt, at the version it was read at.
    ///
    /// This is what every [`Repository`](crate::Repository) answers a load with,
    /// whichever way it stores an aggregate: a row of typed columns, a serialized
    /// blob, an event-store snapshot plus its tail. The root owes nothing — its
    /// pending list starts empty — so a load-then-save writes nothing.
    #[must_use]
    pub fn restore(version: AggregateVersion, entity: A) -> Self {
        Self {
            entity,
            version,
            pending_events: vec![],
            poisoned: false,
        }
    }

    /// Creates an aggregate root from an init event (includes the pending event).
    ///
    /// Used by `UninitAggregateRoot::apply_init()`.
    pub(crate) fn from_init(entity: A, event: A::Event) -> Self {
        Self {
            entity,
            version: AggregateVersion::new(1),
            pending_events: vec![PendingEvent {
                event,
                actor_id: None,
                metadata: None,
            }],
            poisoned: false,
        }
    }

    /// Creates an aggregate root from an init event with actor tracking.
    ///
    /// Used by `UninitAggregateRoot::apply_init_with_actor()`.
    pub(crate) fn from_init_with_actor(entity: A, event: A::Event, actor_id: EntityId) -> Self {
        Self {
            entity,
            version: AggregateVersion::new(1),
            pending_events: vec![PendingEvent {
                event,
                actor_id: Some(actor_id),
                metadata: None,
            }],
            poisoned: false,
        }
    }

    /// Creates an aggregate root from an init event replay (no pending events).
    ///
    /// Used by `UninitAggregateRoot::apply_init_unchecked()`.
    #[cfg(feature = "event-sourcing")]
    pub(crate) fn from_init_replay(entity: A) -> Self {
        Self {
            entity,
            version: AggregateVersion::new(1),
            pending_events: vec![],
            poisoned: false,
        }
    }

    /// Creates an aggregate root for replay using `Entity::new(id)`.
    ///
    /// Used by `load()` for legacy aggregates. For init-event aggregates,
    /// the first event is always an init event, so this path is never reached.
    #[cfg(feature = "event-sourcing")]
    pub(crate) fn new_for_replay(id: EntityId) -> Self {
        Self {
            entity: A::new(id),
            version: AggregateVersion::initial(),
            pending_events: Vec::new(),
            poisoned: false,
        }
    }

    /// Returns the aggregate type name.
    #[must_use]
    pub fn aggregate_type() -> crate::AggregateType {
        A::aggregate_type()
    }

    /// Applies a delete event, consuming self and returning a deleted aggregate root.
    ///
    /// This is a type-state transition: `AggregateRoot<A>` → `DeletedAggregateRoot<A>`.
    /// The entity is consumed by `DeleteEvent::delete()`, producing `A::DeletedState`.
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails.
    pub fn apply_delete<E: DeleteEvent<A> + Into<A::Event>>(
        mut self,
        event: E,
    ) -> Result<DeletedAggregateRoot<A>, A::Error> {
        let poisoned = self.poisoned;
        event.validate_delete(&self.entity)?;
        let entity_id = self.entity.entity_id();
        let state = event.delete(self.entity);
        event.post_validate_delete(&state)?;
        let version = self.version.next();
        let mut pending = std::mem::take(&mut self.pending_events);
        pending.push(PendingEvent {
            event: event.into(),
            actor_id: None,
            metadata: None,
        });
        Ok(DeletedAggregateRoot::from_delete_with_pending(
            state, entity_id, version, pending, poisoned,
        ))
    }

    /// Applies a delete event with actor tracking.
    ///
    /// Like [`apply_delete()`](Self::apply_delete), but records the actor's entity ID
    /// alongside the event. At commit time, the actor ID flows into
    /// `EventEnvelope::created_by`.
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails.
    pub fn apply_delete_with_actor<E: DeleteEvent<A> + Into<A::Event>>(
        mut self,
        event: E,
        actor_id: EntityId,
    ) -> Result<DeletedAggregateRoot<A>, A::Error> {
        let poisoned = self.poisoned;
        event.validate_delete(&self.entity)?;
        let entity_id = self.entity.entity_id();
        let state = event.delete(self.entity);
        event.post_validate_delete(&state)?;
        let version = self.version.next();
        let mut pending = std::mem::take(&mut self.pending_events);
        pending.push(PendingEvent {
            event: event.into(),
            actor_id: Some(actor_id),
            metadata: None,
        });
        Ok(DeletedAggregateRoot::from_delete_with_pending(
            state, entity_id, version, pending, poisoned,
        ))
    }

    /// Applies a delete event without validation (for event replay).
    ///
    /// Uses `EventApplicator::dispatch_delete_unchecked` which skips validation.
    /// Consumes the aggregate root.
    #[cfg(feature = "event-sourcing")]
    pub(crate) fn apply_delete_unchecked(self, event: &A::Event) -> DeletedAggregateRoot<A> {
        let entity_id = self.entity.entity_id();
        let version = self.version.next();
        let state = EventApplicator::dispatch_delete_unchecked(event, self.entity);
        DeletedAggregateRoot::from_delete_replay(state, entity_id, version)
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
        let events: Vec<&A::Event> = self.pending_events.iter().map(|pe| &pe.event).collect();
        state.serialize_field("pending_events", &events)?;
        state.end()
    }
}

impl<E: Clone> Clone for PendingEvent<E> {
    fn clone(&self) -> Self {
        Self {
            event: self.event.clone(),
            actor_id: self.actor_id,
            metadata: self.metadata.clone(),
        }
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
            poisoned: self.poisoned,
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

    // An event whose apply mutates value first, then post_validate rejects a
    // value above the cap — used to exercise the poison path (L9).
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct CappedEvent {
        amount: i32,
        max_value: i32,
        timestamp: chrono::DateTime<Utc>,
    }

    // Test event enum
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum CounterEvent {
        Incremented(IncrementedEvent),
        Reset(ResetEvent),
        Capped(CappedEvent),
    }

    impl DomainEvent for CounterEvent {
        type Aggregate = CounterEntity;

        fn event_type(&self) -> &'static str {
            match self {
                CounterEvent::Incremented(_) => "Counter.Incremented",
                CounterEvent::Reset(_) => "Counter.Reset",
                CounterEvent::Capped(_) => "Counter.Capped",
            }
        }

        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            match self {
                CounterEvent::Incremented(e) => e.timestamp,
                CounterEvent::Reset(e) => e.timestamp,
                CounterEvent::Capped(e) => e.timestamp,
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

    impl From<CappedEvent> for CounterEvent {
        fn from(e: CappedEvent) -> Self {
            CounterEvent::Capped(e)
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

    impl ApplyEvent<CounterEntity> for CappedEvent {
        fn apply(&self, entity: &mut CounterEntity) {
            entity.value += self.amount;
        }

        fn post_validate(&self, entity: &CounterEntity) -> Result<(), TestError> {
            if entity.value > self.max_value {
                return Err(TestError);
            }
            Ok(())
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
                CounterEvent::Capped(e) => {
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
                CounterEvent::Capped(e) => e.apply(entity),
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

    impl crate::DefaultEntity for CounterEntity {}

    impl Aggregate for CounterEntity {
        type Event = CounterEvent;
        type Error = TestError;
        type DeletedState = Self;
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
        assert_eq!(counter.version(), AggregateVersion::initial());
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
        assert_eq!(counter.version(), AggregateVersion::new(1));
        assert_eq!(counter.pending_events().len(), 1);
    }

    #[test]
    fn test_aggregate_root_multiple_events() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        counter.increment(5).unwrap();
        counter.increment(3).unwrap();
        counter.increment(2).unwrap();

        assert_eq!(counter.value, 10);
        assert_eq!(counter.version(), AggregateVersion::new(3));
        assert_eq!(counter.pending_events().len(), 3);
    }

    #[test]
    fn test_aggregate_root_reset() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        counter.increment(10).unwrap();
        counter.reset().unwrap();

        assert_eq!(counter.value, 0);
        assert_eq!(counter.version(), AggregateVersion::new(2));
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
        assert_eq!(counter.version(), AggregateVersion::new(4));
        assert_eq!(counter.pending_events().len(), 0);
    }

    #[test]
    fn test_aggregate_root_restore() {
        let entity = CounterEntity {
            id: EntityId::new(),
            value: 42,
        };

        let counter = AggregateRoot::<CounterEntity>::restore(AggregateVersion::new(5), entity);

        assert_eq!(counter.value, 42);
        assert_eq!(counter.version(), AggregateVersion::new(5));
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
    fn test_aggregate_root_apply_with_actor() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        let actor_id = EntityId::new();

        counter
            .apply_with_actor(
                IncrementedEvent {
                    amount: 7,
                    timestamp: Utc::now(),
                },
                actor_id,
            )
            .unwrap();

        assert_eq!(counter.value, 7);
        assert_eq!(counter.version(), AggregateVersion::new(1));
        assert_eq!(counter.pending_events().len(), 1);

        // Verify actor_id is stored
        let pending = counter.pending_events_with_actors();
        assert_eq!(pending[0].actor_id, Some(actor_id));
    }

    #[test]
    fn test_apply_without_actor_has_none_actor_id() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        counter.increment(5).unwrap();

        let pending = counter.pending_events_with_actors();
        assert_eq!(pending[0].actor_id, None);
    }

    #[test]
    fn test_mixed_actor_and_non_actor_events() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        let actor_id = EntityId::new();

        counter.increment(5).unwrap();
        counter
            .apply_with_actor(
                IncrementedEvent {
                    amount: 3,
                    timestamp: Utc::now(),
                },
                actor_id,
            )
            .unwrap();
        counter.increment(2).unwrap();

        assert_eq!(counter.value, 10);
        assert_eq!(counter.pending_events().len(), 3);

        let pending = counter.pending_events_with_actors();
        assert_eq!(pending[0].actor_id, None);
        assert_eq!(pending[1].actor_id, Some(actor_id));
        assert_eq!(pending[2].actor_id, None);
    }

    #[test]
    fn test_aggregate_root_apply_with_metadata() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        let metadata = crate::EventMetadata::new()
            .with_correlation_id(uuid::Uuid::new_v4())
            .with_causation_id(uuid::Uuid::new_v4());

        counter
            .apply_with_metadata(
                IncrementedEvent {
                    amount: 5,
                    timestamp: Utc::now(),
                },
                metadata.clone(),
            )
            .unwrap();

        assert_eq!(counter.value, 5);
        assert_eq!(counter.version(), AggregateVersion::new(1));

        let pending = counter.pending_events_with_actors();
        assert_eq!(pending[0].metadata, Some(metadata));
    }

    #[test]
    fn test_set_pending_metadata() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        // Apply without metadata
        counter.increment(5).unwrap();
        counter.increment(3).unwrap();

        let metadata = crate::EventMetadata::new().with_correlation_id(uuid::Uuid::new_v4());

        counter.set_pending_metadata(&metadata);

        let pending = counter.pending_events_with_actors();
        assert_eq!(pending[0].metadata, Some(metadata.clone()));
        assert_eq!(pending[1].metadata, Some(metadata));
    }

    #[test]
    fn test_set_pending_metadata_does_not_overwrite_existing() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        let existing_metadata = crate::EventMetadata::new().with_causation_id(uuid::Uuid::new_v4());

        counter
            .apply_with_metadata(
                IncrementedEvent {
                    amount: 5,
                    timestamp: Utc::now(),
                },
                existing_metadata.clone(),
            )
            .unwrap();
        counter.increment(3).unwrap();

        let new_metadata = crate::EventMetadata::new().with_correlation_id(uuid::Uuid::new_v4());

        counter.set_pending_metadata(&new_metadata);

        let pending = counter.pending_events_with_actors();
        // First event keeps its existing metadata
        assert_eq!(pending[0].metadata, Some(existing_metadata));
        // Second event gets the new metadata
        assert_eq!(pending[1].metadata, Some(new_metadata));
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

    // === Poison tests (L9) ===

    #[test]
    fn test_aggregate_root_new_is_not_poisoned() {
        let counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        assert!(!counter.is_poisoned());
    }

    #[test]
    fn test_successful_apply_does_not_poison() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter.increment(5).unwrap();
        assert!(!counter.is_poisoned());
    }

    #[test]
    fn test_capped_apply_within_limit_succeeds() {
        // A CappedEvent that stays within the cap passes post_validate and
        // leaves the aggregate clean (covers the accepting branch).
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter
            .apply(CappedEvent {
                amount: 5,
                max_value: 10,
                timestamp: Utc::now(),
            })
            .unwrap();
        assert!(!counter.is_poisoned());
        assert_eq!(counter.value, 5);

        // Replaying the same event unchecked reproduces the state.
        let mut replay = AggregateRoot::<CounterEntity>::new(EntityId::new());
        replay.apply_unchecked(&CounterEvent::Capped(CappedEvent {
            amount: 5,
            max_value: 10,
            timestamp: Utc::now(),
        }));
        assert_eq!(replay.value, 5);
    }

    #[test]
    fn test_failed_apply_poisons_aggregate() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        // CappedEvent's apply mutates value first, then post_validate rejects a
        // value above the cap — so the event is rejected after the entity is
        // already mutated through the real apply() path.
        let result = counter.apply(CappedEvent {
            amount: 25,
            max_value: 20,
            timestamp: Utc::now(),
        });

        assert!(result.is_err());
        assert!(
            counter.is_poisoned(),
            "a rejected apply must poison the aggregate"
        );
        // The version was NOT bumped and no event was recorded, yet the entity
        // was already mutated by the apply closure: this is the inconsistency
        // the poison flag guards against.
        assert_eq!(counter.version(), AggregateVersion::initial());
        assert_eq!(counter.pending_events().len(), 0);
        assert_eq!(counter.value, 25);
    }

    #[test]
    fn test_failed_apply_with_actor_poisons_aggregate() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        let result = counter.apply_with_actor(
            CappedEvent {
                amount: 25,
                max_value: 20,
                timestamp: Utc::now(),
            },
            EntityId::new(),
        );

        assert!(result.is_err());
        assert!(counter.is_poisoned());
    }

    #[test]
    fn test_failed_apply_with_metadata_poisons_aggregate() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        let result = counter.apply_with_metadata(
            CappedEvent {
                amount: 25,
                max_value: 20,
                timestamp: Utc::now(),
            },
            crate::EventMetadata::new(),
        );

        assert!(result.is_err());
        assert!(counter.is_poisoned());
    }

    #[test]
    fn test_counter_event_domain_event_accessors() {
        // Exercises the DomainEvent accessors across all CounterEvent variants,
        // including the Capped variant used by the poison tests.
        let ts = Utc::now();
        let events = [
            CounterEvent::Incremented(IncrementedEvent {
                amount: 1,
                timestamp: ts,
            }),
            CounterEvent::Reset(ResetEvent { timestamp: ts }),
            CounterEvent::Capped(CappedEvent {
                amount: 1,
                max_value: 1,
                timestamp: ts,
            }),
        ];
        let expected_types = ["Counter.Incremented", "Counter.Reset", "Counter.Capped"];
        for (event, expected) in events.iter().zip(expected_types) {
            assert_eq!(event.event_type(), expected);
            assert_eq!(event.event_version(), crate::EventVersion::new(1));
            assert_eq!(event.occurred_at(), ts);
        }
    }

    #[test]
    fn test_clone_preserves_poison() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        let _ = counter.apply(CappedEvent {
            amount: 25,
            max_value: 20,
            timestamp: Utc::now(),
        });
        assert!(counter.is_poisoned());

        let cloned = counter.clone();
        assert!(cloned.is_poisoned());
    }

    // === Delete event tests ===

    struct ClosedEvent {
        timestamp: chrono::DateTime<Utc>,
    }

    impl crate::DeleteEvent<CounterEntity> for ClosedEvent {
        fn delete(&self, mut entity: CounterEntity) -> CounterEntity {
            entity.value = -1; // Mark as closed
            entity
        }
    }

    impl From<ClosedEvent> for CounterEvent {
        fn from(e: ClosedEvent) -> Self {
            CounterEvent::Reset(ResetEvent {
                timestamp: e.timestamp,
            })
        }
    }

    #[test]
    fn test_aggregate_root_apply_delete() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter.increment(10).unwrap();

        let id = counter.entity_id();
        let deleted = counter
            .apply_delete(ClosedEvent {
                timestamp: Utc::now(),
            })
            .unwrap();

        assert_eq!(deleted.entity_id(), id);
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.version(), AggregateVersion::new(2));
        // Pending events include the increment + the delete event
        assert_eq!(deleted.pending_events().len(), 2);
    }

    #[test]
    fn test_aggregate_root_apply_delete_transfers_pending_events() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter.increment(5).unwrap();
        counter.increment(3).unwrap();

        let deleted = counter
            .apply_delete(ClosedEvent {
                timestamp: Utc::now(),
            })
            .unwrap();

        // 2 increments + 1 delete = 3 pending events
        assert_eq!(deleted.pending_events().len(), 3);
        assert_eq!(deleted.version(), AggregateVersion::new(3));
    }

    #[test]
    fn test_aggregate_root_apply_delete_with_actor() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter.increment(10).unwrap();

        let actor_id = EntityId::new();
        let deleted = counter
            .apply_delete_with_actor(
                ClosedEvent {
                    timestamp: Utc::now(),
                },
                actor_id,
            )
            .unwrap();

        assert_eq!(deleted.version(), AggregateVersion::new(2));
        let pending = deleted.pending_events_with_actors();
        // First event (increment) has no actor
        assert_eq!(pending[0].actor_id, None);
        // Delete event has actor
        assert_eq!(pending[1].actor_id, Some(actor_id));
    }

    // Delete event with validation
    struct ValidatedCloseEvent {
        timestamp: chrono::DateTime<Utc>,
    }

    impl crate::DeleteEvent<CounterEntity> for ValidatedCloseEvent {
        fn validate_delete(&self, entity: &CounterEntity) -> Result<(), TestError> {
            if entity.value > 0 {
                return Err(TestError);
            }
            Ok(())
        }
    }

    impl From<ValidatedCloseEvent> for CounterEvent {
        fn from(e: ValidatedCloseEvent) -> Self {
            CounterEvent::Reset(ResetEvent {
                timestamp: e.timestamp,
            })
        }
    }

    #[test]
    fn test_aggregate_root_apply_delete_validation_fails() {
        let mut counter = AggregateRoot::<CounterEntity>::new(EntityId::new());
        counter.increment(10).unwrap();

        let result = counter.apply_delete(ValidatedCloseEvent {
            timestamp: Utc::now(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_aggregate_root_apply_delete_validation_succeeds() {
        let counter = AggregateRoot::<CounterEntity>::new(EntityId::new());

        let deleted = counter
            .apply_delete(ValidatedCloseEvent {
                timestamp: Utc::now(),
            })
            .unwrap();

        assert_eq!(deleted.version(), AggregateVersion::new(1));
    }
}
