//! `AggregateRoot<A>` — infrastructure wrapper for event-sourced aggregates.
//!
//! Provides version tracking, pending event management, and event application
//! for any type implementing `Aggregate`. Access entity fields via `Deref`
//! (read-only); state changes must go through `apply()`.

use crate::{
    Aggregate, AggregateVersion, DefaultEntity, DeleteEvent, DeletedAggregateRoot, EntityId,
    EventApplicator, EventMetadata, StoredVersion,
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
    /// Set when a `dispatch` fails; see [`is_poisoned`](Self::is_poisoned).
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
    /// rejected event, but its version and pending events do not). The
    /// commit path (event-sourced commit and state-store save alike) checks
    /// this and refuses with [`crate::Error::InvalidState`] — but further
    /// `apply*`/`apply_delete*` calls do **not** check it, and keep mutating
    /// the same inconsistent entity. Check this after any `apply*` failure;
    /// if it is `true`, discard the aggregate and reload it rather than
    /// issue further commands on it.
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
    #[cfg(any(test, feature = "event-sourcing", feature = "state-store"))]
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
    /// 2. Runs [`EventApplicator::validate_only`](crate::EventApplicator::validate_only)
    /// 3. Dispatches through `EventApplicator::dispatch` (validate → apply →
    ///    `post_validate`)
    /// 4. Increments the version
    /// 5. Adds the event to pending events
    ///
    /// # Errors
    ///
    /// A `validate_only` refusal (step 2) leaves the aggregate untouched and
    /// usable; a `dispatch` failure (step 3) **poisons** it instead — see
    /// [`is_poisoned`](Self::is_poisoned) and
    /// [`EventApplicator::validate_only`](crate::EventApplicator::validate_only)
    /// for which failures land in which bucket.
    ///
    /// The apply closure must be **pure and deterministic** — it must never
    /// read the clock, an RNG, or any external state. Generate any such value
    /// once in the command and pass it in as an event field, so that replaying
    /// the stored event always reproduces the same state.
    pub fn apply<E: Into<A::Event>>(&mut self, event: E) -> Result<(), A::Error> {
        let event = event.into();
        EventApplicator::validate_only(&event, &self.entity)?;
        self.dispatch_and_record(PendingEvent {
            event,
            actor_id: None,
            metadata: None,
        })
    }

    /// Applies an event with actor tracking.
    ///
    /// Like [`apply()`](Self::apply), but records the actor's entity ID
    /// alongside the event. At commit time, the actor ID flows into
    /// `EventEnvelope::created_by`.
    ///
    /// # Errors
    ///
    /// Returns an error if validation or dispatch fails. Like
    /// [`apply()`](Self::apply): a `validate_only` refusal does not poison
    /// the aggregate, while a `dispatch` failure does — see
    /// [`apply()`](Self::apply) for the full split.
    pub fn apply_with_actor<E: Into<A::Event>>(
        &mut self,
        event: E,
        actor_id: EntityId,
    ) -> Result<(), A::Error> {
        let event = event.into();
        EventApplicator::validate_only(&event, &self.entity)?;
        self.dispatch_and_record(PendingEvent {
            event,
            actor_id: Some(actor_id),
            metadata: None,
        })
    }

    /// Applies an event with causation metadata.
    ///
    /// Like [`apply()`](Self::apply), but attaches metadata to the pending
    /// event. At commit time, the metadata merges into `EventEnvelope::metadata`.
    /// Used by the policy system to propagate causation tracking.
    ///
    /// Unlike [`apply()`](Self::apply), this skips
    /// [`EventApplicator::validate_only`](crate::EventApplicator::validate_only)
    /// and dispatches directly, so it has no refusal-vs-poison split: even a
    /// `define_events!` event refused by its `@validate` clause poisons the
    /// aggregate through this path.
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails. Every failure
    /// **poisons** the aggregate.
    pub fn apply_with_metadata<E: Into<A::Event>>(
        &mut self,
        event: E,
        metadata: EventMetadata,
    ) -> Result<(), A::Error> {
        self.dispatch_and_record(PendingEvent {
            event: event.into(),
            actor_id: None,
            metadata: Some(metadata),
        })
    }

    /// Dispatches a pending event's inner event and, on success, records it.
    ///
    /// Shared by `apply`, `apply_with_actor` and `apply_with_metadata`: each
    /// has already decided whether `validate_only` runs first, and hands
    /// this the `PendingEvent` it wants recorded if `dispatch` succeeds.
    fn dispatch_and_record(&mut self, pending: PendingEvent<A::Event>) -> Result<(), A::Error> {
        if let Err(error) = EventApplicator::dispatch(&pending.event, &mut self.entity) {
            self.poisoned = true;
            return Err(error);
        }
        self.version = self.version.next();
        self.pending_events.push(pending);
        Ok(())
    }

    /// Sets metadata on all pending events that don't already have metadata.
    ///
    /// Used by `PolicyContext::commit()` to inject causation tracking
    /// into pending events before delegating to the event store.
    #[cfg(any(test, feature = "event-sourcing"))]
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

    /// Converts what a store read back into the entity and wraps it in a
    /// root, at the version it was read at.
    ///
    /// This is what every [`Repository`](crate::Repository) answers a load with,
    /// whichever way it stores an aggregate: a row of typed columns, a serialized
    /// blob, an event-store snapshot plus its tail. The root owes nothing — its
    /// pending list starts empty — so a load-then-save writes nothing.
    ///
    /// `stored` is whatever shape the store hands back — the entity itself,
    /// or a row it reads into first — and is converted into `A` through
    /// `TryFrom`. The version read back from storage becomes a
    /// [`StoredVersion`] through `StoredVersion::try_from`, which refuses
    /// one no stored aggregate can have: that check happens once, where the
    /// version enters from storage.
    ///
    /// # Errors
    ///
    /// Returns `<A as TryFrom<R>>::Error` as is when `stored` fails to
    /// convert into `A`. Passing the entity itself (`R = A`) can never
    /// fail — its error type is [`Infallible`](std::convert::Infallible),
    /// so the result is always `Ok` and destructures irrefutably on stable:
    /// `let Ok(root) = AggregateRoot::restore(version, entity);`.
    ///
    /// # Examples
    ///
    /// Restoring from a row the store reads back, whose conversion to the
    /// entity can be refused:
    ///
    /// ```
    /// # use event_sauce_core::{Aggregate, AggregateError, AggregateRoot, AggregateVersion, DomainEvent, Entity, EntityId, EventApplicator, EventVersion, StoredVersion};
    /// # use chrono::Utc;
    /// # use thiserror::Error;
    /// # #[derive(Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Counter { id: EntityId, value: i32 }
    /// # impl Entity for Counter {
    /// #     fn new(id: EntityId) -> Self { Self { id, value: 0 } }
    /// #     fn entity_id(&self) -> EntityId { self.id }
    /// # }
    /// # #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    /// # enum CounterEvent { Incremented { amount: i32 } }
    /// # impl DomainEvent for CounterEvent {
    /// #     type Aggregate = Counter;
    /// #     fn event_type(&self) -> &'static str { "Incremented" }
    /// #     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
    /// #     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
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
    /// # #[derive(Debug, thiserror::Error)]
    /// # #[error("Counter error")]
    /// # struct CounterError;
    /// # impl AggregateError for CounterError {}
    /// # impl Aggregate for Counter {
    /// #     type Event = CounterEvent;
    /// #     type Error = CounterError;
    /// #     type DeletedState = Self;
    /// # }
    /// /// A row as the store reads it back: `value` has not been checked yet.
    /// struct CounterRow { id: EntityId, value: i32 }
    ///
    /// #[derive(Debug, Error)]
    /// #[error("counter value cannot be negative: {0}")]
    /// struct NegativeValue(i32);
    ///
    /// impl TryFrom<CounterRow> for Counter {
    ///     type Error = NegativeValue;
    ///     fn try_from(row: CounterRow) -> Result<Self, Self::Error> {
    ///         if row.value < 0 {
    ///             return Err(NegativeValue(row.value));
    ///         }
    ///         Ok(Self { id: row.id, value: row.value })
    ///     }
    /// }
    ///
    /// let version = StoredVersion::try_from(AggregateVersion::new(3))?;
    ///
    /// let row = CounterRow { id: EntityId::new(), value: 42 };
    /// let counter = AggregateRoot::<Counter>::restore(version, row)?;
    /// assert_eq!(counter.value, 42);
    /// assert_eq!(counter.version(), AggregateVersion::new(3));
    /// assert!(counter.pending_events().is_empty());
    ///
    /// let bad_row = CounterRow { id: EntityId::new(), value: -1 };
    /// assert!(AggregateRoot::<Counter>::restore(version, bad_row).is_err());
    ///
    /// let entity = Counter { id: EntityId::new(), value: 7 };
    /// let Ok(counter) = AggregateRoot::<Counter>::restore(version, entity);
    /// assert_eq!(counter.value, 7);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn restore<R>(version: StoredVersion, stored: R) -> Result<Self, <A as TryFrom<R>>::Error>
    where
        A: TryFrom<R>,
    {
        Ok(Self {
            entity: A::try_from(stored)?,
            version: version.into(),
            pending_events: vec![],
            poisoned: false,
        })
    }

    /// Starts replaying a legacy (non-init-event) aggregate from a freshly
    /// constructed entity, at version 0, with no pending events.
    ///
    /// Unlike [`restore`](Self::restore), this is not reconstructing
    /// previously-stored state — the version genuinely starts at 0 because no
    /// event has been applied yet; the caller immediately replays the event
    /// stream onto it with [`apply_unchecked`](Self::apply_unchecked). Used
    /// by `load_any()`.
    #[cfg(feature = "event-sourcing")]
    pub(crate) fn start_replay(entity: A) -> Self {
        Self {
            entity,
            version: AggregateVersion::initial(),
            pending_events: vec![],
            poisoned: false,
        }
    }

    /// Creates an aggregate root from an init event, recorded as its one
    /// pending event at version 1.
    ///
    /// Used by `UninitAggregateRoot::apply_init()`/`apply_init_with_actor()`.
    pub(crate) fn from_init(entity: A, event: A::Event, actor_id: Option<EntityId>) -> Self {
        Self {
            entity,
            version: AggregateVersion::new(1),
            pending_events: vec![PendingEvent {
                event,
                actor_id,
                metadata: None,
            }],
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
        self,
        event: E,
    ) -> Result<DeletedAggregateRoot<A>, A::Error> {
        self.delete_as(event, None)
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
        self,
        event: E,
        actor_id: EntityId,
    ) -> Result<DeletedAggregateRoot<A>, A::Error> {
        self.delete_as(event, Some(actor_id))
    }

    fn delete_as<E: DeleteEvent<A> + Into<A::Event>>(
        mut self,
        event: E,
        actor_id: Option<EntityId>,
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
            actor_id,
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
        DeletedAggregateRoot::from_delete_with_pending(state, entity_id, version, Vec::new(), false)
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
mod tests;
