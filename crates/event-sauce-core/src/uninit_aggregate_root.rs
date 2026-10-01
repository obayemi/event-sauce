//! `UninitAggregateRoot<A>` — uninitialized aggregate awaiting an init event.
//!
//! This type represents the "before creation" state in the type-state pattern.
//! It can only transition to `AggregateRoot<A>` by applying an init event,
//! enforcing aggregate lifecycle at compile time.

use std::marker::PhantomData;

#[cfg(feature = "event-sourcing")]
use crate::EventApplicator;
use crate::{Aggregate, AggregateRoot, EntityId, InitEvent};

/// An uninitialized aggregate root awaiting an init event.
///
/// This type enforces the aggregate lifecycle at compile time:
/// - `UninitAggregateRoot<A>` can only become `AggregateRoot<A>` via `apply_init()`
/// - `AggregateRoot<A>` can only mutate via `apply()` (regular events)
///
/// # Examples
///
/// ```ignore
/// let uninit = UninitAggregateRoot::<User>::new(EntityId::new());
/// let agg: AggregateRoot<User> = uninit.apply_init(CreatedEvent {
///     email: "alice@example.com".into(),
///     name: "Alice".into(),
/// })?;
/// // Now agg is a fully initialized AggregateRoot<User>
/// ```
#[derive(Debug)]
pub struct UninitAggregateRoot<A: Aggregate> {
    id: EntityId,
    _phantom: PhantomData<A>,
}

impl<A: Aggregate> UninitAggregateRoot<A> {
    /// Creates a new uninitialized aggregate root with the given ID.
    #[must_use]
    pub fn new(id: EntityId) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }

    /// Returns the entity ID.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        self.id
    }

    /// Applies an init event, consuming self and returning an initialized `AggregateRoot`.
    ///
    /// Runs the full init lifecycle:
    /// 1. `validate_init()` — pre-condition check
    /// 2. `init()` — construct the entity
    /// 3. `post_validate_init()` — invariant check
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails.
    pub fn apply_init<E: InitEvent<A> + Into<A::Event>>(
        self,
        event: E,
    ) -> Result<AggregateRoot<A>, A::Error> {
        self.init_as(event, None)
    }

    /// Applies an init event with actor tracking.
    ///
    /// Like [`apply_init()`](Self::apply_init), but records the actor's entity ID
    /// alongside the event. At commit time, the actor ID flows into
    /// `EventEnvelope::created_by`.
    ///
    /// # Errors
    ///
    /// Returns an error if validation (pre or post) fails.
    pub fn apply_init_with_actor<E: InitEvent<A> + Into<A::Event>>(
        self,
        event: E,
        actor_id: EntityId,
    ) -> Result<AggregateRoot<A>, A::Error> {
        self.init_as(event, Some(actor_id))
    }

    fn init_as<E: InitEvent<A> + Into<A::Event>>(
        self,
        event: E,
        actor_id: Option<EntityId>,
    ) -> Result<AggregateRoot<A>, A::Error> {
        event.validate_init()?;
        let entity = event.init(self.id);
        event.post_validate_init(&entity)?;
        Ok(AggregateRoot::from_init(entity, event.into(), actor_id))
    }

    /// Replays an init event without validation. Used by `load()`.
    #[cfg(feature = "event-sourcing")]
    pub(crate) fn apply_init_unchecked(self, event: &A::Event) -> AggregateRoot<A> {
        let entity = EventApplicator::dispatch_init_unchecked(event, self.id);
        AggregateRoot::restore(crate::StoredVersion::FIRST, entity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_fixtures::SimpleTestEntity;

    #[test]
    fn test_uninit_aggregate_root_new() {
        let id = EntityId::new();
        let uninit = UninitAggregateRoot::<SimpleTestEntity>::new(id);
        assert_eq!(uninit.entity_id(), id);
    }

    #[test]
    fn test_uninit_aggregate_root_debug() {
        let uninit = UninitAggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        let debug = format!("{uninit:?}");
        assert!(debug.contains("UninitAggregateRoot"));
    }

    #[test]
    fn test_apply_init_with_actor() {
        use crate::test_fixtures::SimpleTestInit;
        use crate::AggregateVersion;

        let uninit = UninitAggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        let id = uninit.entity_id();
        let actor_id = EntityId::new();

        let agg = uninit
            .apply_init_with_actor(SimpleTestInit { value: 7 }, actor_id)
            .unwrap();
        assert_eq!(agg.entity_id(), id);
        assert_eq!(agg.value, 7);
        assert_eq!(agg.version(), AggregateVersion::new(1));
        assert_eq!(agg.pending_events().len(), 1);

        let pending = agg.pending_events_with_actors();
        assert_eq!(pending[0].actor_id, Some(actor_id));
    }
}
