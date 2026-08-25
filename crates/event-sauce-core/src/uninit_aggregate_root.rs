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
        event.validate_init()?;
        let entity = event.init(self.id);
        event.post_validate_init(&entity)?;
        Ok(AggregateRoot::from_init(entity, event.into()))
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
        event.validate_init()?;
        let entity = event.init(self.id);
        event.post_validate_init(&entity)?;
        Ok(AggregateRoot::from_init_with_actor(
            entity,
            event.into(),
            actor_id,
        ))
    }

    /// Replays an init event without validation. Used by `load()`.
    #[cfg(feature = "event-sourcing")]
    pub(crate) fn apply_init_unchecked(self, event: &A::Event) -> AggregateRoot<A> {
        let entity = EventApplicator::dispatch_init_unchecked(event, self.id);
        AggregateRoot::from_init_replay(entity)
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

    // Test apply_init_with_actor using a local init event fixture
    use crate::{AggregateError, AggregateVersion, DomainEvent, EventVersion};
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, thiserror::Error)]
    #[error("User error")]
    struct UserError;

    impl AggregateError for UserError {}

    #[derive(Debug, Serialize, Deserialize)]
    struct User {
        id: EntityId,
        email: String,
    }

    impl crate::Entity for User {
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum UserEvent {
        Created {
            email: String,
            timestamp: chrono::DateTime<Utc>,
        },
    }

    impl DomainEvent for UserEvent {
        type Aggregate = User;
        fn event_type(&self) -> &'static str {
            "User.Created"
        }
        fn event_version(&self) -> EventVersion {
            EventVersion::new(1)
        }
        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            match self {
                UserEvent::Created { timestamp, .. } => *timestamp,
            }
        }
    }

    impl crate::EventApplicator<User> for UserEvent {
        fn dispatch(&self, _user: &mut User) -> Result<(), UserError> {
            Ok(())
        }
        fn dispatch_unchecked(&self, _user: &mut User) {}
        fn is_init(&self) -> bool {
            true
        }
        fn dispatch_init(&self, id: EntityId) -> Result<User, UserError> {
            match self {
                UserEvent::Created { email, .. } => Ok(User {
                    id,
                    email: email.clone(),
                }),
            }
        }
        fn dispatch_init_unchecked(&self, id: EntityId) -> User {
            match self {
                UserEvent::Created { email, .. } => User {
                    id,
                    email: email.clone(),
                },
            }
        }
    }

    impl crate::Aggregate for User {
        type Event = UserEvent;
        type Error = UserError;
        type DeletedState = Self;
    }

    #[derive(Debug, Clone)]
    struct UserCreatedEvent {
        email: String,
        timestamp: chrono::DateTime<Utc>,
    }

    impl From<UserCreatedEvent> for UserEvent {
        fn from(e: UserCreatedEvent) -> Self {
            UserEvent::Created {
                email: e.email,
                timestamp: e.timestamp,
            }
        }
    }

    impl InitEvent<User> for UserCreatedEvent {
        fn init(&self, id: EntityId) -> User {
            User {
                id,
                email: self.email.clone(),
            }
        }
    }

    #[test]
    fn test_apply_init_with_actor() {
        let uninit = UninitAggregateRoot::<User>::new(EntityId::new());
        let id = uninit.entity_id();
        let actor_id = EntityId::new();

        let event = UserCreatedEvent {
            email: "alice@example.com".to_string(),
            timestamp: Utc::now(),
        };

        let agg = uninit.apply_init_with_actor(event, actor_id).unwrap();
        assert_eq!(agg.entity_id(), id);
        assert_eq!(agg.email, "alice@example.com");
        assert_eq!(agg.version(), AggregateVersion::new(1));
        assert_eq!(agg.pending_events().len(), 1);

        // Verify actor_id is stored
        let pending = agg.pending_events_with_actors();
        assert_eq!(pending[0].actor_id, Some(actor_id));
    }
}
