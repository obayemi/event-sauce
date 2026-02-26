//! Init event trait for aggregate construction from events.
//!
//! The `InitEvent` trait is the counterpart to `ApplyEvent` for aggregates
//! that are constructed from an initial event rather than from default state.
//! While `ApplyEvent` mutates an existing aggregate, `InitEvent` creates
//! a new aggregate from event data.

use crate::{Aggregate, EntityId};

/// Trait for events that construct a new aggregate from scratch.
///
/// This is used with the type-state pattern: an uninitialized aggregate
/// (`UninitAggregateRoot<A>`) transitions to an initialized aggregate
/// (`AggregateRoot<A>`) by applying an init event.
///
/// # Lifecycle
///
/// 1. `validate_init()` — Pre-condition check (event data only, no aggregate)
/// 2. `init()` — Constructs the aggregate from the entity ID and event data
/// 3. `post_validate_init()` — Invariant check on the constructed aggregate
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::{Aggregate, EntityId, InitEvent};
///
/// struct CreatedEvent {
///     email: String,
///     name: String,
/// }
///
/// impl InitEvent<User> for CreatedEvent {
///     fn init(&self, id: EntityId) -> User {
///         User {
///             id,
///             email: self.email.clone(),
///             name: self.name.clone(),
///         }
///     }
/// }
/// ```
pub trait InitEvent<A: Aggregate> {
    /// Pre-condition validation (event data only, no aggregate exists yet).
    ///
    /// # Errors
    ///
    /// Returns an error if the event data is invalid.
    fn validate_init(&self) -> Result<(), A::Error> {
        Ok(())
    }

    /// Constructs a new aggregate entity from the given ID and event data.
    fn init(&self, id: EntityId) -> A;

    /// Post-condition validation on the newly constructed aggregate.
    ///
    /// # Errors
    ///
    /// Returns an error if the constructed aggregate violates invariants.
    fn post_validate_init(&self, _aggregate: &A) -> Result<(), A::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AggregateError, AggregateVersion, DomainEvent, Entity, EventApplicator, EventVersion,
    };
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, thiserror::Error)]
    enum UserError {
        #[error("Email cannot be empty")]
        EmptyEmail,
        #[error("Name too short")]
        NameTooShort,
    }

    impl AggregateError for UserError {}

    #[derive(Debug, Serialize, Deserialize)]
    struct User {
        id: EntityId,
        email: String,
        name: String,
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
            name: String,
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

    impl EventApplicator<User> for UserEvent {
        fn dispatch(&self, _aggregate: &mut User) -> Result<(), UserError> {
            Ok(())
        }
        fn dispatch_unchecked(&self, _aggregate: &mut User) {}

        fn is_init(&self) -> bool {
            matches!(self, UserEvent::Created { .. })
        }

        fn dispatch_init(&self, id: EntityId) -> Result<User, UserError> {
            match self {
                UserEvent::Created {
                    email,
                    name,
                    timestamp,
                } => {
                    let init = UserCreatedEvent {
                        email: email.clone(),
                        name: name.clone(),
                        timestamp: *timestamp,
                    };
                    init.validate_init()?;
                    let entity = init.init(id);
                    init.post_validate_init(&entity)?;
                    Ok(entity)
                }
            }
        }

        fn dispatch_init_unchecked(&self, id: EntityId) -> User {
            match self {
                UserEvent::Created {
                    email,
                    name,
                    timestamp,
                } => {
                    let init = UserCreatedEvent {
                        email: email.clone(),
                        name: name.clone(),
                        timestamp: *timestamp,
                    };
                    init.init(id)
                }
            }
        }
    }

    impl Aggregate for User {
        type Event = UserEvent;
        type Error = UserError;
    }

    #[derive(Debug, Clone)]
    struct UserCreatedEvent {
        email: String,
        name: String,
        timestamp: chrono::DateTime<Utc>,
    }

    impl From<UserCreatedEvent> for UserEvent {
        fn from(event: UserCreatedEvent) -> Self {
            UserEvent::Created {
                email: event.email,
                name: event.name,
                timestamp: event.timestamp,
            }
        }
    }

    impl InitEvent<User> for UserCreatedEvent {
        fn validate_init(&self) -> Result<(), UserError> {
            if self.email.is_empty() {
                return Err(UserError::EmptyEmail);
            }
            Ok(())
        }

        fn init(&self, id: EntityId) -> User {
            User {
                id,
                email: self.email.clone(),
                name: self.name.clone(),
            }
        }

        fn post_validate_init(&self, user: &User) -> Result<(), UserError> {
            if user.name.len() < 2 {
                return Err(UserError::NameTooShort);
            }
            Ok(())
        }
    }

    #[test]
    fn test_init_event_basic() {
        let event = UserCreatedEvent {
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            timestamp: Utc::now(),
        };
        let id = EntityId::new();

        let user = event.init(id);
        assert_eq!(user.entity_id(), id);
        assert_eq!(user.email, "alice@example.com");
        assert_eq!(user.name, "Alice");
    }

    #[test]
    fn test_init_event_validate_succeeds() {
        let event = UserCreatedEvent {
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            timestamp: Utc::now(),
        };
        assert!(event.validate_init().is_ok());
    }

    #[test]
    fn test_init_event_validate_fails_empty_email() {
        let event = UserCreatedEvent {
            email: String::new(),
            name: "Alice".to_string(),
            timestamp: Utc::now(),
        };
        let result = event.validate_init();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Email cannot be empty");
    }

    #[test]
    fn test_init_event_post_validate_succeeds() {
        let event = UserCreatedEvent {
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            timestamp: Utc::now(),
        };
        let id = EntityId::new();
        let user = event.init(id);
        assert!(event.post_validate_init(&user).is_ok());
    }

    #[test]
    fn test_init_event_post_validate_fails_short_name() {
        let event = UserCreatedEvent {
            email: "a@b.com".to_string(),
            name: "A".to_string(),
            timestamp: Utc::now(),
        };
        let id = EntityId::new();
        let user = event.init(id);
        let result = event.post_validate_init(&user);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Name too short");
    }

    #[test]
    fn test_init_event_full_lifecycle() {
        let event = UserCreatedEvent {
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            timestamp: Utc::now(),
        };
        let id = EntityId::new();

        event.validate_init().unwrap();
        let user = event.init(id);
        event.post_validate_init(&user).unwrap();

        assert_eq!(user.email, "alice@example.com");
        assert_eq!(user.name, "Alice");
    }

    #[test]
    fn test_uninit_aggregate_root_with_init_event() {
        let uninit = crate::UninitAggregateRoot::<User>::new(EntityId::new());
        let id = uninit.entity_id();

        let event = UserCreatedEvent {
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            timestamp: Utc::now(),
        };

        let agg = uninit.apply_init(event).unwrap();
        assert_eq!(agg.entity_id(), id);
        assert_eq!(agg.email, "alice@example.com");
        assert_eq!(agg.name, "Alice");
        assert_eq!(agg.version(), AggregateVersion::new(1));
        assert_eq!(agg.pending_events().len(), 1);
    }

    #[test]
    fn test_uninit_aggregate_root_apply_init_validation_failure() {
        let uninit = crate::UninitAggregateRoot::<User>::new(EntityId::new());

        let event = UserCreatedEvent {
            email: String::new(),
            name: "Alice".to_string(),
            timestamp: Utc::now(),
        };

        let result = uninit.apply_init(event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Email cannot be empty");
    }

    #[test]
    fn test_uninit_aggregate_root_apply_init_post_validation_failure() {
        let uninit = crate::UninitAggregateRoot::<User>::new(EntityId::new());

        let event = UserCreatedEvent {
            email: "a@b.com".to_string(),
            name: "A".to_string(),
            timestamp: Utc::now(),
        };

        let result = uninit.apply_init(event);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Name too short");
    }
}
