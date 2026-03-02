//! Actor event traits for permission-validated, actor-required events.
//!
//! These traits enable compile-time enforcement that certain events require
//! an actor (the entity performing the action). Actor validation happens
//! at command time only — replayed events skip validation (historical facts).
//!
//! # Design
//!
//! - **`ActorEvent<A>`** — for regular events that require an actor
//! - **`ActorInitEvent<A>`** — for init events that require an actor
//!
//! The actor is typically an `AggregateRoot<T>`, providing typed state access
//! for permission validation. The actor's entity ID flows into `created_by`
//! on the event envelope at commit time.

use crate::{Aggregate, Entity};

/// Trait for events that require an actor for permission validation.
///
/// Implement this trait on event structs that should only be applied
/// when an authorized actor performs the action. The `Actor` associated
/// type determines which entity type can authorize this event.
///
/// # Lifecycle
///
/// When applying via `command_handler!` with `@actor`:
/// 1. `validate_actor()` is called with the aggregate state and actor entity
/// 2. If validation passes, the event is applied via `AggregateRoot::apply_with_actor()`
/// 3. The actor's entity ID is stored alongside the pending event
/// 4. On `commit()`, the actor ID flows into `EventEnvelope::created_by`
///
/// During replay, actor validation is **skipped** — events are historical facts.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::ActorEvent;
///
/// impl ActorEvent<Order> for ItemAddedEvent {
///     type Actor = User;
///
///     fn validate_actor(&self, order: &Order, actor: &User) -> Result<(), OrderError> {
///         if !actor.can_modify_orders() {
///             return Err(OrderError::PermissionDenied);
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait ActorEvent<A: Aggregate> {
    /// The entity type that acts as the actor for this event.
    type Actor: Entity;

    /// Validates the actor's permission to perform this action.
    ///
    /// Called at command time with the current aggregate state and actor entity.
    /// The default implementation allows all actors.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is not authorized.
    fn validate_actor(&self, _aggregate: &A, _actor: &Self::Actor) -> Result<(), A::Error> {
        Ok(())
    }
}

/// Trait for init events that require an actor for permission validation.
///
/// Similar to [`ActorEvent`], but for events that construct new aggregates.
/// Since no aggregate exists yet, validation only has access to the actor entity.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::ActorInitEvent;
///
/// impl ActorInitEvent<Order> for CreatedEvent {
///     type Actor = Admin;
///
///     fn validate_init_actor(&self, actor: &Admin) -> Result<(), OrderError> {
///         if !actor.can_create_orders() {
///             return Err(OrderError::PermissionDenied);
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait ActorInitEvent<A: Aggregate> {
    /// The entity type that acts as the actor for this init event.
    type Actor: Entity;

    /// Validates the actor's permission to create this aggregate.
    ///
    /// Called at command time with only the actor entity (no aggregate exists yet).
    /// The default implementation allows all actors.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is not authorized.
    fn validate_init_actor(&self, _actor: &Self::Actor) -> Result<(), A::Error> {
        Ok(())
    }
}

/// Trait for delete events that require an actor for permission validation.
///
/// Similar to [`ActorEvent`], but for events that terminate aggregates.
/// The actor is validated at command time; validation is skipped during replay.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::ActorDeleteEvent;
///
/// impl ActorDeleteEvent<Order> for CancelledEvent {
///     type Actor = Admin;
///
///     fn validate_delete_actor(&self, order: &Order, actor: &Admin) -> Result<(), OrderError> {
///         if !actor.can_cancel_orders() {
///             return Err(OrderError::PermissionDenied);
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait ActorDeleteEvent<A: Aggregate> {
    /// The entity type that acts as the actor for this delete event.
    type Actor: Entity;

    /// Validates the actor's permission to delete this aggregate.
    ///
    /// Called at command time with the current aggregate state and actor entity.
    /// The default implementation allows all actors.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor is not authorized.
    fn validate_delete_actor(&self, _aggregate: &A, _actor: &Self::Actor) -> Result<(), A::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateError, EntityId};

    // Test aggregate
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Order {
        id: EntityId,
        status: String,
    }

    impl crate::Entity for Order {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                status: "new".to_string(),
            }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for Order {}

    #[derive(Debug, thiserror::Error)]
    enum OrderError {
        #[error("Permission denied")]
        PermissionDenied,
    }

    impl AggregateError for OrderError {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum OrderEvent {
        ItemAdded { item_id: String },
    }

    impl crate::DomainEvent for OrderEvent {
        type Aggregate = Order;
        fn event_type(&self) -> &'static str {
            "Order.ItemAdded"
        }
        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }
        fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
    }

    impl crate::EventApplicator<Order> for OrderEvent {
        fn dispatch(&self, _order: &mut Order) -> Result<(), OrderError> {
            Ok(())
        }
        fn dispatch_unchecked(&self, _order: &mut Order) {}
    }

    impl crate::Aggregate for Order {
        type Event = OrderEvent;
        type Error = OrderError;
        type DeletedState = Self;
    }

    // Test actor
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct User {
        id: EntityId,
        can_modify: bool,
    }

    impl crate::Entity for User {
        fn new(id: EntityId) -> Self {
            Self {
                id,
                can_modify: false,
            }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for User {}

    // Test event
    struct ItemAddedEvent {
        _item_id: String,
    }

    impl ActorEvent<Order> for ItemAddedEvent {
        type Actor = User;

        fn validate_actor(&self, _order: &Order, actor: &User) -> Result<(), OrderError> {
            if !actor.can_modify {
                return Err(OrderError::PermissionDenied);
            }
            Ok(())
        }
    }

    #[test]
    fn test_actor_event_validate_succeeds() {
        let order = Order::new(EntityId::new());
        let user = User {
            id: EntityId::new(),
            can_modify: true,
        };
        let event = ItemAddedEvent {
            _item_id: "item-1".to_string(),
        };

        assert!(event.validate_actor(&order, &user).is_ok());
    }

    #[test]
    fn test_actor_event_validate_fails() {
        let order = Order::new(EntityId::new());
        let user = User {
            id: EntityId::new(),
            can_modify: false,
        };
        let event = ItemAddedEvent {
            _item_id: "item-1".to_string(),
        };

        let result = event.validate_actor(&order, &user);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Permission denied");
    }

    // Test default implementation allows all actors
    struct NoValidationEvent;

    impl ActorEvent<Order> for NoValidationEvent {
        type Actor = User;
    }

    #[test]
    fn test_actor_event_default_allows_all() {
        let order = Order::new(EntityId::new());
        let user = User::new(EntityId::new());
        let event = NoValidationEvent;

        assert!(event.validate_actor(&order, &user).is_ok());
    }

    // Test ActorInitEvent
    struct CreatedEvent;

    impl ActorInitEvent<Order> for CreatedEvent {
        type Actor = User;

        fn validate_init_actor(&self, actor: &User) -> Result<(), OrderError> {
            if !actor.can_modify {
                return Err(OrderError::PermissionDenied);
            }
            Ok(())
        }
    }

    #[test]
    fn test_actor_init_event_validate_succeeds() {
        let user = User {
            id: EntityId::new(),
            can_modify: true,
        };
        let event = CreatedEvent;

        assert!(event.validate_init_actor(&user).is_ok());
    }

    #[test]
    fn test_actor_init_event_validate_fails() {
        let user = User {
            id: EntityId::new(),
            can_modify: false,
        };
        let event = CreatedEvent;

        let result = event.validate_init_actor(&user);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Permission denied");
    }

    // Test default ActorInitEvent allows all
    struct NoValidationInitEvent;

    impl ActorInitEvent<Order> for NoValidationInitEvent {
        type Actor = User;
    }

    #[test]
    fn test_actor_init_event_default_allows_all() {
        let user = User::new(EntityId::new());
        let event = NoValidationInitEvent;

        assert!(event.validate_init_actor(&user).is_ok());
    }

    // Test ActorDeleteEvent
    struct CancelledEvent;

    impl ActorDeleteEvent<Order> for CancelledEvent {
        type Actor = User;

        fn validate_delete_actor(&self, _order: &Order, actor: &User) -> Result<(), OrderError> {
            if !actor.can_modify {
                return Err(OrderError::PermissionDenied);
            }
            Ok(())
        }
    }

    #[test]
    fn test_actor_delete_event_validate_succeeds() {
        let order = Order::new(EntityId::new());
        let user = User {
            id: EntityId::new(),
            can_modify: true,
        };
        let event = CancelledEvent;

        assert!(event.validate_delete_actor(&order, &user).is_ok());
    }

    #[test]
    fn test_actor_delete_event_validate_fails() {
        let order = Order::new(EntityId::new());
        let user = User {
            id: EntityId::new(),
            can_modify: false,
        };
        let event = CancelledEvent;

        let result = event.validate_delete_actor(&order, &user);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Permission denied");
    }

    // Test default ActorDeleteEvent allows all
    struct NoValidationDeleteEvent;

    impl ActorDeleteEvent<Order> for NoValidationDeleteEvent {
        type Actor = User;
    }

    #[test]
    fn test_actor_delete_event_default_allows_all() {
        let order = Order::new(EntityId::new());
        let user = User::new(EntityId::new());
        let event = NoValidationDeleteEvent;

        assert!(event.validate_delete_actor(&order, &user).is_ok());
    }
}
