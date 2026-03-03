//! Typed aggregate ID traits for type-safe aggregate loading.
//!
//! These traits enable compile-time validation that the correct ID type
//! is used when loading a specific aggregate. For example, using a `GroupId`
//! to load a `Group` aggregate is valid, but using a `UserId` to load a
//! `Group` would be a compile error.
//!
//! Raw `EntityId` continues to work everywhere — typed IDs are purely opt-in.

use crate::{Aggregate, EntityId};

/// A typed aggregate ID that carries type information about its aggregate.
///
/// Implementing this trait allows `ReactorContext::load()` to infer the
/// aggregate type automatically from the ID type.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{AggregateId, EntityId};
/// # use event_sauce_core::{Aggregate, AggregateError, Entity, DefaultEntity, DomainEvent, EventApplicator, EventVersion};
/// # use serde::{Serialize, Deserialize};
/// # use thiserror::Error;
/// # use chrono::Utc;
/// #
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # struct Group { id: EntityId }
/// # impl Entity for Group {
/// #     fn new(id: EntityId) -> Self { Self { id } }
/// #     fn entity_id(&self) -> EntityId { self.id }
/// # }
/// # impl DefaultEntity for Group {}
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # enum GroupEvent { Noop }
/// # impl DomainEvent for GroupEvent {
/// #     type Aggregate = Group;
/// #     fn event_type(&self) -> &'static str { "Group.Noop" }
/// #     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
/// #     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
/// # }
/// # impl EventApplicator<Group> for GroupEvent {
/// #     fn dispatch(&self, _: &mut Group) -> Result<(), GroupError> { Ok(()) }
/// #     fn dispatch_unchecked(&self, _: &mut Group) {}
/// # }
/// # #[derive(Debug, Error)]
/// # #[error("err")]
/// # struct GroupError;
/// # impl AggregateError for GroupError {}
/// # impl Aggregate for Group {
/// #     type Event = GroupEvent;
/// #     type Error = GroupError;
/// #     type DeletedState = Self;
/// # }
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// struct GroupId(EntityId);
///
/// impl AggregateId for GroupId {
///     type Aggregate = Group;
///     fn as_entity_id(&self) -> EntityId { self.0 }
/// }
/// ```
pub trait AggregateId: Send + Sync {
    /// The aggregate type this ID is associated with.
    type Aggregate: Aggregate;

    /// Returns the underlying `EntityId`.
    fn as_entity_id(&self) -> EntityId;
}

/// Marker trait: this ID type can be used to load aggregate `A`.
///
/// This prevents type errors like using a `UserId` to load a `Group`.
/// Raw `EntityId` implements this for all aggregates (backward compatible).
/// Typed IDs implement this only for their associated aggregate.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{EntityId, EntityIdFor};
/// # use event_sauce_core::{Aggregate, AggregateError, Entity, DefaultEntity, DomainEvent, EventApplicator, EventVersion};
/// # use serde::{Serialize, Deserialize};
/// # use thiserror::Error;
/// # use chrono::Utc;
/// #
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # struct User { id: EntityId }
/// # impl Entity for User {
/// #     fn new(id: EntityId) -> Self { Self { id } }
/// #     fn entity_id(&self) -> EntityId { self.id }
/// # }
/// # impl DefaultEntity for User {}
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # enum UserEvent { Noop }
/// # impl DomainEvent for UserEvent {
/// #     type Aggregate = User;
/// #     fn event_type(&self) -> &'static str { "User.Noop" }
/// #     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
/// #     fn occurred_at(&self) -> chrono::DateTime<Utc> { Utc::now() }
/// # }
/// # impl EventApplicator<User> for UserEvent {
/// #     fn dispatch(&self, _: &mut User) -> Result<(), UserError> { Ok(()) }
/// #     fn dispatch_unchecked(&self, _: &mut User) {}
/// # }
/// # #[derive(Debug, Error)]
/// # #[error("err")]
/// # struct UserError;
/// # impl AggregateError for UserError {}
/// # impl Aggregate for User {
/// #     type Event = UserEvent;
/// #     type Error = UserError;
/// #     type DeletedState = Self;
/// # }
///
/// // EntityId works for any aggregate
/// let id = EntityId::new();
/// let _: EntityId = EntityIdFor::<User>::entity_id(&id);
/// ```
pub trait EntityIdFor<A: Aggregate> {
    /// Returns the `EntityId` for loading aggregate `A`.
    fn entity_id(&self) -> EntityId;
}

// Blanket: raw EntityId works for any aggregate (backward compatible)
impl<A: Aggregate> EntityIdFor<A> for EntityId {
    fn entity_id(&self) -> EntityId {
        *self
    }
}

// Blanket: any AggregateId automatically implements EntityIdFor its aggregate
impl<T: AggregateId> EntityIdFor<T::Aggregate> for T {
    fn entity_id(&self) -> EntityId {
        self.as_entity_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::SimpleTestEntity;

    #[test]
    fn test_entity_id_implements_entity_id_for_any_aggregate() {
        let id = EntityId::new();
        let result: EntityId = EntityIdFor::<SimpleTestEntity>::entity_id(&id);
        assert_eq!(result, id);
    }

    #[test]
    fn test_aggregate_id_implements_entity_id_for() {
        #[derive(Debug, Clone, Copy)]
        struct TestId(EntityId);

        impl AggregateId for TestId {
            type Aggregate = SimpleTestEntity;
            fn as_entity_id(&self) -> EntityId {
                self.0
            }
        }

        let inner = EntityId::new();
        let typed_id = TestId(inner);

        // AggregateId::as_entity_id
        assert_eq!(typed_id.as_entity_id(), inner);

        // EntityIdFor blanket
        let result: EntityId = EntityIdFor::<SimpleTestEntity>::entity_id(&typed_id);
        assert_eq!(result, inner);
    }

    #[test]
    fn test_entity_id_for_different_aggregates() {
        // EntityId works for any aggregate via blanket impl
        let id = EntityId::new();
        let a: EntityId = EntityIdFor::<SimpleTestEntity>::entity_id(&id);
        assert_eq!(a, id);
    }
}
