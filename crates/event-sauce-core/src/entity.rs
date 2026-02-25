//! Entity trait for domain objects with identity.
//!
//! The `Entity` trait represents a domain object that has a unique identity
//! and can be constructed from an ID. This is the fundamental building block
//! for domain-driven design in event-sauce.

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::EntityId;

/// Trait for domain entities with identity.
///
/// An entity is a domain object that has a distinct identity that runs through
/// time and different representations. All entities use `EntityId` as their
/// identifier type.
///
/// # Requirements
///
/// - Must be `Serialize` and `DeserializeOwned` for persistence
/// - Must be `Send + Sync` for async usage
/// - Must provide a constructor from `EntityId`
/// - Must expose its identity
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Entity, EntityId};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User {
///     id: EntityId,
///     email: String,
///     name: String,
/// }
///
/// impl Entity for User {
///     fn new(id: EntityId) -> Self {
///         Self {
///             id,
///             email: String::new(),
///             name: String::new(),
///         }
///     }
///
///     fn entity_id(&self) -> EntityId {
///         self.id
///     }
/// }
///
/// let user = User::new(EntityId::new());
/// let id = user.entity_id();
/// # let _ = id;
/// ```
pub trait Entity: Serialize + DeserializeOwned + Send + Sync + Sized {
    /// Creates a new entity with the given identifier.
    ///
    /// The entity should be initialized with default/empty state.
    fn new(id: EntityId) -> Self;

    /// Returns the entity's unique identifier.
    fn entity_id(&self) -> EntityId;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::SimpleTestEntity;

    #[test]
    fn test_entity_new() {
        let id = EntityId::new();
        let entity = SimpleTestEntity::new(id);

        assert_eq!(entity.entity_id(), id);
        assert_eq!(entity.value, 0);
    }

    #[test]
    fn test_entity_identity_preserved() {
        let id = EntityId::new();
        let entity = SimpleTestEntity::new(id);

        assert_eq!(entity.entity_id(), id);
    }

    #[test]
    fn test_entity_different_ids() {
        let entity1 = SimpleTestEntity::new(EntityId::new());
        let entity2 = SimpleTestEntity::new(EntityId::new());

        assert_ne!(entity1.entity_id(), entity2.entity_id());
    }

    #[test]
    fn test_entity_serialization() {
        let id = EntityId::new();
        let entity = SimpleTestEntity::new(id);

        let serialized = serde_json::to_string(&entity).unwrap();
        let deserialized: SimpleTestEntity = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.entity_id(), id);
        assert_eq!(deserialized.value, 0);
    }
}
