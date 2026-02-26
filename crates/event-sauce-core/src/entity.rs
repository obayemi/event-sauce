//! Entity trait for domain objects with identity.
//!
//! The `Entity` trait represents a domain object that has a unique identity
//! and can be constructed from an ID. This is the fundamental building block
//! for domain-driven design in event-sauce.
//!
//! # `DefaultEntity` Marker
//!
//! Aggregates that support construction from just an ID (without init events)
//! should also implement `DefaultEntity`. This marker guarantees that
//! `Entity::new(id)` won't panic. Aggregates using init events should NOT
//! implement `DefaultEntity`.

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
/// - Must expose its identity
///
/// # Default `new()` Behavior
///
/// The default implementation of `new()` panics. Aggregates that support
/// construction from just an ID should override `new()` and implement
/// [`DefaultEntity`]. Aggregates using init events leave the default panic
/// in place and do NOT implement `DefaultEntity`.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Entity, DefaultEntity, EntityId};
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
/// impl DefaultEntity for User {}
///
/// let user = User::new(EntityId::new());
/// let id = user.entity_id();
/// # let _ = id;
/// ```
pub trait Entity: Serialize + DeserializeOwned + Send + Sync + Sized {
    /// Creates a new entity with the given identifier.
    ///
    /// The entity should be initialized with default/empty state.
    /// Override for aggregates that don't use init events.
    ///
    /// # Panics
    ///
    /// The default implementation panics. Aggregates using init events
    /// rely on this default; the panic is a safety net that should never
    /// be reached in correct usage.
    #[must_use]
    fn new(id: EntityId) -> Self {
        let _ = id;
        panic!(
            "Entity::new() not supported for {}; use init events",
            std::any::type_name::<Self>()
        )
    }

    /// Returns the entity's unique identifier.
    fn entity_id(&self) -> EntityId;
}

/// Marker trait guaranteeing that `Entity::new(id)` is implemented and won't panic.
///
/// Implement for aggregates that support construction from just an ID
/// (i.e., aggregates that do NOT use init events).
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Entity, DefaultEntity, EntityId};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Counter {
///     id: EntityId,
///     value: i32,
/// }
///
/// impl Entity for Counter {
///     fn new(id: EntityId) -> Self {
///         Self { id, value: 0 }
///     }
///     fn entity_id(&self) -> EntityId { self.id }
/// }
///
/// impl DefaultEntity for Counter {}
/// ```
pub trait DefaultEntity: Entity {}

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

    #[test]
    fn test_default_entity_marker_is_implemented() {
        fn assert_default_entity<T: DefaultEntity>() {}
        assert_default_entity::<SimpleTestEntity>();
    }

    #[test]
    #[should_panic(expected = "Entity::new() not supported for")]
    fn test_entity_default_new_panics() {
        // Entity that doesn't override new() should panic
        #[derive(serde::Serialize, serde::Deserialize)]
        struct InitOnlyEntity {
            id: EntityId,
            name: String,
        }

        impl Entity for InitOnlyEntity {
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }

        let _ = InitOnlyEntity::new(EntityId::new());
    }
}
