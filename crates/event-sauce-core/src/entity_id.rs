//! Entity identifier type.
//!
//! Provides the concrete `EntityId` type used as the unique identifier
//! for all entities in the system. All entities use the same ID type,
//! backed by UUID v4.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Unique identifier for entities.
///
/// A concrete newtype wrapper around `Uuid` that provides a single,
/// universal identifier type for all entities. Unlike the previous
/// `AggregateId` trait-based approach, all entities share the same ID type,
/// simplifying the type system while maintaining UUID-backed uniqueness.
///
/// # Examples
///
/// ```
/// use event_sauce_core::EntityId;
/// use uuid::Uuid;
///
/// // Create a new random ID
/// let id = EntityId::new();
///
/// // Create from an existing UUID
/// let uuid = Uuid::new_v4();
/// let id = EntityId::from(uuid);
/// assert_eq!(id.as_uuid(), uuid);
///
/// // Create a nil (all zeros) ID
/// let nil = EntityId::nil();
/// assert_eq!(nil.as_uuid(), Uuid::nil());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct EntityId(Uuid);

impl EntityId {
    /// Creates a new random entity ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EntityId;
    ///
    /// let id1 = EntityId::new();
    /// let id2 = EntityId::new();
    /// assert_ne!(id1, id2);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a nil (all zeros) entity ID.
    ///
    /// Useful for testing or as a placeholder.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EntityId;
    ///
    /// let id = EntityId::nil();
    /// assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    /// ```
    #[must_use]
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Returns the underlying UUID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EntityId;
    /// use uuid::Uuid;
    ///
    /// let uuid = Uuid::new_v4();
    /// let id = EntityId::from(uuid);
    /// assert_eq!(id.as_uuid(), uuid);
    /// ```
    #[must_use]
    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for EntityId {
    /// Creates a new random `EntityId`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EntityId;
    ///
    /// let id: EntityId = Default::default();
    /// assert_ne!(id, EntityId::nil());
    /// ```
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<Uuid> for EntityId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<EntityId> for Uuid {
    fn from(id: EntityId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_id_new() {
        let id1 = EntityId::new();
        let id2 = EntityId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_entity_id_nil() {
        let id = EntityId::nil();
        assert_eq!(id.as_uuid(), Uuid::nil());
        assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_entity_id_as_uuid() {
        let uuid = Uuid::new_v4();
        let id = EntityId::from(uuid);
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn test_entity_id_default() {
        let id1: EntityId = EntityId::default();
        let id2: EntityId = EntityId::default();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_entity_id_display() {
        let uuid = Uuid::nil();
        let id = EntityId::from(uuid);
        assert_eq!(format!("{id}"), uuid.to_string());
    }

    #[test]
    fn test_entity_id_debug() {
        let id = EntityId::new();
        let debug = format!("{id:?}");
        assert!(debug.contains("EntityId"));
    }

    #[test]
    fn test_entity_id_from_uuid() {
        let uuid = Uuid::new_v4();
        let id: EntityId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn test_entity_id_into_uuid() {
        let uuid = Uuid::new_v4();
        let id = EntityId::from(uuid);
        let converted: Uuid = id.into();
        assert_eq!(converted, uuid);
    }

    #[test]
    fn test_entity_id_equality() {
        let uuid = Uuid::new_v4();
        let id1 = EntityId::from(uuid);
        let id2 = EntityId::from(uuid);
        let id3 = EntityId::new();

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_entity_id_clone() {
        let id1 = EntityId::new();
        let id2 = id1;
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_entity_id_hash() {
        use std::collections::HashSet;

        let uuid = Uuid::new_v4();
        let id1 = EntityId::from(uuid);
        let id2 = EntityId::from(uuid);

        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2);

        assert_eq!(set.len(), 1);
        assert!(set.contains(&id1));
    }

    #[test]
    fn test_entity_id_serialization() {
        let id = EntityId::new();

        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: EntityId = serde_json::from_str(&serialized).unwrap();

        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_entity_id_serialization_format() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id = EntityId::from(uuid);

        let serialized = serde_json::to_string(&id).unwrap();
        assert_eq!(serialized, r#""550e8400-e29b-41d4-a716-446655440000""#);
    }

    #[test]
    fn test_entity_id_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<EntityId>();
        assert_sync::<EntityId>();
    }
}
