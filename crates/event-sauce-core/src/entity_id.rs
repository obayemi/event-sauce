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

    /// Creates a deterministic, namespaced entity ID from a name.
    ///
    /// Uses UUID version 5 (SHA-1) so the same `(namespace, name)` pair always
    /// yields the same `EntityId`. This makes aggregate creation idempotent
    /// under retries or message redelivery and lets an aggregate be looked up
    /// by its *natural key* (e.g. an email, an external system identifier)
    /// without maintaining a separate lookup table.
    ///
    /// Pick a stable, application-specific `namespace` UUID (often a constant)
    /// to partition names so identical names in different domains do not
    /// collide. For binary names, use
    /// [`from_namespace_bytes`](Self::from_namespace_bytes).
    ///
    /// Contrast with [`new`](Self::new), which produces a random (UUID v4) ID
    /// for aggregates that have no natural key.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EntityId;
    /// use uuid::Uuid;
    ///
    /// let namespace = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
    ///
    /// // Deterministic: the same name always maps to the same id.
    /// let a = EntityId::from_namespace(namespace, "alice@example.com");
    /// let b = EntityId::from_namespace(namespace, "alice@example.com");
    /// assert_eq!(a, b);
    ///
    /// // Distinct names map to distinct ids.
    /// let c = EntityId::from_namespace(namespace, "bob@example.com");
    /// assert_ne!(a, c);
    /// ```
    #[must_use]
    pub fn from_namespace(namespace: Uuid, name: &str) -> Self {
        Self(Uuid::new_v5(&namespace, name.as_bytes()))
    }

    /// Creates a deterministic, namespaced entity ID from raw bytes.
    ///
    /// Identical to [`from_namespace`](Self::from_namespace) but accepts an
    /// arbitrary byte slice as the name. Because `&str` hashing uses its UTF-8
    /// bytes, `from_namespace(ns, "alice")` and
    /// `from_namespace_bytes(ns, b"alice")` produce the same `EntityId`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EntityId;
    /// use uuid::Uuid;
    ///
    /// let namespace = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
    ///
    /// let from_str = EntityId::from_namespace(namespace, "alice");
    /// let from_bytes = EntityId::from_namespace_bytes(namespace, b"alice");
    /// assert_eq!(from_str, from_bytes);
    /// ```
    #[must_use]
    pub fn from_namespace_bytes(namespace: Uuid, name: &[u8]) -> Self {
        Self(Uuid::new_v5(&namespace, name))
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

    // === L3: deterministic / namespaced (UUIDv5) entity IDs ===

    #[test]
    fn test_from_namespace_is_deterministic_for_same_name() {
        // The same (namespace, name) pair must always yield the same id, so
        // that aggregate creation is idempotent under retries/redelivery and an
        // aggregate can be looked up by its natural key.
        let namespace = Uuid::new_v4();

        let a = EntityId::from_namespace(namespace, "alice");
        let b = EntityId::from_namespace(namespace, "alice");

        assert_eq!(a, b, "same namespace + name must produce the same EntityId");
    }

    #[test]
    fn test_from_namespace_differs_by_name() {
        let namespace = Uuid::new_v4();

        let alice = EntityId::from_namespace(namespace, "alice");
        let bob = EntityId::from_namespace(namespace, "bob");

        assert_ne!(alice, bob, "different names must produce different ids");
    }

    #[test]
    fn test_from_namespace_differs_by_namespace() {
        let ns_one = Uuid::new_v4();
        let ns_two = Uuid::new_v4();

        let in_one = EntityId::from_namespace(ns_one, "alice");
        let in_two = EntityId::from_namespace(ns_two, "alice");

        assert_ne!(
            in_one, in_two,
            "the same name in different namespaces must produce different ids"
        );
    }

    #[test]
    fn test_from_namespace_produces_uuid_v5() {
        // Deterministic, namespaced ids must be backed by UUID version 5 (SHA-1),
        // mirroring the Python eventsourcing version-5 id convention.
        let namespace = Uuid::new_v4();

        let id = EntityId::from_namespace(namespace, "alice");

        assert_eq!(
            id.as_uuid().get_version(),
            Some(uuid::Version::Sha1),
            "from_namespace must produce a UUIDv5 (SHA-1) id"
        );
    }

    #[test]
    fn test_from_namespace_bytes_matches_str_form() {
        // The bytes-based constructor must agree with the &str constructor for
        // identical input, since &str hashing uses the UTF-8 bytes.
        let namespace = Uuid::new_v4();

        let from_str = EntityId::from_namespace(namespace, "alice");
        let from_bytes = EntityId::from_namespace_bytes(namespace, b"alice");

        assert_eq!(from_str, from_bytes);
    }
}
