//! Aggregate identifier trait and types.
//!
//! Defines the `AggregateId` trait which all aggregate identifiers must implement.

use std::fmt::{Debug, Display};
use std::hash::Hash;
use uuid::Uuid;

/// Trait for aggregate identifiers.
///
/// All aggregate identifiers must implement this trait. An aggregate ID uniquely
/// identifies an aggregate instance within the event store.
///
/// # Requirements
///
/// - Must be `Clone`, `Debug`, `Display`, `PartialEq`, `Eq`, and `Hash`
/// - Must be `Send + Sync` for async usage
/// - Should be a unique identifier (UUID, composite key, etc.)
/// - Must be convertible to a UUID for event store operations
///
/// # Examples
///
/// ```
/// use event_sauce_core::AggregateId;
/// use uuid::Uuid;
/// use std::fmt;
///
/// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// struct UserId(Uuid);
///
/// impl UserId {
///     fn new() -> Self {
///         Self(Uuid::new_v4())
///     }
/// }
///
/// impl fmt::Display for UserId {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "User-{}", self.0)
///     }
/// }
///
/// impl AggregateId for UserId {
///     fn to_uuid(&self) -> Uuid {
///         self.0
///     }
/// }
/// ```
pub trait AggregateId: Clone + Debug + Display + PartialEq + Eq + Hash + Send + Sync {
    /// Converts the aggregate ID to a UUID.
    ///
    /// This is used by the event store for persistence.
    fn to_uuid(&self) -> Uuid;
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // Test aggregate ID implementation
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestId(Uuid);

    impl TestId {
        fn new() -> Self {
            Self(Uuid::new_v4())
        }

        fn from_uuid(uuid: Uuid) -> Self {
            Self(uuid)
        }
    }

    impl Display for TestId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Test-{}", self.0)
        }
    }

    impl AggregateId for TestId {
        fn to_uuid(&self) -> Uuid {
            self.0
        }
    }

    #[test]
    fn test_aggregate_id_can_be_cloned() {
        let id1 = TestId::new();
        let id2 = id1.clone();
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_aggregate_id_equality() {
        let uuid = Uuid::new_v4();
        let id1 = TestId::from_uuid(uuid);
        let id2 = TestId::from_uuid(uuid);
        let id3 = TestId::new();

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_aggregate_id_display() {
        let uuid = Uuid::nil();
        let id = TestId::from_uuid(uuid);
        let display = format!("{}", id);
        assert!(display.starts_with("Test-"));
        assert!(display.contains(&uuid.to_string()));
    }

    #[test]
    fn test_aggregate_id_debug() {
        let id = TestId::new();
        let debug = format!("{:?}", id);
        assert!(debug.contains("TestId"));
    }

    #[test]
    fn test_aggregate_id_hash() {
        use std::collections::HashSet;

        let uuid = Uuid::new_v4();
        let id1 = TestId::from_uuid(uuid);
        let id2 = TestId::from_uuid(uuid);

        let mut set = HashSet::new();
        set.insert(id1.clone());
        set.insert(id2.clone());

        // Same UUID should result in only one entry in the set
        assert_eq!(set.len(), 1);
        assert!(set.contains(&id1));
    }

    #[test]
    fn test_aggregate_id_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<TestId>();
        assert_sync::<TestId>();
    }

    // Test with different types
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct IntegerId(i64);

    impl Display for IntegerId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl AggregateId for IntegerId {
        fn to_uuid(&self) -> Uuid {
            // For non-UUID IDs, we can create a deterministic UUID from the ID
            // This is just for testing purposes
            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&self.0.to_le_bytes());
            Uuid::from_bytes(bytes)
        }
    }

    #[test]
    fn test_integer_aggregate_id() {
        let id1 = IntegerId(42);
        let id2 = IntegerId(42);
        let id3 = IntegerId(99);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
        assert_eq!(format!("{}", id1), "42");
    }

    // Test with string-based ID
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct StringId(String);

    impl Display for StringId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl AggregateId for StringId {
        fn to_uuid(&self) -> Uuid {
            // For string IDs, we can create a UUID from the string hash
            // This is just for testing purposes
            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hasher;

            let mut hasher = DefaultHasher::new();
            hasher.write(self.0.as_bytes());
            let hash = hasher.finish();

            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&hash.to_le_bytes());
            Uuid::from_bytes(bytes)
        }
    }

    #[test]
    fn test_string_aggregate_id() {
        let id1 = StringId("user-123".to_string());
        let id2 = StringId("user-123".to_string());
        let id3 = StringId("user-456".to_string());

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
        assert_eq!(format!("{}", id1), "user-123");
    }

    #[test]
    fn test_to_uuid() {
        let uuid = Uuid::new_v4();
        let id = TestId::from_uuid(uuid);
        assert_eq!(id.to_uuid(), uuid);
    }

    #[test]
    fn test_to_uuid_deterministic() {
        // Same input should produce same UUID
        let id1 = IntegerId(42);
        let id2 = IntegerId(42);
        assert_eq!(id1.to_uuid(), id2.to_uuid());

        // Different input should produce different UUID
        let id3 = IntegerId(99);
        assert_ne!(id1.to_uuid(), id3.to_uuid());
    }
}
