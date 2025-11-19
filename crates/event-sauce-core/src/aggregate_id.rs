//! Aggregate identifier types.
//!
//! Defines the `AggregateId` trait for creating strongly-typed aggregate identifiers.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::{Debug, Display};
use uuid::Uuid;

/// Trait for aggregate identifiers.
///
/// All aggregate IDs must be convertible to/from UUID for storage, but can provide
/// domain-specific types and display formats at the domain layer.
///
/// # Type Safety
///
/// Using the trait allows for compile-time type safety between different aggregate IDs:
///
/// ```rust
/// # use event_sauce_core::AggregateId;
/// # use uuid::Uuid;
/// # use std::fmt;
/// # #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// # struct CounterId(Uuid);
/// # impl AggregateId for CounterId {
/// #     fn to_uuid(&self) -> Uuid { self.0 }
/// #     fn from_uuid(uuid: Uuid) -> Self { Self(uuid) }
/// # }
/// # impl Display for CounterId {
/// #     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
/// # }
/// # #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// # struct UserId(Uuid);
/// # impl AggregateId for UserId {
/// #     fn to_uuid(&self) -> Uuid { self.0 }
/// #     fn from_uuid(uuid: Uuid) -> Self { Self(uuid) }
/// # }
/// # impl Display for UserId {
/// #     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
/// # }
/// fn process_counter(id: CounterId) { /* ... */ }
/// fn process_user(id: UserId) { /* ... */ }
///
/// let counter_id = CounterId::new();
/// let user_id = UserId::new();
///
/// process_counter(counter_id); // ✓ OK
/// // process_counter(user_id);  // ✗ Compile error - type safety!
/// ```
///
/// # Examples
///
/// ## Using DefaultAggregateId
///
/// ```
/// use event_sauce_core::{AggregateId, DefaultAggregateId};
///
/// let id = DefaultAggregateId::new();
/// println!("ID: {}", id);
/// ```
///
/// ## Creating Custom IDs with derive macro
///
/// ```ignore
/// use event_sauce_macros::AggregateId;
///
/// #[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// #[repr(transparent)]
/// struct CounterId(uuid::Uuid);
///
/// let counter_id = CounterId::new();
/// // Can use all AggregateId trait methods
/// let uuid = counter_id.to_uuid();
/// ```
pub trait AggregateId:
    Debug + Display + Clone + Send + Sync + std::hash::Hash + Eq + Serialize + DeserializeOwned
{
    /// Converts this ID to a UUID for storage.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{AggregateId, DefaultAggregateId};
    ///
    /// let id = DefaultAggregateId::new();
    /// let uuid = id.to_uuid();
    /// # let _ = uuid;
    /// ```
    fn to_uuid(&self) -> Uuid;

    /// Creates an ID from a UUID (when loading from storage).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{AggregateId, DefaultAggregateId};
    /// use uuid::Uuid;
    ///
    /// let uuid = Uuid::new_v4();
    /// let id = DefaultAggregateId::from_uuid(uuid);
    /// assert_eq!(id.to_uuid(), uuid);
    /// ```
    fn from_uuid(uuid: Uuid) -> Self;

    /// Creates a new random ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{AggregateId, DefaultAggregateId};
    ///
    /// let id1 = DefaultAggregateId::new();
    /// let id2 = DefaultAggregateId::new();
    /// assert_ne!(id1, id2);
    /// ```
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::from_uuid(Uuid::new_v4())
    }

    /// Creates a nil (all zeros) ID.
    ///
    /// Useful for uninitialized aggregates or as a placeholder.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{AggregateId, DefaultAggregateId};
    ///
    /// let id = DefaultAggregateId::nil();
    /// assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    /// ```
    fn nil() -> Self
    where
        Self: Sized,
    {
        Self::from_uuid(Uuid::nil())
    }
}

/// Default aggregate ID implementation.
///
/// A simple UUID wrapper for use when you don't need custom aggregate ID types.
/// This provides the standard implementation that works for most use cases.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{AggregateId, DefaultAggregateId};
///
/// let id = DefaultAggregateId::new();
/// println!("ID: {}", id);
///
/// let uuid = id.to_uuid();
/// let id2 = DefaultAggregateId::from_uuid(uuid);
/// assert_eq!(id, id2);
/// ```
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DefaultAggregateId(Uuid);

impl AggregateId for DefaultAggregateId {
    fn to_uuid(&self) -> Uuid {
        self.0
    }

    fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Display for DefaultAggregateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for DefaultAggregateId {
    /// Creates a new random `DefaultAggregateId`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::DefaultAggregateId;
    ///
    /// let id: DefaultAggregateId = Default::default();
    /// # let _ = id;
    /// ```
    fn default() -> Self {
        Self::new()
    }
}

impl From<Uuid> for DefaultAggregateId {
    fn from(uuid: Uuid) -> Self {
        Self::from_uuid(uuid)
    }
}

impl From<DefaultAggregateId> for Uuid {
    fn from(id: DefaultAggregateId) -> Self {
        id.to_uuid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test custom aggregate ID implementation
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestCounterId(Uuid);

    impl AggregateId for TestCounterId {
        fn to_uuid(&self) -> Uuid {
            self.0
        }

        fn from_uuid(uuid: Uuid) -> Self {
            Self(uuid)
        }
    }

    impl Display for TestCounterId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Counter-{}", self.0)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestUserId(Uuid);

    impl AggregateId for TestUserId {
        fn to_uuid(&self) -> Uuid {
            self.0
        }

        fn from_uuid(uuid: Uuid) -> Self {
            Self(uuid)
        }
    }

    impl Display for TestUserId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "User-{}", self.0)
        }
    }

    // Tests for trait default implementations
    #[test]
    fn test_aggregate_id_trait_new() {
        let id1 = TestCounterId::new();
        let id2 = TestCounterId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_aggregate_id_trait_nil() {
        let id = TestCounterId::nil();
        assert_eq!(id.to_uuid(), Uuid::nil());
        assert_eq!(
            id.to_string(),
            "Counter-00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn test_aggregate_id_trait_from_uuid() {
        let uuid = Uuid::new_v4();
        let id = TestCounterId::from_uuid(uuid);
        assert_eq!(id.to_uuid(), uuid);
    }

    #[test]
    fn test_aggregate_id_trait_custom_display() {
        let uuid = Uuid::nil();
        let counter_id = TestCounterId::from_uuid(uuid);
        let user_id = TestUserId::from_uuid(uuid);

        assert!(counter_id.to_string().starts_with("Counter-"));
        assert!(user_id.to_string().starts_with("User-"));
    }

    #[test]
    fn test_aggregate_id_type_safety() {
        // This test verifies that different ID types are distinct at compile time
        fn accept_counter_id(_id: TestCounterId) {}
        fn accept_user_id(_id: TestUserId) {}

        let counter_id = TestCounterId::new();
        let user_id = TestUserId::new();

        accept_counter_id(counter_id);
        accept_user_id(user_id);

        // The following would not compile (which is good - type safety!)
        // accept_counter_id(user_id);  // Compile error
        // accept_user_id(counter_id);  // Compile error
    }

    // Tests for DefaultAggregateId
    #[test]
    fn test_default_aggregate_id_new() {
        let id1 = DefaultAggregateId::new();
        let id2 = DefaultAggregateId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_default_aggregate_id_nil() {
        let id = DefaultAggregateId::nil();
        assert_eq!(id.to_uuid(), Uuid::nil());
        assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_default_aggregate_id_default() {
        let id1: DefaultAggregateId = Default::default();
        let id2: DefaultAggregateId = Default::default();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_default_aggregate_id_from_uuid() {
        let uuid = Uuid::new_v4();
        let id: DefaultAggregateId = uuid.into();
        assert_eq!(id.to_uuid(), uuid);
    }

    #[test]
    fn test_default_aggregate_id_to_uuid() {
        let id = DefaultAggregateId::new();
        let uuid = id.to_uuid();
        assert_eq!(DefaultAggregateId::from_uuid(uuid), id);
    }

    #[test]
    fn test_default_aggregate_id_clone() {
        let id1 = DefaultAggregateId::new();
        let id2 = id1;
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_default_aggregate_id_equality() {
        let uuid = Uuid::new_v4();
        let id1 = DefaultAggregateId::from_uuid(uuid);
        let id2 = DefaultAggregateId::from_uuid(uuid);
        let id3 = DefaultAggregateId::new();

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_default_aggregate_id_display() {
        let uuid = Uuid::nil();
        let id = DefaultAggregateId::from_uuid(uuid);
        let display = format!("{id}");
        assert_eq!(display, uuid.to_string());
    }

    #[test]
    fn test_default_aggregate_id_debug() {
        let id = DefaultAggregateId::new();
        let debug = format!("{id:?}");
        assert!(debug.contains("DefaultAggregateId"));
    }

    #[test]
    fn test_default_aggregate_id_hash() {
        use std::collections::HashSet;

        let uuid = Uuid::new_v4();
        let id1 = DefaultAggregateId::from_uuid(uuid);
        let id2 = DefaultAggregateId::from_uuid(uuid);

        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2);

        assert_eq!(set.len(), 1);
        assert!(set.contains(&id1));
    }

    #[test]
    fn test_default_aggregate_id_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<DefaultAggregateId>();
        assert_sync::<DefaultAggregateId>();
    }

    #[test]
    fn test_default_aggregate_id_from_conversion() {
        let uuid = Uuid::new_v4();
        let id: DefaultAggregateId = uuid.into();
        assert_eq!(id.to_uuid(), uuid);
    }

    #[test]
    fn test_default_aggregate_id_into_conversion() {
        let uuid = Uuid::new_v4();
        let id = DefaultAggregateId::from_uuid(uuid);
        let converted: Uuid = id.into();
        assert_eq!(converted, uuid);
    }

    #[test]
    fn test_default_aggregate_id_serialization() {
        let id = DefaultAggregateId::new();

        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: DefaultAggregateId = serde_json::from_str(&serialized).unwrap();

        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_default_aggregate_id_serialization_format() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id = DefaultAggregateId::from_uuid(uuid);

        let serialized = serde_json::to_string(&id).unwrap();
        assert_eq!(serialized, r#""550e8400-e29b-41d4-a716-446655440000""#);
    }

    #[test]
    fn test_custom_aggregate_id_serialization() {
        let id = TestCounterId::new();

        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: TestCounterId = serde_json::from_str(&serialized).unwrap();

        assert_eq!(id.to_uuid(), deserialized.to_uuid());
    }
}
