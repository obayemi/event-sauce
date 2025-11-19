//! Aggregate identifier types.
//!
//! Defines the `AggregateId` struct which represents an aggregate identifier.

use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use uuid::Uuid;

/// Aggregate identifier.
///
/// A unique identifier for an aggregate instance. This is a transparent wrapper
/// around a UUID, ensuring efficient representation and compatibility with the
/// event store.
///
/// # Usage
///
/// You can use `AggregateId` directly, or create strongly-typed wrappers using
/// the newtype pattern:
///
/// # Examples
///
/// ## Direct Usage
///
/// ```
/// use event_sauce_core::AggregateId;
///
/// let id = AggregateId::new();
/// println!("ID: {}", id);
/// ```
///
/// ## Creating from UUID
///
/// ```
/// use event_sauce_core::AggregateId;
/// use uuid::Uuid;
///
/// let uuid = Uuid::new_v4();
/// let id = AggregateId::from(uuid);
/// assert_eq!(id.to_uuid(), uuid);
/// ```
///
/// ## Nil ID (uninitialized)
///
/// ```
/// use event_sauce_core::AggregateId;
///
/// let id = AggregateId::nil();
/// assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
/// ```
///
/// ## Newtype Pattern (Strongly-Typed IDs)
///
/// ### Option 1: Using the derive macro (recommended - just 3 lines!)
///
/// ```ignore
/// use event_sauce_macros::AggregateId;
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AggregateId)]
/// #[repr(transparent)]
/// struct UserId(AggregateId);
///
/// let user_id = UserId::new();
/// // Can use AggregateId methods directly via Deref
/// let uuid = user_id.to_uuid();
/// println!("{}", user_id);
/// ```
///
/// With custom display format:
///
/// ```ignore
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AggregateId)]
/// #[repr(transparent)]
/// #[display("User-{}")]
/// struct UserId(AggregateId);
///
/// let user_id = UserId::new();
/// assert!(format!("{}", user_id).starts_with("User-"));
/// ```
///
/// ### Option 2: Using derive_more
///
/// ```ignore
/// use derive_more::{Deref, AsRef, From, Display};
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deref, AsRef, From, Display)]
/// #[display(fmt = "User-{}", _0)]
/// #[repr(transparent)]
/// struct UserId(AggregateId);
///
/// impl UserId {
///     fn new() -> Self {
///         Self(AggregateId::new())
///     }
/// }
///
/// let user_id = UserId::new();
/// let uuid = user_id.to_uuid();
/// println!("{}", user_id);
/// ```
///
/// ### Option 3: Manual implementation
///
/// ```
/// use event_sauce_core::AggregateId;
/// use std::ops::Deref;
/// use std::fmt;
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// #[repr(transparent)]
/// struct UserId(AggregateId);
///
/// impl UserId {
///     fn new() -> Self {
///         Self(AggregateId::new())
///     }
/// }
///
/// impl Deref for UserId {
///     type Target = AggregateId;
///     fn deref(&self) -> &Self::Target {
///         &self.0
///     }
/// }
///
/// impl fmt::Display for UserId {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "User-{}", self.0)
///     }
/// }
///
/// let user_id = UserId::new();
/// let uuid = user_id.to_uuid();
/// println!("{}", user_id);
/// # let _ = uuid;
/// ```
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AggregateId(Uuid);

impl AggregateId {
    /// Creates a new random `AggregateId`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateId;
    ///
    /// let id1 = AggregateId::new();
    /// let id2 = AggregateId::new();
    /// assert_ne!(id1, id2);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a nil (all zeros) `AggregateId`.
    ///
    /// Useful for uninitialized aggregates or as a placeholder.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateId;
    ///
    /// let id = AggregateId::nil();
    /// assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    /// ```
    #[must_use]
    pub const fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Converts the aggregate ID to a UUID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateId;
    ///
    /// let id = AggregateId::new();
    /// let uuid = id.to_uuid();
    /// # let _ = uuid;
    /// ```
    #[must_use]
    pub const fn to_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for AggregateId {
    /// Creates a new random `AggregateId`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateId;
    ///
    /// let id: AggregateId = Default::default();
    /// # let _ = id;
    /// ```
    fn default() -> Self {
        Self::new()
    }
}

impl Display for AggregateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for AggregateId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<AggregateId> for Uuid {
    fn from(id: AggregateId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_id_new() {
        let id1 = AggregateId::new();
        let id2 = AggregateId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_aggregate_id_nil() {
        let id = AggregateId::nil();
        assert_eq!(id.to_uuid(), Uuid::nil());
        assert_eq!(id.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_aggregate_id_default() {
        let id1: AggregateId = Default::default();
        let id2: AggregateId = Default::default();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_aggregate_id_from_uuid() {
        let uuid = Uuid::new_v4();
        let id: AggregateId = uuid.into();
        assert_eq!(id.to_uuid(), uuid);
    }

    #[test]
    fn test_aggregate_id_to_uuid() {
        let id = AggregateId::new();
        let uuid = id.to_uuid();
        // Just verify we can convert back
        assert_eq!(AggregateId::from(uuid), id);
    }

    #[test]
    fn test_aggregate_id_clone() {
        let id1 = AggregateId::new();
        let id2 = id1;
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_aggregate_id_equality() {
        let uuid = Uuid::new_v4();
        let id1 = AggregateId::from(uuid);
        let id2 = AggregateId::from(uuid);
        let id3 = AggregateId::new();

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_aggregate_id_display() {
        let uuid = Uuid::nil();
        let id = AggregateId::from(uuid);
        let display = format!("{id}");
        assert_eq!(display, uuid.to_string());
    }

    #[test]
    fn test_aggregate_id_debug() {
        let id = AggregateId::new();
        let debug = format!("{id:?}");
        assert!(debug.contains("AggregateId"));
    }

    #[test]
    fn test_aggregate_id_hash() {
        use std::collections::HashSet;

        let uuid = Uuid::new_v4();
        let id1 = AggregateId::from(uuid);
        let id2 = AggregateId::from(uuid);

        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2);

        // Same UUID should result in only one entry in the set
        assert_eq!(set.len(), 1);
        assert!(set.contains(&id1));
    }

    #[test]
    fn test_aggregate_id_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<AggregateId>();
        assert_sync::<AggregateId>();
    }

    #[test]
    fn test_aggregate_id_from_conversion() {
        let uuid = Uuid::new_v4();
        let id: AggregateId = uuid.into();
        assert_eq!(id.to_uuid(), uuid);
    }

    #[test]
    fn test_aggregate_id_into_conversion() {
        let uuid = Uuid::new_v4();
        let id = AggregateId::from(uuid);
        let converted: Uuid = id.into();
        assert_eq!(converted, uuid);
    }

    #[test]
    fn test_aggregate_id_serialization() {
        let id = AggregateId::new();

        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: AggregateId = serde_json::from_str(&serialized).unwrap();

        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_aggregate_id_serialization_format() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let id = AggregateId::from(uuid);

        let serialized = serde_json::to_string(&id).unwrap();
        // Should serialize as a quoted UUID string
        assert_eq!(serialized, r#""550e8400-e29b-41d4-a716-446655440000""#);
    }

    // Test newtype pattern usage with transparent access
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[repr(transparent)]
    struct UserId(AggregateId);

    impl UserId {
        fn new() -> Self {
            Self(AggregateId::new())
        }
    }

    impl std::ops::Deref for UserId {
        type Target = AggregateId;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl AsRef<AggregateId> for UserId {
        fn as_ref(&self) -> &AggregateId {
            &self.0
        }
    }

    impl From<UserId> for AggregateId {
        fn from(id: UserId) -> Self {
            id.0
        }
    }

    impl From<AggregateId> for UserId {
        fn from(id: AggregateId) -> Self {
            Self(id)
        }
    }

    impl Display for UserId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "User-{}", self.0)
        }
    }

    #[test]
    fn test_newtype_pattern() {
        let user_id = UserId::new();
        // Can access AggregateId via Deref
        let _uuid = user_id.to_uuid();
        // Can use as_ref
        let _aggregate_id: &AggregateId = user_id.as_ref();
    }

    #[test]
    fn test_newtype_pattern_display() {
        let aggregate_id = AggregateId::nil();
        let user_id = UserId::from(aggregate_id);
        let display = format!("{user_id}");
        assert!(display.starts_with("User-"));
        assert!(display.contains(&Uuid::nil().to_string()));
    }

    #[test]
    fn test_newtype_pattern_conversion() {
        let original = AggregateId::new();
        let user_id = UserId::from(original);
        let converted: AggregateId = user_id.into();
        assert_eq!(original, converted);
    }

    #[test]
    fn test_newtype_pattern_deref() {
        let user_id = UserId::new();
        // Via Deref, can call AggregateId methods directly
        let uuid = user_id.to_uuid();
        assert_eq!(*user_id, AggregateId::from(uuid));
    }
}
