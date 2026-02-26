//! Typed aggregate type name.
//!
//! Provides [`AggregateType`], a newtype around `Cow<'static, str>` that represents
//! the aggregate type name used for stream partitioning and event metadata.
//!
//! # Stability
//!
//! Unlike `std::any::type_name()`, which is documented as having no stability
//! guarantee, `AggregateType` values produced by the `#[aggregate]` macro use
//! `stringify!()` which is deterministic and compiler-version-stable. This is
//! important because aggregate type names are persisted in event stores and
//! used as stream partition keys.

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;

/// A typed aggregate type name.
///
/// Wraps a `Cow<'static, str>` to support both zero-allocation compile-time
/// string literals (from `stringify!()`) and owned strings (from database
/// deserialization).
///
/// # Serialization
///
/// Serializes transparently as a plain string, ensuring backward-compatible
/// JSON and database formats.
///
/// # Examples
///
/// ```
/// use event_sauce_core::AggregateType;
///
/// // From a static string (zero allocation)
/// let agg_type = AggregateType::new("User");
/// assert_eq!(agg_type.as_str(), "User");
/// assert_eq!(agg_type, "User");
///
/// // From an owned string (e.g., from database)
/// let agg_type = AggregateType::from_owned("Order".to_string());
/// assert_eq!(agg_type.as_str(), "Order");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AggregateType(Cow<'static, str>);

impl AggregateType {
    /// Creates a new `AggregateType` from a static string.
    ///
    /// This is the preferred constructor for compile-time-known names
    /// (e.g., from `stringify!()` in macro-generated code).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateType;
    ///
    /// let agg_type = AggregateType::new("User");
    /// assert_eq!(agg_type.as_str(), "User");
    /// ```
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }

    /// Creates a new `AggregateType` from an owned string.
    ///
    /// Useful when the name comes from runtime data (e.g., database rows).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateType;
    ///
    /// let agg_type = AggregateType::from_owned("Order".to_string());
    /// assert_eq!(agg_type.as_str(), "Order");
    /// ```
    #[must_use]
    pub fn from_owned(name: String) -> Self {
        Self(Cow::Owned(name))
    }

    /// Returns the aggregate type name as a string slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateType;
    ///
    /// let agg_type = AggregateType::new("User");
    /// assert_eq!(agg_type.as_str(), "User");
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AggregateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&'static str> for AggregateType {
    fn from(s: &'static str) -> Self {
        Self::new(s)
    }
}

impl From<String> for AggregateType {
    fn from(s: String) -> Self {
        Self::from_owned(s)
    }
}

impl AsRef<str> for AggregateType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for AggregateType {
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<&str> for AggregateType {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

impl PartialEq<String> for AggregateType {
    fn eq(&self, other: &String) -> bool {
        self.0.as_ref() == other.as_str()
    }
}

impl PartialEq<AggregateType> for &str {
    fn eq(&self, other: &AggregateType) -> bool {
        *self == other.0.as_ref()
    }
}

impl PartialEq<AggregateType> for String {
    fn eq(&self, other: &AggregateType) -> bool {
        self.as_str() == other.0.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_new_from_static_str() {
        let agg_type = AggregateType::new("User");
        assert_eq!(agg_type.as_str(), "User");
    }

    #[test]
    fn test_from_owned_string() {
        let agg_type = AggregateType::from_owned("Order".to_string());
        assert_eq!(agg_type.as_str(), "Order");
    }

    #[test]
    fn test_display() {
        let agg_type = AggregateType::new("Product");
        assert_eq!(format!("{agg_type}"), "Product");
    }

    #[test]
    fn test_equality_same_type() {
        let a = AggregateType::new("User");
        let b = AggregateType::new("User");
        assert_eq!(a, b);
    }

    #[test]
    fn test_equality_borrowed_vs_owned() {
        let borrowed = AggregateType::new("User");
        let owned = AggregateType::from_owned("User".to_string());
        assert_eq!(borrowed, owned);
    }

    #[test]
    fn test_inequality() {
        let a = AggregateType::new("User");
        let b = AggregateType::new("Order");
        assert_ne!(a, b);
    }

    #[test]
    fn test_partial_eq_str() {
        let agg_type = AggregateType::new("User");
        assert_eq!(agg_type, *"User");
    }

    #[test]
    fn test_partial_eq_str_ref() {
        let agg_type = AggregateType::new("User");
        assert_eq!(agg_type, "User");
    }

    #[test]
    fn test_partial_eq_string() {
        let agg_type = AggregateType::new("User");
        assert_eq!(agg_type, "User".to_string());
    }

    #[test]
    fn test_partial_eq_reverse_str_ref() {
        let agg_type = AggregateType::new("User");
        assert_eq!("User", agg_type);
    }

    #[test]
    fn test_partial_eq_reverse_string() {
        let agg_type = AggregateType::new("User");
        assert_eq!("User".to_string(), agg_type);
    }

    #[test]
    fn test_from_static_str() {
        let agg_type: AggregateType = "User".into();
        assert_eq!(agg_type.as_str(), "User");
    }

    #[test]
    fn test_from_string() {
        let agg_type: AggregateType = "Order".to_string().into();
        assert_eq!(agg_type.as_str(), "Order");
    }

    #[test]
    fn test_as_ref() {
        let agg_type = AggregateType::new("User");
        let s: &str = agg_type.as_ref();
        assert_eq!(s, "User");
    }

    #[test]
    fn test_hash_consistency() {
        let borrowed = AggregateType::new("User");
        let owned = AggregateType::from_owned("User".to_string());

        let mut set = HashSet::new();
        set.insert(borrowed);
        // Owned variant with same string should be considered equal
        assert!(set.contains(&owned));
    }

    #[test]
    fn test_clone() {
        let original = AggregateType::new("User");
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_serde_json_roundtrip() {
        let agg_type = AggregateType::new("User");
        let json = serde_json::to_string(&agg_type).unwrap();
        assert_eq!(json, "\"User\"");

        let deserialized: AggregateType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, agg_type);
    }

    #[test]
    fn test_serde_transparent_serializes_as_plain_string() {
        let agg_type = AggregateType::new("Order");
        let value = serde_json::to_value(&agg_type).unwrap();
        // Should be a plain string, not an object
        assert!(value.is_string());
        assert_eq!(value.as_str().unwrap(), "Order");
    }

    #[test]
    fn test_debug() {
        let agg_type = AggregateType::new("User");
        let debug = format!("{agg_type:?}");
        assert!(debug.contains("User"));
    }
}
