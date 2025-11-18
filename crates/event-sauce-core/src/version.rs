//! Version type for optimistic concurrency control.
//!
//! The `Version` type represents the version of an aggregate or event stream.
//! It's used for optimistic concurrency control to prevent conflicting updates.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Version number for optimistic concurrency control.
///
/// Represents the current version of an aggregate or event stream.
/// Versions start at 0 and increment with each event.
///
/// # Examples
///
/// ```
/// use event_sauce_core::Version;
///
/// let v1 = Version::new(0);
/// let v2 = Version::new(1);
///
/// assert!(v2 > v1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Version(i32);

impl Version {
    /// Creates a new version.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Version;
    ///
    /// let version = Version::new(0);
    /// assert_eq!(version.as_i32(), 0);
    /// ```
    #[must_use]
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    /// Returns the version as an i32.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Version;
    ///
    /// let version = Version::new(42);
    /// assert_eq!(version.as_i32(), 42);
    /// ```
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self.0
    }

    /// Returns the next version.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Version;
    ///
    /// let v1 = Version::new(5);
    /// let v2 = v1.next();
    /// assert_eq!(v2.as_i32(), 6);
    /// ```
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Returns the initial version (0).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::Version;
    ///
    /// let initial = Version::initial();
    /// assert_eq!(initial.as_i32(), 0);
    /// ```
    #[must_use]
    pub const fn initial() -> Self {
        Self(0)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl From<i32> for Version {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<Version> for i32 {
    fn from(version: Version) -> Self {
        version.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_new() {
        let version = Version::new(42);
        assert_eq!(version.as_i32(), 42);
    }

    #[test]
    fn test_version_initial() {
        let version = Version::initial();
        assert_eq!(version.as_i32(), 0);
    }

    #[test]
    fn test_version_next() {
        let v1 = Version::new(5);
        let v2 = v1.next();
        assert_eq!(v2.as_i32(), 6);
    }

    #[test]
    fn test_version_next_chain() {
        let v1 = Version::initial();
        let v2 = v1.next();
        let v3 = v2.next();
        let v4 = v3.next();

        assert_eq!(v1.as_i32(), 0);
        assert_eq!(v2.as_i32(), 1);
        assert_eq!(v3.as_i32(), 2);
        assert_eq!(v4.as_i32(), 3);
    }

    #[test]
    fn test_version_ordering() {
        let v1 = Version::new(1);
        let v2 = Version::new(2);
        let v3 = Version::new(3);

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v1 < v3);
        assert!(v2 > v1);
    }

    #[test]
    fn test_version_equality() {
        let v1 = Version::new(5);
        let v2 = Version::new(5);
        let v3 = Version::new(6);

        assert_eq!(v1, v2);
        assert_ne!(v1, v3);
    }

    #[test]
    fn test_version_display() {
        let version = Version::new(42);
        assert_eq!(format!("{version}"), "v42");
    }

    #[test]
    fn test_version_from_i32() {
        let version: Version = 42.into();
        assert_eq!(version.as_i32(), 42);
    }

    #[test]
    fn test_version_into_i32() {
        let version = Version::new(42);
        let value: i32 = version.into();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_version_clone() {
        let v1 = Version::new(10);
        let v2 = v1;
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_version_debug() {
        let version = Version::new(42);
        let debug_str = format!("{version:?}");
        assert!(debug_str.contains("42"));
    }
}
