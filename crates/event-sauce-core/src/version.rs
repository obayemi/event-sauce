//! Version types for aggregate versioning and event schema versioning.
//!
//! Two distinct version types prevent mixing aggregate stream positions
//! with event schema versions at compile time:
//!
//! - [`AggregateVersion`] — stream position / optimistic concurrency control
//! - [`EventVersion`] — event schema version for evolution/migration

use serde::{Deserialize, Serialize};
use std::fmt;

/// Version number for aggregate stream position and optimistic concurrency control.
///
/// Represents how many events have been applied to an aggregate.
/// Versions start at 0 and increment with each event.
///
/// # Examples
///
/// ```
/// use event_sauce_core::AggregateVersion;
///
/// let v1 = AggregateVersion::new(0);
/// let v2 = AggregateVersion::new(1);
///
/// assert!(v2 > v1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AggregateVersion(u64);

impl AggregateVersion {
    /// Creates a new aggregate version.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateVersion;
    ///
    /// let version = AggregateVersion::new(0);
    /// assert_eq!(version.as_u64(), 0);
    /// ```
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the version as a u64.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateVersion;
    ///
    /// let version = AggregateVersion::new(42);
    /// assert_eq!(version.as_u64(), 42);
    /// ```
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the next version.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::AggregateVersion;
    ///
    /// let v1 = AggregateVersion::new(5);
    /// let v2 = v1.next();
    /// assert_eq!(v2.as_u64(), 6);
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
    /// use event_sauce_core::AggregateVersion;
    ///
    /// let initial = AggregateVersion::initial();
    /// assert_eq!(initial.as_u64(), 0);
    /// ```
    #[must_use]
    pub const fn initial() -> Self {
        Self(0)
    }
}

impl fmt::Display for AggregateVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl From<u64> for AggregateVersion {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<AggregateVersion> for u64 {
    fn from(version: AggregateVersion) -> Self {
        version.0
    }
}

/// Version number for event schema versioning.
///
/// Represents which schema version an event type uses,
/// enabling event evolution and migration.
///
/// # Examples
///
/// ```
/// use event_sauce_core::EventVersion;
///
/// let v1 = EventVersion::new(1);
/// let v2 = EventVersion::new(2);
///
/// assert!(v2 > v1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventVersion(u64);

impl EventVersion {
    /// Creates a new event version.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventVersion;
    ///
    /// let version = EventVersion::new(1);
    /// assert_eq!(version.as_u64(), 1);
    /// ```
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the version as a u64.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventVersion;
    ///
    /// let version = EventVersion::new(42);
    /// assert_eq!(version.as_u64(), 42);
    /// ```
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for EventVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl From<u64> for EventVersion {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<EventVersion> for u64 {
    fn from(version: EventVersion) -> Self {
        version.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === AggregateVersion tests ===

    #[test]
    fn test_aggregate_version_new() {
        let version = AggregateVersion::new(42);
        assert_eq!(version.as_u64(), 42);
    }

    #[test]
    fn test_aggregate_version_initial() {
        let version = AggregateVersion::initial();
        assert_eq!(version.as_u64(), 0);
    }

    #[test]
    fn test_aggregate_version_next() {
        let v1 = AggregateVersion::new(5);
        let v2 = v1.next();
        assert_eq!(v2.as_u64(), 6);
    }

    #[test]
    fn test_aggregate_version_next_chain() {
        let v1 = AggregateVersion::initial();
        let v2 = v1.next();
        let v3 = v2.next();
        let v4 = v3.next();

        assert_eq!(v1.as_u64(), 0);
        assert_eq!(v2.as_u64(), 1);
        assert_eq!(v3.as_u64(), 2);
        assert_eq!(v4.as_u64(), 3);
    }

    #[test]
    fn test_aggregate_version_ordering() {
        let v1 = AggregateVersion::new(1);
        let v2 = AggregateVersion::new(2);
        let v3 = AggregateVersion::new(3);

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v1 < v3);
        assert!(v2 > v1);
    }

    #[test]
    fn test_aggregate_version_display() {
        let version = AggregateVersion::new(42);
        assert_eq!(format!("{version}"), "v42");
    }

    #[test]
    fn test_aggregate_version_from_u64() {
        let version: AggregateVersion = 42u64.into();
        assert_eq!(version.as_u64(), 42);
    }

    #[test]
    fn test_aggregate_version_into_u64() {
        let version = AggregateVersion::new(42);
        let value: u64 = version.into();
        assert_eq!(value, 42);
    }

    // === EventVersion tests ===

    #[test]
    fn test_event_version_new() {
        let version = EventVersion::new(1);
        assert_eq!(version.as_u64(), 1);
    }

    #[test]
    fn test_event_version_ordering() {
        let v1 = EventVersion::new(1);
        let v2 = EventVersion::new(2);
        let v3 = EventVersion::new(3);

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v1 < v3);
        assert!(v2 > v1);
    }

    #[test]
    fn test_event_version_display() {
        let version = EventVersion::new(2);
        assert_eq!(format!("{version}"), "v2");
    }

    #[test]
    fn test_event_version_from_u64() {
        let version: EventVersion = 3u64.into();
        assert_eq!(version.as_u64(), 3);
    }

    #[test]
    fn test_event_version_into_u64() {
        let version = EventVersion::new(5);
        let value: u64 = version.into();
        assert_eq!(value, 5);
    }

    // === Type safety tests ===

    #[test]
    fn test_types_are_distinct() {
        // Both types hold the same value but are not interchangeable
        let agg = AggregateVersion::new(1);
        let evt = EventVersion::new(1);

        // They both display the same way
        assert_eq!(format!("{agg}"), format!("{evt}"));

        // But they are distinct types (this is a compile-time guarantee)
        assert_eq!(agg.as_u64(), evt.as_u64());
    }
}
