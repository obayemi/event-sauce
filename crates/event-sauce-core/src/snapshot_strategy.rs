//! Snapshot strategy definitions for controlling when snapshots are created.
//!
//! Snapshot strategies determine when aggregate snapshots should be taken during
//! the commit process. Different strategies can be applied to different aggregate
//! types to optimize performance based on usage patterns.
//!
//! # Available Strategies
//!
//! - [`AlwaysSnapshot`] - Creates a snapshot on every commit
//! - [`NeverSnapshot`] - Never creates snapshots
//! - [`EveryNEvents`] - Creates snapshots at specific version intervals
//!
//! # Examples
//!
//! ```
//! use event_sauce_core::{SnapshotStrategy, AlwaysSnapshot, NeverSnapshot, EveryNEvents, Version};
//!
//! // Strategy that always snapshots
//! let always = AlwaysSnapshot;
//! assert!(always.should_snapshot(Version::new(1)));
//! assert!(always.should_snapshot(Version::new(100)));
//!
//! // Strategy that never snapshots
//! let never = NeverSnapshot;
//! assert!(!never.should_snapshot(Version::new(1)));
//! assert!(!never.should_snapshot(Version::new(100)));
//!
//! // Strategy that snapshots every 100 events
//! let every_100 = EveryNEvents(100);
//! assert!(!every_100.should_snapshot(Version::new(99)));
//! assert!(every_100.should_snapshot(Version::new(100)));
//! assert!(!every_100.should_snapshot(Version::new(101)));
//! assert!(every_100.should_snapshot(Version::new(200)));
//! ```

use crate::Version;

/// Trait for determining when snapshots should be created.
///
/// Implementations of this trait define the policy for snapshot creation
/// based on the current version of an aggregate.
pub trait SnapshotStrategy: Send + Sync {
    /// Determines whether a snapshot should be created at the given version.
    ///
    /// # Arguments
    ///
    /// * `current_version` - The version of the aggregate after the latest commit
    ///
    /// # Returns
    ///
    /// `true` if a snapshot should be created, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotStrategy, EveryNEvents, Version};
    ///
    /// let strategy = EveryNEvents(50);
    /// assert!(!strategy.should_snapshot(Version::new(49)));
    /// assert!(strategy.should_snapshot(Version::new(50)));
    /// assert!(strategy.should_snapshot(Version::new(100)));
    /// ```
    fn should_snapshot(&self, current_version: Version) -> bool;
}

/// Snapshot strategy that creates a snapshot on every commit.
///
/// This strategy is useful for aggregates that:
/// - Are frequently read and rarely updated
/// - Have expensive reconstruction logic
/// - Need guaranteed fast load times
///
/// # Trade-offs
///
/// - **Pros**: Fastest possible load times, always up-to-date snapshots
/// - **Cons**: Highest storage overhead, more write operations
///
/// # Examples
///
/// ```
/// use event_sauce_core::{SnapshotStrategy, AlwaysSnapshot, Version};
///
/// let strategy = AlwaysSnapshot;
/// assert!(strategy.should_snapshot(Version::new(1)));
/// assert!(strategy.should_snapshot(Version::new(2)));
/// assert!(strategy.should_snapshot(Version::new(1000)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlwaysSnapshot;

impl SnapshotStrategy for AlwaysSnapshot {
    fn should_snapshot(&self, _current_version: Version) -> bool {
        true
    }
}

/// Snapshot strategy that never creates snapshots.
///
/// This strategy is useful for aggregates that:
/// - Have very few events (always fast to reconstruct)
/// - Are write-heavy and rarely read
/// - Don't benefit from snapshot optimization
///
/// # Trade-offs
///
/// - **Pros**: No storage overhead, fewer write operations
/// - **Cons**: Load time increases with event count
///
/// # Examples
///
/// ```
/// use event_sauce_core::{SnapshotStrategy, NeverSnapshot, Version};
///
/// let strategy = NeverSnapshot;
/// assert!(!strategy.should_snapshot(Version::new(1)));
/// assert!(!strategy.should_snapshot(Version::new(100)));
/// assert!(!strategy.should_snapshot(Version::new(10000)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeverSnapshot;

impl SnapshotStrategy for NeverSnapshot {
    fn should_snapshot(&self, _current_version: Version) -> bool {
        false
    }
}

/// Snapshot strategy that creates snapshots every N events.
///
/// This strategy creates a snapshot when the aggregate version is exactly
/// divisible by the specified interval. For example, `EveryNEvents(100)`
/// will create snapshots at versions 100, 200, 300, etc.
///
/// This strategy is useful for aggregates that:
/// - Have moderate to high event counts
/// - Need to balance storage vs load performance
/// - Have predictable event growth patterns
///
/// # Trade-offs
///
/// - **Pros**: Balanced storage overhead, predictable snapshot frequency
/// - **Cons**: Load time varies based on distance from last snapshot
///
/// # Examples
///
/// ```
/// use event_sauce_core::{SnapshotStrategy, EveryNEvents, Version};
///
/// let strategy = EveryNEvents(100);
///
/// // No snapshot at versions before the interval
/// assert!(!strategy.should_snapshot(Version::new(1)));
/// assert!(!strategy.should_snapshot(Version::new(50)));
/// assert!(!strategy.should_snapshot(Version::new(99)));
///
/// // Snapshot at exact intervals
/// assert!(strategy.should_snapshot(Version::new(100)));
/// assert!(strategy.should_snapshot(Version::new(200)));
/// assert!(strategy.should_snapshot(Version::new(300)));
///
/// // No snapshot between intervals
/// assert!(!strategy.should_snapshot(Version::new(101)));
/// assert!(!strategy.should_snapshot(Version::new(250)));
/// ```
///
/// # Panics
///
/// Creating `EveryNEvents(0)` will panic as it would create infinite snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EveryNEvents(pub u32);

impl EveryNEvents {
    /// Creates a new `EveryNEvents` strategy.
    ///
    /// # Arguments
    ///
    /// * `interval` - The number of events between snapshots
    ///
    /// # Panics
    ///
    /// Panics if `interval` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EveryNEvents;
    ///
    /// let strategy = EveryNEvents::new(50);
    /// assert_eq!(strategy.interval(), 50);
    /// ```
    #[must_use] pub fn new(interval: u32) -> Self {
        assert!(interval > 0, "Snapshot interval must be greater than 0");
        Self(interval)
    }

    /// Returns the snapshot interval.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EveryNEvents;
    ///
    /// let strategy = EveryNEvents(100);
    /// assert_eq!(strategy.interval(), 100);
    /// ```
    #[must_use] pub fn interval(&self) -> u32 {
        self.0
    }
}

impl SnapshotStrategy for EveryNEvents {
    fn should_snapshot(&self, current_version: Version) -> bool {
        if current_version.as_i32() <= 0 {
            return false;
        }
        (current_version.as_i32() as u32) % self.0 == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_always_snapshot_returns_true_for_all_versions() {
        let strategy = AlwaysSnapshot;

        assert!(strategy.should_snapshot(Version::new(0)));
        assert!(strategy.should_snapshot(Version::new(1)));
        assert!(strategy.should_snapshot(Version::new(10)));
        assert!(strategy.should_snapshot(Version::new(100)));
        assert!(strategy.should_snapshot(Version::new(1000)));
        assert!(strategy.should_snapshot(Version::new(i32::MAX)));
    }

    #[test]
    fn test_never_snapshot_returns_false_for_all_versions() {
        let strategy = NeverSnapshot;

        assert!(!strategy.should_snapshot(Version::new(0)));
        assert!(!strategy.should_snapshot(Version::new(1)));
        assert!(!strategy.should_snapshot(Version::new(10)));
        assert!(!strategy.should_snapshot(Version::new(100)));
        assert!(!strategy.should_snapshot(Version::new(1000)));
        assert!(!strategy.should_snapshot(Version::new(i32::MAX)));
    }

    #[test]
    fn test_every_n_events_snapshots_at_exact_intervals() {
        let strategy = EveryNEvents(100);

        // Should snapshot at exact multiples of 100
        assert!(strategy.should_snapshot(Version::new(100)));
        assert!(strategy.should_snapshot(Version::new(200)));
        assert!(strategy.should_snapshot(Version::new(300)));
        assert!(strategy.should_snapshot(Version::new(1000)));
        assert!(strategy.should_snapshot(Version::new(10000)));
    }

    #[test]
    fn test_every_n_events_does_not_snapshot_between_intervals() {
        let strategy = EveryNEvents(100);

        // Should not snapshot before first interval
        assert!(!strategy.should_snapshot(Version::new(0)));
        assert!(!strategy.should_snapshot(Version::new(1)));
        assert!(!strategy.should_snapshot(Version::new(50)));
        assert!(!strategy.should_snapshot(Version::new(99)));

        // Should not snapshot between intervals
        assert!(!strategy.should_snapshot(Version::new(101)));
        assert!(!strategy.should_snapshot(Version::new(150)));
        assert!(!strategy.should_snapshot(Version::new(199)));
        assert!(!strategy.should_snapshot(Version::new(201)));
    }

    #[test]
    fn test_every_n_events_with_small_interval() {
        let strategy = EveryNEvents(5);

        assert!(!strategy.should_snapshot(Version::new(0)));
        assert!(!strategy.should_snapshot(Version::new(1)));
        assert!(!strategy.should_snapshot(Version::new(4)));
        assert!(strategy.should_snapshot(Version::new(5)));
        assert!(!strategy.should_snapshot(Version::new(6)));
        assert!(strategy.should_snapshot(Version::new(10)));
        assert!(strategy.should_snapshot(Version::new(15)));
        assert!(!strategy.should_snapshot(Version::new(16)));
    }

    #[test]
    fn test_every_n_events_with_large_interval() {
        let strategy = EveryNEvents(10000);

        assert!(!strategy.should_snapshot(Version::new(9999)));
        assert!(strategy.should_snapshot(Version::new(10000)));
        assert!(!strategy.should_snapshot(Version::new(10001)));
        assert!(strategy.should_snapshot(Version::new(20000)));
    }

    #[test]
    fn test_every_n_events_new_constructor() {
        let strategy = EveryNEvents::new(50);
        assert_eq!(strategy.interval(), 50);
        assert!(strategy.should_snapshot(Version::new(50)));
        assert!(!strategy.should_snapshot(Version::new(49)));
    }

    #[test]
    fn test_every_n_events_interval_getter() {
        assert_eq!(EveryNEvents(100).interval(), 100);
        assert_eq!(EveryNEvents(1).interval(), 1);
        assert_eq!(EveryNEvents(u32::MAX).interval(), u32::MAX);
    }

    #[test]
    #[should_panic(expected = "Snapshot interval must be greater than 0")]
    fn test_every_n_events_panics_on_zero_interval() {
        let _ = EveryNEvents::new(0);
    }

    #[test]
    fn test_strategy_trait_object_always() {
        let strategy: &dyn SnapshotStrategy = &AlwaysSnapshot;
        assert!(strategy.should_snapshot(Version::new(1)));
        assert!(strategy.should_snapshot(Version::new(100)));
    }

    #[test]
    fn test_strategy_trait_object_never() {
        let strategy: &dyn SnapshotStrategy = &NeverSnapshot;
        assert!(!strategy.should_snapshot(Version::new(1)));
        assert!(!strategy.should_snapshot(Version::new(100)));
    }

    #[test]
    fn test_strategy_trait_object_every_n() {
        let strategy: &dyn SnapshotStrategy = &EveryNEvents(50);
        assert!(!strategy.should_snapshot(Version::new(49)));
        assert!(strategy.should_snapshot(Version::new(50)));
        assert!(!strategy.should_snapshot(Version::new(51)));
        assert!(strategy.should_snapshot(Version::new(100)));
    }

    #[test]
    fn test_strategy_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlwaysSnapshot>();
        assert_send_sync::<NeverSnapshot>();
        assert_send_sync::<EveryNEvents>();
    }

    #[test]
    fn test_strategy_debug_format() {
        assert_eq!(format!("{AlwaysSnapshot:?}"), "AlwaysSnapshot");
        assert_eq!(format!("{NeverSnapshot:?}"), "NeverSnapshot");
        assert_eq!(format!("{:?}", EveryNEvents(100)), "EveryNEvents(100)");
    }

    #[test]
    fn test_strategy_equality() {
        assert_eq!(AlwaysSnapshot, AlwaysSnapshot);
        assert_eq!(NeverSnapshot, NeverSnapshot);
        assert_eq!(EveryNEvents(100), EveryNEvents(100));
        assert_ne!(EveryNEvents(100), EveryNEvents(50));
    }

    #[test]
    fn test_strategy_clone() {
        let always = AlwaysSnapshot;
        let always_clone = always;
        assert_eq!(always, always_clone);

        let never = NeverSnapshot;
        let never_clone = never;
        assert_eq!(never, never_clone);

        let every = EveryNEvents(100);
        let every_clone = every;
        assert_eq!(every, every_clone);
    }
}
