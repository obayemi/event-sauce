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
//! use event_sauce_core::{SnapshotStrategy, AlwaysSnapshot, NeverSnapshot, EveryNEvents, AggregateVersion};
//!
//! // Strategy that always snapshots
//! let always = AlwaysSnapshot;
//! assert!(always.should_snapshot(AggregateVersion::new(1)));
//! assert!(always.should_snapshot(AggregateVersion::new(100)));
//!
//! // Strategy that never snapshots
//! let never = NeverSnapshot;
//! assert!(!never.should_snapshot(AggregateVersion::new(1)));
//! assert!(!never.should_snapshot(AggregateVersion::new(100)));
//!
//! // Strategy that snapshots every 100 events
//! let every_100 = EveryNEvents::try_new(100).unwrap();
//! assert!(!every_100.should_snapshot(AggregateVersion::new(99)));
//! assert!(every_100.should_snapshot(AggregateVersion::new(100)));
//! assert!(!every_100.should_snapshot(AggregateVersion::new(101)));
//! assert!(every_100.should_snapshot(AggregateVersion::new(200)));
//! ```

use std::num::NonZeroU32;

use crate::AggregateVersion;

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
    /// use event_sauce_core::{SnapshotStrategy, EveryNEvents, AggregateVersion};
    ///
    /// let strategy = EveryNEvents::try_new(50).unwrap();
    /// assert!(!strategy.should_snapshot(AggregateVersion::new(49)));
    /// assert!(strategy.should_snapshot(AggregateVersion::new(50)));
    /// assert!(strategy.should_snapshot(AggregateVersion::new(100)));
    /// ```
    fn should_snapshot(&self, current_version: AggregateVersion) -> bool;

    /// Determines whether a snapshot should be created for a commit that
    /// advanced the aggregate from `previous_version` to `current_version`.
    ///
    /// A single commit can append several events at once, advancing the
    /// version *past* a snapshot boundary without landing exactly on it (for
    /// example version 1 -> 6 with `EveryNEvents(5)`). Strategies that fire on
    /// boundaries should override this method to detect such *crossings*, so a
    /// multi-event commit still triggers a snapshot.
    ///
    /// The default implementation delegates to [`should_snapshot`] using only
    /// the post-commit `current_version`, preserving exact-landing semantics for
    /// strategies (like [`AlwaysSnapshot`] / [`NeverSnapshot`]) that do not care
    /// about boundaries.
    ///
    /// [`should_snapshot`]: SnapshotStrategy::should_snapshot
    ///
    /// # Arguments
    ///
    /// * `previous_version` - The aggregate version before the committed events
    /// * `current_version` - The aggregate version after the committed events
    ///
    /// # Returns
    ///
    /// `true` if a snapshot should be created, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotStrategy, EveryNEvents, AggregateVersion};
    ///
    /// let strategy = EveryNEvents::try_new(5).unwrap();
    /// // A single commit from version 1 to 6 crosses the boundary at 5.
    /// assert!(strategy.should_snapshot_range(
    ///     AggregateVersion::new(1),
    ///     AggregateVersion::new(6),
    /// ));
    /// ```
    fn should_snapshot_range(
        &self,
        previous_version: AggregateVersion,
        current_version: AggregateVersion,
    ) -> bool {
        let _ = previous_version;
        self.should_snapshot(current_version)
    }
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
/// use event_sauce_core::{SnapshotStrategy, AlwaysSnapshot, AggregateVersion};
///
/// let strategy = AlwaysSnapshot;
/// assert!(strategy.should_snapshot(AggregateVersion::new(1)));
/// assert!(strategy.should_snapshot(AggregateVersion::new(2)));
/// assert!(strategy.should_snapshot(AggregateVersion::new(1000)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlwaysSnapshot;

impl SnapshotStrategy for AlwaysSnapshot {
    fn should_snapshot(&self, _current_version: AggregateVersion) -> bool {
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
/// use event_sauce_core::{SnapshotStrategy, NeverSnapshot, AggregateVersion};
///
/// let strategy = NeverSnapshot;
/// assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
/// assert!(!strategy.should_snapshot(AggregateVersion::new(100)));
/// assert!(!strategy.should_snapshot(AggregateVersion::new(10000)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeverSnapshot;

impl SnapshotStrategy for NeverSnapshot {
    fn should_snapshot(&self, _current_version: AggregateVersion) -> bool {
        false
    }
}

/// Snapshot strategy that creates snapshots every N events.
///
/// This strategy creates a snapshot whenever a commit advances the aggregate
/// across an interval boundary. For example, `EveryNEvents(100)` snapshots at
/// versions 100, 200, 300, etc.
///
/// Because a single commit may append several events at once, the boundary is
/// detected on *crossing* rather than exact landing: a commit that takes the
/// version from 1 to 6 with `EveryNEvents(5)` still produces a snapshot even
/// though it never lands exactly on 5. See
/// [`should_snapshot_range`](SnapshotStrategy::should_snapshot_range).
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
/// use event_sauce_core::{SnapshotStrategy, EveryNEvents, AggregateVersion};
///
/// let strategy = EveryNEvents::try_new(100).unwrap();
///
/// // No snapshot at versions before the interval
/// assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
/// assert!(!strategy.should_snapshot(AggregateVersion::new(50)));
/// assert!(!strategy.should_snapshot(AggregateVersion::new(99)));
///
/// // Snapshot at exact intervals
/// assert!(strategy.should_snapshot(AggregateVersion::new(100)));
/// assert!(strategy.should_snapshot(AggregateVersion::new(200)));
/// assert!(strategy.should_snapshot(AggregateVersion::new(300)));
///
/// // No snapshot between intervals
/// assert!(!strategy.should_snapshot(AggregateVersion::new(101)));
/// assert!(!strategy.should_snapshot(AggregateVersion::new(250)));
/// ```
///
/// A zero interval is unrepresentable: the field holds a [`NonZeroU32`], so
/// there is no `should_snapshot`/`should_snapshot_range` call that could ever
/// divide by zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EveryNEvents(NonZeroU32);

impl EveryNEvents {
    /// The interval [`SnapshotConfig`](crate::SnapshotConfig)'s built-in
    /// default uses: a snapshot every 100 events.
    pub const DEFAULT_INTERVAL: NonZeroU32 = match NonZeroU32::new(100) {
        Some(n) => n,
        None => unreachable!(),
    };

    /// Creates a new `EveryNEvents` strategy from a non-zero interval.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EveryNEvents;
    /// use std::num::NonZeroU32;
    ///
    /// let strategy = EveryNEvents::new(NonZeroU32::new(50).unwrap());
    /// assert_eq!(strategy.interval(), 50);
    /// ```
    #[must_use]
    pub const fn new(interval: NonZeroU32) -> Self {
        Self(interval)
    }

    /// Creates a new `EveryNEvents` strategy from a plain `u32`, or `None` if
    /// `interval` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EveryNEvents;
    ///
    /// assert!(EveryNEvents::try_new(0).is_none());
    /// assert_eq!(EveryNEvents::try_new(50).unwrap().interval(), 50);
    /// ```
    #[must_use]
    pub fn try_new(interval: u32) -> Option<Self> {
        NonZeroU32::new(interval).map(Self)
    }

    /// Returns the snapshot interval.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EveryNEvents;
    ///
    /// let strategy = EveryNEvents::try_new(100).unwrap();
    /// assert_eq!(strategy.interval(), 100);
    /// ```
    #[must_use]
    pub fn interval(&self) -> u32 {
        self.0.get()
    }
}

impl SnapshotStrategy for EveryNEvents {
    fn should_snapshot(&self, current_version: AggregateVersion) -> bool {
        if current_version.as_i64() == 0 {
            return false;
        }
        current_version.as_i64() % i64::from(self.0.get()) == 0
    }

    fn should_snapshot_range(
        &self,
        previous_version: AggregateVersion,
        current_version: AggregateVersion,
    ) -> bool {
        let interval = i64::from(self.0.get());
        // Number of completed intervals before and after the commit. A snapshot
        // boundary is crossed whenever the count increases, which covers both
        // exact landings (e.g. 4 -> 5) and multi-event jumps (e.g. 1 -> 6).
        let previous_intervals = previous_version.as_i64().max(0) / interval;
        let current_intervals = current_version.as_i64().max(0) / interval;
        current_intervals > previous_intervals
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_always_snapshot_returns_true_for_all_versions() {
        let strategy = AlwaysSnapshot;

        assert!(strategy.should_snapshot(AggregateVersion::new(0)));
        assert!(strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(strategy.should_snapshot(AggregateVersion::new(10)));
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
        assert!(strategy.should_snapshot(AggregateVersion::new(1000)));
        assert!(strategy.should_snapshot(AggregateVersion::new(i64::MAX)));
    }

    #[test]
    fn test_never_snapshot_returns_false_for_all_versions() {
        let strategy = NeverSnapshot;

        assert!(!strategy.should_snapshot(AggregateVersion::new(0)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(10)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(100)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(1000)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(i64::MAX)));
    }

    #[test]
    fn test_every_n_events_snapshots_at_exact_intervals() {
        let strategy = EveryNEvents::try_new(100).unwrap();

        // Should snapshot at exact multiples of 100
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
        assert!(strategy.should_snapshot(AggregateVersion::new(200)));
        assert!(strategy.should_snapshot(AggregateVersion::new(300)));
        assert!(strategy.should_snapshot(AggregateVersion::new(1000)));
        assert!(strategy.should_snapshot(AggregateVersion::new(10000)));
    }

    #[test]
    fn test_every_n_events_does_not_snapshot_between_intervals() {
        let strategy = EveryNEvents::try_new(100).unwrap();

        // Should not snapshot before first interval
        assert!(!strategy.should_snapshot(AggregateVersion::new(0)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(50)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(99)));

        // Should not snapshot between intervals
        assert!(!strategy.should_snapshot(AggregateVersion::new(101)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(150)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(199)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(201)));
    }

    #[test]
    fn test_every_n_events_with_small_interval() {
        let strategy = EveryNEvents::try_new(5).unwrap();

        assert!(!strategy.should_snapshot(AggregateVersion::new(0)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(4)));
        assert!(strategy.should_snapshot(AggregateVersion::new(5)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(6)));
        assert!(strategy.should_snapshot(AggregateVersion::new(10)));
        assert!(strategy.should_snapshot(AggregateVersion::new(15)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(16)));
    }

    #[test]
    fn test_every_n_events_with_large_interval() {
        let strategy = EveryNEvents::try_new(10000).unwrap();

        assert!(!strategy.should_snapshot(AggregateVersion::new(9999)));
        assert!(strategy.should_snapshot(AggregateVersion::new(10000)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(10001)));
        assert!(strategy.should_snapshot(AggregateVersion::new(20000)));
    }

    #[test]
    fn test_every_n_events_new_constructor() {
        let strategy = EveryNEvents::new(NonZeroU32::new(50).unwrap());
        assert_eq!(strategy.interval(), 50);
        assert!(strategy.should_snapshot(AggregateVersion::new(50)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(49)));
    }

    #[test]
    fn test_every_n_events_interval_getter() {
        assert_eq!(EveryNEvents::try_new(100).unwrap().interval(), 100);
        assert_eq!(EveryNEvents::try_new(1).unwrap().interval(), 1);
        assert_eq!(
            EveryNEvents::try_new(u32::MAX).unwrap().interval(),
            u32::MAX
        );
    }

    #[test]
    fn test_every_n_events_try_new_rejects_zero() {
        assert!(EveryNEvents::try_new(0).is_none());
    }

    #[test]
    fn test_every_n_events_try_new_accepts_positive() {
        assert_eq!(EveryNEvents::try_new(1).unwrap().interval(), 1);
    }

    #[test]
    fn test_strategy_trait_object_always() {
        let strategy: &dyn SnapshotStrategy = &AlwaysSnapshot;
        assert!(strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_strategy_trait_object_never() {
        let strategy: &dyn SnapshotStrategy = &NeverSnapshot;
        assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_strategy_trait_object_every_n() {
        let strategy: &dyn SnapshotStrategy = &EveryNEvents::try_new(50).unwrap();
        assert!(!strategy.should_snapshot(AggregateVersion::new(49)));
        assert!(strategy.should_snapshot(AggregateVersion::new(50)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(51)));
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_every_n_events_range_fires_when_commit_crosses_boundary() {
        let strategy = EveryNEvents::try_new(5).unwrap();

        // Multi-event commit jumps over the boundary without landing on it.
        assert!(strategy.should_snapshot_range(AggregateVersion::new(1), AggregateVersion::new(6)));
        // Single event landing exactly on the boundary.
        assert!(strategy.should_snapshot_range(AggregateVersion::new(4), AggregateVersion::new(5)));
        // Crossing multiple boundaries at once still fires once.
        assert!(strategy.should_snapshot_range(AggregateVersion::new(2), AggregateVersion::new(13)));
    }

    #[test]
    fn test_every_n_events_range_does_not_fire_without_crossing() {
        let strategy = EveryNEvents::try_new(5).unwrap();

        // Commit stays within the same interval.
        assert!(!strategy.should_snapshot_range(AggregateVersion::new(6), AggregateVersion::new(9)));
        // Just past a boundary already counted by the previous commit.
        assert!(!strategy.should_snapshot_range(AggregateVersion::new(5), AggregateVersion::new(9)));
        // No events advanced.
        assert!(!strategy.should_snapshot_range(AggregateVersion::new(7), AggregateVersion::new(7)));
    }

    #[test]
    fn test_default_range_delegates_to_current_version() {
        // Always/Never do not override should_snapshot_range; the default
        // delegates to should_snapshot using only the current version.
        let always = AlwaysSnapshot;
        assert!(always.should_snapshot_range(AggregateVersion::new(3), AggregateVersion::new(4)));

        let never = NeverSnapshot;
        assert!(!never.should_snapshot_range(AggregateVersion::new(3), AggregateVersion::new(4)));
    }
}
