//! Configuration for snapshot behavior in event stores.
//!
//! This module provides [`SnapshotConfig`] which controls:
//! - Default snapshot strategy for all aggregate types
//! - Per-aggregate-type strategy overrides
//! - Whether snapshots are used during aggregate loading
//!
//! # Examples
//!
//! ```
//! use event_sauce_core::{SnapshotConfig, AlwaysSnapshot, NeverSnapshot, EveryNEvents};
//!
//! // Simple configuration with default strategy
//! let config = SnapshotConfig::builder()
//!     .default_strategy(EveryNEvents(100))
//!     .build();
//!
//! // Configuration with per-type overrides
//! let config = SnapshotConfig::builder()
//!     .default_strategy(EveryNEvents(100))
//!     .per_type_override("User", AlwaysSnapshot)
//!     .per_type_override("Order", NeverSnapshot)
//!     .use_snapshots_on_load(true)
//!     .build();
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::snapshot_strategy::{AlwaysSnapshot, EveryNEvents, NeverSnapshot, SnapshotStrategy};

/// Configuration for snapshot creation and loading behavior.
///
/// This configuration is used by [`EventStore`](crate::EventStore) implementations
/// to determine when to create snapshots and whether to use them during loading.
///
/// # Structure
///
/// - **Default strategy**: Applied to all aggregate types unless overridden
/// - **Per-type overrides**: Specific strategies for individual aggregate types
/// - **Load behavior**: Whether to use snapshots when loading aggregates
///
/// # Examples
///
/// ```
/// use event_sauce_core::{SnapshotConfig, EveryNEvents, AlwaysSnapshot, AggregateVersion};
///
/// let config = SnapshotConfig::builder()
///     .default_strategy(EveryNEvents(100))
///     .per_type_override("User", AlwaysSnapshot)
///     .build();
///
/// // Get strategy for a specific aggregate type
/// let user_strategy = config.strategy_for_type("User");
/// assert!(user_strategy.should_snapshot(AggregateVersion::new(1)));
///
/// let order_strategy = config.strategy_for_type("Order");
/// assert!(!order_strategy.should_snapshot(AggregateVersion::new(1)));
/// assert!(order_strategy.should_snapshot(AggregateVersion::new(100)));
/// ```
#[derive(Clone)]
pub struct SnapshotConfig {
    default_strategy: Arc<dyn SnapshotStrategy>,
    per_type_strategies: HashMap<String, Arc<dyn SnapshotStrategy>>,
    use_snapshots_on_load: bool,
}

impl SnapshotConfig {
    /// Creates a new builder for constructing a [`SnapshotConfig`].
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, NeverSnapshot};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(NeverSnapshot)
    ///     .build();
    /// ```
    #[must_use]
    pub fn builder() -> SnapshotConfigBuilder {
        SnapshotConfigBuilder::new()
    }

    /// Creates a configuration that never creates snapshots and never uses them for loading.
    ///
    /// This is useful for:
    /// - Testing scenarios where you want to verify event replay
    /// - Aggregates with very few events
    /// - Disabling snapshot functionality entirely
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, AggregateVersion};
    ///
    /// let config = SnapshotConfig::disabled();
    ///
    /// assert!(!config.use_snapshots_on_load());
    /// let strategy = config.strategy_for_type("User");
    /// assert!(!strategy.should_snapshot(AggregateVersion::new(100)));
    /// ```
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            default_strategy: Arc::new(NeverSnapshot),
            per_type_strategies: HashMap::new(),
            use_snapshots_on_load: false,
        }
    }

    /// Creates a configuration that always creates snapshots on every commit.
    ///
    /// This is useful for:
    /// - Maximizing load performance at the cost of storage
    /// - Aggregates that are read frequently
    /// - Development and testing
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, AggregateVersion};
    ///
    /// let config = SnapshotConfig::always();
    ///
    /// assert!(config.use_snapshots_on_load());
    /// let strategy = config.strategy_for_type("User");
    /// assert!(strategy.should_snapshot(AggregateVersion::new(1)));
    /// assert!(strategy.should_snapshot(AggregateVersion::new(100)));
    /// ```
    #[must_use]
    pub fn always() -> Self {
        Self {
            default_strategy: Arc::new(AlwaysSnapshot),
            per_type_strategies: HashMap::new(),
            use_snapshots_on_load: true,
        }
    }

    /// Returns the snapshot strategy for the specified aggregate type.
    ///
    /// If a per-type override exists for the given type, it is returned.
    /// Otherwise, the default strategy is returned.
    ///
    /// # Arguments
    ///
    /// * `aggregate_type` - The aggregate type name
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents, AlwaysSnapshot, AggregateVersion};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(100))
    ///     .per_type_override("User", AlwaysSnapshot)
    ///     .build();
    ///
    /// // User has override
    /// let user_strategy = config.strategy_for_type("User");
    /// assert!(user_strategy.should_snapshot(AggregateVersion::new(1)));
    ///
    /// // Order uses default
    /// let order_strategy = config.strategy_for_type("Order");
    /// assert!(!order_strategy.should_snapshot(AggregateVersion::new(1)));
    /// assert!(order_strategy.should_snapshot(AggregateVersion::new(100)));
    /// ```
    #[must_use]
    pub fn strategy_for_type(&self, aggregate_type: &str) -> &dyn SnapshotStrategy {
        self.per_type_strategies.get(aggregate_type).map_or_else(
            || self.default_strategy.as_ref(),
            std::convert::AsRef::as_ref,
        )
    }

    /// Returns whether snapshots should be used when loading aggregates.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(100))
    ///     .use_snapshots_on_load(true)
    ///     .build();
    ///
    /// assert!(config.use_snapshots_on_load());
    /// ```
    #[must_use]
    pub fn use_snapshots_on_load(&self) -> bool {
        self.use_snapshots_on_load
    }

    /// Returns a reference to the default strategy.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents, AggregateVersion};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(100))
    ///     .build();
    ///
    /// let strategy = config.default_strategy();
    /// assert!(strategy.should_snapshot(AggregateVersion::new(100)));
    /// ```
    #[must_use]
    pub fn default_strategy(&self) -> &dyn SnapshotStrategy {
        self.default_strategy.as_ref()
    }
}

impl std::fmt::Debug for SnapshotConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotConfig")
            .field("use_snapshots_on_load", &self.use_snapshots_on_load)
            .field(
                "per_type_overrides",
                &self.per_type_strategies.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// Builder for creating [`SnapshotConfig`] instances.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{SnapshotConfig, EveryNEvents, AlwaysSnapshot, NeverSnapshot};
///
/// let config = SnapshotConfig::builder()
///     .default_strategy(EveryNEvents(100))
///     .per_type_override("User", AlwaysSnapshot)
///     .per_type_override("Order", NeverSnapshot)
///     .use_snapshots_on_load(true)
///     .build();
/// ```
pub struct SnapshotConfigBuilder {
    default_strategy: Arc<dyn SnapshotStrategy>,
    per_type_strategies: HashMap<String, Arc<dyn SnapshotStrategy>>,
    use_snapshots_on_load: bool,
}

impl SnapshotConfigBuilder {
    /// Creates a new builder with default settings.
    ///
    /// Default settings:
    /// - Default strategy: `EveryNEvents(100)` - snapshots every 100 events
    /// - No per-type overrides
    /// - Use snapshots on load: `true`
    #[must_use]
    pub fn new() -> Self {
        Self {
            default_strategy: Arc::new(EveryNEvents(100)),
            per_type_strategies: HashMap::new(),
            use_snapshots_on_load: true,
        }
    }

    /// Sets the default snapshot strategy.
    ///
    /// This strategy will be used for all aggregate types unless a per-type
    /// override is specified.
    ///
    /// # Arguments
    ///
    /// * `strategy` - The snapshot strategy to use as default
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(100))
    ///     .build();
    /// ```
    #[must_use]
    pub fn default_strategy<S>(mut self, strategy: S) -> Self
    where
        S: SnapshotStrategy + 'static,
    {
        self.default_strategy = Arc::new(strategy);
        self
    }

    /// Sets a per-type snapshot strategy override.
    ///
    /// When multiple overrides are set for the same aggregate type,
    /// the last one wins.
    ///
    /// # Arguments
    ///
    /// * `aggregate_type` - The aggregate type name
    /// * `strategy` - The snapshot strategy for this type
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents, AlwaysSnapshot};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(100))
    ///     .per_type_override("User", AlwaysSnapshot)
    ///     .per_type_override("Order", EveryNEvents(50))
    ///     .build();
    /// ```
    #[must_use]
    pub fn per_type_override<S>(mut self, aggregate_type: impl Into<String>, strategy: S) -> Self
    where
        S: SnapshotStrategy + 'static,
    {
        self.per_type_strategies
            .insert(aggregate_type.into(), Arc::new(strategy));
        self
    }

    /// Sets whether snapshots should be used when loading aggregates.
    ///
    /// When set to `false`, the `load` function will always
    /// load all events from version 0, ignoring any existing snapshots.
    ///
    /// # Arguments
    ///
    /// * `use_snapshots` - Whether to use snapshots during loading
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(100))
    ///     .use_snapshots_on_load(false)
    ///     .build();
    ///
    /// assert!(!config.use_snapshots_on_load());
    /// ```
    #[must_use]
    pub fn use_snapshots_on_load(mut self, use_snapshots: bool) -> Self {
        self.use_snapshots_on_load = use_snapshots;
        self
    }

    /// Builds the [`SnapshotConfig`].
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(100))
    ///     .build();
    /// ```
    #[must_use]
    pub fn build(self) -> SnapshotConfig {
        SnapshotConfig {
            default_strategy: self.default_strategy,
            per_type_strategies: self.per_type_strategies,
            use_snapshots_on_load: self.use_snapshots_on_load,
        }
    }
}

impl Default for SnapshotConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateVersion, EveryNEvents};

    #[test]
    fn test_builder_default_settings() {
        let config = SnapshotConfig::builder().build();

        assert!(config.use_snapshots_on_load());
        let strategy = config.default_strategy();
        // Default is EveryNEvents(100)
        assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(101)));
        assert!(strategy.should_snapshot(AggregateVersion::new(200)));
    }

    #[test]
    fn test_builder_with_default_strategy() {
        let config = SnapshotConfig::builder()
            .default_strategy(EveryNEvents(100))
            .build();

        let strategy = config.strategy_for_type("User");
        assert!(!strategy.should_snapshot(AggregateVersion::new(99)));
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_builder_with_per_type_override() {
        let config = SnapshotConfig::builder()
            .default_strategy(EveryNEvents(100))
            .per_type_override("User", AlwaysSnapshot)
            .build();

        // User has override
        let user_strategy = config.strategy_for_type("User");
        assert!(user_strategy.should_snapshot(AggregateVersion::new(1)));

        // Order uses default
        let order_strategy = config.strategy_for_type("Order");
        assert!(!order_strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(order_strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_builder_with_multiple_overrides() {
        let config = SnapshotConfig::builder()
            .default_strategy(EveryNEvents(100))
            .per_type_override("User", AlwaysSnapshot)
            .per_type_override("Order", NeverSnapshot)
            .per_type_override("Product", EveryNEvents(50))
            .build();

        let user_strategy = config.strategy_for_type("User");
        assert!(user_strategy.should_snapshot(AggregateVersion::new(1)));

        let order_strategy = config.strategy_for_type("Order");
        assert!(!order_strategy.should_snapshot(AggregateVersion::new(100)));

        let product_strategy = config.strategy_for_type("Product");
        assert!(product_strategy.should_snapshot(AggregateVersion::new(50)));
        assert!(!product_strategy.should_snapshot(AggregateVersion::new(51)));

        let invoice_strategy = config.strategy_for_type("Invoice");
        assert!(invoice_strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_builder_use_snapshots_on_load_true() {
        let config = SnapshotConfig::builder()
            .use_snapshots_on_load(true)
            .build();

        assert!(config.use_snapshots_on_load());
    }

    #[test]
    fn test_builder_use_snapshots_on_load_false() {
        let config = SnapshotConfig::builder()
            .use_snapshots_on_load(false)
            .build();

        assert!(!config.use_snapshots_on_load());
    }

    #[test]
    fn test_disabled_config() {
        let config = SnapshotConfig::disabled();

        assert!(!config.use_snapshots_on_load());
        let strategy = config.strategy_for_type("User");
        assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_always_config() {
        let config = SnapshotConfig::always();

        assert!(config.use_snapshots_on_load());
        let strategy = config.strategy_for_type("User");
        assert!(strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_strategy_for_type_with_override() {
        let config = SnapshotConfig::builder()
            .default_strategy(NeverSnapshot)
            .per_type_override("User", AlwaysSnapshot)
            .build();

        let user_strategy = config.strategy_for_type("User");
        assert!(user_strategy.should_snapshot(AggregateVersion::new(1)));

        let order_strategy = config.strategy_for_type("Order");
        assert!(!order_strategy.should_snapshot(AggregateVersion::new(1)));
    }

    #[test]
    fn test_strategy_for_type_without_override() {
        let config = SnapshotConfig::builder()
            .default_strategy(EveryNEvents(100))
            .build();

        let strategy = config.strategy_for_type("User");
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(99)));
    }

    #[test]
    fn test_config_is_cloneable() {
        let config = SnapshotConfig::builder()
            .default_strategy(EveryNEvents(100))
            .per_type_override("User", AlwaysSnapshot)
            .build();

        let cloned = config.clone();

        assert_eq!(
            config.use_snapshots_on_load(),
            cloned.use_snapshots_on_load()
        );

        let config_strategy = config.strategy_for_type("User");
        let cloned_strategy = cloned.strategy_for_type("User");
        assert_eq!(
            config_strategy.should_snapshot(AggregateVersion::new(1)),
            cloned_strategy.should_snapshot(AggregateVersion::new(1))
        );
    }

    #[test]
    fn test_last_override_wins() {
        let config = SnapshotConfig::builder()
            .default_strategy(NeverSnapshot)
            .per_type_override("User", AlwaysSnapshot)
            .per_type_override("User", EveryNEvents(100))
            .build();

        let strategy = config.strategy_for_type("User");
        assert!(!strategy.should_snapshot(AggregateVersion::new(1)));
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
    }

    #[test]
    fn test_default_strategy_getter() {
        let config = SnapshotConfig::builder()
            .default_strategy(EveryNEvents(100))
            .build();

        let strategy = config.default_strategy();
        assert!(strategy.should_snapshot(AggregateVersion::new(100)));
        assert!(!strategy.should_snapshot(AggregateVersion::new(99)));
    }

    #[test]
    fn test_builder_default_trait() {
        let builder = SnapshotConfigBuilder::default();
        let config = builder.build();

        assert!(config.use_snapshots_on_load());
        // Default is EveryNEvents(100)
        assert!(config
            .default_strategy()
            .should_snapshot(AggregateVersion::new(100)));
        assert!(!config
            .default_strategy()
            .should_snapshot(AggregateVersion::new(99)));
    }
}
