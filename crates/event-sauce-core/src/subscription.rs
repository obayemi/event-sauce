//! Durable subscription system for event stores.
//!
//! Provides durable subscriptions with guaranteed delivery, checkpoint management,
//! and rebuild capabilities. Unlike pub/sub systems, subscriptions always read from
//! the durable event store ensuring eventual consistency and no message loss.

use async_trait::async_trait;
use std::time::Duration;

use crate::{EventEnvelope, Position, Result};

/// Event filter for selective event subscription.
///
/// Allows filtering events by type, aggregate type, or custom criteria.
///
/// # Examples
///
/// ```
/// use event_sauce_core::EventFilter;
///
/// // Filter by event type
/// let filter = EventFilter::by_event_type("UserRegistered");
///
/// // Filter by aggregate type
/// let filter = EventFilter::by_aggregate_type("User");
///
/// // Match all events
/// let filter = EventFilter::all();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventFilter {
    /// Match all events.
    All,
    /// Match events by event type.
    EventType(String),
    /// Match events by aggregate type.
    AggregateType(String),
    /// Match events by both event and aggregate type.
    Both {
        /// Event type to match.
        event_type: String,
        /// Aggregate type to match.
        aggregate_type: String,
    },
}

impl EventFilter {
    /// Creates a filter matching all events.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::all();
    /// ```
    #[must_use]
    pub const fn all() -> Self {
        Self::All
    }

    /// Creates a filter matching a specific event type.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::by_event_type("UserRegistered");
    /// ```
    #[must_use]
    pub fn by_event_type(event_type: impl Into<String>) -> Self {
        Self::EventType(event_type.into())
    }

    /// Creates a filter matching a specific aggregate type.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::by_aggregate_type("User");
    /// ```
    #[must_use]
    pub fn by_aggregate_type(aggregate_type: impl Into<String>) -> Self {
        Self::AggregateType(aggregate_type.into())
    }

    /// Creates a filter matching both event and aggregate type.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::EventFilter;
    ///
    /// let filter = EventFilter::both("UserRegistered", "User");
    /// ```
    #[must_use]
    pub fn both(event_type: impl Into<String>, aggregate_type: impl Into<String>) -> Self {
        Self::Both {
            event_type: event_type.into(),
            aggregate_type: aggregate_type.into(),
        }
    }

    /// Checks if an event matches this filter.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{EventFilter, EventEnvelope, Version};
    /// use uuid::Uuid;
    /// use serde_json::json;
    ///
    /// let filter = EventFilter::by_event_type("UserRegistered");
    /// let envelope = EventEnvelope::new(
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    ///     "User".to_string(),
    ///     "UserRegistered".to_string(),
    ///     Version::new(1),
    ///     json!({}),
    /// );
    ///
    /// assert!(filter.matches(&envelope));
    /// ```
    #[must_use]
    pub fn matches(&self, event: &EventEnvelope) -> bool {
        match self {
            Self::All => true,
            Self::EventType(event_type) => &event.event_type == event_type,
            Self::AggregateType(aggregate_type) => &event.aggregate_type == aggregate_type,
            Self::Both {
                event_type,
                aggregate_type,
            } => &event.event_type == event_type && &event.aggregate_type == aggregate_type,
        }
    }
}

/// Checkpoint strategy for subscription checkpoint management.
///
/// Controls when checkpoints are automatically saved during subscription processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStrategy {
    /// Save checkpoint after every event (safest, highest overhead).
    EveryEvent,
    /// Save checkpoint after every N events.
    EveryN(usize),
    /// Manual checkpoint management (user calls `save_checkpoint` explicitly).
    Manual,
}

/// Error handling policy for subscription processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorPolicy {
    /// Retry failed events indefinitely with backoff.
    Retry,
    /// Skip failed events and continue (logs error).
    Skip,
    /// Fail the entire subscription on error.
    Fail,
}

/// Configuration for durable subscriptions.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// How often to save checkpoints.
    pub checkpoint_strategy: CheckpointStrategy,
    /// Polling interval when no events are available.
    pub polling_interval: Duration,
    /// Error handling policy.
    pub error_policy: ErrorPolicy,
    /// Event filter.
    pub filter: EventFilter,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            checkpoint_strategy: CheckpointStrategy::EveryEvent,
            polling_interval: Duration::from_millis(100),
            error_policy: ErrorPolicy::Fail,
            filter: EventFilter::All,
        }
    }
}

/// Trait for checkpoint storage.
///
/// Checkpoint stores track the progress of subscriptions, enabling
/// resumption after restarts or failures.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Saves a checkpoint.
    async fn save_checkpoint(&self, subscription_name: &str, position: Position) -> Result<()>;

    /// Loads a checkpoint.
    async fn load_checkpoint(&self, subscription_name: &str) -> Result<Option<Position>>;

    /// Deletes a checkpoint.
    async fn delete_checkpoint(&self, subscription_name: &str) -> Result<()>;
}

/// Builder for creating Subscription instances.
///
/// Provides a fluent API for configuring subscriptions.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::Subscription;
///
/// let subscription = Subscription::builder("my-subscription", store)
///     .checkpoint_store(checkpoint_store)
///     .filter(EventFilter::by_event_type("UserCreated"))
///     .checkpoint_strategy(CheckpointStrategy::EveryN(10))
///     .build()?;
/// ```
pub struct SubscriptionBuilder<S> {
    name: String,
    store: std::sync::Arc<S>,
    checkpoint_store: Option<std::sync::Arc<dyn CheckpointStore>>,
    config: SubscriptionConfig,
}

impl<S> SubscriptionBuilder<S>
where
    S: crate::EventStore + 'static,
{
    /// Creates a new subscription builder.
    fn new(name: impl Into<String>, store: std::sync::Arc<S>) -> Self {
        Self {
            name: name.into(),
            store,
            checkpoint_store: None,
            config: SubscriptionConfig::default(),
        }
    }

    /// Sets the checkpoint store.
    #[must_use]
    pub fn checkpoint_store(mut self, store: std::sync::Arc<dyn CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Sets the event filter.
    #[must_use]
    pub fn filter(mut self, filter: EventFilter) -> Self {
        self.config.filter = filter;
        self
    }

    /// Sets the checkpoint strategy.
    #[must_use]
    pub fn checkpoint_strategy(mut self, strategy: CheckpointStrategy) -> Self {
        self.config.checkpoint_strategy = strategy;
        self
    }

    /// Sets the polling interval.
    #[must_use]
    pub fn polling_interval(mut self, interval: Duration) -> Self {
        self.config.polling_interval = interval;
        self
    }

    /// Sets the error policy.
    #[must_use]
    pub fn error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.config.error_policy = policy;
        self
    }

    /// Builds the subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription cannot be created.
    pub fn build(self) -> Result<Subscription<S>> {
        Ok(Subscription {
            name: self.name,
            store: self.store,
            checkpoint_store: self.checkpoint_store,
            config: self.config,
        })
    }
}

/// Durable subscription for consuming events with guaranteed delivery.
///
/// Unlike pub/sub systems, subscriptions always read from the durable event store,
/// ensuring eventual consistency and no message loss.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::{Subscription, EventFilter};
///
/// let subscription = Subscription::builder("orders-projection", store)
///     .checkpoint_store(checkpoint_store)
///     .filter(EventFilter::by_aggregate_type("Order"))
///     .build()?;
///
/// subscription.run(|event| {
///     // Process event
///     println!("Processing: {}", event.event_type);
///     Ok(())
/// }).await?;
/// ```
pub struct Subscription<S> {
    name: String,
    store: std::sync::Arc<S>,
    checkpoint_store: Option<std::sync::Arc<dyn CheckpointStore>>,
    config: SubscriptionConfig,
}

impl<S> Subscription<S>
where
    S: crate::EventStore + 'static,
{
    /// Creates a new subscription builder.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let subscription = Subscription::builder("my-subscription", store)
    ///     .checkpoint_store(checkpoint_store)
    ///     .build()?;
    /// ```
    pub fn builder(name: impl Into<String>, store: std::sync::Arc<S>) -> SubscriptionBuilder<S> {
        SubscriptionBuilder::new(name, store)
    }

    /// Runs the subscription, processing events through the provided handler.
    ///
    /// This method:
    /// 1. Loads the checkpoint (if available)
    /// 2. Streams events from the checkpoint position
    /// 3. Filters events according to the configured filter
    /// 4. Processes each event through the handler
    /// 5. Saves checkpoints according to the configured strategy
    ///
    /// # Examples
    ///
    /// ```ignore
    /// subscription.run(|event| {
    ///     println!("Event: {}", event.event_type);
    ///     Ok(())
    /// }).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Checkpoint loading fails
    /// - Event streaming fails
    /// - Handler returns an error (depending on error policy)
    /// - Checkpoint saving fails
    pub async fn run<F>(&mut self, mut handler: F) -> Result<()>
    where
        F: FnMut(EventEnvelope) -> Result<()>,
    {
        use futures::StreamExt;

        // Load checkpoint to determine starting position
        let start_position = if let Some(ref checkpoint_store) = self.checkpoint_store {
            checkpoint_store
                .load_checkpoint(&self.name)
                .await?
                .unwrap_or(Position::start())
        } else {
            Position::start()
        };

        // Stream all events from the starting position
        let event_stream = self.store.stream_all(start_position).await?;
        futures::pin_mut!(event_stream);

        let mut events_processed = 0usize;
        let mut current_position = start_position;

        while let Some(event_result) = event_stream.next().await {
            let event = event_result?;
            current_position = Position::new(current_position.as_i64() + 1);

            // Apply filter
            if !self.config.filter.matches(&event) {
                continue;
            }

            // Process event through handler
            match handler(event) {
                Ok(()) => {
                    events_processed += 1;

                    // Save checkpoint according to strategy
                    if self.should_save_checkpoint(events_processed) {
                        if let Some(ref checkpoint_store) = self.checkpoint_store {
                            checkpoint_store
                                .save_checkpoint(&self.name, current_position)
                                .await?;
                        }
                    }
                }
                Err(e) => {
                    // Handle error according to policy
                    match self.config.error_policy {
                        ErrorPolicy::Fail => return Err(e),
                        ErrorPolicy::Skip => {
                            eprintln!("Skipping event due to error: {e}");
                        }
                        ErrorPolicy::Retry => {
                            // For now, just fail - full retry logic would need backoff
                            return Err(e);
                        }
                    }
                }
            }
        }

        // Save final checkpoint
        if events_processed > 0 {
            if let Some(ref checkpoint_store) = self.checkpoint_store {
                checkpoint_store
                    .save_checkpoint(&self.name, current_position)
                    .await?;
            }
        }

        Ok(())
    }

    /// Rebuilds the subscription from the beginning by deleting the checkpoint.
    ///
    /// This is useful when:
    /// - Projection schema has changed
    /// - You want to reprocess all events
    /// - Recovery from corrupted state
    ///
    /// # Examples
    ///
    /// ```ignore
    /// subscription.rebuild().await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint deletion fails.
    pub async fn rebuild(&mut self) -> Result<()> {
        if let Some(ref checkpoint_store) = self.checkpoint_store {
            checkpoint_store.delete_checkpoint(&self.name).await?;
        }
        Ok(())
    }

    /// Determines if a checkpoint should be saved based on the configured strategy.
    fn should_save_checkpoint(&self, events_processed: usize) -> bool {
        match self.config.checkpoint_strategy {
            CheckpointStrategy::EveryEvent => true,
            CheckpointStrategy::EveryN(n) => events_processed % n == 0,
            CheckpointStrategy::Manual => false,
        }
    }

    /// Converts the subscription into a tokio stream of events.
    ///
    /// This provides an alternative API to [`run`](Self::run) that returns a stream
    /// which can be consumed using standard tokio stream combinators. The stream:
    ///
    /// 1. Loads the checkpoint to determine starting position
    /// 2. Streams events from the event store
    /// 3. Filters events according to the configured filter
    /// 4. Saves checkpoints according to the configured strategy
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use futures::StreamExt;
    ///
    /// let subscription = Subscription::builder("my-subscription", store)
    ///     .checkpoint_store(checkpoint_store)
    ///     .build()?;
    ///
    /// let mut stream = subscription.into_stream().await?;
    ///
    /// while let Some(event_result) = stream.next().await {
    ///     let event = event_result?;
    ///     // Process event
    ///     println!("Event: {}", event.event_type);
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Checkpoint loading fails
    /// - Event streaming fails
    pub async fn into_stream(
        self,
    ) -> Result<impl futures::Stream<Item = Result<EventEnvelope>>> {
        use async_stream::stream;
        use futures::StreamExt;

        // Extract values from self before moving into stream
        let name = self.name;
        let checkpoint_store = self.checkpoint_store;
        let filter = self.config.filter;
        let checkpoint_strategy = self.config.checkpoint_strategy;
        let store = self.store;

        // Load checkpoint to determine starting position
        let start_position = if let Some(ref checkpoint_store) = checkpoint_store {
            checkpoint_store
                .load_checkpoint(&name)
                .await?
                .unwrap_or(Position::start())
        } else {
            Position::start()
        };

        // Create the subscription stream
        let result_stream = stream! {
            // Get event stream from store inside the stream
            let event_stream = match store.stream_all(start_position).await {
                Ok(s) => s,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };
            futures::pin_mut!(event_stream);

            let mut events_processed = 0usize;
            let mut current_position = start_position;
            let mut last_position = start_position;

            while let Some(event_result) = event_stream.next().await {
                match event_result {
                    Ok(event) => {
                        current_position = Position::new(current_position.as_i64() + 1);

                        // Apply filter
                        if !filter.matches(&event) {
                            continue;
                        }

                        // Update counters
                        events_processed += 1;
                        last_position = current_position;

                        // Save checkpoint according to strategy
                        let should_save = match checkpoint_strategy {
                            CheckpointStrategy::EveryEvent => true,
                            CheckpointStrategy::EveryN(n) => events_processed % n == 0,
                            CheckpointStrategy::Manual => false,
                        };

                        if should_save {
                            if let Some(ref checkpoint_store) = checkpoint_store {
                                if let Err(e) = checkpoint_store
                                    .save_checkpoint(&name, current_position)
                                    .await
                                {
                                    yield Err(e);
                                    return;
                                }
                            }
                        }

                        // Yield the event after checkpoint is saved
                        yield Ok(event);
                    }
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                }
            }

            // Save final checkpoint if any events were processed (respecting strategy)
            if events_processed > 0 && !matches!(checkpoint_strategy, CheckpointStrategy::Manual) {
                if let Some(ref checkpoint_store) = checkpoint_store {
                    if let Err(e) = checkpoint_store
                        .save_checkpoint(&name, last_position)
                        .await
                    {
                        yield Err(e);
                    }
                }
            }
        };

        Ok(result_stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Version;
    use serde_json::json;
    use uuid::Uuid;

    fn create_test_envelope(event_type: &str, aggregate_type: &str) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            aggregate_type.to_string(),
            event_type.to_string(),
            Version::new(1),
            json!({}),
        )
    }

    // EventFilter tests
    #[test]
    fn test_event_filter_all() {
        let filter = EventFilter::all();
        assert_eq!(filter, EventFilter::All);

        let envelope = create_test_envelope("UserCreated", "User");
        assert!(filter.matches(&envelope));
    }

    #[test]
    fn test_event_filter_by_event_type() {
        let filter = EventFilter::by_event_type("UserCreated");

        let matching = create_test_envelope("UserCreated", "User");
        let non_matching = create_test_envelope("UserUpdated", "User");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    #[test]
    fn test_event_filter_by_aggregate_type() {
        let filter = EventFilter::by_aggregate_type("User");

        let matching = create_test_envelope("UserCreated", "User");
        let non_matching = create_test_envelope("OrderCreated", "Order");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    #[test]
    fn test_event_filter_both() {
        let filter = EventFilter::both("UserCreated", "User");

        let matching = create_test_envelope("UserCreated", "User");
        let non_matching_event = create_test_envelope("UserUpdated", "User");
        let non_matching_aggregate = create_test_envelope("UserCreated", "Order");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching_event));
        assert!(!filter.matches(&non_matching_aggregate));
    }

    #[test]
    fn test_event_filter_clone() {
        let filter = EventFilter::by_event_type("Test");
        let cloned = filter.clone();
        assert_eq!(filter, cloned);
    }

    #[test]
    fn test_event_filter_debug() {
        let filter = EventFilter::all();
        let debug = format!("{filter:?}");
        assert!(debug.contains("All"));
    }

    // CheckpointStrategy tests
    #[test]
    fn test_checkpoint_strategy_every_event() {
        let strategy = CheckpointStrategy::EveryEvent;
        assert_eq!(strategy, CheckpointStrategy::EveryEvent);
    }

    #[test]
    fn test_checkpoint_strategy_every_n() {
        let strategy = CheckpointStrategy::EveryN(10);
        assert_eq!(strategy, CheckpointStrategy::EveryN(10));
    }

    #[test]
    fn test_checkpoint_strategy_manual() {
        let strategy = CheckpointStrategy::Manual;
        assert_eq!(strategy, CheckpointStrategy::Manual);
    }

    // ErrorPolicy tests
    #[test]
    fn test_error_policy_retry() {
        let policy = ErrorPolicy::Retry;
        assert_eq!(policy, ErrorPolicy::Retry);
    }

    #[test]
    fn test_error_policy_skip() {
        let policy = ErrorPolicy::Skip;
        assert_eq!(policy, ErrorPolicy::Skip);
    }

    #[test]
    fn test_error_policy_fail() {
        let policy = ErrorPolicy::Fail;
        assert_eq!(policy, ErrorPolicy::Fail);
    }

    // SubscriptionConfig tests
    #[test]
    fn test_subscription_config_default() {
        let config = SubscriptionConfig::default();
        assert_eq!(config.checkpoint_strategy, CheckpointStrategy::EveryEvent);
        assert_eq!(config.polling_interval, Duration::from_millis(100));
        assert_eq!(config.error_policy, ErrorPolicy::Fail);
        assert_eq!(config.filter, EventFilter::All);
    }

    #[test]
    fn test_subscription_config_clone() {
        let config = SubscriptionConfig::default();
        let cloned = config.clone();
        assert_eq!(config.checkpoint_strategy, cloned.checkpoint_strategy);
    }

    // Subscription tests - these should FAIL until we implement the Subscription type
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::sync::RwLock as AsyncRwLock;

    // Mock CheckpointStore for testing
    #[derive(Clone)]
    struct MockCheckpointStore {
        checkpoints: Arc<AsyncRwLock<HashMap<String, Position>>>,
    }

    impl MockCheckpointStore {
        fn new() -> Self {
            Self {
                checkpoints: Arc::new(AsyncRwLock::new(HashMap::new())),
            }
        }

        async fn get(&self, name: &str) -> Option<Position> {
            self.checkpoints.read().await.get(name).copied()
        }
    }

    #[async_trait]
    impl CheckpointStore for MockCheckpointStore {
        async fn save_checkpoint(&self, subscription_name: &str, position: Position) -> Result<()> {
            self.checkpoints
                .write()
                .await
                .insert(subscription_name.to_string(), position);
            Ok(())
        }

        async fn load_checkpoint(&self, subscription_name: &str) -> Result<Option<Position>> {
            Ok(self
                .checkpoints
                .read()
                .await
                .get(subscription_name)
                .copied())
        }

        async fn delete_checkpoint(&self, subscription_name: &str) -> Result<()> {
            self.checkpoints.write().await.remove(subscription_name);
            Ok(())
        }
    }

    // Mock EventStore for testing
    use crate::{EventStore, StreamId};
    use futures::{stream, Stream};

    struct MockEventStore {
        events: Arc<Mutex<Vec<EventEnvelope>>>,
    }

    impl MockEventStore {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn add_event(&self, event: EventEnvelope) {
            self.events.lock().unwrap().push(event);
        }

        fn event_count(&self) -> usize {
            self.events.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            _stream_id: StreamId,
            _events: Vec<EventEnvelope>,
            _expected_version: Version,
        ) -> Result<()> {
            Ok(())
        }

        async fn load_stream(
            &self,
            _stream_id: StreamId,
            _from_version: Version,
        ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
            Ok(stream::empty())
        }

        async fn stream_all(
            &self,
            from_position: Position,
        ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
            let events = self.events.lock().unwrap().clone();
            let from_idx = from_position.as_i64() as usize;
            let filtered_events: Vec<_> = events.into_iter().skip(from_idx).map(Ok).collect();
            Ok(stream::iter(filtered_events))
        }

        async fn get_version(&self, _stream_id: StreamId) -> Result<Version> {
            Ok(Version::initial())
        }
    }

    #[tokio::test]
    async fn test_subscription_builder_basic() {
        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        let _subscription = Subscription::builder("test-subscription", store)
            .checkpoint_store(checkpoint_store)
            .build()
            .unwrap();
    }

    #[tokio::test]
    async fn test_subscription_run_from_beginning() {
        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        // Add some events
        for i in 0..5 {
            store.add_event(create_test_envelope(&format!("Event{i}"), "TestAggregate"));
        }

        let processed = Arc::new(Mutex::new(Vec::<String>::new()));
        let processed_clone = processed.clone();

        let mut subscription = Subscription::builder("test", store)
            .checkpoint_store(checkpoint_store)
            .build()
            .unwrap();

        // Process a few events then stop
        let result = subscription
            .run(|event| {
                processed_clone
                    .lock()
                    .unwrap()
                    .push(event.event_type.clone());
                if processed_clone.lock().unwrap().len() >= 3 {
                    return Err(crate::Error::custom("stop"));
                }
                Ok(())
            })
            .await;

        // Should have processed 3 events and then stopped with error
        assert!(result.is_err());
        assert_eq!(processed.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_subscription_saves_checkpoint() {
        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        store.add_event(create_test_envelope("Event1", "TestAggregate"));
        store.add_event(create_test_envelope("Event2", "TestAggregate"));

        let mut subscription = Subscription::builder("test", store)
            .checkpoint_store(checkpoint_store.clone())
            .checkpoint_strategy(CheckpointStrategy::EveryEvent)
            .build()
            .unwrap();

        subscription.run(|_event| Ok(())).await.unwrap();

        // Checkpoint should be saved
        let checkpoint = checkpoint_store.get("test").await;
        assert!(checkpoint.is_some());
        assert_eq!(checkpoint.unwrap().as_i64(), 2);
    }

    #[tokio::test]
    async fn test_subscription_resumes_from_checkpoint() {
        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        // Add events
        for i in 0..5 {
            store.add_event(create_test_envelope(&format!("Event{i}"), "TestAggregate"));
        }

        // Save checkpoint at position 2
        checkpoint_store
            .save_checkpoint("test", Position::new(2))
            .await
            .unwrap();

        let processed = Arc::new(Mutex::new(Vec::<String>::new()));
        let processed_clone = processed.clone();

        let mut subscription = Subscription::builder("test", store)
            .checkpoint_store(checkpoint_store)
            .build()
            .unwrap();

        // Should start from position 2, processing events 2, 3, 4
        subscription
            .run(|event| {
                processed_clone
                    .lock()
                    .unwrap()
                    .push(event.event_type.clone());
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(processed.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_subscription_rebuild_deletes_checkpoint() {
        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        // Save a checkpoint
        checkpoint_store
            .save_checkpoint("test", Position::new(10))
            .await
            .unwrap();

        let mut subscription = Subscription::builder("test", store)
            .checkpoint_store(checkpoint_store.clone())
            .build()
            .unwrap();

        subscription.rebuild().await.unwrap();

        // Checkpoint should be deleted
        let checkpoint = checkpoint_store.get("test").await;
        assert!(checkpoint.is_none());
    }

    #[tokio::test]
    async fn test_subscription_filters_events() {
        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        store.add_event(create_test_envelope("UserCreated", "User"));
        store.add_event(create_test_envelope("OrderCreated", "Order"));
        store.add_event(create_test_envelope("UserUpdated", "User"));

        let processed = Arc::new(Mutex::new(Vec::<String>::new()));
        let processed_clone = processed.clone();

        let mut subscription = Subscription::builder("test", store)
            .checkpoint_store(checkpoint_store)
            .filter(EventFilter::by_aggregate_type("User"))
            .build()
            .unwrap();

        subscription
            .run(|event| {
                processed_clone
                    .lock()
                    .unwrap()
                    .push(event.event_type.clone());
                Ok(())
            })
            .await
            .unwrap();

        // Should only process User events
        assert_eq!(processed.lock().unwrap().len(), 2);
        assert_eq!(processed.lock().unwrap()[0], "UserCreated");
        assert_eq!(processed.lock().unwrap()[1], "UserUpdated");
    }

    // SubscriptionStream tests - these will FAIL until we implement the stream API
    #[tokio::test]
    async fn test_subscription_into_stream_basic() {
        use futures::StreamExt;

        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        // Add some events
        store.add_event(create_test_envelope("Event1", "TestAggregate"));
        store.add_event(create_test_envelope("Event2", "TestAggregate"));
        store.add_event(create_test_envelope("Event3", "TestAggregate"));

        let subscription = Subscription::builder("test-stream", store)
            .checkpoint_store(checkpoint_store.clone())
            .build()
            .unwrap();

        let stream = subscription.into_stream().await.unwrap();
        tokio::pin!(stream);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            count += 1;
        }

        assert_eq!(count, 3);

        // Checkpoint should be saved
        let checkpoint = checkpoint_store.get("test-stream").await;
        assert!(checkpoint.is_some());
        assert_eq!(checkpoint.unwrap().as_i64(), 3);
    }

    #[tokio::test]
    async fn test_subscription_stream_with_filter() {
        use futures::StreamExt;

        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        store.add_event(create_test_envelope("UserCreated", "User"));
        store.add_event(create_test_envelope("OrderCreated", "Order"));
        store.add_event(create_test_envelope("UserUpdated", "User"));

        let subscription = Subscription::builder("filtered-stream", store)
            .checkpoint_store(checkpoint_store)
            .filter(EventFilter::by_aggregate_type("User"))
            .build()
            .unwrap();

        let stream = subscription.into_stream().await.unwrap();
        tokio::pin!(stream);

        let mut events = Vec::new();
        while let Some(result) = stream.next().await {
            let event = result.unwrap();
            events.push(event.event_type.clone());
        }

        assert_eq!(events.len(), 2);
        assert_eq!(events[0], "UserCreated");
        assert_eq!(events[1], "UserUpdated");
    }

    #[tokio::test]
    async fn test_subscription_stream_resumes_from_checkpoint() {
        use futures::StreamExt;

        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        // Add events
        for i in 0..5 {
            store.add_event(create_test_envelope(&format!("Event{i}"), "TestAggregate"));
        }

        // Save checkpoint at position 2
        checkpoint_store
            .save_checkpoint("resume-stream", Position::new(2))
            .await
            .unwrap();

        let subscription = Subscription::builder("resume-stream", store)
            .checkpoint_store(checkpoint_store)
            .build()
            .unwrap();

        let stream = subscription.into_stream().await.unwrap();
        tokio::pin!(stream);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            count += 1;
        }

        // Should only process events from position 2 onwards (3 events)
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_subscription_stream_checkpoint_strategy_every_n() {
        use futures::StreamExt;

        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        // Add 10 events
        for i in 0..10 {
            store.add_event(create_test_envelope(&format!("Event{i}"), "TestAggregate"));
        }

        let subscription = Subscription::builder("every-n-stream", store)
            .checkpoint_store(checkpoint_store.clone())
            .checkpoint_strategy(CheckpointStrategy::EveryN(3))
            .build()
            .unwrap();

        let stream = subscription.into_stream().await.unwrap();
        tokio::pin!(stream);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            count += 1;

            // Check checkpoint after every 3 events
            if count == 3 {
                let checkpoint = checkpoint_store.get("every-n-stream").await;
                assert!(checkpoint.is_some());
                assert_eq!(checkpoint.unwrap().as_i64(), 3);
            }
        }

        assert_eq!(count, 10);

        // Final checkpoint should be saved
        let checkpoint = checkpoint_store.get("every-n-stream").await;
        assert_eq!(checkpoint.unwrap().as_i64(), 10);
    }

    #[tokio::test]
    async fn test_subscription_stream_manual_checkpoint() {
        use futures::StreamExt;

        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        store.add_event(create_test_envelope("Event1", "TestAggregate"));
        store.add_event(create_test_envelope("Event2", "TestAggregate"));

        let subscription = Subscription::builder("manual-stream", store)
            .checkpoint_store(checkpoint_store.clone())
            .checkpoint_strategy(CheckpointStrategy::Manual)
            .build()
            .unwrap();

        let stream = subscription.into_stream().await.unwrap();
        tokio::pin!(stream);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            count += 1;
        }

        assert_eq!(count, 2);

        // No checkpoint should be saved automatically with Manual strategy
        let checkpoint = checkpoint_store.get("manual-stream").await;
        assert!(checkpoint.is_none());
    }

    #[tokio::test]
    async fn test_subscription_stream_empty() {
        use futures::StreamExt;

        let store = Arc::new(MockEventStore::new());
        let checkpoint_store = Arc::new(MockCheckpointStore::new());

        let subscription = Subscription::builder("empty-stream", store)
            .checkpoint_store(checkpoint_store)
            .build()
            .unwrap();

        let stream = subscription.into_stream().await.unwrap();
        tokio::pin!(stream);

        let mut count = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            count += 1;
        }

        assert_eq!(count, 0);
    }
}
