//! In-memory event store implementation.
//!
//! Provides a fast, thread-safe in-memory implementation of `EventStore`
//! suitable for testing and development.

use async_trait::async_trait;
use event_sauce_core::{
    AggregateVersion, Error, EventEnvelope, EventStore, Position, Result, Snapshot, SnapshotConfig,
    StreamId,
};
use futures::stream::{self, Stream};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// In-memory event store for testing and development.
///
/// This implementation stores all events and snapshots in memory using
/// thread-safe data structures. It provides full `EventStore` functionality
/// including optimistic concurrency control and snapshots.
///
/// # Thread Safety
///
/// This store is thread-safe and can be cloned cheaply (uses `Arc` internally).
///
/// # Examples
///
/// ```
/// use event_sauce_memory::InMemoryEventStore;
/// use event_sauce_core::{EventStore, StreamId, AggregateVersion};
/// use uuid::Uuid;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let store = InMemoryEventStore::new();
///     let stream_id = StreamId::new("User", Uuid::new_v4());
///
///     // Store is ready to use
///     let version = store.get_version(stream_id).await?;
///     assert_eq!(version, AggregateVersion::initial());
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryEventStore {
    inner: Arc<InMemoryEventStoreInner>,
    checkpoint_store: Option<Arc<dyn event_sauce_core::CheckpointStore>>,
}

struct InMemoryEventStoreInner {
    /// Stores events by stream ID
    streams: RwLock<HashMap<StreamId, Vec<EventEnvelope>>>,
    /// Stores snapshots by stream ID
    snapshots: RwLock<HashMap<StreamId, Snapshot>>,
    /// All events in global order for `stream_all`
    global_events: RwLock<Vec<EventEnvelope>>,
    /// Snapshot configuration
    snapshot_config: SnapshotConfig,
}

/// Builder for configuring `InMemoryEventStore`.
///
/// Provides a flexible way to configure the event store with:
/// - Snapshot configuration
/// - Checkpoint store for subscriptions
///
/// # Examples
///
/// ```
/// use event_sauce_memory::InMemoryEventStore;
/// use event_sauce_core::SnapshotConfig;
///
/// let store = InMemoryEventStore::builder()
///     .snapshot_config(SnapshotConfig::builder().build())
///     .build();
/// ```
#[derive(Clone, Default)]
pub struct InMemoryEventStoreBuilder {
    snapshot_config: Option<SnapshotConfig>,
    checkpoint_store: Option<Arc<dyn event_sauce_core::CheckpointStore>>,
}

impl InMemoryEventStore {
    /// Creates a new empty in-memory event store with default snapshot configuration.
    ///
    /// Default configuration: Snapshots every 100 events, enabled on load.
    ///
    /// Equivalent to `InMemoryEventStore::builder().build()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    ///
    /// // Default: snapshots every 100 events
    /// let store = InMemoryEventStore::new();
    ///
    /// // Or use builder for custom configuration
    /// use event_sauce_core::SnapshotConfig;
    /// let store = InMemoryEventStore::builder()
    ///     .snapshot_config(SnapshotConfig::disabled())
    ///     .build();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Creates a new empty in-memory event store with the given snapshot configuration.
    ///
    /// Equivalent to `InMemoryEventStore::builder().snapshot_config(config).build()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(100))
    ///     .build();
    ///
    /// let store = InMemoryEventStore::with_config(config);
    /// ```
    #[must_use]
    pub fn with_config(snapshot_config: SnapshotConfig) -> Self {
        Self::builder().snapshot_config(snapshot_config).build()
    }

    /// Creates a new empty in-memory event store with checkpoint store.
    ///
    /// This is a convenience method that configures both snapshot and checkpoint storage.
    ///
    /// Equivalent to `InMemoryEventStore::builder().snapshot_config(config).checkpoint_store(store).build()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::{InMemoryEventStore, InMemoryCheckpointStore};
    /// use event_sauce_core::{EventStore, SnapshotConfig};
    /// use std::sync::Arc;
    ///
    /// let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
    /// let store = Arc::new(InMemoryEventStore::with_checkpoint_store(
    ///     SnapshotConfig::builder().build(),
    ///     checkpoint_store,
    /// ));
    ///
    /// // subscription_builder is now available via the EventStore trait
    /// // let subscription = store.subscription_builder("my-sub").build()?;
    /// ```
    #[must_use]
    pub fn with_checkpoint_store(
        snapshot_config: SnapshotConfig,
        checkpoint_store: Arc<dyn event_sauce_core::CheckpointStore>,
    ) -> Self {
        Self::builder()
            .snapshot_config(snapshot_config)
            .checkpoint_store(checkpoint_store)
            .build()
    }

    /// Creates a builder for configuring the event store.
    ///
    /// This is the recommended way to create an `InMemoryEventStore` when you need
    /// to customize the configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    /// use event_sauce_core::SnapshotConfig;
    ///
    /// let store = InMemoryEventStore::builder()
    ///     .snapshot_config(SnapshotConfig::disabled())
    ///     .build();
    /// ```
    #[must_use]
    pub fn builder() -> InMemoryEventStoreBuilder {
        InMemoryEventStoreBuilder::new()
    }
}

impl InMemoryEventStoreBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_config: None,
            checkpoint_store: None,
        }
    }

    /// Sets the snapshot configuration.
    ///
    /// Defaults to `SnapshotConfig::builder().build()` (every 100 events).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    /// use event_sauce_core::SnapshotConfig;
    ///
    /// let builder = InMemoryEventStore::builder()
    ///     .snapshot_config(SnapshotConfig::disabled());
    /// ```
    #[must_use]
    pub fn snapshot_config(mut self, config: SnapshotConfig) -> Self {
        self.snapshot_config = Some(config);
        self
    }

    /// Sets the checkpoint store for subscription tracking.
    ///
    /// When a checkpoint store is configured, the event store can create
    /// subscriptions with automatic checkpoint management.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::{InMemoryEventStore, InMemoryCheckpointStore};
    /// use std::sync::Arc;
    ///
    /// let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
    /// let builder = InMemoryEventStore::builder()
    ///     .checkpoint_store(checkpoint_store);
    /// ```
    #[must_use]
    pub fn checkpoint_store(mut self, store: Arc<dyn event_sauce_core::CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Builds the `InMemoryEventStore` with the configured settings.
    ///
    /// # Defaults
    ///
    /// - **Snapshot config**: Every 100 events
    /// - **Checkpoint store**: None
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    ///
    /// let store = InMemoryEventStore::builder().build();
    /// ```
    #[must_use]
    pub fn build(self) -> InMemoryEventStore {
        InMemoryEventStore {
            inner: Arc::new(InMemoryEventStoreInner {
                streams: RwLock::new(HashMap::new()),
                snapshots: RwLock::new(HashMap::new()),
                global_events: RwLock::new(Vec::new()),
                snapshot_config: self
                    .snapshot_config
                    .unwrap_or_else(|| SnapshotConfig::builder().build()),
            }),
            checkpoint_store: self.checkpoint_store,
        }
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
    ) -> Result<()> {
        // Append events to store within a scope to ensure locks are released
        {
            let mut streams = self.inner.streams.write();
            let mut global_events = self.inner.global_events.write();

            // Get current stream
            let stream = streams.entry(stream_id.clone()).or_default();

            // Check version for optimistic concurrency control
            let current_version = AggregateVersion::new(stream.len() as u64);
            if current_version != expected_version {
                return Err(Error::concurrency_conflict(
                    expected_version,
                    current_version,
                ));
            }

            // Append events
            for event in &events {
                stream.push(event.clone());
                global_events.push(event.clone());
            }
        } // Locks are dropped here

        Ok(())
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        let streams = self.inner.streams.read();

        // Get events for this stream
        #[allow(clippy::cast_possible_truncation)]
        let events = streams
            .get(&stream_id)
            .map(|stream| {
                stream
                    .iter()
                    .skip(from_version.as_u64() as usize)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Convert to stream of Results
        Ok(stream::iter(events.into_iter().map(Ok)))
    }

    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        let global_events = self.inner.global_events.read();

        // Get all events from position
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let events = global_events
            .iter()
            .skip(from_position.as_i64() as usize)
            .cloned()
            .collect::<Vec<_>>();

        Ok(stream::iter(events.into_iter().map(Ok)))
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        let streams = self.inner.streams.read();

        #[allow(clippy::cast_possible_wrap)]
        let version = streams
            .get(&stream_id)
            .map_or(AggregateVersion::initial(), |stream| {
                AggregateVersion::new(stream.len() as u64)
            });

        Ok(version)
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let mut snapshots = self.inner.snapshots.write();
        let stream_id = StreamId::new(snapshot.aggregate_type.clone(), snapshot.aggregate_id);
        snapshots.insert(stream_id, snapshot);
        Ok(())
    }

    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        let snapshots = self.inner.snapshots.read();
        Ok(snapshots.get(&stream_id).cloned())
    }

    fn snapshot_config(&self) -> &SnapshotConfig {
        &self.inner.snapshot_config
    }

    fn checkpoint_store(&self) -> Option<std::sync::Arc<dyn event_sauce_core::CheckpointStore>> {
        self.checkpoint_store.clone()
    }
}

#[cfg(test)]
mod tests {
    use event_sauce_core::{
        AggregateVersion, EventEnvelope, EventStore, Position, Snapshot, StreamId,
    };
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    fn create_test_envelope(event_type: &str, aggregate_id: Uuid) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            "TestAggregate".to_string(),
            event_type.to_string(),
            event_sauce_core::EventVersion::new(1),
            json!({"data": "test"}),
        )
    }

    use super::{InMemoryEventStore, InMemoryEventStoreBuilder};

    // GREEN: Test that we can create an InMemoryEventStore
    #[tokio::test]
    async fn test_create_store() {
        let _store = InMemoryEventStore::new();
        // Store is created successfully
    }

    // GREEN: Test appending events to a new stream
    #[tokio::test]
    async fn test_append_to_new_stream() {
        let store = InMemoryEventStore::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let event = create_test_envelope("UserCreated", stream_id.aggregate_id());

        let result = store
            .append(stream_id, vec![event], AggregateVersion::initial())
            .await;
        assert!(result.is_ok());
    }

    // GREEN: Test loading events from a stream
    #[tokio::test]
    async fn test_load_stream() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
            )
            .await
            .unwrap();

        let mut stream = store
            .load_stream(stream_id, AggregateVersion::initial())
            .await
            .unwrap();
        let loaded = stream.next().await.unwrap().unwrap();

        assert_eq!(loaded.event_type, "UserCreated");
    }

    // GREEN: Test getting stream version
    #[tokio::test]
    async fn test_get_version() {
        let store = InMemoryEventStore::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        // New stream should have initial version
        let version = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(version, AggregateVersion::initial());
    }

    // GREEN: Test concurrency conflict detection
    #[tokio::test]
    async fn test_concurrency_conflict() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event1 = create_test_envelope("UserCreated", aggregate_id);
        let event2 = create_test_envelope("UserUpdated", aggregate_id);

        // Append first event
        store
            .append(stream_id.clone(), vec![event1], AggregateVersion::initial())
            .await
            .unwrap();

        // Try to append with wrong version - should fail
        let result = store
            .append(stream_id, vec![event2], AggregateVersion::initial())
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_concurrency_conflict());
    }

    // GREEN: Test streaming all events
    #[tokio::test]
    async fn test_stream_all() {
        let store = InMemoryEventStore::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let stream1 = StreamId::new("User", id1);
        let stream2 = StreamId::new("Order", id2);

        store
            .append(
                stream1,
                vec![create_test_envelope("UserCreated", id1)],
                AggregateVersion::initial(),
            )
            .await
            .unwrap();
        store
            .append(
                stream2,
                vec![create_test_envelope("OrderPlaced", id2)],
                AggregateVersion::initial(),
            )
            .await
            .unwrap();

        let all_events = store.stream_all(Position::start()).await.unwrap();
        let events: Vec<_> = all_events.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 2);
    }

    // GREEN: Test snapshot save and load
    #[tokio::test]
    async fn test_save_and_load_snapshot() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let snapshot = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(10),
            json!({"name": "Alice"}),
        );

        store.save_snapshot(snapshot.clone()).await.unwrap();
        let loaded = store.load_snapshot(stream_id).await.unwrap();

        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().snapshot_version, AggregateVersion::new(10));
    }

    // Additional tests for better coverage

    #[tokio::test]
    async fn test_append_multiple_events() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let events = vec![
            create_test_envelope("UserCreated", aggregate_id),
            create_test_envelope("UserUpdated", aggregate_id),
            create_test_envelope("UserVerified", aggregate_id),
        ];

        store
            .append(stream_id.clone(), events, AggregateVersion::initial())
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(3));
    }

    #[tokio::test]
    async fn test_load_stream_from_version() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Add 5 events
        for i in 0..5 {
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);
            store
                .append(stream_id.clone(), vec![event], AggregateVersion::new(i))
                .await
                .unwrap();
        }

        // Load from version 2
        let stream = store
            .load_stream(stream_id, AggregateVersion::new(2))
            .await
            .unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 3); // Should get events 2, 3, 4
    }

    #[tokio::test]
    async fn test_stream_all_from_position() {
        let store = InMemoryEventStore::new();

        for i in 0..5 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("User", aggregate_id);
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);

            store
                .append(stream_id, vec![event], AggregateVersion::initial())
                .await
                .unwrap();
        }

        // Stream from position 2
        let stream = store.stream_all(Position::new(2)).await.unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 3); // Should get events 2, 3, 4
    }

    #[tokio::test]
    async fn test_load_nonexistent_stream() {
        let store = InMemoryEventStore::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let stream = store
            .load_stream(stream_id, AggregateVersion::initial())
            .await
            .unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_load_nonexistent_snapshot() {
        let store = InMemoryEventStore::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let snapshot = store.load_snapshot(stream_id).await.unwrap();
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_store_is_cloneable() {
        let store = InMemoryEventStore::new();
        let store_clone = store.clone();

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        // Append via original store
        store
            .append(stream_id.clone(), vec![event], AggregateVersion::initial())
            .await
            .unwrap();

        // Read via clone - should see the same data (Arc semantics)
        let version = store_clone.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));
    }

    #[tokio::test]
    async fn test_default_trait() {
        let store = InMemoryEventStore::default();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::initial());
    }

    #[tokio::test]
    async fn test_version_increments_correctly() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Initial version
        let v0 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v0, AggregateVersion::initial());

        // After first append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event1", aggregate_id)],
                AggregateVersion::initial(),
            )
            .await
            .unwrap();
        let v1 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v1, AggregateVersion::new(1));

        // After second append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event2", aggregate_id)],
                AggregateVersion::new(1),
            )
            .await
            .unwrap();
        let v2 = store.get_version(stream_id).await.unwrap();
        assert_eq!(v2, AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_snapshot_overwrites() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Save first snapshot
        let snapshot1 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(5),
            json!({"version": 1}),
        );
        store.save_snapshot(snapshot1).await.unwrap();

        // Save second snapshot (should overwrite)
        let snapshot2 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(10),
            json!({"version": 2}),
        );
        store.save_snapshot(snapshot2).await.unwrap();

        // Should get the latest snapshot
        let loaded = store.load_snapshot(stream_id).await.unwrap().unwrap();
        assert_eq!(loaded.snapshot_version, AggregateVersion::new(10));
    }

    #[tokio::test]
    async fn test_append_without_event_bus_still_works() {
        let store = InMemoryEventStore::new(); // No event bus configured

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        // Should work fine without event bus
        let result = store
            .append(stream_id.clone(), vec![event], AggregateVersion::initial())
            .await;
        assert!(result.is_ok());

        // Events should still be stored
        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));
    }

    // === Checkpoint Store Integration Tests ===

    use super::super::checkpoint_store::InMemoryCheckpointStore;
    use event_sauce_core::{CheckpointStore, SnapshotConfig};

    #[tokio::test]
    async fn test_checkpoint_store_returns_none_by_default() {
        let store = InMemoryEventStore::new();
        assert!(store.checkpoint_store().is_none());
    }

    #[tokio::test]
    async fn test_checkpoint_store_returns_configured_store() {
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let store = InMemoryEventStore::with_checkpoint_store(
            SnapshotConfig::builder().build(),
            checkpoint_store.clone(),
        );

        let retrieved = store.checkpoint_store();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_checkpoint_store_can_save_and_load() {
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let store = InMemoryEventStore::with_checkpoint_store(
            SnapshotConfig::builder().build(),
            checkpoint_store.clone(),
        );

        // Save checkpoint through the store's checkpoint store
        if let Some(cs) = store.checkpoint_store() {
            cs.save_checkpoint("test-sub", Position::new(42))
                .await
                .unwrap();

            let loaded = cs.load_checkpoint("test-sub").await.unwrap();
            assert_eq!(loaded, Some(Position::new(42)));
        } else {
            panic!("Checkpoint store should be configured");
        }
    }

    #[tokio::test]
    async fn test_subscription_builder_without_checkpoint_store() {
        use event_sauce_core::EventFilter;

        let store = Arc::new(InMemoryEventStore::new());

        // Create subscription builder via trait method - should work without checkpoint store
        let subscription = store
            .subscription_builder("test-sub")
            .filter(EventFilter::all())
            .build()
            .unwrap();

        // Subscription should be created successfully
        // (We can't easily test the subscription behavior without running it,
        // but we can verify it builds correctly)
        drop(subscription);
    }

    #[tokio::test]
    async fn test_subscription_builder_with_checkpoint_store() {
        use event_sauce_core::EventFilter;

        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let store = Arc::new(InMemoryEventStore::with_checkpoint_store(
            SnapshotConfig::builder().build(),
            checkpoint_store.clone(),
        ));

        // Add some test events
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        store
            .append(
                stream_id,
                vec![create_test_envelope("UserCreated", aggregate_id)],
                AggregateVersion::initial(),
            )
            .await
            .unwrap();

        // Create subscription with automatic checkpoint store integration via trait method
        let subscription = store
            .subscription_builder("test-sub")
            .filter(EventFilter::all())
            .build()
            .unwrap();

        // Verify subscription was created
        drop(subscription);
    }

    #[tokio::test]
    async fn test_subscription_with_checkpoint_store_integration() {
        use event_sauce_core::EventFilter;
        use futures::StreamExt;

        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let store = Arc::new(InMemoryEventStore::with_checkpoint_store(
            SnapshotConfig::builder().build(),
            checkpoint_store.clone(),
        ));

        // Add events
        for i in 0..5 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("User", aggregate_id);
            store
                .append(
                    stream_id,
                    vec![create_test_envelope(&format!("Event{i}"), aggregate_id)],
                    AggregateVersion::initial(),
                )
                .await
                .unwrap();
        }

        // Create and run subscription via trait method
        let subscription = store
            .subscription_builder("integration-test")
            .filter(EventFilter::all())
            .build()
            .unwrap();

        let stream = subscription.into_stream().await.unwrap();
        futures::pin_mut!(stream); // Pin the stream for iteration
        let mut count = 0;

        while let Some(result) = stream.next().await {
            result.unwrap();
            count += 1;
        }

        assert_eq!(count, 5);

        // Verify checkpoint was saved
        let checkpoint = checkpoint_store
            .load_checkpoint("integration-test")
            .await
            .unwrap();
        assert!(checkpoint.is_some());
    }

    // === Builder Pattern Tests ===

    #[tokio::test]
    async fn test_builder_with_defaults() {
        let store = InMemoryEventStore::builder().build();

        // Should work with defaults
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(stream_id.clone(), vec![event], AggregateVersion::initial())
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));
    }

    #[tokio::test]
    async fn test_builder_with_snapshot_config() {
        let config = SnapshotConfig::disabled();
        let store = InMemoryEventStore::builder()
            .snapshot_config(config.clone())
            .build();

        // Verify snapshot config is set - just check it's accessible
        let _config = store.snapshot_config();
        // Config is accessible - test passes if no panic occurs
    }

    #[tokio::test]
    async fn test_builder_with_checkpoint_store() {
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let store = InMemoryEventStore::builder()
            .checkpoint_store(checkpoint_store.clone())
            .build();

        // Verify checkpoint store is set
        let retrieved = store.checkpoint_store();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_builder_full_configuration() {
        let config = SnapshotConfig::builder()
            .default_strategy(event_sauce_core::EveryNEvents(50))
            .build();
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());

        let store = InMemoryEventStore::builder()
            .snapshot_config(config)
            .checkpoint_store(checkpoint_store.clone())
            .build();

        // Verify both are configured
        assert!(store.checkpoint_store().is_some());
        let _config = store.snapshot_config();
        // Config is accessible - test passes if no panic occurs
    }

    #[tokio::test]
    async fn test_builder_new_equals_default() {
        let builder1 = InMemoryEventStoreBuilder::new();
        let builder2 = InMemoryEventStoreBuilder::default();

        let store1 = builder1.build();
        let store2 = builder2.build();

        // Both should work identically
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store1
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
            )
            .await
            .unwrap();
        store2
            .append(stream_id.clone(), vec![event], AggregateVersion::initial())
            .await
            .unwrap();

        let v1 = store1.get_version(stream_id.clone()).await.unwrap();
        let v2 = store2.get_version(stream_id).await.unwrap();
        assert_eq!(v1, v2);
    }

    #[tokio::test]
    async fn test_new_uses_builder() {
        // Verify that new() produces the same result as builder().build()
        let store1 = InMemoryEventStore::new();
        let store2 = InMemoryEventStore::builder().build();

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store1
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
            )
            .await
            .unwrap();
        store2
            .append(stream_id.clone(), vec![event], AggregateVersion::initial())
            .await
            .unwrap();

        let v1 = store1.get_version(stream_id.clone()).await.unwrap();
        let v2 = store2.get_version(stream_id).await.unwrap();
        assert_eq!(v1, v2);
    }

    #[tokio::test]
    async fn test_with_config_uses_builder() {
        let config = SnapshotConfig::disabled();
        let store1 = InMemoryEventStore::with_config(config.clone());
        let store2 = InMemoryEventStore::builder()
            .snapshot_config(config)
            .build();

        // Both should work identically
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store1
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
            )
            .await
            .unwrap();
        store2
            .append(stream_id.clone(), vec![event], AggregateVersion::initial())
            .await
            .unwrap();

        let v1 = store1.get_version(stream_id.clone()).await.unwrap();
        let v2 = store2.get_version(stream_id).await.unwrap();
        assert_eq!(v1, v2);
    }

    #[tokio::test]
    async fn test_with_checkpoint_store_uses_builder() {
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let config = SnapshotConfig::builder().build();

        let store1 =
            InMemoryEventStore::with_checkpoint_store(config.clone(), checkpoint_store.clone());
        let store2 = InMemoryEventStore::builder()
            .snapshot_config(config)
            .checkpoint_store(checkpoint_store)
            .build();

        // Both should have checkpoint store configured
        assert!(store1.checkpoint_store().is_some());
        assert!(store2.checkpoint_store().is_some());
    }
}
