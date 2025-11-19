//! In-memory event store implementation.
//!
//! Provides a fast, thread-safe in-memory implementation of `EventStore`
//! suitable for testing and development.

use async_trait::async_trait;
use event_sauce_core::{
    Error, EventEnvelope, EventStore, Position, Result, Snapshot, SnapshotConfig, StreamId, Version,
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
/// use event_sauce_core::{EventStore, StreamId, Version};
/// use uuid::Uuid;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let store = InMemoryEventStore::new();
///     let stream_id = StreamId::new("User", Uuid::new_v4());
///
///     // Store is ready to use
///     let version = store.get_version(stream_id).await?;
///     assert_eq!(version, Version::initial());
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryEventStore {
    inner: Arc<InMemoryEventStoreInner>,
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

impl InMemoryEventStore {
    /// Creates a new empty in-memory event store with default snapshot configuration.
    ///
    /// Default configuration: Snapshots every 100 events, enabled on load.
    ///
    /// For custom snapshot behavior, use [`with_config`](Self::with_config) or
    /// [`SnapshotConfig::disabled()`] to turn off snapshots.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    ///
    /// // Default: snapshots every 100 events
    /// let store = InMemoryEventStore::new();
    ///
    /// // Disable snapshots
    /// use event_sauce_core::SnapshotConfig;
    /// let store = InMemoryEventStore::with_config(SnapshotConfig::disabled());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(SnapshotConfig::builder().build())
    }

    /// Creates a new empty in-memory event store with the given snapshot configuration.
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
        Self {
            inner: Arc::new(InMemoryEventStoreInner {
                streams: RwLock::new(HashMap::new()),
                snapshots: RwLock::new(HashMap::new()),
                global_events: RwLock::new(Vec::new()),
                snapshot_config,
            }),
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
        expected_version: Version,
    ) -> Result<()> {
        // Append events to store within a scope to ensure locks are released
        {
            let mut streams = self.inner.streams.write();
            let mut global_events = self.inner.global_events.write();

            // Get current stream
            let stream = streams.entry(stream_id.clone()).or_default();

            // Check version for optimistic concurrency control
            #[allow(clippy::cast_possible_wrap)]
            #[allow(clippy::cast_possible_truncation)]
            let current_version = Version::new(stream.len() as i32);
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
        from_version: Version,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        let streams = self.inner.streams.read();

        // Get events for this stream
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let events = streams
            .get(&stream_id)
            .map(|stream| {
                stream
                    .iter()
                    .skip(from_version.as_i32() as usize)
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

    async fn get_version(&self, stream_id: StreamId) -> Result<Version> {
        let streams = self.inner.streams.read();

        #[allow(clippy::cast_possible_wrap)]
        let version = streams
            .get(&stream_id)
            .map_or(Version::initial(), |stream| {
                #[allow(clippy::cast_possible_truncation)]
                {
                    Version::new(stream.len() as i32)
                }
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
}

#[cfg(test)]
mod tests {
    use event_sauce_core::{EventEnvelope, EventStore, Position, Snapshot, StreamId, Version};
    use futures::StreamExt;
    use serde_json::json;
    use uuid::Uuid;

    fn create_test_envelope(event_type: &str, aggregate_id: Uuid) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            "TestAggregate".to_string(),
            event_type.to_string(),
            Version::new(1),
            json!({"data": "test"}),
        )
    }

    use super::InMemoryEventStore;

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
            .append(stream_id, vec![event], Version::initial())
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
            .append(stream_id.clone(), vec![event.clone()], Version::initial())
            .await
            .unwrap();

        let mut stream = store
            .load_stream(stream_id, Version::initial())
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
        assert_eq!(version, Version::initial());
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
            .append(stream_id.clone(), vec![event1], Version::initial())
            .await
            .unwrap();

        // Try to append with wrong version - should fail
        let result = store
            .append(stream_id, vec![event2], Version::initial())
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
                Version::initial(),
            )
            .await
            .unwrap();
        store
            .append(
                stream2,
                vec![create_test_envelope("OrderPlaced", id2)],
                Version::initial(),
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
            Version::new(10),
            json!({"name": "Alice"}),
        );

        store.save_snapshot(snapshot.clone()).await.unwrap();
        let loaded = store.load_snapshot(stream_id).await.unwrap();

        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().snapshot_version, Version::new(10));
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
            .append(stream_id.clone(), events, Version::initial())
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::new(3));
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
                .append(stream_id.clone(), vec![event], Version::new(i))
                .await
                .unwrap();
        }

        // Load from version 2
        let stream = store.load_stream(stream_id, Version::new(2)).await.unwrap();
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
                .append(stream_id, vec![event], Version::initial())
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
            .load_stream(stream_id, Version::initial())
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
            .append(stream_id.clone(), vec![event], Version::initial())
            .await
            .unwrap();

        // Read via clone - should see the same data (Arc semantics)
        let version = store_clone.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::new(1));
    }

    #[tokio::test]
    async fn test_default_trait() {
        let store = InMemoryEventStore::default();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::initial());
    }

    #[tokio::test]
    async fn test_version_increments_correctly() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Initial version
        let v0 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v0, Version::initial());

        // After first append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event1", aggregate_id)],
                Version::initial(),
            )
            .await
            .unwrap();
        let v1 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v1, Version::new(1));

        // After second append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event2", aggregate_id)],
                Version::new(1),
            )
            .await
            .unwrap();
        let v2 = store.get_version(stream_id).await.unwrap();
        assert_eq!(v2, Version::new(2));
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
            Version::new(5),
            json!({"version": 1}),
        );
        store.save_snapshot(snapshot1).await.unwrap();

        // Save second snapshot (should overwrite)
        let snapshot2 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            Version::new(10),
            json!({"version": 2}),
        );
        store.save_snapshot(snapshot2).await.unwrap();

        // Should get the latest snapshot
        let loaded = store.load_snapshot(stream_id).await.unwrap().unwrap();
        assert_eq!(loaded.snapshot_version, Version::new(10));
    }

    #[tokio::test]
    async fn test_append_without_event_bus_still_works() {
        let store = InMemoryEventStore::new(); // No event bus configured

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        // Should work fine without event bus
        let result = store
            .append(stream_id.clone(), vec![event], Version::initial())
            .await;
        assert!(result.is_ok());

        // Events should still be stored
        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::new(1));
    }
}
