//! Event store trait for event persistence.
//!
//! Defines the `EventStore` trait for persisting and retrieving events with streaming support.

use async_trait::async_trait;
use futures::Stream;
use uuid::Uuid;

use crate::{
    Aggregate, AggregateRoot, DomainEvent, EntityId, EventEnvelope, Result, SnapshotConfig, Version,
};

/// Stream ID uniquely identifying an event stream.
///
/// Combines aggregate type and aggregate ID to create a unique stream identifier.
///
/// # Examples
///
/// ```
/// use event_sauce_core::StreamId;
/// use uuid::Uuid;
///
/// let stream_id = StreamId::new("User", Uuid::new_v4());
/// assert_eq!(stream_id.aggregate_type(), "User");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId {
    aggregate_type: String,
    aggregate_id: Uuid,
}

impl StreamId {
    /// Creates a new stream ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::StreamId;
    /// use uuid::Uuid;
    ///
    /// let stream_id = StreamId::new("Order", Uuid::new_v4());
    /// ```
    #[must_use]
    pub fn new(aggregate_type: impl Into<String>, aggregate_id: Uuid) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id,
        }
    }

    /// Returns the aggregate type.
    #[must_use]
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    /// Returns the aggregate ID.
    #[must_use]
    pub fn aggregate_id(&self) -> Uuid {
        self.aggregate_id
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.aggregate_type, self.aggregate_id)
    }
}

/// Position in the global event stream.
///
/// Used for resuming event streaming from a specific point.
///
/// # Examples
///
/// ```
/// use event_sauce_core::Position;
///
/// let pos = Position::from(100);
/// assert_eq!(pos.as_i64(), 100);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(i64);

impl Position {
    /// Creates a new position.
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the position as an i64.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    /// Returns the start position (0).
    #[must_use]
    pub const fn start() -> Self {
        Self(0)
    }
}

impl From<i64> for Position {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<Position> for i64 {
    fn from(pos: Position) -> Self {
        pos.0
    }
}

/// Snapshot of an aggregate's state.
///
/// Used to optimize aggregate reconstruction by storing periodic state snapshots.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Snapshot, Version};
/// use uuid::Uuid;
/// use serde_json::json;
///
/// let snapshot = Snapshot::new(
///     Uuid::new_v4(),
///     "User".to_string(),
///     Version::new(100),
///     json!({"email": "user@example.com", "status": "active"}),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Aggregate ID.
    pub aggregate_id: Uuid,
    /// Aggregate type.
    pub aggregate_type: String,
    /// Version at which snapshot was taken.
    pub snapshot_version: Version,
    /// Serialized entity state.
    pub snapshot_data: serde_json::Value,
}

impl Snapshot {
    /// Creates a new snapshot.
    #[must_use]
    pub fn new(
        aggregate_id: Uuid,
        aggregate_type: String,
        snapshot_version: Version,
        snapshot_data: serde_json::Value,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type,
            snapshot_version,
            snapshot_data,
        }
    }
}

/// Trait for event store implementations.
///
/// The event store is responsible for persisting and retrieving events,
/// managing snapshots, and enforcing optimistic concurrency control.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Appends events to a stream with optimistic concurrency control.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if the expected version doesn't match.
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<()>;

    /// Loads events from a stream starting at a specific version.
    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: Version,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    /// Streams all events from the store.
    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    /// Gets the current version of a stream.
    async fn get_version(&self, stream_id: StreamId) -> Result<Version>;

    /// Checks if a stream exists.
    async fn stream_exists(&self, stream_id: StreamId) -> Result<bool> {
        let version = self.get_version(stream_id).await?;
        Ok(version != Version::initial())
    }

    /// Saves a snapshot.
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let _ = snapshot;
        Ok(())
    }

    /// Loads a snapshot.
    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        let _ = stream_id;
        Ok(None)
    }

    /// Returns the snapshot configuration for this store.
    fn snapshot_config(&self) -> &SnapshotConfig {
        use std::sync::OnceLock;
        static DISABLED_CONFIG: OnceLock<SnapshotConfig> = OnceLock::new();
        DISABLED_CONFIG.get_or_init(SnapshotConfig::disabled)
    }

    /// Returns the checkpoint store associated with this event store, if any.
    fn checkpoint_store(&self) -> Option<crate::CheckpointStoreRef> {
        None
    }

    /// Creates a subscription builder pre-configured with this event store.
    fn subscription_builder(
        self: &crate::EventStoreRef<Self>,
        name: impl Into<String>,
    ) -> crate::SubscriptionBuilder<Self>
    where
        Self: Sized + 'static,
    {
        let mut builder = crate::SubscriptionBuilder::new(name, crate::EventStoreRef::clone(self));

        if let Some(checkpoint_store) = self.checkpoint_store() {
            builder = builder.checkpoint_store(checkpoint_store);
        }

        builder
    }

    /// Commits pending events from an aggregate root to the event store.
    ///
    /// This method:
    /// 1. Extracts pending events from the aggregate root
    /// 2. Converts them to event envelopes
    /// 3. Appends them to the event store with optimistic concurrency control
    /// 4. Creates a snapshot if the strategy indicates it should
    /// 5. Clears the pending events on success
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if another process modified the aggregate.
    async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::Event: serde::Serialize,
    {
        let pending = aggregate.pending_events();
        if pending.is_empty() {
            return Ok(());
        }

        let aggregate_id = aggregate.entity_id().as_uuid();
        let aggregate_type = AggregateRoot::<A>::aggregate_type();
        let expected_version = Version::new(
            aggregate
                .version()
                .as_u64()
                .saturating_sub(pending.len() as u64),
        );

        let envelopes: Result<Vec<EventEnvelope>> = pending
            .iter()
            .map(|event| event.to_envelope(aggregate_id))
            .collect();
        let envelopes = envelopes?;

        self.append(
            StreamId::new(aggregate_type, aggregate_id),
            envelopes.clone(),
            expected_version,
        )
        .await?;

        // Create snapshot if strategy indicates we should
        let config = self.snapshot_config();
        let strategy = config.strategy_for_type(aggregate_type);
        let current_version = aggregate.version();

        if strategy.should_snapshot(current_version) {
            match serde_json::to_value(aggregate.entity()) {
                Ok(snapshot_data) => {
                    let snapshot = Snapshot::new(
                        aggregate_id,
                        aggregate_type.to_string(),
                        current_version,
                        snapshot_data,
                    );

                    if let Err(e) = self.save_snapshot(snapshot).await {
                        tracing::warn!(
                            aggregate_type = %aggregate_type,
                            aggregate_id = %aggregate_id,
                            error = %e,
                            "Failed to save snapshot"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        aggregate_type = %aggregate_type,
                        aggregate_id = %aggregate_id,
                        error = %e,
                        "Failed to serialize entity for snapshot"
                    );
                }
            }
        }

        aggregate.clear_pending_events();

        Ok(())
    }
}

/// Loads an aggregate from the event store by its ID.
///
/// Returns an `AggregateRoot<A>` reconstructed by replaying events,
/// optionally using a snapshot for optimization.
///
/// # Errors
///
/// Returns an error if events cannot be deserialized or replay fails.
pub async fn load<S, A>(store: &S, id: EntityId) -> Result<AggregateRoot<A>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    use futures::StreamExt;

    let uuid = id.as_uuid();
    let aggregate_type = A::aggregate_type();
    let stream_id = StreamId::new(aggregate_type, uuid);

    // Try to load snapshot if enabled
    let config = store.snapshot_config();
    let (mut aggregate, from_version) = if config.use_snapshots_on_load() {
        match store.load_snapshot(stream_id.clone()).await? {
            Some(snapshot) => {
                let entity: A = serde_json::from_value(snapshot.snapshot_data).map_err(|e| {
                    crate::Error::custom(format!("Failed to deserialize snapshot entity: {e}"))
                })?;

                let aggregate = AggregateRoot::from_snapshot(snapshot.snapshot_version, entity);

                (aggregate, snapshot.snapshot_version.next())
            }
            None => (AggregateRoot::new(id), Version::initial()),
        }
    } else {
        (AggregateRoot::new(id), Version::initial())
    };

    // Load events from the appropriate version
    let event_stream = store.load_stream(stream_id, from_version).await?;
    futures::pin_mut!(event_stream);

    // Replay events
    while let Some(envelope) = event_stream.next().await {
        let envelope = envelope?;
        let event: A::Event = envelope.try_into_event()?;
        aggregate.apply_unchecked(&event);
    }

    Ok(aggregate)
}

/// Counts the number of events in a stream.
///
/// # Errors
///
/// Returns an error if the event store fails to load the stream.
pub async fn count_events<S>(store: &S, stream_id: StreamId) -> Result<usize>
where
    S: EventStore,
{
    use futures::StreamExt;

    let event_stream = store.load_stream(stream_id, Version::initial()).await?;
    futures::pin_mut!(event_stream);

    let mut count = 0;
    while let Some(result) = event_stream.next().await {
        result?;
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
#[allow(clippy::map_unwrap_or)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_new() {
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        assert_eq!(stream_id.aggregate_type(), "User");
        assert_eq!(stream_id.aggregate_id(), aggregate_id);
    }

    #[test]
    fn test_stream_id_display() {
        let aggregate_id = Uuid::nil();
        let stream_id = StreamId::new("Order", aggregate_id);

        let display = stream_id.to_string();
        assert!(display.starts_with("Order:"));
        assert!(display.contains(&aggregate_id.to_string()));
    }

    #[test]
    fn test_stream_id_equality() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let stream1 = StreamId::new("User", id1);
        let stream2 = StreamId::new("User", id1);
        let stream3 = StreamId::new("User", id2);
        let stream4 = StreamId::new("Order", id1);

        assert_eq!(stream1, stream2);
        assert_ne!(stream1, stream3);
        assert_ne!(stream1, stream4);
    }

    #[test]
    fn test_position_new() {
        let pos = Position::new(42);
        assert_eq!(pos.as_i64(), 42);
    }

    #[test]
    fn test_position_start() {
        let pos = Position::start();
        assert_eq!(pos.as_i64(), 0);
    }

    #[test]
    fn test_position_ordering() {
        let pos1 = Position::new(1);
        let pos2 = Position::new(2);
        let pos3 = Position::new(3);

        assert!(pos1 < pos2);
        assert!(pos2 < pos3);
        assert!(pos1 < pos3);
    }

    #[test]
    fn test_position_from_i64() {
        let pos: Position = 100.into();
        assert_eq!(pos.as_i64(), 100);
    }

    #[test]
    fn test_position_into_i64() {
        let pos = Position::new(100);
        let value: i64 = pos.into();
        assert_eq!(value, 100);
    }

    #[test]
    fn test_snapshot_new() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"value": 42});

        let snapshot = Snapshot::new(
            aggregate_id,
            "Counter".to_string(),
            Version::new(10),
            snapshot_data.clone(),
        );

        assert_eq!(snapshot.aggregate_id, aggregate_id);
        assert_eq!(snapshot.aggregate_type, "Counter");
        assert_eq!(snapshot.snapshot_version, Version::new(10));
        assert_eq!(snapshot.snapshot_data, snapshot_data);
    }

    // Tests for default trait implementations
    use crate::test_fixtures::MockEventStore as SharedMockEventStore;

    #[tokio::test]
    async fn test_save_snapshot_default_implementation() {
        let store = SharedMockEventStore::new();
        let snapshot = Snapshot::new(
            Uuid::new_v4(),
            "TestAggregate".to_string(),
            Version::new(10),
            serde_json::json!({"value": 42}),
        );

        let result = store.save_snapshot(snapshot).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_load_snapshot_default_implementation() {
        let store = SharedMockEventStore::new();
        let stream_id = StreamId::new("TestAggregate", Uuid::new_v4());

        let result = store.load_snapshot(stream_id).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_save_and_load_snapshot_default_implementations() {
        let store = SharedMockEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);

        let snapshot = Snapshot::new(
            aggregate_id,
            "TestAggregate".to_string(),
            Version::new(100),
            serde_json::json!({"state": "active"}),
        );
        store.save_snapshot(snapshot).await.unwrap();

        let loaded = store.load_snapshot(stream_id).await.unwrap();
        assert!(loaded.is_none());
    }

    // Tests for stream_exists and count_events
    use async_trait::async_trait;
    use futures::stream;

    struct MockEventStoreWithStreams {
        streams: std::collections::HashMap<StreamId, Vec<EventEnvelope>>,
    }

    impl MockEventStoreWithStreams {
        fn new() -> Self {
            Self {
                streams: std::collections::HashMap::new(),
            }
        }

        fn with_stream(mut self, stream_id: StreamId, count: usize) -> Self {
            let mut events = Vec::new();
            for i in 0..count {
                let envelope = EventEnvelope::new(
                    Uuid::new_v4(),
                    stream_id.aggregate_id(),
                    stream_id.aggregate_type().to_string(),
                    "TestEvent".to_string(),
                    Version::new(i as u64 + 1),
                    serde_json::json!({"index": i}),
                );
                events.push(envelope);
            }
            self.streams.insert(stream_id, events);
            self
        }
    }

    #[async_trait]
    impl EventStore for MockEventStoreWithStreams {
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
            stream_id: StreamId,
            from_version: Version,
        ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
            let events = self
                .streams
                .get(&stream_id)
                .map(|events| {
                    events
                        .iter()
                        .filter(|e| e.event_version >= from_version)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            Ok(stream::iter(events.into_iter().map(Ok)))
        }

        async fn stream_all(
            &self,
            _from_position: Position,
        ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
            Ok(stream::empty())
        }

        async fn get_version(&self, stream_id: StreamId) -> Result<Version> {
            Ok(self
                .streams
                .get(&stream_id)
                .and_then(|events| events.last())
                .map(|e| e.event_version)
                .unwrap_or_else(Version::initial))
        }
    }

    #[tokio::test]
    async fn test_stream_exists_returns_true_for_existing_stream() {
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let store = MockEventStoreWithStreams::new().with_stream(stream_id.clone(), 5);

        let exists = store.stream_exists(stream_id).await.unwrap();
        assert!(exists, "Stream with events should exist");
    }

    #[tokio::test]
    async fn test_stream_exists_returns_false_for_nonexistent_stream() {
        let store = MockEventStoreWithStreams::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let exists = store.stream_exists(stream_id).await.unwrap();
        assert!(!exists, "Nonexistent stream should not exist");
    }

    #[tokio::test]
    async fn test_count_events_returns_correct_count() {
        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let store = MockEventStoreWithStreams::new().with_stream(stream_id.clone(), 10);

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 10, "Should count all events in stream");
    }

    #[tokio::test]
    async fn test_count_events_returns_zero_for_empty_stream() {
        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let store = MockEventStoreWithStreams::new().with_stream(stream_id.clone(), 0);

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 0, "Empty stream should have zero events");
    }

    #[tokio::test]
    async fn test_count_events_returns_zero_for_nonexistent_stream() {
        let store = MockEventStoreWithStreams::new();
        let stream_id = StreamId::new("Order", Uuid::new_v4());

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 0, "Nonexistent stream should have zero events");
    }
}
