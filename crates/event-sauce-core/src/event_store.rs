//! Event store trait for event persistence.
//!
//! Defines the `EventStore` trait for persisting and retrieving events with streaming support.

use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use uuid::Uuid;

use crate::{
    Aggregate, AggregateRoot, AggregateType, AggregateVersion, DomainEvent, EntityId,
    EventEnvelope, Repository, Result, SnapshotConfig,
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
    aggregate_type: AggregateType,
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
    pub fn new(aggregate_type: impl Into<AggregateType>, aggregate_id: Uuid) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id,
        }
    }

    /// Returns the aggregate type.
    #[must_use]
    pub fn aggregate_type(&self) -> &AggregateType {
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
/// use event_sauce_core::{Snapshot, AggregateVersion};
/// use uuid::Uuid;
/// use serde_json::json;
///
/// let snapshot = Snapshot::new(
///     Uuid::new_v4(),
///     "User",
///     AggregateVersion::new(100),
///     json!({"email": "user@example.com", "status": "active"}),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Aggregate ID.
    pub aggregate_id: Uuid,
    /// Aggregate type.
    pub aggregate_type: AggregateType,
    /// [`AggregateVersion`] at which snapshot was taken.
    pub snapshot_version: AggregateVersion,
    /// Serialized entity state.
    pub snapshot_data: serde_json::Value,
}

impl Snapshot {
    /// Creates a new snapshot.
    #[must_use]
    pub fn new(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type: aggregate_type.into(),
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
        expected_version: AggregateVersion,
    ) -> Result<()>;

    /// Loads events from a stream starting at a specific version.
    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    /// Streams all events from the store.
    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    /// Gets the current version of a stream.
    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion>;

    /// Checks if a stream exists.
    async fn stream_exists(&self, stream_id: StreamId) -> Result<bool> {
        let version = self.get_version(stream_id).await?;
        Ok(version != AggregateVersion::initial())
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
        self: &std::sync::Arc<Self>,
        name: impl Into<String>,
    ) -> crate::SubscriptionBuilder<Self>
    where
        Self: Sized + 'static,
    {
        let mut builder = crate::SubscriptionBuilder::new(name, std::sync::Arc::clone(self));

        if let Some(checkpoint_store) = self.checkpoint_store() {
            builder = builder.checkpoint_store(checkpoint_store);
        }

        builder
    }

    /// Creates a subscription builder pre-configured for a projection type.
    ///
    /// Uses `P::NAME` as the subscription name and `P::event_filter()` as the filter.
    /// The checkpoint store is auto-wired if available.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut sub = store
    ///     .projection_subscription::<OrderSummaryProjection>()
    ///     .build()?;
    /// sub.run_projection(&mut projection).await?;
    /// ```
    fn projection_subscription<P: crate::Projection>(
        self: &std::sync::Arc<Self>,
    ) -> crate::SubscriptionBuilder<Self>
    where
        Self: Sized + 'static,
    {
        self.subscription_builder(P::NAME)
            .filter_for_projection::<P>()
    }

    /// Creates a [`Repository`] for the given aggregate type, wrapping this event store.
    ///
    /// This is a convenience method that avoids verbose turbofish syntax.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let store = Arc::new(MyEventStore::new());
    /// let user_repo = store.repository::<User>();
    /// ```
    fn repository<A>(self: &Arc<Self>) -> Repository<Self, A>
    where
        Self: Sized + 'static,
        A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
        A::Event: serde::Serialize + serde::de::DeserializeOwned,
    {
        Repository::new(Arc::clone(self))
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
        let expected_version = AggregateVersion::new(
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
            StreamId::new(aggregate_type.clone(), aggregate_id),
            envelopes.clone(),
            expected_version,
        )
        .await?;

        // Create snapshot if strategy indicates we should
        let config = self.snapshot_config();
        let strategy = config.strategy_for_type(aggregate_type.as_str());
        let current_version = aggregate.version();

        if strategy.should_snapshot(current_version) {
            match serde_json::to_value(aggregate.entity()) {
                Ok(snapshot_data) => {
                    let snapshot = Snapshot::new(
                        aggregate_id,
                        aggregate_type.clone(),
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
/// This function handles both `DefaultEntity` aggregates (legacy) and
/// init-event aggregates. It detects which pattern is used by checking
/// `is_init()` on the first event.
///
/// # Errors
///
/// Returns an error if events cannot be deserialized or replay fails.
pub(crate) async fn load<S, A>(store: &S, id: EntityId) -> Result<AggregateRoot<A>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    use crate::{EventApplicator, UninitAggregateRoot};
    use futures::StreamExt;

    let uuid = id.as_uuid();
    let aggregate_type = A::aggregate_type();
    let stream_id = StreamId::new(aggregate_type, uuid);

    // Try to load snapshot if enabled
    let config = store.snapshot_config();
    if config.use_snapshots_on_load() {
        if let Some(snapshot) = store.load_snapshot(stream_id.clone()).await? {
            let entity: A = serde_json::from_value(snapshot.snapshot_data).map_err(|e| {
                crate::Error::custom(format!("Failed to deserialize snapshot entity: {e}"))
            })?;

            let mut aggregate = AggregateRoot::from_snapshot(snapshot.snapshot_version, entity);
            let from_version = snapshot.snapshot_version;

            let event_stream = store.load_stream(stream_id, from_version).await?;
            futures::pin_mut!(event_stream);

            while let Some(envelope) = event_stream.next().await {
                let envelope = envelope?;
                let event = A::Event::from_envelope(&envelope)?;
                aggregate.apply_unchecked(&event);
            }

            return Ok(aggregate);
        }
    }

    // No snapshot: load all events
    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;
    futures::pin_mut!(event_stream);

    let Some(first_envelope) = event_stream.next().await else {
        // No events: backward compat — returns default-state aggregate.
        // For init-event aggregates, Entity::new panics (data integrity issue).
        return Ok(AggregateRoot::new_for_replay(id));
    };
    let first_event = A::Event::from_envelope(&first_envelope?)?;

    // Detect init vs legacy from first event
    let mut aggregate = if EventApplicator::is_init(&first_event) {
        // Init-event aggregate: type-state transition
        UninitAggregateRoot::<A>::new(id).apply_init_unchecked(&first_event)
    } else {
        // Legacy aggregate: Entity::new(id) + apply first event
        let mut agg = AggregateRoot::<A>::new_for_replay(id);
        agg.apply_unchecked(&first_event);
        agg
    };

    // Replay remaining events (all regular)
    while let Some(envelope) = event_stream.next().await {
        let envelope = envelope?;
        let event = A::Event::from_envelope(&envelope)?;
        aggregate.apply_unchecked(&event);
    }

    Ok(aggregate)
}

/// Counts the number of events in a stream.
///
/// # Errors
///
/// Returns an error if the event store fails to load the stream.
pub(crate) async fn count_events<S>(store: &S, stream_id: StreamId) -> Result<usize>
where
    S: EventStore,
{
    use futures::StreamExt;

    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;
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
            AggregateVersion::new(10),
            snapshot_data.clone(),
        );

        assert_eq!(snapshot.aggregate_id, aggregate_id);
        assert_eq!(snapshot.aggregate_type, "Counter");
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(10));
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
            AggregateVersion::new(10),
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
            AggregateVersion::new(100),
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
                    crate::EventVersion::new(i as u64 + 1),
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
            _expected_version: AggregateVersion,
        ) -> Result<()> {
            Ok(())
        }

        async fn load_stream(
            &self,
            stream_id: StreamId,
            from_version: AggregateVersion,
        ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
            #[allow(clippy::cast_possible_truncation)]
            let events = self
                .streams
                .get(&stream_id)
                .map(|events| {
                    events
                        .iter()
                        .skip(from_version.as_u64() as usize)
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

        async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
            Ok(self
                .streams
                .get(&stream_id)
                .map_or(AggregateVersion::initial(), |events| {
                    AggregateVersion::new(events.len() as u64)
                }))
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

    // === Tests for commit() and load() ===

    use crate::test_fixtures::{SimpleTestEntity, SimpleTestEvent};
    use crate::{AggregateRoot, SnapshotConfig};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Mock event store that supports snapshots, tracks `append`/`save_snapshot` calls,
    /// and can be configured to fail on `save_snapshot`.
    struct CommitTestStore {
        streams: Arc<Mutex<HashMap<StreamId, Vec<EventEnvelope>>>>,
        snapshots: Arc<Mutex<HashMap<StreamId, Snapshot>>>,
        config: SnapshotConfig,
        append_count: Arc<Mutex<u32>>,
        save_snapshot_count: Arc<Mutex<u32>>,
        fail_save_snapshot: bool,
    }

    impl CommitTestStore {
        fn new(config: SnapshotConfig) -> Self {
            Self {
                streams: Arc::new(Mutex::new(HashMap::new())),
                snapshots: Arc::new(Mutex::new(HashMap::new())),
                config,
                append_count: Arc::new(Mutex::new(0)),
                save_snapshot_count: Arc::new(Mutex::new(0)),
                fail_save_snapshot: false,
            }
        }

        fn with_fail_save_snapshot(mut self) -> Self {
            self.fail_save_snapshot = true;
            self
        }

        fn append_count(&self) -> u32 {
            *self.append_count.lock().unwrap()
        }

        fn save_snapshot_count(&self) -> u32 {
            *self.save_snapshot_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl EventStore for CommitTestStore {
        async fn append(
            &self,
            stream_id: StreamId,
            events: Vec<EventEnvelope>,
            _expected_version: AggregateVersion,
        ) -> Result<()> {
            *self.append_count.lock().unwrap() += 1;
            let mut streams = self.streams.lock().unwrap();
            streams.entry(stream_id).or_default().extend(events);
            Ok(())
        }

        async fn load_stream(
            &self,
            stream_id: StreamId,
            from_version: AggregateVersion,
        ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
            let streams = self.streams.lock().unwrap();
            #[allow(clippy::cast_possible_truncation)]
            let events = streams
                .get(&stream_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .skip(from_version.as_u64() as usize)
                .map(Ok)
                .collect::<Vec<_>>();
            Ok(stream::iter(events))
        }

        async fn stream_all(
            &self,
            _from_position: Position,
        ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
            Ok(stream::empty())
        }

        async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
            let streams = self.streams.lock().unwrap();
            let count = streams.get(&stream_id).map_or(0, Vec::len);
            Ok(AggregateVersion::new(count as u64))
        }

        async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
            *self.save_snapshot_count.lock().unwrap() += 1;
            if self.fail_save_snapshot {
                return Err(crate::Error::custom("Snapshot save failed"));
            }
            let stream_id = StreamId::new(snapshot.aggregate_type.clone(), snapshot.aggregate_id);
            self.snapshots.lock().unwrap().insert(stream_id, snapshot);
            Ok(())
        }

        async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
            Ok(self.snapshots.lock().unwrap().get(&stream_id).cloned())
        }

        fn snapshot_config(&self) -> &SnapshotConfig {
            &self.config
        }
    }

    // -- commit() tests --

    #[tokio::test]
    async fn test_commit_with_pending_events() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 99 }).unwrap();

        assert_eq!(agg.pending_events().len(), 2);
        assert_eq!(agg.version(), AggregateVersion::new(2));

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            agg.pending_events().len(),
            0,
            "Pending events should be cleared after commit"
        );
        assert_eq!(
            store.append_count(),
            1,
            "Append should be called exactly once"
        );

        // Verify the events were stored
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();
        assert_eq!(stored.len(), 2, "Two events should be stored");
    }

    #[tokio::test]
    async fn test_commit_with_no_pending_events() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        // No events applied, so commit should be a no-op
        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.append_count(),
            0,
            "Append should not be called when there are no pending events"
        );
    }

    #[tokio::test]
    async fn test_commit_triggers_snapshot_when_strategy_says_yes() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.save_snapshot_count(),
            1,
            "save_snapshot should be called when strategy says yes"
        );

        // Verify snapshot was stored
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshots = store.snapshots.lock().unwrap();
        let snapshot = snapshots.get(&stream_id).expect("Snapshot should exist");
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(1));
        assert_eq!(snapshot.aggregate_type, "SimpleTestEntity");
    }

    #[tokio::test]
    async fn test_commit_does_not_snapshot_when_strategy_says_no() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.save_snapshot_count(),
            0,
            "save_snapshot should not be called when strategy says no"
        );
    }

    #[tokio::test]
    async fn test_commit_handles_snapshot_save_failure_gracefully() {
        let store = CommitTestStore::new(SnapshotConfig::always()).with_fail_save_snapshot();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        // commit should still succeed even when save_snapshot fails
        let result = store.commit(&mut agg).await;
        assert!(
            result.is_ok(),
            "Commit should succeed even if snapshot save fails"
        );

        assert_eq!(store.append_count(), 1, "Events should still be appended");
        assert_eq!(
            store.save_snapshot_count(),
            1,
            "save_snapshot should have been attempted"
        );
        assert!(
            agg.pending_events().is_empty(),
            "Pending events should still be cleared"
        );
    }

    #[tokio::test]
    async fn test_commit_clears_pending_events_after_success() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 2 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 3 }).unwrap();

        assert_eq!(agg.pending_events().len(), 3);

        store.commit(&mut agg).await.unwrap();

        assert!(
            agg.pending_events().is_empty(),
            "All pending events should be cleared after successful commit"
        );
        // AggregateVersion should be preserved
        assert_eq!(agg.version(), AggregateVersion::new(3));
    }

    // -- load() tests --

    #[tokio::test]
    async fn test_load_with_no_snapshot_replays_from_beginning() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // First, commit some events
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 20 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 30 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Load from store
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 30,
            "Entity state should reflect all replayed events"
        );
        assert_eq!(
            loaded.version(),
            AggregateVersion::new(3),
            "AggregateVersion should match number of events"
        );
        assert!(
            loaded.pending_events().is_empty(),
            "Loaded aggregate should have no pending events"
        );
    }

    #[tokio::test]
    async fn test_load_with_snapshot_resumes_from_snapshot_version() {
        let config = SnapshotConfig::always();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();

        // Pre-save a snapshot at version 2 with value=20
        let snapshot_entity = SimpleTestEntity { id, value: 20 };
        let snapshot_data = serde_json::to_value(&snapshot_entity).unwrap();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(2),
            snapshot_data,
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), snapshot);

        // Add all events (skip-based filtering requires full stream).
        // Events at index 0 and 1 are pre-snapshot, 2 and 3 are post-snapshot.
        let envelope1 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestCreated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Created { value: 10 }).unwrap(),
        );
        let envelope2 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 20 }).unwrap(),
        );
        let envelope3 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 30 }).unwrap(),
        );
        let envelope4 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 40 }).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .insert(stream_id, vec![envelope1, envelope2, envelope3, envelope4]);

        // Load from store — should start from snapshot and replay events 3 and 4
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 40,
            "Entity should reflect snapshot + replayed events"
        );
        // AggregateVersion should be snapshot (2) + replayed events (2) = 4
        assert_eq!(loaded.version(), AggregateVersion::new(4));
    }

    #[tokio::test]
    async fn test_load_with_snapshots_disabled_ignores_snapshot() {
        let config = SnapshotConfig::builder()
            .default_strategy(crate::AlwaysSnapshot)
            .use_snapshots_on_load(false)
            .build();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();

        // Save a snapshot that should be ignored
        let snapshot_entity = SimpleTestEntity { id, value: 999 };
        let snapshot_data = serde_json::to_value(&snapshot_entity).unwrap();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(5),
            snapshot_data,
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), snapshot);

        // Add events from the beginning
        let envelope1 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestCreated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Created { value: 10 }).unwrap(),
        );
        let envelope2 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 20 }).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .insert(stream_id, vec![envelope1, envelope2]);

        // Load — should ignore snapshot and replay all events from beginning
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 20,
            "Should reflect full replay, not snapshot value of 999"
        );
        assert_eq!(loaded.version(), AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_load_with_no_events_returns_default() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Load entity that has no events at all
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(loaded.value, 0, "Default entity should have value 0");
        assert_eq!(loaded.version(), AggregateVersion::initial());
        assert!(loaded.pending_events().is_empty());
        assert_eq!(loaded.entity_id(), id);
    }

    // -- repository() convenience method tests --

    #[tokio::test]
    async fn test_repository_convenience_method() {
        let store = Arc::new(SharedMockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let test_id = crate::EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 42 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 42);
    }

    // -- projection_subscription() convenience method tests --

    struct TestProjection {
        count: u64,
    }

    #[async_trait]
    impl crate::Projection for TestProjection {
        type State = u64;
        const NAME: &'static str = "TestProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["UserCreated"])
        }

        async fn handle(&mut self, _envelope: &crate::EventEnvelope) -> crate::Result<()> {
            self.count += 1;
            Ok(())
        }

        fn state(&self) -> &u64 {
            &self.count
        }

        fn state_mut(&mut self) -> &mut u64 {
            &mut self.count
        }
    }

    #[tokio::test]
    async fn test_projection_subscription_convenience_method() {
        use crate::test_fixtures::create_test_envelope;

        let store = Arc::new(SharedMockEventStore::new());

        // Add events — one matching, one not
        store.add_event(create_test_envelope("UserCreated", "User"));
        store.add_event(create_test_envelope("OrderCreated", "Order"));

        let mut subscription = store
            .projection_subscription::<TestProjection>()
            .build()
            .unwrap();

        let mut projection = TestProjection { count: 0 };
        subscription.run_projection(&mut projection).await.unwrap();

        // Only "UserCreated" should be processed (filter from Projection)
        assert_eq!(projection.count, 1);
    }
}
