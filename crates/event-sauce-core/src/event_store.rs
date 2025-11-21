//! Event store trait for event persistence.
//!
//! Defines the `EventStore` trait for persisting and retrieving events with streaming support.

use async_trait::async_trait;
use futures::Stream;
use uuid::Uuid;

use crate::{Aggregate, AggregateId, DomainEvent, EventEnvelope, Result, SnapshotConfig, Version};

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
    /// Serialized aggregate state.
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
/// The event store is responsible for:
/// - Persisting events durably
/// - Loading events for aggregate reconstruction
/// - Streaming events for projections
/// - Managing snapshots for performance
/// - Enforcing optimistic concurrency control
///
/// # Streaming Support
///
/// All read operations return `Stream` for memory-efficient processing
/// of large event sets.
///
/// # Concurrency Control
///
/// Uses optimistic locking via version numbers. Append operations
/// must specify expected version and will fail if actual version differs.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::{EventStore, StreamId, Version, EventEnvelope};
/// use futures::StreamExt;
///
/// async fn example(store: impl EventStore) -> Result<(), Box<dyn std::error::Error>> {
///     let stream_id = StreamId::new("User", uuid::Uuid::new_v4());
///
///     // Append events
///     store.append(stream_id.clone(), vec![envelope], Version::new(0)).await?;
///
///     // Load events as stream
///     let mut events = store.load_stream(stream_id, Version::new(0)).await?;
///     while let Some(event) = events.next().await {
///         println!("Event: {:?}", event?);
///     }
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Appends events to a stream with optimistic concurrency control.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if the expected version doesn't match
    /// the current stream version.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// store.append(stream_id, events, Version::new(5)).await?;
    /// ```
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<()>;

    /// Loads events from a stream starting at a specific version.
    ///
    /// Returns a stream of events for memory-efficient processing.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut stream = store.load_stream(stream_id, Version::new(0)).await?;
    /// while let Some(event) = stream.next().await {
    ///     // Process event
    /// }
    /// ```
    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: Version,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    /// Streams all events from the store.
    ///
    /// Used for projection rebuilds and catch-up subscriptions.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut all_events = store.stream_all(Position::start()).await?;
    /// while let Some(event) = all_events.next().await {
    ///     // Rebuild projection
    /// }
    /// ```
    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    /// Gets the current version of a stream.
    ///
    /// Returns `Version::initial()` if the stream doesn't exist.
    async fn get_version(&self, stream_id: StreamId) -> Result<Version>;

    /// Checks if a stream exists.
    ///
    /// Returns `true` if the stream has any events, `false` otherwise.
    ///
    /// # Default Implementation
    ///
    /// The default implementation uses `get_version()` to check if the stream
    /// exists by comparing the version to `Version::initial()`. Backends may
    /// override this for optimized existence checks.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if store.stream_exists(stream_id).await? {
    ///     println!("Stream exists with events");
    /// }
    /// ```
    async fn stream_exists(&self, stream_id: StreamId) -> Result<bool> {
        let version = self.get_version(stream_id).await?;
        Ok(version != Version::initial())
    }

    /// Saves a snapshot.
    ///
    /// Optional operation - implementations may choose not to support snapshots.
    ///
    /// # Default Implementation
    ///
    /// The default implementation is a no-op that always succeeds.
    /// Implementations without snapshot support can use this default.
    /// Coverage: Tested via `MockEventStore` in tests.
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let _ = snapshot;
        Ok(()) // Default: no-op
    }

    /// Loads a snapshot.
    ///
    /// Returns `None` if no snapshot exists or snapshots are not supported.
    ///
    /// # Default Implementation
    ///
    /// The default implementation always returns `None`.
    /// Implementations without snapshot support can use this default.
    /// Coverage: Tested via `MockEventStore` in tests.
    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        let _ = stream_id;
        Ok(None) // Default: no snapshots
    }

    /// Returns the snapshot configuration for this store.
    ///
    /// The configuration controls:
    /// - When snapshots are created (strategy per aggregate type)
    /// - Whether snapshots are used during loading
    ///
    /// # Default Implementation
    ///
    /// The default implementation returns a disabled configuration.
    /// Implementations can override this to provide custom snapshot behavior.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let config = store.snapshot_config();
    /// let strategy = config.strategy_for_type("User");
    /// if strategy.should_snapshot(aggregate.version()) {
    ///     // Create snapshot
    /// }
    /// ```
    fn snapshot_config(&self) -> &SnapshotConfig {
        use std::sync::OnceLock;
        static DISABLED_CONFIG: OnceLock<SnapshotConfig> = OnceLock::new();
        DISABLED_CONFIG.get_or_init(SnapshotConfig::disabled)
    }

    /// Returns the checkpoint store associated with this event store, if any.
    ///
    /// Checkpoint stores track the progress of subscriptions, enabling
    /// resumption after restarts or failures.
    ///
    /// # Default Implementation
    ///
    /// The default implementation returns `None`, indicating no checkpoint store
    /// is configured. Implementations can override this to provide checkpoint storage.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if let Some(checkpoint_store) = store.checkpoint_store() {
    ///     checkpoint_store.save_checkpoint("my-sub", Position::new(42)).await?;
    /// }
    /// ```
    fn checkpoint_store(&self) -> Option<crate::CheckpointStoreRef> {
        None
    }

    /// Creates a subscription builder pre-configured with this event store.
    ///
    /// If the event store has an associated checkpoint store, it will be
    /// automatically included in the subscription builder.
    ///
    /// This method requires the store to be wrapped in an Arc.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::EventFilter;
    /// use std::sync::Arc;
    ///
    /// let store = Arc::new(event_store);
    ///
    /// let subscription = store
    ///     .subscription_builder("my-subscription")
    ///     .filter(EventFilter::by_event_type("UserCreated"))
    ///     .build()?;
    /// ```
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

    /// Commits pending events from an aggregate to the event store.
    ///
    /// This method:
    /// 1. Extracts pending events from the aggregate
    /// 2. Converts them to event envelopes
    /// 3. Appends them to the event store with optimistic concurrency control
    /// 4. Creates a snapshot if the strategy indicates it should
    /// 5. Clears the pending events from the aggregate on success
    ///
    /// Snapshots are created based on the [`SnapshotConfig`] returned by
    /// [`snapshot_config()`](Self::snapshot_config). The strategy is evaluated
    /// per aggregate type, and snapshot creation failures are logged but don't
    /// fail the commit.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut user = User::new(UserId::new());
    /// user.register("alice@example.com")?;
    /// user.verify_email()?;
    ///
    /// // Commit all pending events (and possibly create a snapshot)
    /// event_store.commit(&mut user).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if another process modified the aggregate.
    async fn commit<A>(&self, aggregate: &mut A) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::Event: serde::Serialize,
    {
        let pending = aggregate.pending_events();
        if pending.is_empty() {
            return Ok(());
        }

        let aggregate_id = aggregate.aggregate_id().to_uuid();
        let aggregate_type = A::aggregate_type();
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let expected_version = Version::new(
            aggregate
                .version()
                .as_i32()
                .saturating_sub(pending.len() as i32),
        );

        // Convert events to envelopes using the new to_envelope() method
        let envelopes: Result<Vec<EventEnvelope>> = pending
            .iter()
            .map(|event| event.to_envelope(aggregate_id))
            .collect();
        let envelopes = envelopes?;

        // Append to store
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
            // Try to serialize and save snapshot (only the state, not the entire aggregate)
            match serde_json::to_value(aggregate.state()) {
                Ok(snapshot_data) => {
                    let snapshot = Snapshot::new(
                        aggregate_id,
                        aggregate_type.to_string(),
                        current_version,
                        snapshot_data,
                    );

                    // Log error but don't fail commit
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
                        "Failed to serialize state for snapshot"
                    );
                }
            }
        }

        // Clear pending events
        aggregate.clear_pending_events();

        Ok(())
    }
}

/// Loads an aggregate from the event store by its ID.
///
/// This helper function:
/// 1. Checks if snapshots are enabled and loads a snapshot if available
/// 2. Loads events from the snapshot version (or from start if no snapshot)
/// 3. Deserializes them into domain events
/// 4. Replays them onto the aggregate instance
///
/// The snapshot behavior is controlled by the store's [`SnapshotConfig`].
/// When `use_snapshots_on_load` is true and a snapshot exists, only events
/// after the snapshot are loaded and replayed, significantly improving
/// performance for aggregates with many events.
///
/// # Note
///
/// Due to Rust's async trait limitations with generic return types, this must be
/// a standalone function rather than a trait method.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::load;
///
/// let user_id = AggregateId::new();
/// let user: User = load(&event_store, user_id).await?;
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The aggregate doesn't exist (no events found)
/// - Events cannot be deserialized
/// - Snapshot cannot be deserialized
/// - Event replay fails
pub async fn load<S, A>(store: &S, aggregate_id: A::Id) -> Result<A>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    use futures::StreamExt;

    let uuid = aggregate_id.to_uuid();
    let aggregate_type = A::aggregate_type();
    let stream_id = StreamId::new(aggregate_type, uuid);

    // Try to load snapshot if enabled
    let config = store.snapshot_config();
    let (mut aggregate, from_version) = if config.use_snapshots_on_load() {
        match store.load_snapshot(stream_id.clone()).await? {
            Some(snapshot) => {
                // Deserialize state from snapshot (not the entire aggregate)
                let state: A::State =
                    serde_json::from_value(snapshot.snapshot_data).map_err(|e| {
                        crate::Error::custom(format!("Failed to deserialize snapshot state: {e}"))
                    })?;

                // Reconstruct aggregate from snapshot components
                let aggregate =
                    A::from_snapshot(aggregate_id.clone(), snapshot.snapshot_version, state);

                // Start loading events from after the snapshot
                (aggregate, snapshot.snapshot_version.next())
            }
            None => {
                // No snapshot, load from beginning
                (A::new(aggregate_id.clone()), Version::initial())
            }
        }
    } else {
        // Snapshots disabled, load from beginning
        (A::new(aggregate_id), Version::initial())
    };

    // Load events from the appropriate version
    let event_stream = store.load_stream(stream_id, from_version).await?;
    futures::pin_mut!(event_stream);

    // Replay events
    while let Some(envelope) = event_stream.next().await {
        let envelope = envelope?;
        // Use the new try_into_event() method for idiomatic event deserialization
        let event: A::Event = envelope.try_into_event()?;

        aggregate.apply_unchecked(&event);
    }

    Ok(aggregate)
}

/// Counts the number of events in a stream.
///
/// Returns the total count of events in the specified stream, or 0 if
/// the stream doesn't exist.
///
/// This is a generic helper function that works with any `EventStore`
/// implementation. It loads all events from the stream and counts them using
/// streaming for memory efficiency. Backends may provide optimized count
/// methods (e.g., `SELECT COUNT(*)`), but this function provides a universal
/// fallback.
///
/// # Note
///
/// Due to Rust's async trait limitations with generic return types, this must be
/// a standalone function rather than a trait method (similar to `load()`).
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::count_events;
///
/// let stream_id = StreamId::new("User", uuid);
/// let count = count_events(&event_store, stream_id).await?;
/// println!("Stream has {} events", count);
/// ```
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
        result?; // Propagate any errors
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
    fn test_stream_id_clone() {
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let cloned = stream_id.clone();

        assert_eq!(stream_id, cloned);
    }

    #[test]
    fn test_stream_id_debug() {
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let debug = format!("{stream_id:?}");

        assert!(debug.contains("StreamId"));
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

    #[test]
    fn test_snapshot_clone() {
        let snapshot = Snapshot::new(
            Uuid::new_v4(),
            "Test".to_string(),
            Version::new(5),
            serde_json::json!({}),
        );

        let cloned = snapshot.clone();
        assert_eq!(snapshot.aggregate_id, cloned.aggregate_id);
        assert_eq!(snapshot.snapshot_version, cloned.snapshot_version);
    }

    #[test]
    fn test_snapshot_debug() {
        let snapshot = Snapshot::new(
            Uuid::new_v4(),
            "Test".to_string(),
            Version::new(5),
            serde_json::json!({}),
        );

        let debug = format!("{snapshot:?}");
        assert!(debug.contains("Snapshot"));
    }

    // Tests for default trait implementations
    use async_trait::async_trait;
    use futures::stream;

    /// Mock `EventStore` for testing default snapshot implementations
    struct MockEventStore;

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            _stream_id: StreamId,
            _events: Vec<crate::EventEnvelope>,
            _expected_version: Version,
        ) -> crate::Result<()> {
            Ok(())
        }

        async fn load_stream(
            &self,
            _stream_id: StreamId,
            _from_version: Version,
        ) -> crate::Result<impl futures::Stream<Item = crate::Result<crate::EventEnvelope>> + Send>
        {
            Ok(stream::empty())
        }

        async fn stream_all(
            &self,
            _from_position: Position,
        ) -> crate::Result<impl futures::Stream<Item = crate::Result<crate::EventEnvelope>> + Send>
        {
            Ok(stream::empty())
        }

        async fn get_version(&self, _stream_id: StreamId) -> crate::Result<Version> {
            Ok(Version::initial())
        }

        // Using default implementations for snapshot methods
    }

    #[tokio::test]
    async fn test_save_snapshot_default_implementation() {
        let store = MockEventStore;
        let snapshot = Snapshot::new(
            Uuid::new_v4(),
            "TestAggregate".to_string(),
            Version::new(10),
            serde_json::json!({"value": 42}),
        );

        // Default implementation should succeed but do nothing
        let result = store.save_snapshot(snapshot).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_load_snapshot_default_implementation() {
        let store = MockEventStore;
        let stream_id = StreamId::new("TestAggregate", Uuid::new_v4());

        // Default implementation should return None
        let result = store.load_snapshot(stream_id).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_save_and_load_snapshot_default_implementations() {
        let store = MockEventStore;
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);

        // Save a snapshot (default does nothing)
        let snapshot = Snapshot::new(
            aggregate_id,
            "TestAggregate".to_string(),
            Version::new(100),
            serde_json::json!({"state": "active"}),
        );
        store.save_snapshot(snapshot).await.unwrap();

        // Load snapshot (default returns None)
        let loaded = store.load_snapshot(stream_id).await.unwrap();
        assert!(loaded.is_none());
    }

    // Test utilities

    use crate::{Aggregate, AggregateError, DefaultAggregateId, DomainEvent};
    use chrono::Utc;
    use thiserror::Error;

    // Test aggregate for event bus testing - now uses DefaultAggregateId

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum TestAggregateEvent {
        Created { value: i32 },
        Updated { value: i32 },
    }

    impl DomainEvent for TestAggregateEvent {
        type Aggregate = TestAgg;

        fn event_type(&self) -> &'static str {
            match self {
                TestAggregateEvent::Created { .. } => "TestAggregate.Created",
                TestAggregateEvent::Updated { .. } => "TestAggregate.Updated",
            }
        }

        fn event_version(&self) -> i32 {
            1
        }

        fn occurred_at(&self) -> chrono::DateTime<Utc> {
            Utc::now()
        }
    }

    #[derive(Debug, Error)]
    #[error("Test aggregate error")]
    struct TestAggErr;

    impl AggregateError for TestAggErr {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct TestAggState {
        value: i32,
    }

    struct TestAgg {
        id: DefaultAggregateId,
        state: TestAggState,
        version: Version,
        pending_events: Vec<TestAggregateEvent>,
    }

    #[allow(dead_code)]
    impl TestAgg {
        fn create(&mut self, value: i32) -> std::result::Result<(), TestAggErr> {
            self.apply(TestAggregateEvent::Created { value })
        }

        fn update(&mut self, value: i32) -> std::result::Result<(), TestAggErr> {
            self.apply(TestAggregateEvent::Updated { value })
        }

        fn apply_event(&mut self, event: &TestAggregateEvent) {
            match event {
                TestAggregateEvent::Created { value } | TestAggregateEvent::Updated { value } => {
                    self.state.value = *value;
                }
            }
        }
    }

    impl Aggregate for TestAgg {
        type Id = DefaultAggregateId;
        type Event = TestAggregateEvent;
        type Error = TestAggErr;
        type State = TestAggState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: TestAggState { value: 0 },
                version: Version::initial(),
                pending_events: Vec::new(),
            }
        }

        fn aggregate_id(&self) -> &Self::Id {
            &self.id
        }

        fn version(&self) -> Version {
            self.version
        }

        fn pending_events(&self) -> &[Self::Event] {
            &self.pending_events
        }

        fn clear_pending_events(&mut self) {
            self.pending_events.clear();
        }

        fn apply<E: Into<Self::Event>>(
            &mut self,
            event: E,
        ) -> std::result::Result<(), Self::Error> {
            let event = event.into();
            self.apply_internal(&event)?;
            self.pending_events.push(event);
            Ok(())
        }

        fn apply_internal(&mut self, event: &Self::Event) -> std::result::Result<(), Self::Error> {
            self.apply_event(event);
            self.version = self.version.next();
            Ok(())
        }

        fn state(&self) -> &Self::State {
            &self.state
        }

        fn from_snapshot(id: Self::Id, version: Version, state: Self::State) -> Self {
            Self {
                id,
                state,
                version,
                pending_events: Vec::new(),
            }
        }
    }

    // Tests for new helper methods: stream_exists() and count_events()

    /// Mock `EventStore` with some streams for testing helper methods
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
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let envelope = EventEnvelope::new(
                    Uuid::new_v4(),
                    stream_id.aggregate_id(),
                    stream_id.aggregate_type().to_string(),
                    "TestEvent".to_string(),
                    Version::new(i as i32 + 1),
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
