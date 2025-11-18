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
            envelopes,
            expected_version,
        )
        .await?;

        // Create snapshot if strategy indicates we should
        let config = self.snapshot_config();
        let strategy = config.strategy_for_type(aggregate_type);
        let current_version = aggregate.version();

        if strategy.should_snapshot(current_version) {
            // Try to serialize and save snapshot, but don't fail commit on error
            match serde_json::to_value(&*aggregate) {
                Ok(snapshot_data) => {
                    let snapshot = Snapshot::new(
                        aggregate_id,
                        aggregate_type.to_string(),
                        current_version,
                        snapshot_data,
                    );

                    // Log error but don't fail commit
                    if let Err(e) = self.save_snapshot(snapshot).await {
                        eprintln!(
                            "Warning: Failed to save snapshot for {} {}: {}",
                            aggregate_type, aggregate_id, e
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to serialize aggregate {} {} for snapshot: {}",
                        aggregate_type, aggregate_id, e
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
/// let user_id = UserId::from(uuid);
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
                // Deserialize aggregate from snapshot
                let aggregate: A = serde_json::from_value(snapshot.snapshot_data).map_err(|e| {
                    crate::Error::custom(format!("Failed to deserialize snapshot: {e}"))
                })?;

                // Start loading events from after the snapshot
                (aggregate, snapshot.snapshot_version.next())
            }
            None => {
                // No snapshot, load from beginning
                (A::new(aggregate_id), Version::initial())
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

#[cfg(test)]
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
        let debug = format!("{:?}", stream_id);

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

        let debug = format!("{:?}", snapshot);
        assert!(debug.contains("Snapshot"));
    }

    // Tests for default trait implementations
    use async_trait::async_trait;
    use futures::stream;

    /// Mock EventStore for testing default snapshot implementations
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
}
