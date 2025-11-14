//! Event store trait for event persistence.
//!
//! Defines the `EventStore` trait for persisting and retrieving events with streaming support.

use async_trait::async_trait;
use futures::Stream;
use uuid::Uuid;

use crate::{EventEnvelope, Result, Version};

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
