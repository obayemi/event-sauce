//! Repository pattern for high-level aggregate persistence.
//!
//! Provides a type-safe, domain-focused API over the raw `EventStore` trait.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::{count_events, load, Aggregate, AggregateId, EventStore, Result, StreamId, Version};

/// Repository provides a high-level API for aggregate persistence.
///
/// Wraps an `EventStore` and provides type-safe operations for a specific
/// aggregate type, abstracting away stream IDs and other low-level concerns.
///
/// # Type Parameters
///
/// - `S`: The event store implementation
/// - `A`: The aggregate type
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::Repository;
///
/// let repo = Repository::<PostgresEventStore, User>::new(store);
///
/// // Load aggregate
/// let user = repo.load(user_id).await?;
///
/// // Save aggregate
/// repo.save(&mut user).await?;
///
/// // Check existence
/// if repo.exists(user_id).await? {
///     println!("User exists");
/// }
/// ```
#[derive(Debug)]
pub struct Repository<S, A> {
    store: Arc<S>,
    _phantom: PhantomData<A>,
}

impl<S, A> Repository<S, A>
where
    S: EventStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Creates a new repository wrapping the given event store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::Repository;
    /// use std::sync::Arc;
    ///
    /// let store = Arc::new(PostgresEventStore::new(pool));
    /// let repo = Repository::<PostgresEventStore, User>::new(store);
    /// ```
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    /// Saves an aggregate to the event store.
    ///
    /// Commits all pending events and clears them on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Event serialization fails
    /// - The event store operation fails
    /// - A concurrency conflict occurs
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut user = repo.load(user_id).await?;
    /// user.update_email("new@example.com")?;
    /// repo.save(&mut user).await?;
    /// ```
    pub async fn save(&self, aggregate: &mut A) -> Result<()> {
        self.store.commit(aggregate).await
    }

    /// Loads an aggregate from the event store.
    ///
    /// Reconstructs the aggregate by replaying all its events,
    /// potentially using a snapshot for optimization.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The aggregate doesn't exist (no events found)
    /// - Event deserialization fails
    /// - Snapshot deserialization fails
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let user = repo.load(user_id).await?;
    /// println!("User email: {}", user.email());
    /// ```
    pub async fn load(&self, id: A::Id) -> Result<A> {
        load(&*self.store, id).await
    }

    /// Checks if an aggregate exists in the event store.
    ///
    /// Returns `true` if the aggregate has any events in the store.
    ///
    /// # Errors
    ///
    /// Returns an error if the event store operation fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if repo.exists(user_id).await? {
    ///     println!("User exists");
    /// } else {
    ///     println!("User not found");
    /// }
    /// ```
    pub async fn exists(&self, id: A::Id) -> Result<bool> {
        let stream_id = StreamId::new(A::aggregate_type(), id.to_uuid());
        self.store.stream_exists(stream_id).await
    }

    /// Gets the current version of an aggregate without loading it.
    ///
    /// Useful for checking version without the overhead of loading
    /// all events and reconstructing state.
    ///
    /// # Errors
    ///
    /// Returns an error if the event store operation fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let version = repo.get_version(user_id).await?;
    /// println!("User is at version {}", version.as_u64());
    /// ```
    pub async fn get_version(&self, id: A::Id) -> Result<Version> {
        let stream_id = StreamId::new(A::aggregate_type(), id.to_uuid());
        self.store.get_version(stream_id).await
    }

    /// Counts the number of events for an aggregate.
    ///
    /// Returns the total number of events in the aggregate's stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the event store operation fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let count = repo.count_events(user_id).await?;
    /// println!("User has {} events", count);
    /// ```
    pub async fn count_events(&self, id: A::Id) -> Result<usize> {
        let stream_id = StreamId::new(A::aggregate_type(), id.to_uuid());
        count_events(&*self.store, stream_id).await
    }
}

impl<S, A> Clone for Repository<S, A> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AggregateError, ApplyEvent, DomainEvent, EventEnvelope, Version};
    use chrono::Utc;
    use serde::{Deserialize, Serialize};
    use std::fmt;
    use uuid::Uuid;

    // Test aggregate ID
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct TestId(Uuid);

    impl TestId {
        fn new() -> Self {
            Self(Uuid::new_v4())
        }
    }

    impl fmt::Display for TestId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Test-{}", self.0)
        }
    }

    impl AggregateId for TestId {
        fn to_uuid(&self) -> Uuid {
            self.0
        }

        fn from_uuid(uuid: Uuid) -> Self {
            Self(uuid)
        }
    }

    // Test events
    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum TestEvent {
        Incremented { amount: i32 },
        Reset,
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestAggregate;

        fn event_type(&self) -> &'static str {
            match self {
                Self::Incremented { .. } => "Incremented",
                Self::Reset => "Reset",
            }
        }

        fn event_version(&self) -> u64 {
            1
        }

        fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
            Utc::now()
        }
    }

    impl ApplyEvent<TestAggregate> for TestEvent {
        fn apply(&self, aggregate: &mut TestAggregate) {
            match self {
                Self::Incremented { amount } => aggregate.state.value += amount,
                Self::Reset => aggregate.state.value = 0,
            }
        }
    }

    // Test aggregate state
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct TestState {
        value: i32,
    }

    // Test aggregate
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestAggregate {
        id: TestId,
        state: TestState,
        version: Version,
        pending_events: Vec<TestEvent>,
    }

    impl crate::EventApplicator<TestAggregate> for TestEvent {
        fn dispatch(&self, aggregate: &mut TestAggregate) -> std::result::Result<(), TestError> {
            self.apply(aggregate);
            Ok(())
        }

        fn dispatch_unchecked(&self, aggregate: &mut TestAggregate) {
            self.apply(aggregate);
        }
    }

    impl Aggregate for TestAggregate {
        type Event = TestEvent;
        type Id = TestId;
        type Error = TestError;
        type State = TestState;

        fn new(id: Self::Id) -> Self {
            Self {
                id,
                state: TestState::default(),
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

        fn push_pending_event(&mut self, event: Self::Event) {
            self.pending_events.push(event);
        }

        fn increment_version(&mut self) {
            self.version = self.version.next();
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

        fn aggregate_type() -> &'static str {
            "TestAggregate"
        }
    }

    impl TestAggregate {
        fn increment(&mut self, amount: i32) -> std::result::Result<(), TestError> {
            self.apply(TestEvent::Incremented { amount })
        }

        fn value(&self) -> i32 {
            self.state.value
        }
    }

    // Test error
    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error("Test error")]
        #[allow(dead_code)]
        TestError,
    }

    impl AggregateError for TestError {}

    // Mock event store for testing
    use async_trait::async_trait;
    use futures::stream;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct MockEventStore {
        streams: Arc<Mutex<HashMap<StreamId, Vec<EventEnvelope>>>>,
    }

    impl MockEventStore {
        fn new() -> Self {
            Self {
                streams: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            stream_id: StreamId,
            events: Vec<EventEnvelope>,
            _expected_version: Version,
        ) -> Result<()> {
            let mut streams = self.streams.lock().unwrap();
            streams.entry(stream_id).or_default().extend(events);
            Ok(())
        }

        async fn load_stream(
            &self,
            stream_id: StreamId,
            _from_version: Version,
        ) -> Result<impl futures::Stream<Item = Result<EventEnvelope>> + Send> {
            let streams = self.streams.lock().unwrap();
            let events = streams
                .get(&stream_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(Ok)
                .collect::<Vec<_>>();
            Ok(stream::iter(events))
        }

        async fn stream_all(
            &self,
            _from_position: crate::Position,
        ) -> Result<impl futures::Stream<Item = Result<EventEnvelope>> + Send> {
            let streams = self.streams.lock().unwrap();
            let events = streams
                .values()
                .flat_map(|v| v.iter())
                .cloned()
                .map(Ok)
                .collect::<Vec<_>>();
            Ok(stream::iter(events))
        }

        async fn get_version(&self, stream_id: StreamId) -> Result<Version> {
            let streams = self.streams.lock().unwrap();
            let count = streams.get(&stream_id).map_or(0, std::vec::Vec::len);
            Ok(Version::new(count as u64))
        }

        async fn load_snapshot(&self, _stream_id: StreamId) -> Result<Option<crate::Snapshot>> {
            Ok(None)
        }

        async fn save_snapshot(&self, _snapshot: crate::Snapshot) -> Result<()> {
            Ok(())
        }

        fn snapshot_config(&self) -> &crate::SnapshotConfig {
            // Use a static config instance
            static CONFIG: std::sync::OnceLock<crate::SnapshotConfig> = std::sync::OnceLock::new();
            CONFIG.get_or_init(crate::SnapshotConfig::disabled)
        }
    }

    #[tokio::test]
    async fn test_repository_new() {
        let store = Arc::new(MockEventStore::new());
        let _repo = Repository::<MockEventStore, TestAggregate>::new(store);
    }

    #[tokio::test]
    async fn test_repository_save_and_load() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestAggregate>::new(store);

        let test_id = TestId::new();
        let mut aggregate = TestAggregate::new(test_id);
        aggregate.increment(5).unwrap();

        // Save aggregate
        repo.save(&mut aggregate).await.unwrap();

        // Load aggregate
        let loaded = repo.load(test_id).await.unwrap();
        assert_eq!(loaded.value(), 5);
    }

    #[tokio::test]
    async fn test_repository_exists() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestAggregate>::new(store);

        let test_id = TestId::new();

        // Should not exist initially
        assert!(!repo.exists(test_id).await.unwrap());

        // Create and save aggregate
        let mut aggregate = TestAggregate::new(test_id);
        aggregate.increment(5).unwrap();
        repo.save(&mut aggregate).await.unwrap();

        // Should exist now
        assert!(repo.exists(test_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_repository_get_version() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestAggregate>::new(store);

        let test_id = TestId::new();
        let mut aggregate = TestAggregate::new(test_id);
        aggregate.increment(5).unwrap();
        aggregate.increment(3).unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let version = repo.get_version(test_id).await.unwrap();
        assert_eq!(version.as_u64(), 2);
    }

    #[tokio::test]
    async fn test_repository_count_events() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestAggregate>::new(store);

        let test_id = TestId::new();
        let mut aggregate = TestAggregate::new(test_id);
        aggregate.increment(5).unwrap();
        aggregate.increment(3).unwrap();
        aggregate.increment(2).unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let count = repo.count_events(test_id).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_repository_clone() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestAggregate>::new(store);
        let _cloned = repo.clone();
    }
}
