//! Repository pattern for high-level aggregate persistence.
//!
//! Provides a type-safe, domain-focused API over the raw `EventStore` trait.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::{
    count_events, load, Aggregate, AggregateRoot, EntityId, EventStore, Result, StreamId, Version,
};

/// Repository provides a high-level API for aggregate persistence.
///
/// Wraps an `EventStore` and provides type-safe operations for a specific
/// aggregate type, working with `AggregateRoot<A>` and `EntityId`.
///
/// # Type Parameters
///
/// - `S`: The event store implementation
/// - `A`: The aggregate type (entity)
///
/// # Examples
///
/// ```ignore
/// use event_sauce_core::Repository;
///
/// let repo = Repository::<PostgresEventStore, User>::new(store);
///
/// let user = repo.load(entity_id).await?;
/// repo.save(&mut user).await?;
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
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    /// Saves an aggregate root to the event store.
    ///
    /// Commits all pending events and clears them on success.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, the event store operation,
    /// or concurrency control fails.
    pub async fn save(&self, aggregate: &mut AggregateRoot<A>) -> Result<()> {
        self.store.commit(aggregate).await
    }

    /// Loads an aggregate from the event store.
    ///
    /// Reconstructs the aggregate by replaying all its events,
    /// potentially using a snapshot for optimization.
    ///
    /// # Errors
    ///
    /// Returns an error if the aggregate doesn't exist or deserialization fails.
    pub async fn load(&self, id: EntityId) -> Result<AggregateRoot<A>> {
        load(&*self.store, id).await
    }

    /// Checks if an aggregate exists in the event store.
    ///
    /// # Errors
    ///
    /// Returns an error if the event store operation fails.
    pub async fn exists(&self, id: EntityId) -> Result<bool> {
        let stream_id = StreamId::new(A::aggregate_type(), id.as_uuid());
        self.store.stream_exists(stream_id).await
    }

    /// Gets the current version of an aggregate without loading it.
    ///
    /// # Errors
    ///
    /// Returns an error if the event store operation fails.
    pub async fn get_version(&self, id: EntityId) -> Result<Version> {
        let stream_id = StreamId::new(A::aggregate_type(), id.as_uuid());
        self.store.get_version(stream_id).await
    }

    /// Counts the number of events for an aggregate.
    ///
    /// # Errors
    ///
    /// Returns an error if the event store operation fails.
    pub async fn count_events(&self, id: EntityId) -> Result<usize> {
        let stream_id = StreamId::new(A::aggregate_type(), id.as_uuid());
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
    use crate::{AggregateError, ApplyEvent, DomainEvent, EntityId, EventEnvelope, Version};
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    // Test events
    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum TestEvent {
        Incremented { amount: i32 },
        Reset,
    }

    impl DomainEvent for TestEvent {
        type Aggregate = TestEntity;

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

    impl ApplyEvent<TestEntity> for TestEvent {
        fn apply(&self, entity: &mut TestEntity) {
            match self {
                Self::Incremented { amount } => entity.value += amount,
                Self::Reset => entity.value = 0,
            }
        }
    }

    // Test entity
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEntity {
        id: EntityId,
        value: i32,
    }

    impl crate::Entity for TestEntity {
        fn new(id: EntityId) -> Self {
            Self { id, value: 0 }
        }

        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::EventApplicator<TestEntity> for TestEvent {
        fn dispatch(&self, entity: &mut TestEntity) -> std::result::Result<(), TestError> {
            self.apply(entity);
            Ok(())
        }

        fn dispatch_unchecked(&self, entity: &mut TestEntity) {
            self.apply(entity);
        }
    }

    impl Aggregate for TestEntity {
        type Event = TestEvent;
        type Error = TestError;

        fn aggregate_type() -> &'static str {
            "TestEntity"
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

    // Mock event store
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
            static CONFIG: std::sync::OnceLock<crate::SnapshotConfig> = std::sync::OnceLock::new();
            CONFIG.get_or_init(crate::SnapshotConfig::disabled)
        }
    }

    #[tokio::test]
    async fn test_repository_new() {
        let store = Arc::new(MockEventStore::new());
        let _repo = Repository::<MockEventStore, TestEntity>::new(store);
    }

    #[tokio::test]
    async fn test_repository_save_and_load() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<TestEntity>::new(test_id);
        aggregate
            .apply(TestEvent::Incremented { amount: 5 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let loaded = repo.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 5);
    }

    #[tokio::test]
    async fn test_repository_exists() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestEntity>::new(store);

        let test_id = EntityId::new();

        assert!(!repo.exists(test_id).await.unwrap());

        let mut aggregate = AggregateRoot::<TestEntity>::new(test_id);
        aggregate
            .apply(TestEvent::Incremented { amount: 5 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        assert!(repo.exists(test_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_repository_get_version() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<TestEntity>::new(test_id);
        aggregate
            .apply(TestEvent::Incremented { amount: 5 })
            .unwrap();
        aggregate
            .apply(TestEvent::Incremented { amount: 3 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let version = repo.get_version(test_id).await.unwrap();
        assert_eq!(version.as_u64(), 2);
    }

    #[tokio::test]
    async fn test_repository_count_events() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<TestEntity>::new(test_id);
        aggregate
            .apply(TestEvent::Incremented { amount: 5 })
            .unwrap();
        aggregate
            .apply(TestEvent::Incremented { amount: 3 })
            .unwrap();
        aggregate
            .apply(TestEvent::Incremented { amount: 2 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let count = repo.count_events(test_id).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_repository_clone() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, TestEntity>::new(store);
        let _cloned = repo.clone();
    }
}
