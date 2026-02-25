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
    use crate::test_fixtures::{MockEventStore, SimpleTestEntity, SimpleTestEvent};
    use crate::EntityId;

    #[tokio::test]
    async fn test_repository_new() {
        let store = Arc::new(MockEventStore::new());
        let _repo = Repository::<MockEventStore, SimpleTestEntity>::new(store);
    }

    #[tokio::test]
    async fn test_repository_save_and_load() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, SimpleTestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let loaded = repo.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 5);
    }

    #[tokio::test]
    async fn test_repository_exists() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, SimpleTestEntity>::new(store);

        let test_id = EntityId::new();

        assert!(!repo.exists(test_id).await.unwrap());

        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        assert!(repo.exists(test_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_repository_get_version() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, SimpleTestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();
        aggregate
            .apply(SimpleTestEvent::Updated { value: 8 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let version = repo.get_version(test_id).await.unwrap();
        assert_eq!(version.as_u64(), 2);
    }

    #[tokio::test]
    async fn test_repository_count_events() {
        let store = Arc::new(MockEventStore::new());
        let repo = Repository::<MockEventStore, SimpleTestEntity>::new(store);

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();
        aggregate
            .apply(SimpleTestEvent::Updated { value: 8 })
            .unwrap();
        aggregate
            .apply(SimpleTestEvent::Updated { value: 10 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();

        let count = repo.count_events(test_id).await.unwrap();
        assert_eq!(count, 3);
    }
}
