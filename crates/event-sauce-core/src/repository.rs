//! Repository pattern for high-level aggregate persistence.
//!
//! [`Repository`] is the persistence-style-agnostic trait that application
//! code should depend on: it exposes aggregate lifecycle operations
//! (`load`, `save`, `modify`, `create_*`, …) without committing to *how*
//! aggregates are persisted.
//!
//! [`EventSourcedRepository`] is its event-store-backed implementation:
//! `load` replays the aggregate's event stream (optionally from a snapshot)
//! and `save` appends the pending events with optimistic concurrency control.

#[cfg(feature = "event-sourcing")]
use std::marker::PhantomData;
#[cfg(feature = "event-sourcing")]
use std::sync::Arc;

use async_trait::async_trait;

#[cfg(feature = "event-sourcing")]
use crate::{
    event_store::{count_events, load, load_any, load_deleted},
    EventStore, StreamId,
};
use crate::{
    Aggregate, AggregateRoot, AggregateVersion, DefaultEntity, DeletedAggregateRoot, EntityId,
    EntityIdFor, InitEvent, Loaded, Result, UninitAggregateRoot,
};

/// Persistence-style-agnostic aggregate persistence.
///
/// Application code written against this trait runs unchanged whether
/// aggregates are event-sourced or state-stored — the persistence style is
/// chosen once, at the composition root, by constructing the matching
/// implementation (e.g. [`EventSourcedRepository`]).
///
/// The `load`/`save` family are the per-implementation primitives; `modify`,
/// `modify_deleted`, and the `create_*` constructors are generic and provided
/// as default methods.
///
/// # Examples
///
/// ```ignore
/// async fn rename_user<R: Repository<User>>(repo: &R, id: EntityId, name: String) -> Result<()> {
///     repo.modify(id, |user| user.rename(name)).await?;
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait Repository<A: Aggregate>: Send + Sync {
    /// Loads an active aggregate.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the aggregate doesn't exist, or an error
    /// if the store operation or deserialization fails.
    async fn load_by_id(&self, id: EntityId) -> Result<AggregateRoot<A>>;

    /// Loads an aggregate, returning its lifecycle state.
    ///
    /// Returns `Loaded::Active` for active aggregates or `Loaded::Deleted`
    /// for deleted ones.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the aggregate doesn't exist or deserialization fails.
    async fn load_any_by_id(&self, id: EntityId) -> Result<Loaded<A>>;

    /// Loads a deleted aggregate.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidState` if the aggregate is still active, or an
    /// error if the aggregate doesn't exist or deserialization fails.
    async fn load_deleted_by_id(&self, id: EntityId) -> Result<DeletedAggregateRoot<A>>;

    /// Persists an aggregate root's pending changes and clears them on success.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, the store operation, or concurrency
    /// control fails.
    async fn save(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>;

    /// Persists several aggregate roots as one logical write.
    ///
    /// On backends with real transactions (e.g. `PostgreSQL`) the whole set
    /// commits atomically: all aggregates are saved or none are. The in-memory
    /// backends apply them per-aggregate (not atomic). Aggregates with no
    /// pending changes are skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, the store operation, or concurrency
    /// control fails. On atomic backends a failure rolls the whole batch back.
    async fn save_all(&self, aggregates: &mut [&mut AggregateRoot<A>]) -> Result<()>;

    /// Persists a deleted aggregate root's pending changes and clears them on success.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, the store operation, or concurrency
    /// control fails.
    async fn save_deleted(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>;

    /// Checks if an aggregate exists.
    ///
    /// The default answers from [`load_any_by_id`](Self::load_any_by_id), so a
    /// deleted aggregate still exists. Override it when the store can answer without
    /// materializing the aggregate — reading a whole entity to return a `bool` is
    /// worth avoiding on a hot path.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    async fn exists_by_id(&self, id: EntityId) -> Result<bool> {
        match self.load_any_by_id(id).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Gets the current version of an aggregate.
    ///
    /// The default reads it off [`load_any_by_id`](Self::load_any_by_id). Override it
    /// when the store can answer without materializing the aggregate — a version is
    /// one column, and loading a whole entity for it is worth avoiding.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    async fn version_by_id(&self, id: EntityId) -> Result<AggregateVersion> {
        Ok(self.load_any_by_id(id).await?.version())
    }

    /// Loads an aggregate, applies a closure that mutates it, and saves the result.
    ///
    /// The most common command-handler pattern condensed into a single call. The
    /// closure receives the loaded aggregate and may apply commands or events.
    ///
    /// Returns `Result<R, ModifyError<A::Error>>`, which distinguishes three
    /// failure modes:
    ///
    /// - [`ModifyError::Load`](crate::ModifyError::Load) — the store failed
    ///   while loading the aggregate.
    /// - [`ModifyError::Domain`](crate::ModifyError::Domain) — the closure returned
    ///   an aggregate-defined error `A::Error`.
    /// - [`ModifyError::Save`](crate::ModifyError::Save) — the store failed
    ///   while persisting the changes.
    ///
    /// Callers that want typed-error handling can match on the variants directly.
    /// Callers whose functions return `Result<_, Error>` can still use `?` thanks
    /// to the `From<ModifyError<E>> for Error` blanket, which flattens the error
    /// transparently.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns `ModifyError::Load` if loading fails, `ModifyError::Domain` if the
    /// closure fails, or `ModifyError::Save` if saving fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Typed matching:
    /// match repo.modify(order_id, |order| order.add_item("laptop".into(), 1, 120_000)).await {
    ///     Ok(()) => {}
    ///     Err(ModifyError::Domain(OrderError::InsufficientStock)) => { /* handle */ }
    ///     Err(e) => return Err(e.into()),
    /// }
    ///
    /// // Ergonomic ? in Result<_, Error> functions:
    /// let value = repo.modify(order_id, |order| {
    ///     order.add_item("laptop".into(), 1, 120_000)?;
    ///     Ok(order.total())
    /// }).await?;
    /// ```
    async fn modify<I, F, R>(
        &self,
        id: I,
        f: F,
    ) -> std::result::Result<R, crate::ModifyError<A::Error>>
    where
        I: EntityIdFor<A> + Send,
        Self: Sized,
        F: FnOnce(&mut AggregateRoot<A>) -> std::result::Result<R, A::Error> + Send,
        R: Send,
    {
        let mut aggregate = self.load(id).await.map_err(crate::ModifyError::Load)?;
        let result = f(&mut aggregate).map_err(crate::ModifyError::Domain)?;
        self.save(&mut aggregate)
            .await
            .map_err(crate::ModifyError::Save)?;
        Ok(result)
    }

    /// Loads a deleted aggregate, applies a closure, and saves any new pending changes.
    ///
    /// Mirrors [`modify`](Self::modify) for the deleted-state lifecycle.
    /// Note: deleted aggregates cannot accept further events through `apply()` —
    /// the closure is mostly useful for inspecting state or appending
    /// metadata-only events.
    ///
    /// Returns `Result<R, ModifyError<A::Error>>` with the same failure-mode
    /// split as [`modify`](Self::modify); `ModifyError::Load` also covers
    /// `Error::InvalidState` when the aggregate is still active.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns `ModifyError::Load` if the aggregate is not deleted or cannot be
    /// loaded, `ModifyError::Domain` if the closure fails, or `ModifyError::Save`
    /// if saving fails.
    async fn modify_deleted<I, F, R>(
        &self,
        id: I,
        f: F,
    ) -> std::result::Result<R, crate::ModifyError<A::Error>>
    where
        I: EntityIdFor<A> + Send,
        Self: Sized,
        F: FnOnce(&mut DeletedAggregateRoot<A>) -> std::result::Result<R, A::Error> + Send,
        R: Send,
    {
        let mut deleted = self
            .load_deleted(id)
            .await
            .map_err(crate::ModifyError::Load)?;
        let result = f(&mut deleted).map_err(crate::ModifyError::Domain)?;
        self.save_deleted(&mut deleted)
            .await
            .map_err(crate::ModifyError::Save)?;
        Ok(result)
    }

    /// Creates an uninitialized aggregate root with a random `EntityId`.
    ///
    /// Use with aggregates that require init events.
    #[must_use]
    fn create_uninit(&self) -> UninitAggregateRoot<A> {
        UninitAggregateRoot::new(EntityId::new())
    }

    /// Creates an uninitialized aggregate root with the given `EntityId`.
    ///
    /// Use with aggregates that require init events.
    #[must_use]
    fn create_uninit_with_id(&self, id: EntityId) -> UninitAggregateRoot<A> {
        UninitAggregateRoot::new(id)
    }

    /// Creates and immediately initializes an aggregate with an init event.
    ///
    /// Convenience for `create_uninit()` + `apply_init()` in one step.
    ///
    /// # Errors
    ///
    /// Returns an error if init event validation fails.
    fn create_with<E: InitEvent<A> + Into<A::Event>>(
        &self,
        event: E,
    ) -> std::result::Result<AggregateRoot<A>, A::Error>
    where
        Self: Sized,
    {
        UninitAggregateRoot::new(EntityId::new()).apply_init(event)
    }

    /// Creates and immediately initializes an aggregate with a given ID and init event.
    ///
    /// # Errors
    ///
    /// Returns an error if init event validation fails.
    fn create_with_id_and<E: InitEvent<A> + Into<A::Event>>(
        &self,
        id: EntityId,
        event: E,
    ) -> std::result::Result<AggregateRoot<A>, A::Error>
    where
        Self: Sized,
    {
        UninitAggregateRoot::new(id).apply_init(event)
    }

    /// Creates a new aggregate root with a random `EntityId`.
    ///
    /// Requires `DefaultEntity`. For init-event aggregates, use
    /// [`create_uninit()`](Self::create_uninit) or [`create_with()`](Self::create_with).
    #[must_use]
    fn create(&self) -> AggregateRoot<A>
    where
        A: DefaultEntity,
    {
        AggregateRoot::new(EntityId::new())
    }

    /// Creates a new aggregate root with the given `EntityId`.
    ///
    /// Requires `DefaultEntity`. For init-event aggregates, use
    /// [`create_uninit_with_id()`](Self::create_uninit_with_id) or
    /// [`create_with_id_and()`](Self::create_with_id_and).
    #[must_use]
    fn create_with_id(&self, id: EntityId) -> AggregateRoot<A>
    where
        A: DefaultEntity,
    {
        AggregateRoot::new(id)
    }

    /// Loads an active aggregate by its typed id.
    ///
    /// # Errors
    ///
    /// What [`Repository::load_by_id`] gives.
    async fn load<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<AggregateRoot<A>>
    where
        Self: Sized,
    {
        self.load_by_id(id.entity_id()).await
    }

    /// Loads an aggregate by its typed id, returning its lifecycle state.
    ///
    /// # Errors
    ///
    /// What [`Repository::load_any_by_id`] gives.
    async fn load_any<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<Loaded<A>>
    where
        Self: Sized,
    {
        self.load_any_by_id(id.entity_id()).await
    }

    /// Loads a deleted aggregate by its typed id.
    ///
    /// # Errors
    ///
    /// What [`Repository::load_deleted_by_id`] gives.
    async fn load_deleted<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<DeletedAggregateRoot<A>>
    where
        Self: Sized,
    {
        self.load_deleted_by_id(id.entity_id()).await
    }

    /// Whether an aggregate with this typed id exists.
    ///
    /// # Errors
    ///
    /// What [`Repository::exists_by_id`] gives.
    async fn exists<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<bool>
    where
        Self: Sized,
    {
        self.exists_by_id(id.entity_id()).await
    }

    /// The stored version of the aggregate with this typed id.
    ///
    /// # Errors
    ///
    /// What [`Repository::version_by_id`] gives.
    async fn get_version<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<AggregateVersion>
    where
        Self: Sized,
    {
        self.version_by_id(id.entity_id()).await
    }
}

/// Event-store-backed [`Repository`] implementation.
///
/// Wraps an `EventStore` and provides type-safe operations for a specific
/// aggregate type: `load` reconstructs the aggregate by replaying its events
/// (optionally from a snapshot), `save` appends the pending events with
/// optimistic concurrency control.
///
/// # Type Parameters
///
/// - `S`: The event store implementation
/// - `A`: The aggregate type (entity)
///
/// # Examples
///
/// ```ignore
/// let repo = store.repository::<User>();
///
/// // Create a new aggregate
/// let mut user = repo.create();
/// user.apply(UserCreatedEvent { name: "Alice".into(), timestamp: Utc::now() })?;
/// repo.save(&mut user).await?;
///
/// // Load an existing aggregate
/// let loaded = repo.load(user.entity_id()).await?;
/// ```
#[cfg(feature = "event-sourcing")]
#[derive(Debug)]
pub struct EventSourcedRepository<S, A> {
    store: Arc<S>,
    _phantom: PhantomData<A>,
}

#[cfg(feature = "event-sourcing")]
impl<S, A> EventSourcedRepository<S, A>
where
    S: EventStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
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

    /// Counts the number of events for an aggregate.
    ///
    /// Event-sourcing-specific: only meaningful when aggregates are persisted
    /// as event streams, so this lives on the concrete repository rather than
    /// the [`Repository`] trait.
    ///
    /// Accepts both raw `EntityId` and typed IDs implementing `EntityIdFor<A>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the event store operation fails.
    pub async fn count_events(&self, id: impl EntityIdFor<A>) -> Result<usize> {
        let stream_id = StreamId::new(A::aggregate_type(), id.entity_id().as_uuid());
        count_events(&*self.store, stream_id).await
    }
}

#[cfg(feature = "event-sourcing")]
#[async_trait]
impl<S, A> Repository<A> for EventSourcedRepository<S, A>
where
    S: EventStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn load_by_id(&self, id: EntityId) -> Result<AggregateRoot<A>> {
        load(&*self.store, id).await
    }

    async fn load_any_by_id(&self, id: EntityId) -> Result<Loaded<A>> {
        load_any(&*self.store, id).await
    }

    async fn load_deleted_by_id(&self, id: EntityId) -> Result<DeletedAggregateRoot<A>> {
        load_deleted(&*self.store, id).await
    }

    async fn save(&self, aggregate: &mut AggregateRoot<A>) -> Result<()> {
        self.store.commit(aggregate).await
    }

    async fn save_all(&self, aggregates: &mut [&mut AggregateRoot<A>]) -> Result<()> {
        let mut prepared = Vec::with_capacity(aggregates.len());
        let mut dirty = Vec::with_capacity(aggregates.len());
        for (index, aggregate) in aggregates.iter_mut().enumerate() {
            if let Some(commit) =
                crate::event_store::prepare_commit(&*self.store, &**aggregate).await?
            {
                prepared.push(commit);
                dirty.push(index);
            }
        }
        crate::event_store::flush_prepared_batch(&*self.store, prepared).await?;
        for index in dirty {
            aggregates[index].clear_pending_events();
        }
        Ok(())
    }

    async fn save_deleted(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()> {
        self.store.commit_deleted(aggregate).await
    }

    async fn exists_by_id(&self, id: EntityId) -> Result<bool> {
        let stream_id = StreamId::new(A::aggregate_type(), id.as_uuid());
        self.store.stream_exists(stream_id).await
    }

    async fn version_by_id(&self, id: EntityId) -> Result<AggregateVersion> {
        let stream_id = StreamId::new(A::aggregate_type(), id.as_uuid());
        self.store.get_version(stream_id).await
    }
}

#[cfg(feature = "event-sourcing")]
impl<S, A> Clone for EventSourcedRepository<S, A> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            _phantom: PhantomData,
        }
    }
}

#[cfg(all(test, feature = "event-sourcing"))]
mod tests {
    use super::*;
    use crate::test_fixtures::{
        MockEventStore, SimpleTestDelete, SimpleTestEntity, SimpleTestEvent, SimpleTestInit,
    };
    use crate::EntityId;

    #[tokio::test]
    async fn test_repository_create_uninit_and_with_id() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let uninit = repo.create_uninit();
        let generated_id = uninit.entity_id();

        let id = EntityId::new();
        let uninit_with_id = repo.create_uninit_with_id(id);
        assert_eq!(uninit_with_id.entity_id(), id);
        assert_ne!(generated_id, id);
    }

    #[tokio::test]
    async fn test_repository_create_with_init_event() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let mut aggregate = repo.create_with(SimpleTestInit { value: 7 }).unwrap();
        assert_eq!(aggregate.value, 7);

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(aggregate.entity_id()).await.unwrap();
        assert_eq!(loaded.value, 7);
    }

    #[tokio::test]
    async fn test_repository_create_with_id_and_init_event() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let aggregate = repo
            .create_with_id_and(id, SimpleTestInit { value: 3 })
            .unwrap();
        assert_eq!(aggregate.entity_id(), id);
        assert_eq!(aggregate.value, 3);
    }

    #[tokio::test]
    async fn test_repository_modify_deleted_roundtrip() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 12 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let aggregate = repo.load(id).await.unwrap();
        let mut deleted = aggregate.apply_delete(SimpleTestDelete).unwrap();
        repo.save_deleted(&mut deleted).await.unwrap();

        let value = repo
            .modify_deleted(id, |deleted| Ok(deleted.state().value))
            .await
            .unwrap();
        assert_eq!(value, 12);
    }

    #[tokio::test]
    async fn test_repository_new() {
        let store = Arc::new(MockEventStore::new());
        let _repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);
    }

    #[tokio::test]
    async fn test_repository_save_and_load() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);

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
    async fn test_generic_code_runs_against_repository_trait() {
        // The acceptance criterion of the state-store split: application code
        // written against `R: Repository<A>` must be persistence-agnostic.
        async fn set_value<R: Repository<SimpleTestEntity>>(
            repo: &R,
            id: EntityId,
            value: i32,
        ) -> Result<i32> {
            let mut aggregate = repo.create_with_id(id);
            aggregate.apply(SimpleTestEvent::Created { value })?;
            repo.save(&mut aggregate).await?;
            let loaded = repo.load(id).await?;
            Ok(loaded.value)
        }

        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        assert_eq!(set_value(&repo, id, 41).await.unwrap(), 41);
    }

    #[tokio::test]
    async fn test_repository_exists() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);

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
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);

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
        assert_eq!(version.as_i64(), 2);
    }

    #[tokio::test]
    async fn test_repository_count_events() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);

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

    #[tokio::test]
    async fn test_repository_from_store_convenience() {
        let store = Arc::new(MockEventStore::new());
        // Use the convenience method instead of EventSourcedRepository::<S, A>::new()
        let repo = store.repository::<SimpleTestEntity>();

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 7 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 7);
    }

    #[tokio::test]
    async fn test_repository_create() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let mut aggregate = repo.create();
        aggregate
            .apply(SimpleTestEvent::Created { value: 99 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(aggregate.entity_id()).await.unwrap();
        assert_eq!(loaded.value, 99);
    }

    #[tokio::test]
    async fn test_repository_create_with_id() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        assert_eq!(aggregate.entity_id(), id);

        aggregate
            .apply(SimpleTestEvent::Created { value: 42 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(id).await.unwrap();
        assert_eq!(loaded.value, 42);
    }

    #[tokio::test]
    async fn test_repository_clone_shares_arc_store() {
        let store = Arc::new(MockEventStore::new());
        let repo = EventSourcedRepository::<MockEventStore, SimpleTestEntity>::new(store);
        let repo_clone = repo.clone();

        let test_id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 42 })
            .unwrap();

        // Save through the original repository
        repo.save(&mut aggregate).await.unwrap();

        // Load through the cloned repository — should see the same data
        let loaded = repo_clone.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 42);
    }

    #[tokio::test]
    async fn test_repository_modify_loads_mutates_and_saves() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let returned = repo
            .modify(id, |agg| {
                agg.apply(SimpleTestEvent::Updated { value: 99 })?;
                Ok(agg.value)
            })
            .await
            .unwrap();

        assert_eq!(returned, 99);
        let reloaded = repo.load(id).await.unwrap();
        assert_eq!(reloaded.value, 99);
    }

    #[tokio::test]
    async fn test_repository_modify_propagates_aggregate_error() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let result = repo
            .modify(id, |_agg| -> std::result::Result<(), _> {
                Err(crate::test_fixtures::SimpleTestError)
            })
            .await;

        assert!(result.is_err());
        let reloaded = repo.load(id).await.unwrap();
        assert_eq!(reloaded.value, 1, "aggregate not mutated on closure error");
    }

    #[tokio::test]
    async fn test_repository_modify_domain_error_is_typed() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let result = repo
            .modify(id, |_agg| -> std::result::Result<(), _> {
                Err(crate::test_fixtures::SimpleTestError)
            })
            .await;

        // The error should be the Domain variant, not Load or Save.
        assert!(result.as_ref().unwrap_err().is_domain());
        assert!(matches!(
            result.unwrap_err(),
            crate::ModifyError::Domain(crate::test_fixtures::SimpleTestError)
        ));
    }

    #[tokio::test]
    async fn test_repository_modify_domain_error_flattens_via_from() {
        // Verify backward-compat: ? in a function returning Result<_, Error> still works.
        // The inner helper is declared before any statements to satisfy clippy::items_after_statements.
        async fn try_modify(
            repo: &crate::EventSourcedRepository<MockEventStore, SimpleTestEntity>,
            id: EntityId,
        ) -> crate::Result<()> {
            repo.modify(id, |_agg| -> std::result::Result<(), _> {
                Err(crate::test_fixtures::SimpleTestError)
            })
            .await?;
            Ok(())
        }

        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let err = try_modify(&repo, id).await.unwrap_err();
        assert!(matches!(err, crate::Error::InvalidState(_)));
    }

    #[tokio::test]
    async fn test_repository_load_any_returns_active() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 10 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let loaded = repo.load_any(id).await.unwrap();
        assert!(loaded.is_active());
    }

    #[tokio::test]
    async fn test_repository_load_deleted_errors_for_active() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 10 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let result = repo.load_deleted(id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_repository_save_deleted() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let entity = SimpleTestEntity { id, value: 42 };
        let pending = vec![crate::aggregate_root::PendingEvent {
            event: SimpleTestEvent::Updated { value: 0 },
            actor_id: None,
            metadata: None,
        }];
        let mut deleted = DeletedAggregateRoot::from_delete_with_pending(
            entity,
            id,
            AggregateVersion::new(1),
            pending,
            false,
        );

        repo.save_deleted(&mut deleted).await.unwrap();
        assert!(deleted.pending_events().is_empty());
    }

    /// A store whose `append` fails once every stream has been attempted
    /// `succeed_first` times, for testing that a batch conflict does not
    /// silently clear aggregates that were never actually persisted.
    struct FailingBatchStore {
        succeed_first: usize,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl EventStore for FailingBatchStore {
        async fn append(
            &self,
            _stream_id: StreamId,
            _events: Vec<crate::EventEnvelope>,
            expected_version: AggregateVersion,
            _claims: Vec<crate::AggregateClaim>,
            _clear_claims: bool,
        ) -> Result<()> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call >= self.succeed_first {
                return Err(crate::Error::concurrency_conflict(
                    expected_version,
                    AggregateVersion::new(expected_version.as_i64() + 1),
                ));
            }
            Ok(())
        }

        async fn load_stream(
            &self,
            _stream_id: StreamId,
            _from_version: AggregateVersion,
        ) -> Result<impl futures::Stream<Item = Result<crate::EventEnvelope>> + Send> {
            Ok(futures::stream::empty())
        }

        async fn stream_all(
            &self,
            _from_position: crate::Position,
        ) -> Result<impl futures::Stream<Item = Result<crate::EventLogEntry>> + Send> {
            Ok(futures::stream::empty())
        }

        async fn get_version(&self, _stream_id: StreamId) -> Result<AggregateVersion> {
            Ok(AggregateVersion::initial())
        }
    }

    #[tokio::test]
    async fn save_all_skips_aggregates_with_no_pending_events() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let mut dirty = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        dirty.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        let mut clean = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());

        repo.save_all(&mut [&mut dirty, &mut clean]).await.unwrap();

        assert!(dirty.pending_events().is_empty());
        assert!(clean.pending_events().is_empty());
        assert_eq!(
            store.get_events().len(),
            1,
            "a clean aggregate must not be persisted by save_all"
        );
    }

    #[tokio::test]
    async fn save_all_keeps_every_pending_event_when_the_batch_fails() {
        let store = Arc::new(FailingBatchStore {
            succeed_first: 1,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let repo = EventSourcedRepository::<FailingBatchStore, SimpleTestEntity>::new(store);

        let mut first = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        first.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        let mut second = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        second.apply(SimpleTestEvent::Created { value: 2 }).unwrap();

        let result = repo.save_all(&mut [&mut first, &mut second]).await;

        assert!(result.is_err());
        assert_eq!(
            first.pending_events().len(),
            1,
            "the whole batch failed: the first aggregate's own append \
             succeeded but the batch as a whole must not be treated as saved"
        );
        assert_eq!(
            second.pending_events().len(),
            1,
            "a batch conflict must not clear an aggregate that was never persisted"
        );
    }

    #[tokio::test]
    async fn save_all_clears_pending_events_once_the_batch_succeeds() {
        let store = Arc::new(FailingBatchStore {
            succeed_first: usize::MAX,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let repo = EventSourcedRepository::<FailingBatchStore, SimpleTestEntity>::new(store);

        let mut first = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        first.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        let mut second = AggregateRoot::<SimpleTestEntity>::new(EntityId::new());
        second.apply(SimpleTestEvent::Created { value: 2 }).unwrap();

        repo.save_all(&mut [&mut first, &mut second]).await.unwrap();

        assert!(first.pending_events().is_empty());
        assert!(second.pending_events().is_empty());
    }
}
