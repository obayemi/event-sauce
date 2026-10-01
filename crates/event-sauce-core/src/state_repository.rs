//! State-store-backed [`Repository`] implementation.

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    aggregate_root::envelopes_from_pending, state_store::StateCommit, Aggregate, AggregateRoot,
    AggregateVersion, DeletedAggregateRoot, DomainEvent, EntityId, Error, Loaded, Repository,
    Result, StateStore, StoredState, StoredVersion, StreamId,
};

/// State-store-backed [`Repository`] implementation.
///
/// Persists each aggregate as a single versioned row of serialized current
/// state: `load` deserializes the row, `save` serializes the entity and writes
/// it with optimistic concurrency control. The pending events still drive
/// every in-memory transition and are handed to the [`StateStore`] inside the
/// [`StateCommit`] (feeding in-transaction projections and the outbox), but
/// they are not stored as a replayable log.
///
/// Application code is identical to the event-sourced style — only the store
/// constructed at the composition root differs:
///
/// ```ignore
/// let repo = state_store.repository::<User>();
/// let mut user = repo.create();
/// user.apply(UserCreatedEvent { name: "Alice".into(), timestamp: Utc::now() })?;
/// repo.save(&mut user).await?;
/// let loaded = repo.load(user.entity_id()).await?;
/// ```
///
/// # Limitations
///
/// Encrypted aggregates ([`Aggregate::is_encrypted`] or events with
/// encrypted fields) are not yet supported and are rejected with
/// `Error::Encryption` — use an event-sourced repository for those.
/// A stored row whose schema version differs from
/// [`Aggregate::snapshot_version()`] fails to load with
/// `Error::InvalidState`: without an event log there is nothing to replay,
/// so state rows must be migrated in place.
#[derive(Debug)]
pub struct StateStoredRepository<S, A> {
    store: Arc<S>,
    _phantom: PhantomData<A>,
}

impl<S, A> StateStoredRepository<S, A>
where
    S: StateStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Creates a new repository wrapping the given state store.
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            _phantom: PhantomData,
        }
    }

    fn reject_encrypted() -> Result<()> {
        if A::is_encrypted() || A::Event::has_any_encrypted_fields() {
            return Err(Error::encryption(
                "state-stored persistence does not yet support encrypted aggregates; \
                 use an event-sourced repository",
            ));
        }
        Ok(())
    }

    fn check_schema_version(state: &StoredState) -> Result<()> {
        if state.schema_version != A::snapshot_version() {
            return Err(Error::invalid_state(format!(
                "stored state for {} {} has schema version {} but the aggregate expects {}; \
                 state rows must be migrated in place (there is no event log to replay)",
                state.aggregate_type,
                state.aggregate_id,
                state.schema_version,
                A::snapshot_version(),
            )));
        }
        Ok(())
    }

    /// Prepares a save for an active or deleted aggregate root: the two
    /// differ only in which state a [`CommitSource`](crate::commit_source::CommitSource)
    /// serializes, whether it carries claims, and whether it is a deletion.
    fn prepare<R: crate::commit_source::CommitSource<A>>(root: &R) -> Result<Option<StateCommit>> {
        Self::reject_encrypted()?;
        root.ensure_not_poisoned("save")?;

        let pending = root.pending_events_with_actors();
        if pending.is_empty() {
            return Ok(None);
        }

        let aggregate_id = root.entity_id().as_uuid();
        let expected_version = root.committed_version();
        let events = envelopes_from_pending::<A::Event>(pending, aggregate_id)?;

        Ok(Some(StateCommit {
            state: StoredState {
                aggregate_id,
                aggregate_type: R::aggregate_type(),
                state_data: root.serialize_state()?,
                version: root.version(),
                is_deleted: R::IS_DELETED,
                schema_version: A::snapshot_version(),
            },
            expected_version,
            events,
            claims: root.claims(),
            clear_claims: R::IS_DELETED,
        }))
    }

    fn stream_id_for(id: EntityId) -> StreamId {
        StreamId::new(A::aggregate_type(), id.as_uuid())
    }
}

#[async_trait]
impl<S, A> Repository<A> for StateStoredRepository<S, A>
where
    S: StateStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn load_by_id(&self, id: EntityId) -> Result<AggregateRoot<A>> {
        self.load_any_by_id(id).await?.into_active()
    }

    async fn load_any_by_id(&self, id: EntityId) -> Result<Loaded<A>> {
        let state = self
            .store
            .load(Self::stream_id_for(id))
            .await?
            .ok_or_else(|| {
                Error::not_found(A::aggregate_type().to_string(), id.as_uuid().to_string())
            })?;
        Self::check_schema_version(&state)?;
        let version = StoredVersion::try_from(state.version)?;

        if state.is_deleted {
            let deleted_state: A::DeletedState = serde_json::from_value(state.state_data)?;
            Ok(Loaded::Deleted(DeletedAggregateRoot::restore(
                deleted_state,
                EntityId::from(state.aggregate_id),
                version,
            )))
        } else {
            let entity: A = serde_json::from_value(state.state_data)?;
            Ok(Loaded::Active(AggregateRoot::restore(version, entity)))
        }
    }

    async fn load_deleted_by_id(&self, id: EntityId) -> Result<DeletedAggregateRoot<A>> {
        self.load_any_by_id(id).await?.into_deleted()
    }

    async fn save(&self, aggregate: &mut AggregateRoot<A>) -> Result<()> {
        if let Some(commit) = Self::prepare(aggregate)? {
            self.store.save(commit).await?;
            aggregate.clear_pending_events();
        }
        Ok(())
    }

    async fn save_all(&self, aggregates: &mut [&mut AggregateRoot<A>]) -> Result<()> {
        let mut commits = Vec::with_capacity(aggregates.len());
        for aggregate in aggregates.iter() {
            if let Some(commit) = Self::prepare(&**aggregate)? {
                commits.push(commit);
            }
        }
        self.store.save_batch(commits).await?;
        for aggregate in aggregates.iter_mut() {
            aggregate.clear_pending_events();
        }
        Ok(())
    }

    async fn save_deleted(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()> {
        if let Some(commit) = Self::prepare(aggregate)? {
            self.store.save(commit).await?;
            aggregate.clear_pending_events();
        }
        Ok(())
    }

    async fn exists_by_id(&self, id: EntityId) -> Result<bool> {
        self.store.exists(Self::stream_id_for(id)).await
    }

    async fn version_by_id(&self, id: EntityId) -> Result<AggregateVersion> {
        self.store.get_version(Self::stream_id_for(id)).await
    }
}

impl<S, A> Clone for StateStoredRepository<S, A> {
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
    use crate::test_fixtures::{
        MockStateStore, SimpleTestDelete, SimpleTestEntity, SimpleTestEvent, SimpleTestInit,
    };

    fn repo() -> (
        Arc<MockStateStore>,
        StateStoredRepository<MockStateStore, SimpleTestEntity>,
    ) {
        let store = Arc::new(MockStateStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        (store, repo)
    }

    #[tokio::test]
    async fn test_generic_code_runs_against_state_repository() {
        // The acceptance criterion of the state-store split: the same generic
        // application code as the event-sourced repository test.
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

        let (_store, repo) = repo();
        let id = EntityId::new();
        assert_eq!(set_value(&repo, id, 41).await.unwrap(), 41);
    }

    #[tokio::test]
    async fn test_save_and_load_roundtrip_tracks_version() {
        let (_store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 5 })
            .unwrap();
        aggregate
            .apply(SimpleTestEvent::Updated { value: 8 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();
        assert!(aggregate.pending_events().is_empty());

        let loaded = repo.load(id).await.unwrap();
        assert_eq!(loaded.value, 8);
        assert_eq!(loaded.version().as_i64(), 2);
        assert_eq!(repo.get_version(id).await.unwrap().as_i64(), 2);
    }

    #[tokio::test]
    async fn test_save_without_pending_events_is_noop() {
        let (store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        repo.save(&mut aggregate).await.unwrap();

        assert!(store.recorded_commits().is_empty());
        assert!(!repo.exists(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_commit_carries_events_claims_and_expected_version() {
        let (store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        aggregate
            .apply(SimpleTestEvent::Updated { value: 2 })
            .unwrap();
        aggregate
            .apply(SimpleTestEvent::Updated { value: 3 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let commits = store.recorded_commits();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[1].expected_version.as_i64(), 1);
        assert_eq!(commits[1].state.version.as_i64(), 3);
        assert_eq!(commits[1].events.len(), 2);
        assert_eq!(commits[1].events[0].event_type, "SimpleTestEntity.Updated");
        assert!(!commits[1].state.is_deleted);
        assert!(!commits[1].clear_claims);
    }

    #[tokio::test]
    async fn test_concurrent_save_conflicts() {
        let (_store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let mut first = repo.load(id).await.unwrap();
        let mut second = repo.load(id).await.unwrap();

        first.apply(SimpleTestEvent::Updated { value: 10 }).unwrap();
        repo.save(&mut first).await.unwrap();

        second
            .apply(SimpleTestEvent::Updated { value: 20 })
            .unwrap();
        let err = repo.save(&mut second).await.unwrap_err();
        assert!(err.is_concurrency_conflict());
    }

    /// Wraps a [`MockStateStore`], failing `save`'s first `failures` calls
    /// with `ConcurrencyConflict` before delegating to the inner store.
    struct FailNTimesStateStore {
        inner: Arc<MockStateStore>,
        remaining_failures: std::sync::Mutex<usize>,
    }

    impl FailNTimesStateStore {
        fn new(inner: Arc<MockStateStore>, failures: usize) -> Self {
            Self {
                inner,
                remaining_failures: std::sync::Mutex::new(failures),
            }
        }
    }

    #[async_trait]
    impl StateStore for FailNTimesStateStore {
        async fn load(&self, stream_id: StreamId) -> Result<Option<StoredState>> {
            self.inner.load(stream_id).await
        }

        async fn save(&self, commit: StateCommit) -> Result<()> {
            let should_fail = {
                let mut remaining = self.remaining_failures.lock().unwrap();
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            };
            if should_fail {
                return Err(Error::concurrency_conflict(
                    commit.expected_version,
                    commit.expected_version,
                ));
            }
            self.inner.save(commit).await
        }
    }

    /// Wraps a [`MockStateStore`] whose `save` never resolves, so a caller
    /// can observe what happens when its save future is polled once and
    /// dropped (a cancelled request) instead of awaited to completion.
    struct HangingStateStore {
        inner: Arc<MockStateStore>,
    }

    #[async_trait]
    impl StateStore for HangingStateStore {
        async fn load(&self, stream_id: StreamId) -> Result<Option<StoredState>> {
            self.inner.load(stream_id).await
        }

        async fn save(&self, _commit: StateCommit) -> Result<()> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn test_save_retries_after_concurrency_conflict_keeps_pending_events() {
        let inner = Arc::new(MockStateStore::new());
        let flaky = Arc::new(FailNTimesStateStore::new(Arc::clone(&inner), 1));
        let repo = flaky.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();

        let err = repo.save(&mut aggregate).await.unwrap_err();
        assert!(err.is_concurrency_conflict());
        assert_eq!(
            aggregate.pending_events().len(),
            1,
            "a failed save must not drop the pending events"
        );

        repo.save(&mut aggregate).await.unwrap();
        assert!(aggregate.pending_events().is_empty());
        assert_eq!(
            inner.recorded_commits().len(),
            1,
            "the retry must record exactly one commit"
        );
    }

    #[tokio::test]
    async fn test_save_deleted_retries_after_concurrency_conflict_keeps_pending_events() {
        let inner = Arc::new(MockStateStore::new());
        let setup_repo = inner.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        let mut aggregate = setup_repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        setup_repo.save(&mut aggregate).await.unwrap();
        let mut deleted = setup_repo
            .load(id)
            .await
            .unwrap()
            .apply_delete(SimpleTestDelete)
            .unwrap();
        let pending_before = deleted.pending_events().len();
        assert!(pending_before > 0);

        let flaky = Arc::new(FailNTimesStateStore::new(Arc::clone(&inner), 1));
        let repo = flaky.repository::<SimpleTestEntity>();

        let err = repo.save_deleted(&mut deleted).await.unwrap_err();
        assert!(err.is_concurrency_conflict());
        assert_eq!(
            deleted.pending_events().len(),
            pending_before,
            "a failed save_deleted must not drop the pending events"
        );

        repo.save_deleted(&mut deleted).await.unwrap();
        assert!(deleted.pending_events().is_empty());
    }

    /// Polls a future exactly once and drops it, without needing an
    /// executor or the (optional, `event-sourcing`-only) `futures` crate —
    /// this test module also builds under `state-store` alone.
    fn poll_once<F: std::future::Future>(fut: F) -> Option<F::Output> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut fut = std::pin::pin!(fut);
        match fut.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(value) => Some(value),
            std::task::Poll::Pending => None,
        }
    }

    #[tokio::test]
    async fn test_save_dropped_future_keeps_pending_events() {
        let inner = Arc::new(MockStateStore::new());
        let hanging = Arc::new(HangingStateStore {
            inner: Arc::clone(&inner),
        });
        let repo = hanging.repository::<SimpleTestEntity>();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        let pending_before = aggregate.pending_events().len();

        assert!(
            poll_once(repo.save(&mut aggregate)).is_none(),
            "the hanging save must not resolve immediately"
        );

        assert_eq!(
            aggregate.pending_events().len(),
            pending_before,
            "a dropped save future must not drop the pending events"
        );
    }

    #[tokio::test]
    async fn test_load_missing_is_not_found() {
        let (_store, repo) = repo();
        let err = repo.load(EntityId::new()).await.unwrap_err();
        assert!(err.is_not_found());
    }

    #[tokio::test]
    async fn test_modify_loads_mutates_and_saves() {
        let (_store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
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
        assert_eq!(repo.load(id).await.unwrap().value, 99);
    }

    #[tokio::test]
    async fn test_delete_lifecycle_roundtrip() {
        let (store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 12 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        let aggregate = repo.load(id).await.unwrap();
        let mut deleted = aggregate.apply_delete(SimpleTestDelete).unwrap();
        repo.save_deleted(&mut deleted).await.unwrap();
        assert!(deleted.pending_events().is_empty());

        let tombstone_commit = store.recorded_commits().pop().unwrap();
        assert!(tombstone_commit.state.is_deleted);
        assert!(tombstone_commit.clear_claims);

        let err = repo.load(id).await.unwrap_err();
        assert!(err.is_aggregate_deleted());
        assert!(repo.load_any(id).await.unwrap().is_deleted());

        let reloaded = repo.load_deleted(id).await.unwrap();
        assert_eq!(reloaded.state().value, 12);

        let value = repo
            .modify_deleted(id, |deleted| Ok(deleted.state().value))
            .await
            .unwrap();
        assert_eq!(value, 12);
    }

    #[tokio::test]
    async fn test_load_deleted_errors_for_active() {
        let (_store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();

        assert!(repo.load_deleted(id).await.is_err());
    }

    #[tokio::test]
    async fn test_save_all_persists_dirty_and_skips_clean() {
        let (store, repo) = repo();

        let dirty_id = EntityId::new();
        let clean_id = EntityId::new();
        let mut dirty = repo.create_with_id(dirty_id);
        dirty.apply(SimpleTestEvent::Created { value: 7 }).unwrap();
        let mut clean = repo.create_with_id(clean_id);

        repo.save_all(&mut [&mut dirty, &mut clean]).await.unwrap();

        assert_eq!(store.recorded_commits().len(), 1);
        assert!(dirty.pending_events().is_empty());
        assert_eq!(repo.load(dirty_id).await.unwrap().value, 7);
        assert!(!repo.exists(clean_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_with_init_event_roundtrip() {
        let (_store, repo) = repo();

        let mut aggregate = repo.create_with(SimpleTestInit { value: 4 }).unwrap();
        repo.save(&mut aggregate).await.unwrap();
        assert_eq!(repo.load(aggregate.entity_id()).await.unwrap().value, 4);
    }

    #[tokio::test]
    async fn test_poisoned_aggregate_is_refused() {
        let (store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        assert!(aggregate.apply(SimpleTestEvent::Rejected).is_err());
        assert!(aggregate.is_poisoned());

        let err = repo.save(&mut aggregate).await.unwrap_err();
        assert!(matches!(err, Error::InvalidState(_)));
        assert!(store.recorded_commits().is_empty());
    }

    #[tokio::test]
    async fn test_poisoned_deleted_aggregate_is_refused() {
        let (store, repo) = repo();

        let id = EntityId::new();
        let mut aggregate = repo.create_with_id(id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        assert!(aggregate.apply(SimpleTestEvent::Rejected).is_err());
        let mut deleted = aggregate.apply_delete(SimpleTestDelete).unwrap();
        assert!(deleted.is_poisoned());

        let err = repo.save_deleted(&mut deleted).await.unwrap_err();
        assert!(matches!(err, Error::InvalidState(_)));
        assert!(store.recorded_commits().is_empty());
    }

    fn stored(
        id: EntityId,
        version: AggregateVersion,
        is_deleted: bool,
        schema_version: u32,
    ) -> StoredState {
        StoredState {
            aggregate_id: id.as_uuid(),
            aggregate_type: SimpleTestEntity::aggregate_type(),
            state_data: serde_json::json!({"id": id.as_uuid(), "value": 1}),
            version,
            is_deleted,
            schema_version,
        }
    }

    #[tokio::test]
    async fn test_schema_version_mismatch_fails_load() {
        let (store, repo) = repo();
        let id = EntityId::new();
        store.insert_raw(stored(
            id,
            AggregateVersion::new(1),
            false,
            SimpleTestEntity::snapshot_version() + 1,
        ));

        let err = repo.load(id).await.unwrap_err();
        assert!(matches!(err, Error::InvalidState(_)));
    }

    #[tokio::test]
    async fn test_corrupt_stored_version_fails_load() {
        for (version, is_deleted) in [
            (AggregateVersion::initial(), false),
            (AggregateVersion::new(-3), true),
        ] {
            let (store, repo) = repo();
            let id = EntityId::new();
            store.insert_raw(stored(
                id,
                version,
                is_deleted,
                SimpleTestEntity::snapshot_version(),
            ));

            let err = repo.load_any(id).await.unwrap_err();
            assert!(matches!(err, Error::InvalidState(_)));
        }
    }

    #[tokio::test]
    async fn test_undeserializable_stored_state_fails_load() {
        for is_deleted in [false, true] {
            let (store, repo) = repo();
            let id = EntityId::new();
            store.insert_raw(StoredState {
                state_data: serde_json::json!({"id": id.as_uuid()}),
                ..stored(
                    id,
                    AggregateVersion::new(1),
                    is_deleted,
                    SimpleTestEntity::snapshot_version(),
                )
            });

            let err = repo.load_any(id).await.unwrap_err();
            assert!(matches!(err, Error::Serialization(_)));
        }
    }

    #[tokio::test]
    async fn test_encrypted_aggregate_is_rejected() {
        use crate::{
            AggregateError, ApplyEvent, DomainEvent, Entity, EventApplicator, EventVersion,
        };
        use serde::{Deserialize, Serialize};

        #[derive(Debug, thiserror::Error)]
        #[error("secret error")]
        struct SecretError;
        impl AggregateError for SecretError {}

        #[derive(Debug, Serialize, Deserialize)]
        struct SecretEntity {
            id: EntityId,
        }
        impl Entity for SecretEntity {
            fn new(id: EntityId) -> Self {
                Self { id }
            }
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }
        impl crate::DefaultEntity for SecretEntity {}

        #[derive(Debug, Clone, Serialize, Deserialize)]
        enum SecretEvent {
            Touched,
        }
        impl DomainEvent for SecretEvent {
            type Aggregate = SecretEntity;
            fn event_type(&self) -> &'static str {
                "SecretEntity.Touched"
            }
            fn event_version(&self) -> EventVersion {
                EventVersion::new(1)
            }
            fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
                chrono::Utc::now()
            }
        }
        impl ApplyEvent<SecretEntity> for SecretEvent {
            fn apply(&self, _entity: &mut SecretEntity) {}
        }
        impl EventApplicator<SecretEntity> for SecretEvent {
            fn dispatch(&self, entity: &mut SecretEntity) -> std::result::Result<(), SecretError> {
                self.apply(entity);
                Ok(())
            }
            fn dispatch_unchecked(&self, entity: &mut SecretEntity) {
                self.apply(entity);
            }
        }
        impl Aggregate for SecretEntity {
            type Event = SecretEvent;
            type Error = SecretError;
            type DeletedState = Self;
            fn is_encrypted() -> bool {
                true
            }
        }

        let store = Arc::new(MockStateStore::new());
        let repo = store.repository::<SecretEntity>();

        let mut aggregate = repo.create();
        aggregate.apply(SecretEvent::Touched).unwrap();
        let err = repo.save(&mut aggregate).await.unwrap_err();
        assert!(err.is_encryption());
    }
}
