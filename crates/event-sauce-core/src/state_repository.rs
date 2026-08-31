//! State-store-backed [`Repository`] implementation.

use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    aggregate_root::envelopes_from_pending, state_store::StateCommit, Aggregate, AggregateRoot,
    AggregateVersion, DeletedAggregateRoot, DomainEvent, EntityId, Error, Loaded,
    Repository, Result, StateStore, StoredState, StreamId,
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

    fn check_not_poisoned(poisoned: bool) -> Result<()> {
        if poisoned {
            return Err(Error::invalid_state(
                "cannot save a poisoned aggregate: a previous apply() failed, \
                 leaving inconsistent state — discard and reload the aggregate",
            ));
        }
        Ok(())
    }

    fn prepare_save(aggregate: &AggregateRoot<A>) -> Result<Option<StateCommit>> {
        Self::reject_encrypted()?;
        Self::check_not_poisoned(aggregate.is_poisoned())?;

        let pending = aggregate.pending_events_with_actors();
        if pending.is_empty() {
            return Ok(None);
        }

        let aggregate_id = aggregate.entity_id().as_uuid();
        #[allow(clippy::cast_possible_wrap)]
        let pending_count = pending.len() as i64;
        let expected_version =
            AggregateVersion::new(aggregate.version().as_i64().saturating_sub(pending_count));
        let events = envelopes_from_pending::<A::Event>(pending, aggregate_id)?;

        Ok(Some(StateCommit {
            state: StoredState {
                aggregate_id,
                aggregate_type: A::aggregate_type(),
                state_data: serde_json::to_value(aggregate.entity())?,
                version: aggregate.version(),
                is_deleted: false,
                schema_version: A::snapshot_version(),
            },
            expected_version,
            events,
            claims: aggregate.entity().claims(),
            clear_claims: false,
        }))
    }

    fn prepare_save_deleted(aggregate: &DeletedAggregateRoot<A>) -> Result<Option<StateCommit>> {
        Self::reject_encrypted()?;
        Self::check_not_poisoned(aggregate.is_poisoned())?;

        let pending = aggregate.pending_events_with_actors();
        if pending.is_empty() {
            return Ok(None);
        }

        let aggregate_id = aggregate.entity_id().as_uuid();
        #[allow(clippy::cast_possible_wrap)]
        let pending_count = pending.len() as i64;
        let expected_version =
            AggregateVersion::new(aggregate.version().as_i64().saturating_sub(pending_count));
        let events = envelopes_from_pending::<A::Event>(pending, aggregate_id)?;

        Ok(Some(StateCommit {
            state: StoredState {
                aggregate_id,
                aggregate_type: A::aggregate_type(),
                state_data: serde_json::to_value(aggregate.state())?,
                version: aggregate.version(),
                is_deleted: true,
                schema_version: A::snapshot_version(),
            },
            expected_version,
            events,
            claims: vec![],
            clear_claims: true,
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

        if state.is_deleted {
            let deleted_state: A::DeletedState = serde_json::from_value(state.state_data)?;
            Ok(Loaded::Deleted(DeletedAggregateRoot::from_snapshot(
                deleted_state,
                EntityId::from(state.aggregate_id),
                state.version,
            )))
        } else {
            let entity: A = serde_json::from_value(state.state_data)?;
            Ok(Loaded::Active(AggregateRoot::from_snapshot(
                state.version,
                entity,
            )))
        }
    }

    async fn load_deleted_by_id(&self, id: EntityId) -> Result<DeletedAggregateRoot<A>> {
        self.load_any_by_id(id).await?.into_deleted()
    }

    async fn save(&self, aggregate: &mut AggregateRoot<A>) -> Result<()> {
        if let Some(commit) = Self::prepare_save(aggregate)? {
            self.store.save(commit).await?;
            aggregate.clear_pending_events();
        }
        Ok(())
    }

    async fn save_all(&self, aggregates: &mut [&mut AggregateRoot<A>]) -> Result<()> {
        let mut commits = Vec::with_capacity(aggregates.len());
        let mut dirty = Vec::with_capacity(aggregates.len());
        for (index, aggregate) in aggregates.iter().enumerate() {
            if let Some(commit) = Self::prepare_save(aggregate)? {
                commits.push(commit);
                dirty.push(index);
            }
        }
        self.store.save_batch(commits).await?;
        for index in dirty {
            aggregates[index].clear_pending_events();
        }
        Ok(())
    }

    async fn save_deleted(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()> {
        if let Some(commit) = Self::prepare_save_deleted(aggregate)? {
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
    async fn test_schema_version_mismatch_fails_load() {
        let (store, repo) = repo();

        let id = EntityId::new();
        store.insert_raw(StoredState {
            aggregate_id: id.as_uuid(),
            aggregate_type: SimpleTestEntity::aggregate_type(),
            state_data: serde_json::json!({"id": id.as_uuid(), "value": 1}),
            version: AggregateVersion::new(1),
            is_deleted: false,
            schema_version: SimpleTestEntity::snapshot_version() + 1,
        });

        let err = repo.load(id).await.unwrap_err();
        assert!(matches!(err, Error::InvalidState(_)));
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
