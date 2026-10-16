//! [`UnitOfWorkRepository`]: the repository-side half of the unit-of-work
//! contract.

use async_trait::async_trait;

use super::{Expectation, PreparedWrite, StreamWrite};
use crate::commit_source::CommitSource;
use crate::{
    Aggregate, AggregateRoot, AggregateVersion, DeletedAggregateRoot, EntityId,
    EventSourcedRepository, EventStore, Repository, Result, StateStore, StateStoredRepository,
    StreamId,
};

/// Extends [`Repository<A>`] with the primitives a unit of work needs:
/// preparing a write without persisting it, and asserting a read-only
/// aggregate's version without writing.
///
/// Implemented by the persistence-style-specific repositories so a unit of
/// work collecting aggregates of several styles can call `prepare`,
/// `prepare_deleted` and `expect` identically on all of them, then hand the
/// results to [`AtomicCommit`](super::AtomicCommit) without ever branching on
/// which style it is holding.
#[async_trait]
pub trait UnitOfWorkRepository<A: Aggregate>: Repository<A> {
    /// Prepares a write for an active aggregate root without persisting it.
    ///
    /// Returns `None` when the root has no pending events — there is nothing
    /// for the unit of work to commit for it.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or encryption fails, or if the root
    /// is poisoned (a previous `apply()` failed).
    async fn prepare(&self, root: &AggregateRoot<A>) -> Result<Option<PreparedWrite>>;

    /// Prepares a write for a deleted aggregate root without persisting it.
    ///
    /// Mirrors [`prepare`](Self::prepare) for the deleted-state lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or encryption fails, or if the root
    /// is poisoned.
    async fn prepare_deleted(
        &self,
        root: &DeletedAggregateRoot<A>,
    ) -> Result<Option<PreparedWrite>>;

    /// Builds the version assertion for an aggregate this unit of work only
    /// read: that its stream or row is still at `version` when the unit of
    /// work commits.
    fn expect(&self, id: EntityId, version: AggregateVersion) -> Expectation;
}

/// Prepares an event-sourced root's stream write against `store`, shared by
/// [`EventSourcedRepository`] and the unit of work's own registry.
pub(super) async fn prepare_stream_write<S, A, R>(
    store: &S,
    root: &R,
) -> Result<Option<PreparedWrite>>
where
    S: EventStore + ?Sized,
    A: Aggregate,
    A::Event: serde::Serialize,
    R: CommitSource<A>,
{
    Ok(crate::event_store::prepare_commit(store, root)
        .await?
        .map(|prepared| {
            PreparedWrite::Stream(StreamWrite {
                commit: prepared.commit,
                snapshot: prepared.snapshot,
            })
        }))
}

/// Prepares a state-stored root's row write, shared by
/// [`StateStoredRepository`] and the unit of work's own registry.
pub(super) fn prepare_state_write<S, A, R>(root: &R) -> Result<Option<PreparedWrite>>
where
    S: StateStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
    R: CommitSource<A>,
{
    Ok(StateStoredRepository::<S, A>::prepare(root)?.map(PreparedWrite::State))
}

/// Asserts an event-sourced aggregate's stream is still at `version`.
pub(super) fn stream_expectation<A: Aggregate>(
    id: EntityId,
    version: AggregateVersion,
) -> Expectation {
    Expectation::Stream {
        stream_id: StreamId::new(A::aggregate_type(), id.as_uuid()),
        version,
    }
}

/// Asserts a state-stored aggregate's row is still at `version`.
pub(super) fn state_expectation<A: Aggregate>(
    id: EntityId,
    version: AggregateVersion,
) -> Expectation {
    Expectation::State {
        stream_id: StreamId::new(A::aggregate_type(), id.as_uuid()),
        version,
    }
}

#[async_trait]
impl<S, A> UnitOfWorkRepository<A> for EventSourcedRepository<S, A>
where
    S: EventStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn prepare(&self, root: &AggregateRoot<A>) -> Result<Option<PreparedWrite>> {
        prepare_stream_write(self.store(), root).await
    }

    async fn prepare_deleted(
        &self,
        root: &DeletedAggregateRoot<A>,
    ) -> Result<Option<PreparedWrite>> {
        prepare_stream_write(self.store(), root).await
    }

    fn expect(&self, id: EntityId, version: AggregateVersion) -> Expectation {
        stream_expectation::<A>(id, version)
    }
}

#[async_trait]
impl<S, A> UnitOfWorkRepository<A> for StateStoredRepository<S, A>
where
    S: StateStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn prepare(&self, root: &AggregateRoot<A>) -> Result<Option<PreparedWrite>> {
        prepare_state_write::<S, A, _>(root)
    }

    async fn prepare_deleted(
        &self,
        root: &DeletedAggregateRoot<A>,
    ) -> Result<Option<PreparedWrite>> {
        prepare_state_write::<S, A, _>(root)
    }

    fn expect(&self, id: EntityId, version: AggregateVersion) -> Expectation {
        state_expectation::<A>(id, version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{
        MockEventStore, MockStateStore, SimpleTestDelete, SimpleTestEntity, SimpleTestEvent,
    };
    use crate::{EventSourcedRepository, EventStore};
    use std::sync::Arc;

    fn repo() -> (
        Arc<MockStateStore>,
        StateStoredRepository<MockStateStore, SimpleTestEntity>,
    ) {
        let store = Arc::new(MockStateStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        (store, repo)
    }

    #[tokio::test]
    async fn prepare_returns_none_for_a_clean_root() {
        let (_store, repo) = repo();
        let aggregate = repo.create_with_id(EntityId::new());

        let prepared = UnitOfWorkRepository::prepare(&repo, &aggregate)
            .await
            .unwrap();

        assert!(prepared.is_none());
    }

    #[tokio::test]
    async fn prepare_returns_a_state_write_for_a_dirty_root() {
        let (_store, repo) = repo();
        let mut aggregate = repo.create_with_id(EntityId::new());
        aggregate
            .apply(SimpleTestEvent::Created { value: 7 })
            .unwrap();

        let prepared = UnitOfWorkRepository::prepare(&repo, &aggregate)
            .await
            .unwrap()
            .expect("a dirty root must prepare a write");

        match prepared {
            PreparedWrite::State(commit) => {
                assert_eq!(commit.state.version, AggregateVersion::new(1));
                assert!(!commit.state.is_deleted);
            }
            PreparedWrite::Stream(_) => panic!("a state repository must prepare a state write"),
        }
        assert_eq!(
            aggregate.pending_events().len(),
            1,
            "prepare must not clear pending events — it does not persist anything"
        );
    }

    #[tokio::test]
    async fn prepare_deleted_returns_a_state_write_marked_deleted() {
        let (_store, repo) = repo();
        let mut aggregate = repo.create_with_id(EntityId::new());
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(aggregate.entity_id()).await.unwrap();
        let deleted = loaded.apply_delete(SimpleTestDelete).unwrap();

        let prepared = UnitOfWorkRepository::prepare_deleted(&repo, &deleted)
            .await
            .unwrap()
            .expect("a dirty deleted root must prepare a write");

        match prepared {
            PreparedWrite::State(commit) => {
                assert!(commit.state.is_deleted);
                assert!(commit.clear_claims);
            }
            PreparedWrite::Stream(_) => panic!("a state repository must prepare a state write"),
        }
    }

    #[tokio::test]
    async fn expect_builds_a_state_expectation_for_the_given_id_and_version() {
        let (_store, repo) = repo();
        let id = EntityId::new();
        let version = AggregateVersion::new(3);

        let expectation = UnitOfWorkRepository::expect(&repo, id, version);

        match expectation {
            Expectation::State {
                stream_id,
                version: v,
            } => {
                assert_eq!(stream_id.aggregate_id(), id.as_uuid());
                assert_eq!(
                    stream_id.aggregate_type(),
                    &SimpleTestEntity::aggregate_type()
                );
                assert_eq!(v, version);
            }
            Expectation::Stream { .. } => {
                panic!("a state repository must build a state expectation")
            }
        }
    }

    fn event_sourced_repo() -> EventSourcedRepository<MockEventStore, SimpleTestEntity> {
        Arc::new(MockEventStore::new()).repository::<SimpleTestEntity>()
    }

    #[tokio::test]
    async fn event_sourced_prepare_returns_none_for_a_clean_root() {
        let repo = event_sourced_repo();
        let aggregate = repo.create_with_id(EntityId::new());

        let prepared = UnitOfWorkRepository::prepare(&repo, &aggregate)
            .await
            .unwrap();

        assert!(prepared.is_none());
    }

    #[tokio::test]
    async fn event_sourced_prepare_returns_a_stream_write_for_a_dirty_root() {
        let repo = event_sourced_repo();
        let mut aggregate = repo.create_with_id(EntityId::new());
        aggregate
            .apply(SimpleTestEvent::Created { value: 7 })
            .unwrap();

        let prepared = UnitOfWorkRepository::prepare(&repo, &aggregate)
            .await
            .unwrap()
            .expect("a dirty root must prepare a write");

        match prepared {
            PreparedWrite::Stream(write) => {
                assert_eq!(write.commit.expected_version, AggregateVersion::initial());
                assert_eq!(write.commit.events.len(), 1);
                assert!(!write.commit.clear_claims);
            }
            PreparedWrite::State(_) => {
                panic!("an event-sourced repository must prepare a stream write")
            }
        }
        assert_eq!(aggregate.pending_events().len(), 1);
    }

    #[tokio::test]
    async fn event_sourced_prepare_deleted_returns_a_stream_write_clearing_claims() {
        let repo = event_sourced_repo();
        let mut aggregate = repo.create_with_id(EntityId::new());
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        repo.save(&mut aggregate).await.unwrap();
        let deleted = repo
            .load(aggregate.entity_id())
            .await
            .unwrap()
            .apply_delete(SimpleTestDelete)
            .unwrap();

        let prepared = UnitOfWorkRepository::prepare_deleted(&repo, &deleted)
            .await
            .unwrap()
            .expect("a dirty deleted root must prepare a write");

        match prepared {
            PreparedWrite::Stream(write) => {
                assert_eq!(write.commit.expected_version, AggregateVersion::new(1));
                assert!(write.commit.clear_claims);
            }
            PreparedWrite::State(_) => {
                panic!("an event-sourced repository must prepare a stream write")
            }
        }
    }

    #[tokio::test]
    async fn event_sourced_expect_builds_a_stream_expectation() {
        let repo = event_sourced_repo();
        let id = EntityId::new();
        let version = AggregateVersion::new(4);

        let expectation = UnitOfWorkRepository::expect(&repo, id, version);

        match expectation {
            Expectation::Stream {
                stream_id,
                version: v,
            } => {
                assert_eq!(
                    stream_id,
                    StreamId::new(SimpleTestEntity::aggregate_type(), id.as_uuid())
                );
                assert_eq!(v, version);
            }
            Expectation::State { .. } => {
                panic!("an event-sourced repository must build a stream expectation")
            }
        }
    }

    #[tokio::test]
    async fn event_sourced_prepare_rejects_a_poisoned_root() {
        let repo = event_sourced_repo();
        let mut aggregate = repo.create_with_id(EntityId::new());
        aggregate
            .apply(SimpleTestEvent::Created { value: 1 })
            .unwrap();
        aggregate.apply(SimpleTestEvent::Rejected).unwrap_err();

        let err = UnitOfWorkRepository::prepare(&repo, &aggregate)
            .await
            .unwrap_err();

        assert!(err.is_invalid_state());
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct EncryptedEntity {
        id: EntityId,
    }

    impl crate::Entity for EncryptedEntity {
        fn new(id: EntityId) -> Self {
            Self { id }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }
    impl crate::DefaultEntity for EncryptedEntity {}

    #[derive(Debug, thiserror::Error)]
    #[error("encrypted entity error")]
    struct EncryptedEntityError;
    impl crate::AggregateError for EncryptedEntityError {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum EncryptedEvent {
        Touched,
    }
    impl crate::DomainEvent for EncryptedEvent {
        type Aggregate = EncryptedEntity;
        fn event_type(&self) -> &'static str {
            "EncryptedEntity.Touched"
        }
        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }
        fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
    }
    impl crate::ApplyEvent<EncryptedEntity> for EncryptedEvent {
        fn apply(&self, _entity: &mut EncryptedEntity) {}
    }
    impl crate::EventApplicator<EncryptedEntity> for EncryptedEvent {
        fn dispatch(
            &self,
            entity: &mut EncryptedEntity,
        ) -> std::result::Result<(), EncryptedEntityError> {
            crate::ApplyEvent::apply(self, entity);
            Ok(())
        }
        fn dispatch_unchecked(&self, entity: &mut EncryptedEntity) {
            crate::ApplyEvent::apply(self, entity);
        }
    }
    impl crate::Aggregate for EncryptedEntity {
        type Event = EncryptedEvent;
        type Error = EncryptedEntityError;
        type DeletedState = Self;
        fn is_encrypted() -> bool {
            true
        }
    }

    #[tokio::test]
    async fn prepare_rejects_an_encrypted_aggregate() {
        let store = Arc::new(MockStateStore::new());
        let repo = store.repository::<EncryptedEntity>();
        let mut aggregate = repo.create_with_id(EntityId::new());
        aggregate.apply(EncryptedEvent::Touched).unwrap();

        let err = UnitOfWorkRepository::prepare(&repo, &aggregate)
            .await
            .unwrap_err();

        assert!(err.is_encryption());
    }
}
