//! [`UnitOfWorkRepository`]: the repository-side half of the unit-of-work
//! contract.

use async_trait::async_trait;

use super::{Expectation, PreparedWrite};
use crate::{
    Aggregate, AggregateRoot, AggregateVersion, DeletedAggregateRoot, EntityId, Repository, Result,
    StateStore, StateStoredRepository, StreamId,
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

#[async_trait]
impl<S, A> UnitOfWorkRepository<A> for StateStoredRepository<S, A>
where
    S: StateStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn prepare(&self, root: &AggregateRoot<A>) -> Result<Option<PreparedWrite>> {
        Ok(Self::prepare(root)?.map(PreparedWrite::State))
    }

    async fn prepare_deleted(
        &self,
        root: &DeletedAggregateRoot<A>,
    ) -> Result<Option<PreparedWrite>> {
        Ok(Self::prepare(root)?.map(PreparedWrite::State))
    }

    fn expect(&self, id: EntityId, version: AggregateVersion) -> Expectation {
        Expectation::State {
            stream_id: StreamId::new(A::aggregate_type(), id.as_uuid()),
            version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{
        MockStateStore, SimpleTestDelete, SimpleTestEntity, SimpleTestEvent,
    };
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
