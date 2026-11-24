use super::*;
use crate::test_fixtures::{SimpleTestDelete, SimpleTestEntity, SimpleTestEvent};
use crate::unit_of_work::test_support::Stores;
use crate::{EventStore, Repository, StateStore, StreamId};

fn state_repo(
    stores: &Stores,
) -> crate::StateStoredRepository<crate::test_fixtures::MockStateStore, SimpleTestEntity> {
    stores.states.repository::<SimpleTestEntity>()
}

fn event_repo(
    stores: &Stores,
) -> crate::EventSourcedRepository<crate::test_fixtures::MockEventStore, SimpleTestEntity> {
    stores.events.repository::<SimpleTestEntity>()
}

#[tokio::test]
async fn state_prepare_returns_none_for_a_clean_root() {
    let stores = Stores::new();
    let root = state_repo(&stores).create_with_id(EntityId::new());

    let prepared = StateStored.prepare(&stores, &root).await.unwrap();

    assert!(prepared.is_none());
}

#[tokio::test]
async fn state_prepare_returns_a_state_write_for_a_dirty_root() {
    let stores = Stores::new();
    let mut root = state_repo(&stores).create_with_id(EntityId::new());
    root.apply(SimpleTestEvent::Created { value: 7 }).unwrap();

    let prepared = StateStored
        .prepare(&stores, &root)
        .await
        .unwrap()
        .expect("a dirty root must prepare a write");

    match prepared {
        PreparedWrite::State(commit) => {
            assert_eq!(commit.state.version, AggregateVersion::new(1));
            assert!(!commit.state.is_deleted);
        }
        PreparedWrite::Stream(_) => panic!("a state aggregate must prepare a state write"),
    }
    assert_eq!(
        root.pending_events().len(),
        1,
        "prepare must not clear pending events — it does not persist anything"
    );
}

#[tokio::test]
async fn state_prepare_deleted_returns_a_state_write_marked_deleted() {
    let stores = Stores::new();
    let repo = state_repo(&stores);
    let mut root = repo.create_with_id(EntityId::new());
    root.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
    repo.save(&mut root).await.unwrap();
    let deleted = repo
        .load(root.entity_id())
        .await
        .unwrap()
        .apply_delete(SimpleTestDelete)
        .unwrap();

    let prepared = StateStored
        .prepare_deleted(&stores, &deleted)
        .await
        .unwrap()
        .expect("a dirty deleted root must prepare a write");

    match prepared {
        PreparedWrite::State(commit) => {
            assert!(commit.state.is_deleted);
            assert!(commit.clear_claims);
        }
        PreparedWrite::Stream(_) => panic!("a state aggregate must prepare a state write"),
    }
}

#[test]
fn state_expect_builds_a_state_expectation_for_the_given_id_and_version() {
    let id = EntityId::new();
    let version = AggregateVersion::new(3);

    let expectation = Persistence::<Stores, SimpleTestEntity>::expect(&StateStored, id, version);

    match expectation {
        Expectation::State {
            stream_id,
            version: v,
        } => {
            assert_eq!(
                stream_id,
                StreamId::new(SimpleTestEntity::aggregate_type(), id.as_uuid())
            );
            assert_eq!(v, version);
        }
        Expectation::Stream { .. } => panic!("a state aggregate must build a state expectation"),
    }
}

#[tokio::test]
async fn stream_prepare_returns_none_for_a_clean_root() {
    let stores = Stores::new();
    let root = event_repo(&stores).create_with_id(EntityId::new());

    let prepared = EventSourced.prepare(&stores, &root).await.unwrap();

    assert!(prepared.is_none());
}

#[tokio::test]
async fn stream_prepare_returns_a_stream_write_for_a_dirty_root() {
    let stores = Stores::new();
    let mut root = event_repo(&stores).create_with_id(EntityId::new());
    root.apply(SimpleTestEvent::Created { value: 7 }).unwrap();

    let prepared = EventSourced
        .prepare(&stores, &root)
        .await
        .unwrap()
        .expect("a dirty root must prepare a write");

    match prepared {
        PreparedWrite::Stream(write) => {
            assert_eq!(write.commit.expected_version, AggregateVersion::initial());
            assert_eq!(write.commit.events.len(), 1);
            assert!(!write.commit.clear_claims);
        }
        PreparedWrite::State(_) => panic!("an event-sourced aggregate must prepare a stream write"),
    }
    assert_eq!(root.pending_events().len(), 1);
}

#[tokio::test]
async fn stream_prepare_deleted_returns_a_stream_write_clearing_claims() {
    let stores = Stores::new();
    let id = stores.seed_entity(1).await;
    let deleted = event_repo(&stores)
        .load(id)
        .await
        .unwrap()
        .apply_delete(SimpleTestDelete)
        .unwrap();

    let prepared = EventSourced
        .prepare_deleted(&stores, &deleted)
        .await
        .unwrap()
        .expect("a dirty deleted root must prepare a write");

    match prepared {
        PreparedWrite::Stream(write) => {
            assert_eq!(write.commit.expected_version, AggregateVersion::new(1));
            assert!(write.commit.clear_claims);
        }
        PreparedWrite::State(_) => panic!("an event-sourced aggregate must prepare a stream write"),
    }
}

#[test]
fn stream_expect_builds_a_stream_expectation() {
    let id = EntityId::new();
    let version = AggregateVersion::new(4);

    let expectation = Persistence::<Stores, SimpleTestEntity>::expect(&EventSourced, id, version);

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
            panic!("an event-sourced aggregate must build a stream expectation")
        }
    }
}

#[tokio::test]
async fn stream_prepare_rejects_a_poisoned_root() {
    let stores = Stores::new();
    let mut root = event_repo(&stores).create_with_id(EntityId::new());
    root.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
    root.apply(SimpleTestEvent::Rejected).unwrap_err();

    let err = EventSourced.prepare(&stores, &root).await.unwrap_err();

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
async fn state_prepare_rejects_an_encrypted_aggregate() {
    let stores = Stores::new();
    let repo = stores.states.repository::<EncryptedEntity>();
    let mut root = repo.create_with_id(EntityId::new());
    root.apply(EncryptedEvent::Touched).unwrap();

    let err = StateStored.prepare(&stores, &root).await.unwrap_err();

    assert!(err.is_encryption());
}
