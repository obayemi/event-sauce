use super::*;
use crate::test_fixtures::{SimpleTestEntity, SimpleTestEvent, SimpleTestInit};
use crate::unit_of_work::test_support::{uow_store, Stores, Tally, TallyEvent};
use crate::{
    AggregateVersion, EntityId, EventStore, Repository, SnapshotConfig, StateStore, StoredState,
};

#[tokio::test]
async fn a_modified_event_sourced_aggregate_is_committed() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;

    let uow = UnitOfWork::new(&store);
    uow.load::<SimpleTestEntity>(id)
        .await
        .unwrap()
        .apply(SimpleTestEvent::Updated { value: 9 })
        .unwrap();
    uow.commit().await.unwrap();

    assert_eq!(store.store().entity_value(id).await, 9);
    assert_eq!(store.store().commits(), 1);
}

#[tokio::test]
async fn a_modified_state_stored_aggregate_is_committed() {
    let store = uow_store();
    let id = store.store().seed_tally(10).await;

    let uow = UnitOfWork::new(&store);
    uow.load::<Tally>(id)
        .await
        .unwrap()
        .apply(TallyEvent::Added { amount: 5 })
        .unwrap();
    uow.commit().await.unwrap();

    assert_eq!(store.store().tally_total(id).await, 15);
}

#[tokio::test]
async fn aggregates_of_both_styles_commit_together() {
    let store = uow_store();
    let entity = store.store().seed_entity(1).await;
    let tally = store.store().seed_tally(1).await;

    let uow = UnitOfWork::new(&store);
    let mut a = uow.load::<SimpleTestEntity>(entity).await.unwrap();
    let mut b = uow.load::<Tally>(tally).await.unwrap();
    a.apply(SimpleTestEvent::Updated { value: 2 }).unwrap();
    b.apply(TallyEvent::Added { amount: 2 }).unwrap();
    drop((a, b));
    uow.commit().await.unwrap();

    assert_eq!(store.store().entity_value(entity).await, 2);
    assert_eq!(store.store().tally_total(tally).await, 3);
    assert_eq!(store.store().commits(), 1);
}

#[tokio::test]
async fn loading_a_missing_aggregate_is_a_lifecycle_error() {
    let store = uow_store();
    let uow = UnitOfWork::new(&store);

    let err = uow
        .load::<SimpleTestEntity>(EntityId::new())
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        UnitOfWorkError::Lifecycle {
            expected: Lifecycle::Active,
            found: Lifecycle::Missing
        }
    ));
}

#[tokio::test]
async fn loading_a_deleted_aggregate_is_a_lifecycle_error() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let repo = store.store().events.repository::<SimpleTestEntity>();
    let mut deleted = repo
        .load(id)
        .await
        .unwrap()
        .apply_delete(crate::test_fixtures::SimpleTestDelete)
        .unwrap();
    repo.save_deleted(&mut deleted).await.unwrap();
    let uow = UnitOfWork::new(&store);

    let err = uow.load::<SimpleTestEntity>(id).await.unwrap_err();

    assert!(matches!(
        err,
        UnitOfWorkError::Lifecycle {
            expected: Lifecycle::Active,
            found: Lifecycle::Deleted
        }
    ));
}

#[tokio::test]
async fn a_store_failure_on_load_is_a_store_error() {
    let store = uow_store();
    let id = EntityId::new();
    store.store().states.insert_raw(StoredState {
        aggregate_id: id.as_uuid(),
        aggregate_type: Tally::aggregate_type(),
        state_data: serde_json::json!({}),
        version: AggregateVersion::new(1),
        is_deleted: false,
        schema_version: 99,
    });
    let uow = UnitOfWork::new(&store);

    let err = uow.load::<Tally>(id).await.unwrap_err();
    assert!(matches!(err, UnitOfWorkError::Store(e) if e.is_invalid_state()));

    let err = uow.peek::<Tally>(id).await.unwrap_err();
    assert!(matches!(err, UnitOfWorkError::Store(e) if e.is_invalid_state()));
}

#[tokio::test]
async fn an_unregistered_aggregate_type_is_refused() {
    let store = UnitOfWorkStore::new(Stores::new()).state_stored::<Tally>();
    let uow = UnitOfWork::new(&store);

    let err = uow
        .load::<SimpleTestEntity>(EntityId::new())
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        UnitOfWorkError::Unregistered { aggregate_type } if aggregate_type == SimpleTestEntity::aggregate_type()
    ));
}

#[tokio::test]
async fn loading_a_checked_out_aggregate_is_refused() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    let _held = uow.load::<SimpleTestEntity>(id).await.unwrap();
    let err = uow.load::<SimpleTestEntity>(id).await.unwrap_err();

    assert!(matches!(
        err,
        UnitOfWorkError::AlreadyCheckedOut { id: refused, .. } if refused == id
    ));
}

#[tokio::test]
async fn loading_a_returned_aggregate_hands_out_the_modified_root() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    uow.load::<SimpleTestEntity>(id)
        .await
        .unwrap()
        .apply(SimpleTestEvent::Updated { value: 5 })
        .unwrap();
    let mut again = uow.load::<SimpleTestEntity>(id).await.unwrap();
    assert_eq!(again.value, 5);
    assert_eq!(again.pending_events().len(), 1);
    again.apply(SimpleTestEvent::Updated { value: 6 }).unwrap();
    drop(again);
    uow.commit().await.unwrap();

    assert_eq!(store.store().entity_value(id).await, 6);
    assert_eq!(
        store.store().events.get_version(stream(id)).await.unwrap(),
        AggregateVersion::new(3)
    );
}

fn stream(id: EntityId) -> crate::StreamId {
    crate::StreamId::new(SimpleTestEntity::aggregate_type(), id.as_uuid())
}

#[tokio::test]
async fn a_read_only_aggregate_changed_concurrently_makes_the_commit_conflict() {
    let store = uow_store();
    let read = store.store().seed_entity(1).await;
    let written = store.store().seed_tally(1).await;
    let uow = UnitOfWork::new(&store);

    let _ = uow.load::<SimpleTestEntity>(read).await.unwrap().value;
    uow.load::<Tally>(written)
        .await
        .unwrap()
        .apply(TallyEvent::Added { amount: 1 })
        .unwrap();
    store.store().update_entity(read, 2).await;

    let err = uow.commit().await.unwrap_err();

    assert!(err.is_concurrency_conflict());
    assert_eq!(store.store().tally_total(written).await, 1);
}

#[tokio::test]
async fn a_unit_of_work_without_writes_commits_nothing() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    drop(uow.load::<SimpleTestEntity>(id).await.unwrap());
    store.store().update_entity(id, 2).await;
    uow.commit().await.unwrap();

    assert_eq!(store.store().commits(), 0);
}

#[tokio::test]
async fn peeking_reads_committed_state_even_while_checked_out() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    let mut held = uow.load::<SimpleTestEntity>(id).await.unwrap();
    held.apply(SimpleTestEvent::Updated { value: 7 }).unwrap();
    let seen = uow.peek::<SimpleTestEntity>(id).await.unwrap().unwrap();

    assert_eq!(seen.into_active().unwrap().value, 1);
}

#[tokio::test]
async fn a_peeked_aggregate_changed_concurrently_does_not_conflict() {
    let store = uow_store();
    let peeked = store.store().seed_entity(1).await;
    let written = store.store().seed_tally(1).await;
    let uow = UnitOfWork::new(&store);

    uow.peek::<SimpleTestEntity>(peeked).await.unwrap();
    uow.load::<Tally>(written)
        .await
        .unwrap()
        .apply(TallyEvent::Added { amount: 1 })
        .unwrap();
    store.store().update_entity(peeked, 2).await;
    uow.commit().await.unwrap();

    assert_eq!(store.store().tally_total(written).await, 2);
}

#[tokio::test]
async fn peeking_a_missing_aggregate_is_none() {
    let store = uow_store();
    let uow = UnitOfWork::new(&store);

    assert!(uow
        .peek::<SimpleTestEntity>(EntityId::new())
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_created_aggregate_is_committed_as_new() {
    let store = uow_store();
    let id = EntityId::new();
    let uow = UnitOfWork::new(&store);

    drop(
        uow.create::<SimpleTestEntity>(id, |root| root.apply_init(SimpleTestInit { value: 4 }))
            .unwrap(),
    );
    uow.commit().await.unwrap();

    assert_eq!(store.store().entity_value(id).await, 4);
}

#[tokio::test]
async fn creating_an_aggregate_that_already_exists_conflicts_on_commit() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    drop(
        uow.create::<SimpleTestEntity>(id, |root| root.apply_init(SimpleTestInit { value: 4 }))
            .unwrap(),
    );
    let err = uow.commit().await.unwrap_err();

    assert!(err.is_concurrency_conflict());
    assert_eq!(store.store().entity_value(id).await, 1);
}

#[tokio::test]
async fn creating_an_aggregate_this_unit_of_work_holds_is_a_lifecycle_error() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);
    drop(uow.load::<SimpleTestEntity>(id).await.unwrap());

    let err = uow
        .create::<SimpleTestEntity>(id, |root| root.apply_init(SimpleTestInit { value: 4 }))
        .unwrap_err();

    assert!(matches!(
        err,
        UnitOfWorkError::Lifecycle {
            expected: Lifecycle::Missing,
            found: Lifecycle::Active
        }
    ));
}

#[tokio::test]
async fn a_failing_create_is_a_domain_error() {
    let store = uow_store();
    let uow = UnitOfWork::new(&store);

    let err = uow
        .create::<SimpleTestEntity>(EntityId::new(), |_| {
            Err(crate::test_fixtures::SimpleTestError)
        })
        .unwrap_err();

    assert!(matches!(err, UnitOfWorkError::Domain(_)));
}

#[tokio::test]
async fn a_create_returning_another_aggregate_is_refused() {
    let store = uow_store();
    let uow = UnitOfWork::new(&store);

    let err = uow
        .create::<SimpleTestEntity>(EntityId::new(), |_| Ok(AggregateRoot::new(EntityId::new())))
        .unwrap_err();

    assert!(matches!(err, UnitOfWorkError::Store(e) if e.is_invalid_state()));
}

#[tokio::test]
async fn creating_an_unregistered_aggregate_type_is_refused() {
    let store = UnitOfWorkStore::new(Stores::new());
    let uow = UnitOfWork::new(&store);

    let err = uow
        .create::<SimpleTestEntity>(EntityId::new(), |root| {
            root.apply_init(SimpleTestInit { value: 1 })
        })
        .unwrap_err();

    assert!(matches!(err, UnitOfWorkError::Unregistered { .. }));
}

#[tokio::test]
async fn a_leaked_handle_refuses_the_commit() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    let mut leaked = uow.load::<SimpleTestEntity>(id).await.unwrap();
    leaked.apply(SimpleTestEvent::Updated { value: 3 }).unwrap();
    std::mem::forget(leaked);
    let err = uow.commit().await.unwrap_err();

    assert!(matches!(err, UnitOfWorkError::LeakedHandle(1)));
    assert_eq!(store.store().entity_value(id).await, 1);
}

#[tokio::test]
async fn a_poisoned_aggregate_refuses_the_commit() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    uow.load::<SimpleTestEntity>(id)
        .await
        .unwrap()
        .apply(SimpleTestEvent::Rejected)
        .unwrap_err();
    let err = uow.commit().await.unwrap_err();

    assert!(matches!(err, UnitOfWorkError::Store(e) if e.is_invalid_state()));
    assert_eq!(store.store().commits(), 0);
}

#[tokio::test]
async fn a_committed_snapshot_is_saved_once_the_commit_is_durable() {
    use crate::event_store::test_support::CommitTestStore;
    let config = SnapshotConfig::builder()
        .default_strategy(crate::AlwaysSnapshot)
        .build();
    let store = UnitOfWorkStore::new(Stores::with_events(CommitTestStore::new(config)))
        .event_sourced::<SimpleTestEntity>();
    let uow = UnitOfWork::new(&store);

    drop(
        uow.create::<SimpleTestEntity>(EntityId::new(), |root| {
            root.apply_init(SimpleTestInit { value: 1 })
        })
        .unwrap(),
    );
    uow.commit().await.unwrap();

    assert_eq!(store.store().events.save_snapshot_count(), 1);
}

#[test]
fn a_unit_of_work_debug_output_names_it() {
    let store = uow_store();
    assert!(format!("{:?}", UnitOfWork::new(&store)).starts_with("UnitOfWork"));
}

#[tokio::test]
async fn an_untouched_state_row_is_asserted_too() {
    let store = uow_store();
    let read = store.store().seed_tally(1).await;
    let written = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    drop(uow.load::<Tally>(read).await.unwrap());
    uow.load::<SimpleTestEntity>(written)
        .await
        .unwrap()
        .apply(SimpleTestEvent::Updated { value: 2 })
        .unwrap();
    let repo = store.store().states.repository::<Tally>();
    let mut concurrent = repo.load(read).await.unwrap();
    concurrent.apply(TallyEvent::Added { amount: 1 }).unwrap();
    repo.save(&mut concurrent).await.unwrap();

    assert!(uow.commit().await.unwrap_err().is_concurrency_conflict());
}
