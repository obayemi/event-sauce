use crate::test_fixtures::{SimpleTestDelete, SimpleTestEntity, SimpleTestError, SimpleTestEvent};
use crate::unit_of_work::test_support::{uow_store, Tally, TallyClosed};
use crate::{EventStore, Lifecycle, Repository, Tracked, UnitOfWork, UnitOfWorkError};

#[test]
fn a_tracked_handle_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Tracked<'static, SimpleTestEntity>>();
}

#[tokio::test]
async fn delete_with_commits_the_deleted_root() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    uow.load::<SimpleTestEntity>(id)
        .await
        .unwrap()
        .delete_with(|root| root.apply_delete(SimpleTestDelete))
        .unwrap();
    uow.commit().await.unwrap();

    let loaded = store
        .store()
        .events
        .repository::<SimpleTestEntity>()
        .load_any(id)
        .await
        .unwrap();
    assert!(loaded.is_deleted());
}

#[tokio::test]
async fn a_deleted_aggregate_can_be_neither_loaded_nor_created_again() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);
    uow.load::<SimpleTestEntity>(id)
        .await
        .unwrap()
        .delete_with(|root| root.apply_delete(SimpleTestDelete))
        .unwrap();

    let load = uow.load::<SimpleTestEntity>(id).await.unwrap_err();
    let create = uow
        .create::<SimpleTestEntity>(id, |_| Err(SimpleTestError))
        .unwrap_err();

    assert!(matches!(
        load,
        UnitOfWorkError::Lifecycle {
            expected: Lifecycle::Active,
            found: Lifecycle::Deleted
        }
    ));
    assert!(matches!(create, UnitOfWorkError::Domain(_)));
}

#[tokio::test]
async fn a_failed_delete_with_poisons_the_unit_of_work() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    let err = uow
        .load::<SimpleTestEntity>(id)
        .await
        .unwrap()
        .delete_with(|_| Err(SimpleTestError))
        .unwrap_err();
    assert!(matches!(err, UnitOfWorkError::Domain(_)));

    let reload = uow.load::<SimpleTestEntity>(id).await.unwrap_err();
    assert!(matches!(reload, UnitOfWorkError::DeleteFailed { id: failed, .. } if failed == id));
    let create = uow
        .create::<SimpleTestEntity>(id, |root| {
            root.apply_init(crate::test_fixtures::SimpleTestInit { value: 1 })
        })
        .unwrap_err();
    assert!(matches!(
        create,
        UnitOfWorkError::Lifecycle {
            expected: Lifecycle::Missing,
            found: Lifecycle::Deleted
        }
    ));
    let commit = uow.commit().await.unwrap_err();
    assert!(matches!(commit, UnitOfWorkError::DeleteFailed { id: failed, .. } if failed == id));
}

#[tokio::test]
async fn a_handle_derefs_to_its_root_and_shows_it_in_debug_output() {
    let store = uow_store();
    let id = store.store().seed_entity(3).await;
    let uow = UnitOfWork::new(&store);

    let mut tracked = uow.load::<SimpleTestEntity>(id).await.unwrap();
    tracked
        .apply(SimpleTestEvent::Updated { value: 4 })
        .unwrap();

    assert_eq!(tracked.value, 4);
    assert_eq!(tracked.entity_id(), id);
    assert!(format!("{tracked:?}").contains("Tracked"));
}

#[tokio::test]
async fn delete_with_commits_a_state_stored_deletion() {
    let store = uow_store();
    let id = store.store().seed_tally(1).await;
    let uow = UnitOfWork::new(&store);

    uow.load::<Tally>(id)
        .await
        .unwrap()
        .delete_with(|root| root.apply_delete(TallyClosed))
        .unwrap();
    uow.commit().await.unwrap();

    let row = store.store().states.recorded_commits().pop().unwrap();
    assert!(row.state.is_deleted);
}
