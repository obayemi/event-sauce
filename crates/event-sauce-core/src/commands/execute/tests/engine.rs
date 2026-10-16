use crate::commands::{CommandError, RetryPolicy, Root};
use crate::test_fixtures::{SimpleTestEntity, SimpleTestError, SimpleTestEvent, SimpleTestInit};
use crate::{AggregateRoot, EntityId, Repository};

fn update_to(
    value: i32,
) -> impl Fn(
    Root<SimpleTestEntity>,
    &(),
) -> std::result::Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
    move |root, _ctx| {
        let mut active = root.active()?;
        active.apply(SimpleTestEvent::Updated { value })?;
        Ok(active.into())
    }
}

fn create_with(
    value: i32,
) -> impl Fn(
    Root<SimpleTestEntity>,
    &(),
) -> std::result::Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
    move |root, _ctx| {
        let uninit = root.missing()?;
        Ok(uninit.apply_init(SimpleTestInit { value })?.into())
    }
}

async fn seed_active<R: Repository<SimpleTestEntity>>(repo: &R, id: EntityId) {
    let mut seed = AggregateRoot::<SimpleTestEntity>::new(id);
    seed.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
    repo.save(&mut seed).await.unwrap();
}

#[cfg(feature = "event-sourcing")]
fn delete() -> impl Fn(
    Root<SimpleTestEntity>,
    &(),
) -> std::result::Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
    |root, _ctx| {
        let active = root.active()?;
        Ok(active
            .apply_delete(crate::test_fixtures::SimpleTestDelete)?
            .into())
    }
}

#[cfg(feature = "event-sourcing")]
fn reject() -> impl Fn(
    Root<SimpleTestEntity>,
    &(),
) -> std::result::Result<Root<SimpleTestEntity>, CommandError<SimpleTestError>> {
    |root, _ctx| {
        let mut active = root.active()?;
        active.apply(SimpleTestEvent::Rejected)?;
        Ok(active.into())
    }
}

#[cfg(feature = "event-sourcing")]
fn swap_identity() -> impl Fn(
    Root<SimpleTestEntity>,
    &(),
) -> std::result::Result<
    Root<SimpleTestEntity>,
    CommandError<SimpleTestError>,
> {
    |root, _ctx| {
        let _active = root.active()?;
        Ok(Root::Active(AggregateRoot::<SimpleTestEntity>::new(
            EntityId::new(),
        )))
    }
}

#[cfg(feature = "event-sourcing")]
mod event_sourced {
    use super::*;
    use crate::commands::ExecuteError;
    use crate::event_store::EventStore;
    use crate::test_fixtures::{FailNTimesEventStore, MockEventStore};
    use crate::EventEnvelope;
    use std::sync::Arc;

    #[tokio::test]
    async fn succeeds_on_the_first_attempt() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&repo, id).await;

        let executed = repo
            .execute(id, &update_to(42), &(), RetryPolicy::default())
            .await
            .unwrap();

        assert_eq!(executed.attempts.get(), 1);
        assert_eq!(executed.root.active().unwrap().value, 42);
        assert_eq!(repo.load(id).await.unwrap().value, 42);
    }

    #[tokio::test]
    async fn creates_from_missing_via_an_init_command() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();

        let executed = repo
            .execute(id, &create_with(7), &(), RetryPolicy::default())
            .await
            .unwrap();

        assert_eq!(executed.attempts.get(), 1);
        assert_eq!(executed.root.active().unwrap().value, 7);
        assert_eq!(repo.load(id).await.unwrap().value, 7);
    }

    #[tokio::test]
    async fn deletes_an_active_aggregate() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&repo, id).await;

        let executed = repo
            .execute(id, &delete(), &(), RetryPolicy::default())
            .await
            .unwrap();

        assert!(matches!(executed.root, Root::Deleted(_)));
        assert!(repo.load_any(id).await.unwrap().is_deleted());
    }

    #[tokio::test]
    async fn domain_refusal_is_not_retried_and_saves_nothing() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&repo, id).await;

        let err = repo
            .execute(id, &reject(), &(), RetryPolicy::default())
            .await
            .unwrap_err();

        assert!(matches!(err, ExecuteError::Domain(SimpleTestError)));
        assert_eq!(repo.load(id).await.unwrap().value, 1);
    }

    #[tokio::test]
    async fn lifecycle_refusal_is_reported() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&repo, id).await;

        // Entity is active; `create_with` requires it to be missing.
        let err = repo
            .execute(id, &create_with(1), &(), RetryPolicy::default())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ExecuteError::Lifecycle {
                expected: crate::Lifecycle::Missing,
                found: crate::Lifecycle::Active,
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn retries_once_after_a_concurrency_conflict_then_succeeds() {
        let inner = Arc::new(MockEventStore::new());
        let setup = inner.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&setup, id).await;

        let flaky = Arc::new(FailNTimesEventStore::new(Arc::clone(&inner), 1));
        let repo = flaky.repository::<SimpleTestEntity>();

        let executed = repo
            .execute(id, &update_to(99), &(), RetryPolicy::default())
            .await
            .unwrap();

        assert_eq!(executed.attempts.get(), 2);
        assert_eq!(executed.root.active().unwrap().value, 99);
    }

    #[tokio::test(start_paused = true)]
    async fn conflicts_exhausted_returns_save_error_after_max_attempts() {
        let inner = Arc::new(MockEventStore::new());
        let setup = inner.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&setup, id).await;

        let flaky = Arc::new(FailNTimesEventStore::new(Arc::clone(&inner), usize::MAX));
        let repo = flaky.repository::<SimpleTestEntity>();
        let retry = RetryPolicy::default().with_max_attempts(std::num::NonZeroU32::new(3).unwrap());

        let err = repo
            .execute(id, &update_to(99), &(), retry)
            .await
            .unwrap_err();

        assert!(err.is_concurrency_conflict());
        assert!(matches!(err, ExecuteError::Save(_)));
    }

    #[tokio::test]
    async fn non_concurrency_save_error_is_not_retried() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&repo, id).await;
        // Poison the aggregate so the next save fails for a reason other
        // than a concurrency conflict.
        let mut loaded = repo.load(id).await.unwrap();
        assert!(loaded.apply(SimpleTestEvent::Rejected).is_err());
        assert!(loaded.is_poisoned());

        // Craft a command that hands back this already-poisoned root.
        let poisoned = std::sync::Mutex::new(Some(loaded));
        let command = move |_root: Root<SimpleTestEntity>, _ctx: &()| {
            Ok(Root::Active(poisoned.lock().unwrap().take().unwrap()))
        };

        let err = repo
            .execute(id, &command, &(), RetryPolicy::default())
            .await
            .unwrap_err();

        assert!(!err.is_concurrency_conflict());
        assert!(matches!(err, ExecuteError::Save(_)));
    }

    #[tokio::test]
    async fn load_error_other_than_not_found_is_reported() {
        let store = Arc::new(MockEventStore::new());
        let id = EntityId::new();
        store.add_event(EventEnvelope::new(
            uuid::Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestEntity.Updated".to_string(),
            crate::EventVersion::new(1),
            serde_json::json!({ "totally": "unrecognized" }),
        ));
        let repo = store.repository::<SimpleTestEntity>();

        let err = repo
            .execute(id, &update_to(1), &(), RetryPolicy::default())
            .await
            .unwrap_err();

        assert!(matches!(err, ExecuteError::Load(e) if !e.is_not_found()));
    }

    #[tokio::test]
    async fn id_mismatch_is_reported_as_a_save_error() {
        let store = Arc::new(MockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&repo, id).await;

        let err = repo
            .execute(id, &swap_identity(), &(), RetryPolicy::default())
            .await
            .unwrap_err();

        assert!(matches!(err, ExecuteError::Save(e) if e.is_invalid_state()));
    }
}

#[cfg(feature = "state-store")]
mod state_stored {
    use super::*;
    use crate::test_fixtures::{FailNTimesStateStore, MockStateStore};
    use crate::StateStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn succeeds_on_the_first_attempt() {
        let store = Arc::new(MockStateStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&repo, id).await;

        let executed = repo
            .execute(id, &update_to(42), &(), RetryPolicy::default())
            .await
            .unwrap();

        assert_eq!(executed.attempts.get(), 1);
        assert_eq!(repo.load(id).await.unwrap().value, 42);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_once_after_a_concurrency_conflict_then_succeeds() {
        let inner = Arc::new(MockStateStore::new());
        let setup = inner.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&setup, id).await;

        let flaky = Arc::new(FailNTimesStateStore::new(Arc::clone(&inner), 1));
        let repo = flaky.repository::<SimpleTestEntity>();

        let executed = repo
            .execute(id, &update_to(99), &(), RetryPolicy::default())
            .await
            .unwrap();

        assert_eq!(executed.attempts.get(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn conflicts_exhausted_returns_save_error_after_max_attempts() {
        let inner = Arc::new(MockStateStore::new());
        let setup = inner.repository::<SimpleTestEntity>();
        let id = EntityId::new();
        seed_active(&setup, id).await;

        let flaky = Arc::new(FailNTimesStateStore::new(Arc::clone(&inner), usize::MAX));
        let repo = flaky.repository::<SimpleTestEntity>();
        let retry = RetryPolicy::default().with_max_attempts(std::num::NonZeroU32::new(3).unwrap());

        let err = repo
            .execute(id, &update_to(99), &(), retry)
            .await
            .unwrap_err();

        assert!(err.is_concurrency_conflict());
    }

    #[tokio::test]
    async fn creates_from_missing_via_an_init_command() {
        let store = Arc::new(MockStateStore::new());
        let repo = store.repository::<SimpleTestEntity>();
        let id = EntityId::new();

        let executed = repo
            .execute(id, &create_with(5), &(), RetryPolicy::default())
            .await
            .unwrap();

        assert_eq!(executed.root.active().unwrap().value, 5);
    }
}
