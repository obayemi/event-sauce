use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use super::*;
use crate::test_fixtures::{SimpleTestEntity, SimpleTestError, SimpleTestEvent};
use crate::unit_of_work::test_support::{uow_store, Stores, Tally, TallyEvent};
use crate::{EntityId, Lifecycle};

/// Adds `amount` to a tally, deciding on an event-sourced entity it only
/// reads; the first `interfere` attempts change that entity concurrently.
struct AddIfPositive<'s> {
    stores: &'s Stores,
    guard: EntityId,
    tally: EntityId,
    amount: i64,
    interfere: u32,
    attempts: AtomicU32,
}

impl<'s> AddIfPositive<'s> {
    fn new(stores: &'s Stores, guard: EntityId, tally: EntityId, interfere: u32) -> Self {
        Self {
            stores,
            guard,
            tally,
            amount: 5,
            interfere,
            attempts: AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl UnitOfWorkCommand<Stores> for AddIfPositive<'_> {
    async fn handle(&self, uow: &UnitOfWork<'_, Stores>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let guard = uow.load::<SimpleTestEntity>(self.guard).await?;
        if guard.value <= 0 {
            return Err(UnitOfWorkError::Lifecycle {
                expected: Lifecycle::Active,
                found: Lifecycle::Missing,
            });
        }
        drop(guard);
        let mut tally = uow.load::<Tally>(self.tally).await?;
        tally.apply(TallyEvent::Added {
            amount: self.amount,
        })?;
        if attempt <= self.interfere {
            self.stores.update_entity(self.guard, 1).await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn execute_commits_what_the_command_changed() {
    let store = uow_store();
    let guard = store.store().seed_entity(1).await;
    let tally = store.store().seed_tally(0).await;
    let command = AddIfPositive::new(store.store(), guard, tally, 0);

    let executed = store
        .execute(&command, &(), RetryPolicy::default())
        .await
        .unwrap();

    assert_eq!(executed.attempts.get(), 1);
    assert_eq!(store.store().tally_total(tally).await, 5);
}

#[tokio::test]
async fn a_failing_command_commits_nothing() {
    let store = uow_store();
    let guard = store.store().seed_entity(0).await;
    let tally = store.store().seed_tally(0).await;
    let command = AddIfPositive::new(store.store(), guard, tally, 0);

    let err = store
        .execute(&command, &(), RetryPolicy::default())
        .await
        .unwrap_err();

    assert!(matches!(err, UnitOfWorkError::Lifecycle { .. }));
    assert_eq!(store.store().commits(), 0);
    assert_eq!(command.attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_conflict_on_a_read_aggregate_retries_the_whole_command() {
    let store = uow_store();
    let guard = store.store().seed_entity(1).await;
    let tally = store.store().seed_tally(0).await;
    let command = AddIfPositive::new(store.store(), guard, tally, 1);

    let executed = store
        .execute(&command, &(), RetryPolicy::default())
        .await
        .unwrap();

    assert_eq!(executed.attempts.get(), 2);
    assert_eq!(store.store().tally_total(tally).await, 5);
}

#[tokio::test(start_paused = true)]
async fn retries_give_up_with_the_conflict_once_the_policy_is_exhausted() {
    let store = uow_store();
    let guard = store.store().seed_entity(1).await;
    let tally = store.store().seed_tally(0).await;
    let command = AddIfPositive::new(store.store(), guard, tally, u32::MAX);
    let policy = RetryPolicy::default()
        .with_max_attempts(NonZeroU32::new(3).unwrap())
        .with_initial_backoff(Duration::from_millis(5));

    let err = store.execute(&command, &(), policy).await.unwrap_err();

    assert!(err.is_concurrency_conflict());
    assert_eq!(command.attempts.load(Ordering::SeqCst), 3);
    assert_eq!(store.store().tally_total(tally).await, 0);
}

#[tokio::test]
async fn a_conflict_is_not_retried_without_backoff_under_a_single_attempt_policy() {
    let store = uow_store();
    let guard = store.store().seed_entity(1).await;
    let tally = store.store().seed_tally(0).await;
    let command = AddIfPositive::new(store.store(), guard, tally, 1);

    let err = store
        .execute(&command, &(), RetryPolicy::none())
        .await
        .unwrap_err();

    assert!(err.is_concurrency_conflict());
    assert_eq!(command.attempts.load(Ordering::SeqCst), 1);
}

struct Leak(EntityId);

#[async_trait]
impl UnitOfWorkCommand<Stores> for Leak {
    async fn handle(&self, uow: &UnitOfWork<'_, Stores>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        std::mem::forget(uow.load::<SimpleTestEntity>(self.0).await?);
        Ok(())
    }
}

#[tokio::test]
async fn a_commit_failure_other_than_a_conflict_is_never_retried() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;

    let err = store
        .execute(&Leak(id), &(), RetryPolicy::default())
        .await
        .unwrap_err();

    assert!(matches!(err, UnitOfWorkError::LeakedHandle(1)));
}

struct SetWithContext(EntityId);

#[async_trait]
impl UnitOfWorkCommand<Stores, i32> for SetWithContext {
    async fn handle(
        &self,
        uow: &UnitOfWork<'_, Stores>,
        value: &i32,
    ) -> Result<(), UnitOfWorkError> {
        uow.load::<SimpleTestEntity>(self.0)
            .await?
            .apply(SimpleTestEvent::Updated { value: *value })?;
        Ok(())
    }
}

#[tokio::test]
async fn a_command_receives_its_context() {
    let store = uow_store();
    let id = store.store().seed_entity(1).await;

    store
        .execute(&SetWithContext(id), &42, RetryPolicy::default())
        .await
        .unwrap();

    assert_eq!(store.store().entity_value(id).await, 42);
}

#[tokio::test]
async fn registering_a_type_again_replaces_its_persistence() {
    let store = UnitOfWorkStore::new(Stores::new())
        .event_sourced::<SimpleTestEntity>()
        .state_stored::<SimpleTestEntity>();
    let id = store.store().seed_entity(1).await;
    let uow = UnitOfWork::new(&store);

    assert!(uow.peek::<SimpleTestEntity>(id).await.unwrap().is_none());
}

#[test]
fn debug_output_counts_registered_types() {
    assert!(format!("{:?}", uow_store()).contains("registered: 2"));
}

#[test]
fn an_aggregate_error_converts_inside_a_handler() {
    fn reject() -> Result<(), UnitOfWorkError> {
        Err(SimpleTestError)?
    }
    assert!(matches!(reject(), Err(UnitOfWorkError::Domain(_))));
}

struct NamedLeakless;

#[async_trait]
impl UnitOfWorkCommand<Stores> for NamedLeakless {
    async fn handle(
        &self,
        _uow: &UnitOfWork<'_, Stores>,
        _ctx: &(),
    ) -> Result<(), UnitOfWorkError> {
        Ok(())
    }

    fn name(&self) -> Option<&str> {
        Some("named")
    }
}

#[test]
fn a_unit_of_work_command_reports_no_name_unless_it_overrides_it() {
    assert_eq!(NamedLeakless.name(), Some("named"));
    assert_eq!(Leak(EntityId::new()).name(), None);
}

struct Retag<'s> {
    stores: &'s Stores,
    entity: EntityId,
    tally: EntityId,
    interfere: u32,
    attempts: AtomicU32,
    tagged: std::sync::Mutex<Vec<(Option<String>, Option<String>)>>,
}

#[async_trait]
impl UnitOfWorkCommand<Stores> for Retag<'_> {
    async fn handle(&self, uow: &UnitOfWork<'_, Stores>, _ctx: &()) -> Result<(), UnitOfWorkError> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let mut entity = uow.load::<SimpleTestEntity>(self.entity).await?;
        entity.apply(SimpleTestEvent::Updated { value: 9 })?;
        let mut tally = uow.load::<Tally>(self.tally).await?;
        tally.apply(TallyEvent::Added { amount: 1 })?;
        let tally_metadata = tally.pending_events_with_actors()[0].metadata.clone();
        let entity_metadata = entity.pending_events_with_actors()[0].metadata.clone();
        self.tagged.lock().unwrap().push((
            entity_metadata.and_then(|m| m.correlation_id),
            tally_metadata.and_then(|m| m.command),
        ));
        if attempt <= self.interfere {
            self.stores.update_entity(self.entity, 1).await;
        }
        Ok(())
    }

    fn name(&self) -> Option<&str> {
        Some("retag")
    }
}

#[tokio::test(start_paused = true)]
async fn every_aggregate_of_a_unit_of_work_shares_one_correlation_id_across_retries() {
    let store = uow_store();
    let entity = store.store().seed_entity(1).await;
    let tally = store.store().seed_tally(0).await;
    let command = Retag {
        stores: store.store(),
        entity,
        tally,
        interfere: 1,
        attempts: AtomicU32::new(0),
        tagged: std::sync::Mutex::new(Vec::new()),
    };

    let executed = store
        .execute(&command, &(), RetryPolicy::default())
        .await
        .unwrap();

    assert_eq!(executed.attempts.get(), 2);
    let tagged = command.tagged.lock().unwrap().clone();
    let id = tagged[0].0.clone().expect("a correlation id is generated");
    assert!(tagged
        .iter()
        .all(|(correlation, command)| *correlation == Some(id.clone())
            && command.as_deref() == Some("retag")));
    let committed = store.store().events.get_events().pop().unwrap();
    let metadata = committed.metadata.expect("the committed event is tagged");
    assert_eq!(metadata.correlation_id, Some(id));
    assert_eq!(metadata.command.as_deref(), Some("retag"));
}

#[tokio::test]
async fn the_execution_context_does_not_outlive_the_unit_of_work() {
    let store = uow_store();
    let guard = store.store().seed_entity(1).await;
    let tally = store.store().seed_tally(0).await;
    let command = AddIfPositive::new(store.store(), guard, tally, 0);

    store
        .execute(&command, &(), RetryPolicy::default())
        .await
        .unwrap();

    assert_eq!(crate::event_context::current_snapshot(), None);
}
