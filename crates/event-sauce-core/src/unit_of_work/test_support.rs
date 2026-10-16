//! Test fixtures for the unit of work: a [`TransactionalStore`] over core's
//! mock stores, and a second aggregate type to mix persistence styles.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{AtomicCommit, TransactionalStore, UnitOfWorkStore};
use crate::test_fixtures::{MockEventStore, MockStateStore, SimpleTestEntity, SimpleTestEvent};
use crate::{
    AggregateError, AggregateVersion, ApplyEvent, EntityId, Error, EventApplicator, EventStore,
    EventVersion, Repository, Result, StateStore, StreamId,
};

/// A [`TransactionalStore`] over an event store and a [`MockStateStore`].
///
/// `commit_atomic` checks every version before applying anything, under a
/// lock serializing commits — atomic enough for single-process tests.
pub(super) struct Stores<E = MockEventStore> {
    pub(super) events: Arc<E>,
    pub(super) states: Arc<MockStateStore>,
    lock: tokio::sync::Mutex<()>,
    commits: AtomicUsize,
}

impl Stores {
    pub(super) fn new() -> Self {
        Self::with_events(MockEventStore::new())
    }
}

impl<E: EventStore + 'static> Stores<E> {
    pub(super) fn with_events(events: E) -> Self {
        Self {
            events: Arc::new(events),
            states: Arc::new(MockStateStore::new()),
            lock: tokio::sync::Mutex::new(()),
            commits: AtomicUsize::new(0),
        }
    }

    /// How many `commit_atomic` calls succeeded.
    pub(super) fn commits(&self) -> usize {
        self.commits.load(Ordering::SeqCst)
    }

    /// Saves a new event-sourced [`SimpleTestEntity`] holding `value`.
    pub(super) async fn seed_entity(&self, value: i32) -> EntityId {
        let repo = self.events.repository::<SimpleTestEntity>();
        let mut root = repo.create_with_id(EntityId::new());
        root.apply(SimpleTestEvent::Created { value }).unwrap();
        repo.save(&mut root).await.unwrap();
        root.entity_id()
    }

    /// Sets an event-sourced [`SimpleTestEntity`]'s value outside any unit
    /// of work, as a concurrent writer would.
    pub(super) async fn update_entity(&self, id: EntityId, value: i32) {
        let repo = self.events.repository::<SimpleTestEntity>();
        let mut root = repo.load(id).await.unwrap();
        root.apply(SimpleTestEvent::Updated { value }).unwrap();
        repo.save(&mut root).await.unwrap();
    }

    /// Reads an event-sourced [`SimpleTestEntity`]'s committed value.
    pub(super) async fn entity_value(&self, id: EntityId) -> i32 {
        self.events
            .repository::<SimpleTestEntity>()
            .load(id)
            .await
            .unwrap()
            .value
    }

    /// Saves a new state-stored [`Tally`] holding `total`.
    pub(super) async fn seed_tally(&self, total: i64) -> EntityId {
        let repo = self.states.repository::<Tally>();
        let mut root = repo.create_with_id(EntityId::new());
        root.apply(TallyEvent::Added { amount: total }).unwrap();
        repo.save(&mut root).await.unwrap();
        root.entity_id()
    }

    /// Reads a state-stored [`Tally`]'s committed total.
    pub(super) async fn tally_total(&self, id: EntityId) -> i64 {
        self.states
            .repository::<Tally>()
            .load(id)
            .await
            .unwrap()
            .total
    }
}

fn ensure_at(actual: AggregateVersion, expected: AggregateVersion) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(Error::concurrency_conflict(expected, actual))
    }
}

#[async_trait]
impl<E: EventStore + 'static> TransactionalStore for Stores<E> {
    type Events = E;
    type States = MockStateStore;

    fn event_store(&self) -> &E {
        &self.events
    }

    fn state_store(&self) -> &MockStateStore {
        &self.states
    }

    async fn commit_atomic(&self, commit: AtomicCommit) -> Result<()> {
        let _serialized = self.lock.lock().await;
        let stream_checks = commit
            .stream_writes
            .iter()
            .map(|write| (&write.stream_id, write.expected_version))
            .chain(commit.stream_expectations.iter().map(|(id, v)| (id, *v)));
        for (stream_id, expected) in stream_checks {
            ensure_at(self.events.get_version(stream_id.clone()).await?, expected)?;
        }
        let state_rows: Vec<(StreamId, AggregateVersion)> = commit
            .state_writes
            .iter()
            .map(|write| (write.state.stream_id(), write.expected_version))
            .chain(commit.state_expectations.iter().cloned())
            .collect();
        for (stream_id, expected) in state_rows {
            ensure_at(self.states.get_version(stream_id).await?, expected)?;
        }
        for c in commit.stream_writes {
            self.events
                .append(
                    c.stream_id,
                    c.events,
                    c.expected_version,
                    c.claims,
                    c.clear_claims,
                )
                .await?;
        }
        for write in commit.state_writes {
            self.states.save(write).await?;
        }
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A unit-of-work store over fresh [`Stores`], with [`SimpleTestEntity`]
/// event-sourced and [`Tally`] state-stored.
pub(super) fn uow_store() -> UnitOfWorkStore<Stores> {
    UnitOfWorkStore::new(Stores::new())
        .event_sourced::<SimpleTestEntity>()
        .state_stored::<Tally>()
}

/// A second aggregate type, so one unit of work can mix persistence styles.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Tally {
    id: EntityId,
    pub(super) total: i64,
}

impl crate::Entity for Tally {
    fn new(id: EntityId) -> Self {
        Self { id, total: 0 }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl crate::DefaultEntity for Tally {}

#[derive(Debug, thiserror::Error)]
#[error("tally error")]
pub(super) struct TallyError;

impl AggregateError for TallyError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum TallyEvent {
    Added { amount: i64 },
    Closed,
}

impl crate::DomainEvent for TallyEvent {
    type Aggregate = Tally;

    fn event_type(&self) -> &'static str {
        match self {
            Self::Added { .. } => "Tally.Added",
            Self::Closed => "Tally.Closed",
        }
    }

    fn event_version(&self) -> EventVersion {
        EventVersion::new(1)
    }

    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl ApplyEvent<Tally> for TallyEvent {
    fn apply(&self, tally: &mut Tally) {
        if let Self::Added { amount } = self {
            tally.total += amount;
        }
    }
}

impl EventApplicator<Tally> for TallyEvent {
    fn dispatch(&self, tally: &mut Tally) -> std::result::Result<(), TallyError> {
        ApplyEvent::apply(self, tally);
        Ok(())
    }

    fn dispatch_unchecked(&self, tally: &mut Tally) {
        ApplyEvent::apply(self, tally);
    }

    fn is_delete(&self) -> bool {
        matches!(self, Self::Closed)
    }

    fn dispatch_delete(&self, tally: Tally) -> std::result::Result<Tally, TallyError> {
        Ok(tally)
    }

    fn dispatch_delete_unchecked(&self, tally: Tally) -> Tally {
        tally
    }
}

/// Deletes a [`Tally`].
pub(super) struct TallyClosed;

impl crate::DeleteEvent<Tally> for TallyClosed {
    fn delete(&self, tally: Tally) -> Tally {
        tally
    }
}

impl From<TallyClosed> for TallyEvent {
    fn from(_: TallyClosed) -> Self {
        Self::Closed
    }
}

impl crate::Aggregate for Tally {
    type Event = TallyEvent;
    type Error = TallyError;
    type DeletedState = Self;
}
