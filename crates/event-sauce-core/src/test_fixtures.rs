//! Shared test fixtures for event-sauce-core tests.
//!
//! Provides reusable test types and mocks to reduce scaffolding duplication
//! across test modules.

#[cfg(any(feature = "event-sourcing", feature = "state-store"))]
use async_trait::async_trait;
use chrono::Utc;
#[cfg(feature = "event-sourcing")]
use futures::stream;
use serde::{Deserialize, Serialize};
#[cfg(any(feature = "event-sourcing", feature = "state-store"))]
use std::collections::HashMap;
#[cfg(feature = "event-sourcing")]
use std::sync::Arc;
#[cfg(any(feature = "event-sourcing", feature = "state-store"))]
use std::sync::{Mutex, RwLock};

use crate::{AggregateError, ApplyEvent, EntityId, EventApplicator, EventEnvelope, EventVersion};
#[cfg(any(feature = "event-sourcing", feature = "state-store"))]
use crate::{AggregateVersion, Result, StreamId};
#[cfg(feature = "event-sourcing")]
use crate::{CheckpointStore, EventLogEntry, EventStore, Position};
#[cfg(feature = "state-store")]
use crate::{StateCommit, StateStore, StoredState};

/// Simple error type for test aggregates.
#[derive(Debug, thiserror::Error)]
#[error("test error")]
pub struct SimpleTestError;

impl AggregateError for SimpleTestError {}

/// Simple entity for testing aggregate behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleTestEntity {
    /// Entity identifier.
    pub id: EntityId,
    /// A simple integer value for testing state changes.
    pub value: i32,
}

impl crate::Entity for SimpleTestEntity {
    fn new(id: EntityId) -> Self {
        Self { id, value: 0 }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl crate::DefaultEntity for SimpleTestEntity {}

/// Events for the simple test entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SimpleTestEvent {
    /// Entity was created with a value.
    Created {
        /// Initial value.
        value: i32,
    },
    /// Entity value was updated.
    Updated {
        /// New value.
        value: i32,
    },
    /// Entity was deleted.
    Deleted,
    /// Always rejected on apply; used to test poisoning paths.
    Rejected,
}

impl crate::DomainEvent for SimpleTestEvent {
    type Aggregate = SimpleTestEntity;

    fn event_type(&self) -> &'static str {
        match self {
            Self::Created { .. } => "SimpleTestEntity.Created",
            Self::Updated { .. } => "SimpleTestEntity.Updated",
            Self::Deleted => "SimpleTestEntity.Deleted",
            Self::Rejected => "SimpleTestEntity.Rejected",
        }
    }

    fn event_version(&self) -> EventVersion {
        EventVersion::new(1)
    }

    fn occurred_at(&self) -> chrono::DateTime<Utc> {
        Utc::now()
    }
}

impl ApplyEvent<SimpleTestEntity> for SimpleTestEvent {
    fn apply(&self, entity: &mut SimpleTestEntity) {
        match self {
            Self::Created { value } | Self::Updated { value } => {
                entity.value = *value;
            }
            Self::Deleted | Self::Rejected => {}
        }
    }
}

impl EventApplicator<SimpleTestEntity> for SimpleTestEvent {
    fn dispatch(&self, entity: &mut SimpleTestEntity) -> std::result::Result<(), SimpleTestError> {
        if matches!(self, Self::Rejected) {
            return Err(SimpleTestError);
        }
        self.apply(entity);
        Ok(())
    }

    fn dispatch_unchecked(&self, entity: &mut SimpleTestEntity) {
        self.apply(entity);
    }

    fn is_delete(&self) -> bool {
        matches!(self, Self::Deleted)
    }

    fn dispatch_delete(
        &self,
        entity: SimpleTestEntity,
    ) -> std::result::Result<SimpleTestEntity, SimpleTestError> {
        Ok(entity)
    }

    fn dispatch_delete_unchecked(&self, entity: SimpleTestEntity) -> SimpleTestEntity {
        entity
    }
}

/// Init event fixture for [`SimpleTestEntity`].
#[derive(Debug, Clone)]
pub struct SimpleTestInit {
    /// Initial value.
    pub value: i32,
}

impl crate::InitEvent<SimpleTestEntity> for SimpleTestInit {
    fn init(&self, id: EntityId) -> SimpleTestEntity {
        SimpleTestEntity {
            id,
            value: self.value,
        }
    }
}

impl From<SimpleTestInit> for SimpleTestEvent {
    fn from(event: SimpleTestInit) -> Self {
        Self::Created { value: event.value }
    }
}

/// Delete event fixture for [`SimpleTestEntity`].
#[derive(Debug, Clone)]
pub struct SimpleTestDelete;

impl crate::DeleteEvent<SimpleTestEntity> for SimpleTestDelete {
    fn delete(&self, entity: SimpleTestEntity) -> SimpleTestEntity {
        entity
    }
}

impl From<SimpleTestDelete> for SimpleTestEvent {
    fn from(_event: SimpleTestDelete) -> Self {
        Self::Deleted
    }
}

impl crate::Aggregate for SimpleTestEntity {
    type Event = SimpleTestEvent;
    type Error = SimpleTestError;
    type DeletedState = Self;
}

/// HashMap-based mock event store for testing.
///
/// Tracks events per-stream (for `load_stream`) and globally (for `stream_all`),
/// preserving insertion order across streams.
#[cfg(feature = "event-sourcing")]
#[derive(Debug)]
pub struct MockEventStore {
    streams: Arc<Mutex<HashMap<StreamId, Vec<EventEnvelope>>>>,
    /// Global log paired with the store-issued position (1-based, dense).
    global_log: Arc<Mutex<Vec<(Position, EventEnvelope)>>>,
}

#[cfg(feature = "event-sourcing")]
impl Default for MockEventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "event-sourcing")]
impl MockEventStore {
    /// Creates a new empty mock event store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            global_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns all events in the global log (for test verification).
    #[must_use]
    pub fn get_events(&self) -> Vec<EventEnvelope> {
        self.global_log
            .lock()
            .unwrap()
            .iter()
            .map(|(_, e)| e.clone())
            .collect()
    }

    /// Assigns the next 1-based position and pushes the event onto the global
    /// log. Positions stay dense for the mock — gaps are a real-backend concern.
    fn push_global(log: &mut Vec<(Position, EventEnvelope)>, event: EventEnvelope) {
        #[allow(clippy::cast_possible_wrap)]
        let position = Position::new(log.len() as i64 + 1);
        log.push((position, event));
    }

    /// Adds an event directly (bypassing append), useful for subscription tests.
    pub fn add_event(&self, event: EventEnvelope) {
        Self::push_global(&mut self.global_log.lock().unwrap(), event.clone());
        self.streams
            .lock()
            .unwrap()
            .entry(StreamId::new(
                event.aggregate_type.clone(),
                event.aggregate_id,
            ))
            .or_default()
            .push(event);
    }
}

#[cfg(feature = "event-sourcing")]
#[async_trait]
impl EventStore for MockEventStore {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        _expected_version: AggregateVersion,
        _claims: Vec<crate::AggregateClaim>,
        _clear_claims: bool,
    ) -> Result<()> {
        {
            let mut log = self.global_log.lock().unwrap();
            for event in &events {
                Self::push_global(&mut log, event.clone());
            }
        }
        let mut streams = self.streams.lock().unwrap();
        streams.entry(stream_id).or_default().extend(events);
        Ok(())
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl futures::Stream<Item = Result<EventEnvelope>> + Send> {
        let streams = self.streams.lock().unwrap();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let events = streams
            .get(&stream_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .skip(from_version.as_i64() as usize)
            .map(Ok)
            .collect::<Vec<_>>();
        Ok(stream::iter(events))
    }

    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl futures::Stream<Item = Result<EventLogEntry>> + Send> {
        let entries: Vec<_> = self
            .global_log
            .lock()
            .unwrap()
            .iter()
            .filter(|(position, _)| *position > from_position)
            .map(|(position, envelope)| {
                Ok(EventLogEntry {
                    position: *position,
                    envelope: envelope.clone(),
                })
            })
            .collect();
        Ok(stream::iter(entries))
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        let streams = self.streams.lock().unwrap();
        let count = streams.get(&stream_id).map_or(0, std::vec::Vec::len);
        #[allow(clippy::cast_possible_wrap)]
        Ok(AggregateVersion::new(count as i64))
    }

    async fn load_snapshot(&self, _stream_id: StreamId) -> Result<Option<crate::Snapshot>> {
        Ok(None)
    }

    async fn save_snapshot(&self, _snapshot: crate::Snapshot) -> Result<()> {
        Ok(())
    }

    fn snapshot_config(&self) -> &crate::SnapshotConfig {
        static CONFIG: std::sync::OnceLock<crate::SnapshotConfig> = std::sync::OnceLock::new();
        CONFIG.get_or_init(crate::SnapshotConfig::disabled)
    }
}

/// Mock checkpoint store for subscription tests.
#[cfg(feature = "event-sourcing")]
#[derive(Clone)]
pub struct MockCheckpointStore {
    checkpoints: Arc<RwLock<HashMap<String, MockCheckpointEntry>>>,
}

#[cfg(feature = "event-sourcing")]
#[derive(Clone)]
struct MockCheckpointEntry {
    position: Position,
    lease: Option<MockLease>,
}

#[cfg(feature = "event-sourcing")]
impl Default for MockCheckpointEntry {
    fn default() -> Self {
        Self {
            position: Position::start(),
            lease: None,
        }
    }
}

#[cfg(feature = "event-sourcing")]
#[derive(Clone)]
struct MockLease {
    worker_id: String,
    expires_at: std::time::Instant,
}

#[cfg(feature = "event-sourcing")]
impl Default for MockCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "event-sourcing")]
impl MockCheckpointStore {
    /// Creates a new empty mock checkpoint store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Gets the checkpoint for a subscription.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Position> {
        self.checkpoints
            .read()
            .unwrap()
            .get(name)
            .map(|entry| entry.position)
    }
}

#[cfg(feature = "event-sourcing")]
#[async_trait]
impl CheckpointStore for MockCheckpointStore {
    async fn save_checkpoint(&self, subscription_name: &str, position: Position) -> Result<()> {
        let mut guard = self.checkpoints.write().unwrap();
        let entry = guard.entry(subscription_name.to_string()).or_default();
        entry.position = position;
        Ok(())
    }

    async fn load_checkpoint(&self, subscription_name: &str) -> Result<Option<Position>> {
        Ok(self
            .checkpoints
            .read()
            .unwrap()
            .get(subscription_name)
            .map(|entry| entry.position))
    }

    async fn delete_checkpoint(&self, subscription_name: &str) -> Result<()> {
        self.checkpoints.write().unwrap().remove(subscription_name);
        Ok(())
    }

    async fn try_acquire_lease(
        &self,
        subscription_name: &str,
        worker_id: &str,
        lease_duration: std::time::Duration,
    ) -> Result<Option<Position>> {
        let mut guard = self.checkpoints.write().unwrap();
        let now = std::time::Instant::now();
        let entry = guard.entry(subscription_name.to_string()).or_default();

        let can_take = match &entry.lease {
            None => true,
            Some(lease) => lease.expires_at <= now || lease.worker_id == worker_id,
        };
        if !can_take {
            return Ok(None);
        }
        entry.lease = Some(MockLease {
            worker_id: worker_id.to_string(),
            expires_at: now + lease_duration,
        });
        Ok(Some(entry.position))
    }

    async fn renew_lease(
        &self,
        subscription_name: &str,
        worker_id: &str,
        lease_duration: std::time::Duration,
    ) -> Result<()> {
        let mut guard = self.checkpoints.write().unwrap();
        let now = std::time::Instant::now();
        let Some(entry) = guard.get_mut(subscription_name) else {
            return Err(crate::Error::custom(format!(
                "lease lost: no entry for {subscription_name}"
            )));
        };
        match &entry.lease {
            Some(lease) if lease.worker_id == worker_id && lease.expires_at > now => {
                entry.lease = Some(MockLease {
                    worker_id: worker_id.to_string(),
                    expires_at: now + lease_duration,
                });
                Ok(())
            }
            _ => Err(crate::Error::custom(format!(
                "lease lost: not held by {worker_id}"
            ))),
        }
    }

    async fn release_lease(&self, subscription_name: &str, worker_id: &str) -> Result<()> {
        let mut guard = self.checkpoints.write().unwrap();
        if let Some(entry) = guard.get_mut(subscription_name) {
            if let Some(lease) = &entry.lease {
                if lease.worker_id == worker_id {
                    entry.lease = None;
                }
            }
        }
        Ok(())
    }
}

/// Creates a test event envelope with the given event and aggregate types.
#[must_use]
pub fn create_test_envelope(event_type: &str, aggregate_type: &str) -> EventEnvelope {
    EventEnvelope::new(
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        aggregate_type.to_string(),
        event_type.to_string(),
        EventVersion::new(1),
        serde_json::json!({}),
    )
}

/// HashMap-based mock state store for testing.
///
/// Enforces optimistic concurrency on save; claims and projections are
/// backend concerns and are not modeled here.
#[cfg(feature = "state-store")]
#[derive(Debug, Default)]
pub struct MockStateStore {
    states: RwLock<HashMap<StreamId, StoredState>>,
    commits: Mutex<Vec<StateCommit>>,
}

#[cfg(feature = "state-store")]
impl MockStateStore {
    /// Creates an empty mock state store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a raw state row, bypassing version checks (for test setup).
    pub fn insert_raw(&self, state: StoredState) {
        self.states
            .write()
            .unwrap()
            .insert(state.stream_id(), state);
    }

    /// Returns every commit accepted by [`save`](StateStore::save), in order.
    #[must_use]
    pub fn recorded_commits(&self) -> Vec<StateCommit> {
        self.commits.lock().unwrap().clone()
    }
}

#[cfg(feature = "state-store")]
#[async_trait]
impl StateStore for MockStateStore {
    async fn load(&self, stream_id: StreamId) -> Result<Option<StoredState>> {
        Ok(self.states.read().unwrap().get(&stream_id).cloned())
    }

    async fn save(&self, commit: StateCommit) -> Result<()> {
        let mut states = self.states.write().unwrap();
        let key = commit.state.stream_id();
        let current = states
            .get(&key)
            .map_or_else(AggregateVersion::initial, |state| state.version);
        if current != commit.expected_version {
            return Err(crate::Error::concurrency_conflict(
                commit.expected_version,
                current,
            ));
        }
        states.insert(key, commit.state.clone());
        self.commits.lock().unwrap().push(commit);
        Ok(())
    }
}

/// A simple counter entity for testing and doc examples.
#[derive(Debug, Serialize, Deserialize)]
pub struct TestCounter {
    /// The entity's ID.
    pub id: EntityId,
    /// The counter value.
    pub value: i32,
}

impl crate::Entity for TestCounter {
    fn new(id: EntityId) -> Self {
        Self { id, value: 0 }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl crate::DefaultEntity for TestCounter {}

/// Events for the test counter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TestCounterEvent {
    /// Value was incremented.
    Incremented {
        /// Amount to increment by.
        amount: i32,
    },
}

impl crate::DomainEvent for TestCounterEvent {
    type Aggregate = TestCounter;
    fn event_type(&self) -> &'static str {
        "TestCounter.Incremented"
    }
    fn event_version(&self) -> EventVersion {
        EventVersion::new(1)
    }
    fn occurred_at(&self) -> chrono::DateTime<Utc> {
        Utc::now()
    }
}

impl EventApplicator<TestCounter> for TestCounterEvent {
    fn dispatch(&self, counter: &mut TestCounter) -> std::result::Result<(), TestCounterError> {
        match self {
            TestCounterEvent::Incremented { amount } => counter.value += amount,
        }
        Ok(())
    }
    fn dispatch_unchecked(&self, counter: &mut TestCounter) {
        match self {
            TestCounterEvent::Incremented { amount } => counter.value += amount,
        }
    }
}

/// Error type for the test counter.
#[derive(Debug, thiserror::Error)]
#[error("Test counter error")]
pub struct TestCounterError;

impl AggregateError for TestCounterError {}

impl crate::Aggregate for TestCounter {
    type Event = TestCounterEvent;
    type Error = TestCounterError;
    type DeletedState = Self;
}

#[cfg(all(test, feature = "event-sourcing"))]
mod tests {
    use super::*;
    use crate::{
        AggregateRoot, CheckpointStore, DefaultEntity, DomainEvent, Entity, EventApplicator,
        EventStore, Position, StreamId,
    };
    use futures::StreamExt;

    // --- MockEventStore Default ---

    #[test]
    fn mock_event_store_default_creates_empty_store() {
        let store = MockEventStore::default();
        assert!(store.streams.lock().unwrap().is_empty());
        assert!(store.global_log.lock().unwrap().is_empty());
    }

    // --- MockCheckpointStore Default ---

    #[test]
    fn mock_checkpoint_store_default_creates_empty_store() {
        let store = MockCheckpointStore::default();
        assert!(store.get("anything").is_none());
    }

    // --- MockCheckpointStore::delete_checkpoint ---

    #[tokio::test]
    async fn mock_checkpoint_store_delete_removes_checkpoint() {
        let store = MockCheckpointStore::new();
        store
            .save_checkpoint("sub", Position::new(5))
            .await
            .unwrap();
        assert_eq!(store.get("sub"), Some(Position::new(5)));

        store.delete_checkpoint("sub").await.unwrap();
        assert!(store.get("sub").is_none());
    }

    // --- MockEventStore::stream_all ---

    #[tokio::test]
    async fn mock_event_store_stream_all_returns_events_in_order() {
        let store = MockEventStore::new();
        let env1 = create_test_envelope("Ev1", "Agg");
        let env2 = create_test_envelope("Ev2", "Agg");
        store.add_event(env1.clone());
        store.add_event(env2.clone());

        let stream = store.stream_all(Position::new(0)).await.unwrap();
        let entries: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
        assert_eq!(entries.len(), 2);
        // Positions are store-issued, 1-based, and strictly monotonic.
        assert_eq!(entries[0].position, Position::new(1));
        assert_eq!(entries[0].envelope.event_type, env1.event_type);
        assert_eq!(entries[1].position, Position::new(2));
        assert_eq!(entries[1].envelope.event_type, env2.event_type);
    }

    #[tokio::test]
    async fn mock_event_store_stream_all_skips_with_offset() {
        let store = MockEventStore::new();
        store.add_event(create_test_envelope("Ev1", "Agg"));
        store.add_event(create_test_envelope("Ev2", "Agg"));

        // Strictly greater than position 1, so only the second event remains.
        let stream = store.stream_all(Position::new(1)).await.unwrap();
        let entries: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].position, Position::new(2));
    }

    // --- MockEventStore::get_version ---

    #[tokio::test]
    async fn mock_event_store_get_version_empty_stream() {
        let store = MockEventStore::new();
        let stream_id = StreamId::new("Test", uuid::Uuid::new_v4());
        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version.as_i64(), 0);
    }

    #[tokio::test]
    async fn mock_event_store_get_version_after_append() {
        let store = MockEventStore::new();
        let id = uuid::Uuid::new_v4();
        let stream_id = StreamId::new("Test", id);
        let env = create_test_envelope("Ev1", "Test");
        store
            .append(
                stream_id.clone(),
                vec![env],
                crate::AggregateVersion::new(0),
                vec![],
                false,
            )
            .await
            .unwrap();
        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version.as_i64(), 1);
    }

    // --- MockEventStore snapshot methods ---

    #[tokio::test]
    async fn mock_event_store_load_snapshot_returns_none() {
        let store = MockEventStore::new();
        let stream_id = StreamId::new("Test", uuid::Uuid::new_v4());
        assert!(store.load_snapshot(stream_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mock_event_store_save_snapshot_succeeds() {
        let store = MockEventStore::new();
        let snapshot = crate::Snapshot::new(
            uuid::Uuid::new_v4(),
            "Test",
            crate::AggregateVersion::new(1),
            serde_json::json!({}),
        );
        store.save_snapshot(snapshot).await.unwrap();
    }

    #[test]
    fn mock_event_store_snapshot_config_is_disabled() {
        let store = MockEventStore::new();
        assert!(!store.snapshot_config().use_snapshots_on_load());
    }

    // --- MockEventStore::get_events ---

    #[test]
    fn mock_event_store_get_events_returns_all_events() {
        let store = MockEventStore::new();
        let env1 = create_test_envelope("Ev1", "Agg");
        let env2 = create_test_envelope("Ev2", "Agg");
        store.add_event(env1.clone());
        store.add_event(env2.clone());

        let events = store.get_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, env1.id);
        assert_eq!(events[1].id, env2.id);
    }

    #[test]
    fn mock_event_store_get_events_empty() {
        let store = MockEventStore::new();
        assert!(store.get_events().is_empty());
    }

    // --- SimpleTestEvent::dispatch_unchecked ---

    #[test]
    fn simple_test_event_dispatch_unchecked_applies_created() {
        let mut entity = SimpleTestEntity::new(EntityId::new());
        let event = SimpleTestEvent::Created { value: 42 };
        EventApplicator::dispatch_unchecked(&event, &mut entity);
        assert_eq!(entity.value, 42);
    }

    #[test]
    fn simple_test_event_dispatch_unchecked_applies_updated() {
        let mut entity = SimpleTestEntity::new(EntityId::new());
        let event = SimpleTestEvent::Updated { value: 99 };
        EventApplicator::dispatch_unchecked(&event, &mut entity);
        assert_eq!(entity.value, 99);
    }

    // --- SimpleTestError ---

    #[test]
    fn simple_test_error_display() {
        let err = SimpleTestError;
        assert_eq!(err.to_string(), "test error");
    }

    // --- TestCounter ---

    #[test]
    fn test_counter_entity_new() {
        let id = EntityId::new();
        let counter = TestCounter::new(id);
        assert_eq!(counter.entity_id(), id);
        assert_eq!(counter.value, 0);
    }

    #[test]
    fn test_counter_default_entity() {
        // Verify TestCounter implements DefaultEntity (compile-time check exercised at runtime)
        fn assert_default_entity<T: DefaultEntity>() {}
        assert_default_entity::<TestCounter>();
    }

    // --- TestCounterEvent ---

    #[test]
    fn test_counter_event_type() {
        let event = TestCounterEvent::Incremented { amount: 1 };
        assert_eq!(event.event_type(), "TestCounter.Incremented");
    }

    #[test]
    fn test_counter_event_version() {
        let event = TestCounterEvent::Incremented { amount: 1 };
        assert_eq!(event.event_version(), EventVersion::new(1));
    }

    #[test]
    fn test_counter_event_occurred_at() {
        let before = Utc::now();
        let event = TestCounterEvent::Incremented { amount: 1 };
        let at = event.occurred_at();
        let after = Utc::now();
        assert!(at >= before && at <= after);
    }

    #[test]
    fn test_counter_event_dispatch() {
        let mut counter = TestCounter::new(EntityId::new());
        let event = TestCounterEvent::Incremented { amount: 7 };
        EventApplicator::dispatch(&event, &mut counter).unwrap();
        assert_eq!(counter.value, 7);
    }

    #[test]
    fn test_counter_event_dispatch_unchecked() {
        let mut counter = TestCounter::new(EntityId::new());
        let event = TestCounterEvent::Incremented { amount: 3 };
        EventApplicator::dispatch_unchecked(&event, &mut counter);
        assert_eq!(counter.value, 3);
    }

    // --- TestCounterError ---

    #[test]
    fn test_counter_error_display() {
        let err = TestCounterError;
        assert_eq!(err.to_string(), "Test counter error");
    }

    // --- TestCounter as Aggregate ---

    #[test]
    fn test_counter_aggregate_apply() {
        let id = EntityId::new();
        let mut agg = AggregateRoot::<TestCounter>::new(id);
        agg.apply(TestCounterEvent::Incremented { amount: 5 })
            .unwrap();
        assert_eq!(agg.entity().value, 5);
    }
}
