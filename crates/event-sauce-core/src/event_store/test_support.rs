//! Shared test doubles for the commit and load pipelines: in-memory event
//! stores, crypto mocks, and small encrypted/deletable aggregate fixtures.

use super::*;
use crate::EntityId;
use async_trait::async_trait;
use futures::stream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub(crate) struct MockEventStoreWithStreams {
    streams: std::collections::HashMap<StreamId, Vec<EventEnvelope>>,
}

impl MockEventStoreWithStreams {
    pub(crate) fn new() -> Self {
        Self {
            streams: std::collections::HashMap::new(),
        }
    }

    pub(crate) fn with_stream(mut self, stream_id: StreamId, count: usize) -> Self {
        let mut events = Vec::new();
        for i in 0..count {
            #[allow(clippy::cast_possible_wrap)]
            let envelope = EventEnvelope::new(
                Uuid::new_v4(),
                stream_id.aggregate_id(),
                stream_id.aggregate_type().to_string(),
                "TestEvent".to_string(),
                crate::EventVersion::new(i as i64 + 1),
                serde_json::json!({"index": i}),
            );
            events.push(envelope);
        }
        self.streams.insert(stream_id, events);
        self
    }
}

#[async_trait]
impl EventStore for MockEventStoreWithStreams {
    async fn append(
        &self,
        _stream_id: StreamId,
        _events: Vec<EventEnvelope>,
        _expected_version: AggregateVersion,
        _claims: Vec<AggregateClaim>,
        _clear_claims: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let events = self
            .streams
            .get(&stream_id)
            .map(|events| {
                events
                    .iter()
                    .skip(from_version.as_i64() as usize)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Ok(stream::iter(events.into_iter().map(Ok)))
    }

    async fn stream_all(
        &self,
        _from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventLogEntry>> + Send> {
        Ok(stream::empty())
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        #[allow(clippy::cast_possible_wrap)]
        let version = self
            .streams
            .get(&stream_id)
            .map_or(AggregateVersion::initial(), |events| {
                AggregateVersion::new(events.len() as i64)
            });
        Ok(version)
    }
}

/// Mock event store that supports snapshots, tracks `append`/`save_snapshot` calls,
/// and can be configured to fail on `save_snapshot`.
pub(crate) struct CommitTestStore {
    pub(crate) streams: Arc<Mutex<HashMap<StreamId, Vec<EventEnvelope>>>>,
    pub(crate) snapshots: Arc<Mutex<HashMap<StreamId, Snapshot>>>,
    config: SnapshotConfig,
    append_count: Arc<Mutex<u32>>,
    save_snapshot_count: Arc<Mutex<u32>>,
    fail_save_snapshot: bool,
    /// Number of `append` calls left to fail with a `ConcurrencyConflict`
    /// before it starts succeeding, for cancel/retry-safety tests.
    fail_append_times: Arc<Mutex<u32>>,
    /// When `true`, `append` never resolves — for cancellation-safety
    /// tests that drop the in-flight commit future.
    hang_append: bool,
    key_store: Option<Arc<dyn crate::CryptoKeyStore>>,
    provider: Option<Arc<dyn crate::CryptoProvider>>,
}

impl CommitTestStore {
    pub(crate) fn new(config: SnapshotConfig) -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            config,
            append_count: Arc::new(Mutex::new(0)),
            save_snapshot_count: Arc::new(Mutex::new(0)),
            fail_save_snapshot: false,
            fail_append_times: Arc::new(Mutex::new(0)),
            hang_append: false,
            key_store: None,
            provider: None,
        }
    }

    /// Installs a crypto key store and provider.
    pub(crate) fn with_crypto(
        mut self,
        key_store: Arc<dyn crate::CryptoKeyStore>,
        provider: Arc<dyn crate::CryptoProvider>,
    ) -> Self {
        self.key_store = Some(key_store);
        self.provider = Some(provider);
        self
    }

    pub(crate) fn with_fail_save_snapshot(mut self) -> Self {
        self.fail_save_snapshot = true;
        self
    }

    pub(crate) fn with_fail_append_times(mut self, times: u32) -> Self {
        self.fail_append_times = Arc::new(Mutex::new(times));
        self
    }

    pub(crate) fn with_hang_append(mut self) -> Self {
        self.hang_append = true;
        self
    }

    pub(crate) fn append_count(&self) -> u32 {
        *self.append_count.lock().unwrap()
    }

    pub(crate) fn save_snapshot_count(&self) -> u32 {
        *self.save_snapshot_count.lock().unwrap()
    }
}

#[async_trait]
impl EventStore for CommitTestStore {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        _claims: Vec<AggregateClaim>,
        _clear_claims: bool,
    ) -> Result<()> {
        *self.append_count.lock().unwrap() += 1;
        if self.hang_append {
            futures::future::pending::<()>().await;
        }
        {
            let mut remaining = self.fail_append_times.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(crate::Error::concurrency_conflict(
                    expected_version,
                    AggregateVersion::new(expected_version.as_i64() + 1),
                ));
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
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
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
        _from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventLogEntry>> + Send> {
        Ok(stream::empty())
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        let streams = self.streams.lock().unwrap();
        let count = streams.get(&stream_id).map_or(0, Vec::len);
        #[allow(clippy::cast_possible_wrap)]
        Ok(AggregateVersion::new(count as i64))
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        *self.save_snapshot_count.lock().unwrap() += 1;
        if self.fail_save_snapshot {
            return Err(crate::Error::custom("Snapshot save failed"));
        }
        let stream_id = StreamId::new(snapshot.aggregate_type.clone(), snapshot.aggregate_id);
        self.snapshots.lock().unwrap().insert(stream_id, snapshot);
        Ok(())
    }

    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        Ok(self.snapshots.lock().unwrap().get(&stream_id).cloned())
    }

    fn snapshot_config(&self) -> &SnapshotConfig {
        &self.config
    }

    fn crypto_key_store(&self) -> Option<&dyn crate::CryptoKeyStore> {
        self.key_store.as_deref()
    }

    fn crypto_provider(&self) -> Option<&dyn crate::CryptoProvider> {
        self.provider.as_deref()
    }
}

/// Fully-encrypted aggregate: [`Aggregate::is_encrypted`] is `true`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SecretThing {
    id: EntityId,
    pub(crate) email: String,
}

impl crate::Entity for SecretThing {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            email: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl crate::DefaultEntity for SecretThing {}

#[derive(Debug, thiserror::Error)]
#[error("secret thing error")]
pub(crate) struct SecretThingError;

impl crate::AggregateError for SecretThingError {}

impl Aggregate for SecretThing {
    type Event = SecretCreated;
    type Error = SecretThingError;
    type DeletedState = Self;

    fn is_encrypted() -> bool {
        true
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct SecretCreated {
    pub(crate) email: String,
}

impl crate::DomainEvent for SecretCreated {
    type Aggregate = SecretThing;
    fn event_type(&self) -> &'static str {
        "SecretThing.Created"
    }
    fn event_version(&self) -> crate::EventVersion {
        crate::EventVersion::new(1)
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl crate::ApplyEvent<SecretThing> for SecretCreated {
    fn apply(&self, entity: &mut SecretThing) {
        entity.email.clone_from(&self.email);
    }
}

impl crate::EventApplicator<SecretThing> for SecretCreated {
    fn dispatch(&self, entity: &mut SecretThing) -> std::result::Result<(), SecretThingError> {
        crate::ApplyEvent::apply(self, entity);
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut SecretThing) {
        crate::ApplyEvent::apply(self, entity);
    }
}

/// Field-encrypted (not fully encrypted) aggregate: only `secret` is
/// declared as an encrypted field.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PartialSecretThing {
    id: EntityId,
    pub(crate) public: String,
    pub(crate) secret: String,
}

impl crate::Entity for PartialSecretThing {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            public: String::new(),
            secret: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl crate::DefaultEntity for PartialSecretThing {}

#[derive(Debug, thiserror::Error)]
#[error("partial secret thing error")]
pub(crate) struct PartialSecretThingError;

impl crate::AggregateError for PartialSecretThingError {}

impl Aggregate for PartialSecretThing {
    type Event = PartialSecretCreated;
    type Error = PartialSecretThingError;
    type DeletedState = Self;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PartialSecretCreated {
    pub(crate) public: String,
    pub(crate) secret: String,
}

impl crate::DomainEvent for PartialSecretCreated {
    type Aggregate = PartialSecretThing;
    fn event_type(&self) -> &'static str {
        "PartialSecretThing.Created"
    }
    fn event_version(&self) -> crate::EventVersion {
        crate::EventVersion::new(1)
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
    fn encrypted_fields(&self) -> &'static [&'static str] {
        &["secret"]
    }
    fn has_any_encrypted_fields() -> bool {
        true
    }
}

impl crate::ApplyEvent<PartialSecretThing> for PartialSecretCreated {
    fn apply(&self, entity: &mut PartialSecretThing) {
        entity.public.clone_from(&self.public);
        entity.secret.clone_from(&self.secret);
    }
}

impl crate::EventApplicator<PartialSecretThing> for PartialSecretCreated {
    fn dispatch(
        &self,
        entity: &mut PartialSecretThing,
    ) -> std::result::Result<(), PartialSecretThingError> {
        crate::ApplyEvent::apply(self, entity);
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut PartialSecretThing) {
        crate::ApplyEvent::apply(self, entity);
    }
}

/// In-memory [`crate::CryptoKeyStore`] with an atomic `get_or_insert_key`,
/// so it can stand in for a real backend in concurrency-sensitive
/// assertions.
#[derive(Default)]
pub(crate) struct MockCryptoKeyStore {
    keys: Mutex<HashMap<Uuid, Vec<u8>>>,
    shredded: Mutex<std::collections::HashSet<Uuid>>,
}

#[async_trait]
impl crate::CryptoKeyStore for MockCryptoKeyStore {
    async fn get_key(&self, aggregate_id: Uuid) -> Result<Option<Vec<u8>>> {
        Ok(self.keys.lock().unwrap().get(&aggregate_id).cloned())
    }

    async fn upsert_key(&self, aggregate_id: Uuid, key: Vec<u8>) -> Result<()> {
        self.shredded.lock().unwrap().remove(&aggregate_id);
        self.keys.lock().unwrap().insert(aggregate_id, key);
        Ok(())
    }

    async fn delete_key(&self, aggregate_id: Uuid) -> Result<()> {
        self.keys.lock().unwrap().remove(&aggregate_id);
        self.shredded.lock().unwrap().insert(aggregate_id);
        Ok(())
    }

    async fn get_or_insert_key(&self, aggregate_id: Uuid, candidate: Vec<u8>) -> Result<Vec<u8>> {
        if self.shredded.lock().unwrap().contains(&aggregate_id) {
            return Err(crate::Error::key_not_found(aggregate_id));
        }
        let mut keys = self.keys.lock().unwrap();
        Ok(keys.entry(aggregate_id).or_insert(candidate).clone())
    }

    async fn is_shredded(&self, aggregate_id: Uuid) -> Result<bool> {
        Ok(self.shredded.lock().unwrap().contains(&aggregate_id))
    }
}

/// A [`MockCryptoKeyStore`] whose `get_or_insert_key` always returns a
/// fixed key, as if another writer had already won the race — used to
/// prove `ensure_crypto_key` returns the winning key, not its own
/// candidate.
pub(crate) struct AlwaysWinsKeyStore {
    pub(crate) winning_key: Vec<u8>,
}

#[async_trait]
impl crate::CryptoKeyStore for AlwaysWinsKeyStore {
    async fn get_key(&self, _aggregate_id: Uuid) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn upsert_key(&self, _aggregate_id: Uuid, _key: Vec<u8>) -> Result<()> {
        Ok(())
    }
    async fn delete_key(&self, _aggregate_id: Uuid) -> Result<()> {
        Ok(())
    }
    async fn get_or_insert_key(&self, _aggregate_id: Uuid, _candidate: Vec<u8>) -> Result<Vec<u8>> {
        Ok(self.winning_key.clone())
    }
    async fn is_shredded(&self, _aggregate_id: Uuid) -> Result<bool> {
        Ok(false)
    }
}

/// Builds a [`CommitTestStore`] with snapshots disabled and a fresh
/// [`MockCryptoKeyStore`] plus
/// [`AadCheckingCryptoProvider`](crate::test_fixtures::AadCheckingCryptoProvider)
/// installed.
pub(crate) fn crypto_test_store() -> (CommitTestStore, Arc<MockCryptoKeyStore>) {
    let key_store = Arc::new(MockCryptoKeyStore::default());
    let store = CommitTestStore::new(SnapshotConfig::disabled()).with_crypto(
        key_store.clone(),
        Arc::new(crate::test_fixtures::AadCheckingCryptoProvider),
    );
    (store, key_store)
}

/// Entity that supports delete events for testing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DeletableEntity {
    pub(crate) id: EntityId,
    pub(crate) value: i32,
}

impl crate::Entity for DeletableEntity {
    fn new(id: EntityId) -> Self {
        Self { id, value: 0 }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl crate::DefaultEntity for DeletableEntity {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum DeletableEvent {
    Created { value: i32 },
    Updated { value: i32 },
    Deleted { reason: String },
}

impl crate::DomainEvent for DeletableEvent {
    type Aggregate = DeletableEntity;
    fn event_type(&self) -> &'static str {
        match self {
            Self::Created { .. } => "DeletableEntity.Created",
            Self::Updated { .. } => "DeletableEntity.Updated",
            Self::Deleted { .. } => "DeletableEntity.Deleted",
        }
    }
    fn event_version(&self) -> crate::EventVersion {
        crate::EventVersion::new(1)
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl crate::EventApplicator<DeletableEntity> for DeletableEvent {
    fn dispatch(&self, entity: &mut DeletableEntity) -> std::result::Result<(), DeletableError> {
        match self {
            Self::Created { value } | Self::Updated { value } => entity.value = *value,
            Self::Deleted { .. } => {}
        }
        Ok(())
    }

    fn dispatch_unchecked(&self, entity: &mut DeletableEntity) {
        match self {
            Self::Created { value } | Self::Updated { value } => entity.value = *value,
            Self::Deleted { .. } => {}
        }
    }

    fn is_delete(&self) -> bool {
        matches!(self, Self::Deleted { .. })
    }

    fn dispatch_delete(
        &self,
        mut aggregate: DeletableEntity,
    ) -> std::result::Result<DeletableEntity, DeletableError> {
        if let Self::Deleted { .. } = self {
            aggregate.value = -1; // Mark as deleted
        }
        Ok(aggregate)
    }

    fn dispatch_delete_unchecked(&self, mut aggregate: DeletableEntity) -> DeletableEntity {
        if let Self::Deleted { .. } = self {
            aggregate.value = -1; // Mark as deleted
        }
        aggregate
    }
}

impl crate::DeleteEvent<DeletableEntity> for DeletableEvent {
    fn delete(&self, mut entity: DeletableEntity) -> DeletableEntity {
        entity.value = -1;
        entity
    }
}

#[derive(Debug, thiserror::Error)]
#[error("deletable error")]
pub(crate) struct DeletableError;

impl crate::AggregateError for DeletableError {}

impl crate::Aggregate for DeletableEntity {
    type Event = DeletableEvent;
    type Error = DeletableError;
    type DeletedState = Self;

    fn claims(&self) -> Vec<AggregateClaim> {
        vec![AggregateClaim::new(
            "DeletableEntity.value",
            serde_json::json!(self.value),
        )]
    }
}
