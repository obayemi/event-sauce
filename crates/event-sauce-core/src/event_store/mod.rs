//! Event store trait for event persistence.
//!
//! Defines the `EventStore` trait for persisting and retrieving events with streaming support.
//!
//! The commit pipeline (preparing and flushing writes) lives in [`commit`];
//! loading and replay live in [`load`].

mod commit;
mod load;

pub(crate) use commit::{
    flush_prepared, flush_prepared_batch, prepare_commit, prepare_commit_deleted,
};
pub(crate) use load::{count_events, load, load_any, load_deleted};

use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use uuid::Uuid;

use crate::{
    Aggregate, AggregateClaim, AggregateRoot, AggregateType, AggregateVersion,
    DeletedAggregateRoot, EventEnvelope, EventLogEntry, EventSourcedRepository, Position, Result,
    SnapshotConfig, StreamId,
};

/// A prepared but not-yet-persisted commit.
///
/// Contains all the data needed to persist events and an optional snapshot.
/// Created by [`prepare_commit()`] and flushed by [`flush_prepared()`].
#[derive(Debug)]
pub(crate) struct PreparedCommit {
    pub stream_id: StreamId,
    pub events: Vec<EventEnvelope>,
    pub expected_version: AggregateVersion,
    pub snapshot: Option<Snapshot>,
    pub claims: Vec<AggregateClaim>,
    pub clear_claims: bool,
}

/// A single stream's contribution to a multi-stream atomic write.
///
/// Mirrors the parameters of [`EventStore::append`] for one stream. A batch of
/// `StreamCommit`s is passed to [`EventStore::append_batch`] so that several
/// aggregates can be persisted together — atomically on backends that support
/// it (e.g. `PostgreSQL`), or stream-by-stream on backends whose default
/// implementation simply loops [`append`](EventStore::append).
///
/// This is the consistency-boundary primitive behind atomic multi-aggregate
/// policy reactions and [`Repository::save_all`](crate::Repository::save_all).
///
/// # Examples
///
/// ```
/// use event_sauce_core::{AggregateVersion, StreamCommit, StreamId};
/// use uuid::Uuid;
///
/// let commit = StreamCommit {
///     stream_id: StreamId::new("Account", Uuid::new_v4()),
///     events: vec![],
///     expected_version: AggregateVersion::initial(),
///     claims: vec![],
///     clear_claims: false,
/// };
/// assert_eq!(commit.expected_version, AggregateVersion::initial());
/// ```
#[derive(Debug, Clone)]
pub struct StreamCommit {
    /// The stream the events belong to.
    pub stream_id: StreamId,
    /// The events to append, in order.
    pub events: Vec<EventEnvelope>,
    /// The version the stream is expected to be at (optimistic concurrency).
    pub expected_version: AggregateVersion,
    /// Uniqueness claims to enforce transactionally.
    pub claims: Vec<AggregateClaim>,
    /// When true, all existing claims for the aggregate are cleared.
    pub clear_claims: bool,
}

impl From<&PreparedCommit> for StreamCommit {
    fn from(prepared: &PreparedCommit) -> Self {
        Self {
            stream_id: prepared.stream_id.clone(),
            events: prepared.events.clone(),
            expected_version: prepared.expected_version,
            claims: prepared.claims.clone(),
            clear_claims: prepared.clear_claims,
        }
    }
}

/// Snapshot of an aggregate's state.
///
/// Used to optimize aggregate reconstruction by storing periodic state snapshots.
/// For deleted aggregates, `is_deleted` is `true` and `snapshot_data` contains
/// the serialized `DeletedState` rather than the aggregate itself.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{Snapshot, AggregateVersion};
/// use uuid::Uuid;
/// use serde_json::json;
///
/// let snapshot = Snapshot::new(
///     Uuid::new_v4(),
///     "User",
///     AggregateVersion::new(100),
///     json!({"email": "user@example.com", "status": "active"}),
/// );
/// assert!(!snapshot.is_deleted);
/// ```
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Aggregate ID.
    pub aggregate_id: Uuid,
    /// Aggregate type.
    pub aggregate_type: AggregateType,
    /// [`AggregateVersion`] at which snapshot was taken.
    pub snapshot_version: AggregateVersion,
    /// Serialized entity state.
    pub snapshot_data: serde_json::Value,
    /// Whether this snapshot represents a deleted aggregate.
    pub is_deleted: bool,
    /// Schema version of the serialized state, stamped from
    /// [`Aggregate::snapshot_version()`](crate::Aggregate::snapshot_version)
    /// at write time.
    ///
    /// On load, a snapshot whose stamp no longer matches the aggregate's
    /// current `snapshot_version()` is treated as a cache miss and discarded
    /// in favour of full event replay (snapshot is a cache, never the source
    /// of truth). Snapshots written before this field existed read back as `0`.
    pub snapshot_schema_version: u32,
}

impl Snapshot {
    /// Creates a new snapshot for an active aggregate at schema version `0`.
    ///
    /// Use [`Snapshot::new_with_schema_version`] to stamp a specific
    /// [`Aggregate::snapshot_version()`](crate::Aggregate::snapshot_version).
    #[must_use]
    pub fn new(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
    ) -> Self {
        Self::new_with_schema_version(
            aggregate_id,
            aggregate_type,
            snapshot_version,
            snapshot_data,
            0,
        )
    }

    /// Creates a new snapshot for an active aggregate at a specific schema version.
    #[must_use]
    pub fn new_with_schema_version(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
        snapshot_schema_version: u32,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type: aggregate_type.into(),
            snapshot_version,
            snapshot_data,
            is_deleted: false,
            snapshot_schema_version,
        }
    }

    /// Creates a new snapshot for a deleted aggregate at schema version `0`.
    ///
    /// The `snapshot_data` should contain the serialized `A::DeletedState`.
    /// Use [`Snapshot::new_deleted_with_schema_version`] to stamp a specific
    /// [`Aggregate::snapshot_version()`](crate::Aggregate::snapshot_version).
    #[must_use]
    pub fn new_deleted(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
    ) -> Self {
        Self::new_deleted_with_schema_version(
            aggregate_id,
            aggregate_type,
            snapshot_version,
            snapshot_data,
            0,
        )
    }

    /// Creates a new snapshot for a deleted aggregate at a specific schema version.
    ///
    /// The `snapshot_data` should contain the serialized `A::DeletedState`.
    #[must_use]
    pub fn new_deleted_with_schema_version(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
        snapshot_schema_version: u32,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type: aggregate_type.into(),
            snapshot_version,
            snapshot_data,
            is_deleted: true,
            snapshot_schema_version,
        }
    }
}

/// Trait for event store implementations.
///
/// The event store is responsible for persisting and retrieving events,
/// managing snapshots, and enforcing optimistic concurrency control.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Appends events to a stream with optimistic concurrency control.
    ///
    /// The `claims` parameter specifies uniqueness claims to enforce transactionally.
    /// When `clear_claims` is true, all existing claims for the aggregate are removed
    /// (used for deleted aggregates).
    ///
    /// # Global ordering guarantee
    ///
    /// Implementations MUST assign global positions in commit order: once an
    /// event with global position `N` is visible to a reader (via
    /// [`stream_all`](Self::stream_all)), every event with position `< N` is
    /// already committed and visible. This lets [`Position`]-based consumers
    /// (projections, policies, checkpoints) scan `position > checkpoint` in
    /// ascending order without ever skipping a still-uncommitted lower position.
    /// Positions are not guaranteed to be dense — rolled-back appends may leave
    /// gaps — only that no *committed* event is ever reordered behind a higher
    /// committed position. Backends serialize only the id-allocating window;
    /// reads stay concurrent.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if the expected version doesn't match.
    /// Returns `Error::ClaimConflict` if a claim is already held by another aggregate.
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        claims: Vec<AggregateClaim>,
        clear_claims: bool,
    ) -> Result<()>;

    /// Appends several streams' events as one logical write.
    ///
    /// This is the consistency-boundary primitive behind atomic multi-aggregate
    /// policy reactions (see [`PolicyContext::flush`](crate::PolicyContext)) and
    /// [`Repository::save_all`](crate::Repository::save_all). Each [`StreamCommit`] carries the same data as a
    /// single [`append`](Self::append) call for one stream.
    ///
    /// # Atomicity
    ///
    /// The default implementation simply loops [`append`](Self::append) over each
    /// commit, so it is **not** atomic: a partial failure leaves earlier commits
    /// persisted. Backends with real transactions (e.g. `PostgreSQL`) override this
    /// to run the whole batch in a single transaction — all commits succeed or
    /// none do. The in-memory backend keeps the per-stream default.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if any commit's expected version
    /// doesn't match, or `Error::ClaimConflict` if a claim is already held by
    /// another aggregate. On atomic backends the whole batch rolls back; on the
    /// looping default, commits before the failing one are already persisted.
    async fn append_batch(&self, commits: Vec<StreamCommit>) -> Result<()> {
        for commit in commits {
            self.append(
                commit.stream_id,
                commit.events,
                commit.expected_version,
                commit.claims,
                commit.clear_claims,
            )
            .await?;
        }
        Ok(())
    }

    /// Loads events from a stream starting at a specific version.
    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    /// Streams every event in the global log whose position is strictly greater
    /// than `from_position`, in ascending position order.
    ///
    /// Each item is an [`EventLogEntry`] pairing the store-issued global
    /// [`Position`] with the [`EventEnvelope`]. Checkpoint
    /// [`entry.position`](EventLogEntry::position) of the last processed entry to
    /// resume exactly where you left off — positions are opaque, strictly
    /// monotonic tokens, never a reconstructed count (see [`Position`]).
    ///
    /// Pass [`Position::start`] to stream the whole log.
    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventLogEntry>> + Send>;

    /// Returns the highest position currently in the global event log, or
    /// [`Position::start`] if the log is empty.
    ///
    /// This is the position a brand-new consumer should checkpoint to skip all
    /// existing history and only process events appended from now on.
    ///
    /// The default implementation streams the whole log via
    /// [`stream_all`](Self::stream_all) and returns the last entry's position;
    /// backends that can answer cheaply (e.g. `SELECT MAX(id)`) should override
    /// it.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails.
    async fn max_position(&self) -> Result<Position> {
        use futures::StreamExt;

        let stream = self.stream_all(Position::start()).await?;
        futures::pin_mut!(stream);

        let mut max = Position::start();
        while let Some(entry) = stream.next().await {
            max = entry?.position;
        }
        Ok(max)
    }

    /// Gets the current version of a stream.
    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion>;

    /// Checks if a stream exists.
    async fn stream_exists(&self, stream_id: StreamId) -> Result<bool> {
        let version = self.get_version(stream_id).await?;
        Ok(version != AggregateVersion::initial())
    }

    /// Saves a snapshot.
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let _ = snapshot;
        Ok(())
    }

    /// Loads a snapshot.
    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        let _ = stream_id;
        Ok(None)
    }

    /// Returns the snapshot configuration for this store.
    fn snapshot_config(&self) -> &SnapshotConfig {
        use std::sync::OnceLock;
        static DISABLED_CONFIG: OnceLock<SnapshotConfig> = OnceLock::new();
        DISABLED_CONFIG.get_or_init(SnapshotConfig::disabled)
    }

    /// Returns the checkpoint store associated with this event store, if any.
    fn checkpoint_store(&self) -> Option<crate::CheckpointStoreRef> {
        None
    }

    /// Returns the crypto key store for encrypted aggregate encryption, if configured.
    fn crypto_key_store(&self) -> Option<&dyn crate::CryptoKeyStore> {
        None
    }

    /// Returns the crypto provider for encrypted aggregate encryption, if configured.
    fn crypto_provider(&self) -> Option<&dyn crate::CryptoProvider> {
        None
    }

    /// Creates a [`PolicyRunner`](crate::PolicyRunner) pre-configured with this event store.
    ///
    /// The checkpoint store is auto-wired from this store's
    /// [`checkpoint_store()`](Self::checkpoint_store). Returns an error if no
    /// checkpoint store is configured, since policies require checkpoint support.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let runner = store.policy_runner()?
    ///     .register(Arc::new(MyPolicy));
    /// runner.process_pending().await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidState` if no checkpoint store is configured.
    fn policy_runner(self: &Arc<Self>) -> Result<crate::PolicyRunner<Self>>
    where
        Self: Sized + 'static,
    {
        let cp = self.checkpoint_store().ok_or_else(|| {
            crate::Error::invalid_state("PolicyRunner requires a checkpoint store")
        })?;
        Ok(crate::PolicyRunner::new(Arc::clone(self), cp))
    }

    /// Creates an [`EventSourcedRepository`] for the given aggregate type, wrapping this event store.
    ///
    /// This is a convenience method that avoids verbose turbofish syntax.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let store = Arc::new(MyEventStore::new());
    /// let user_repo = store.repository::<User>();
    /// ```
    fn repository<A>(self: &Arc<Self>) -> EventSourcedRepository<Self, A>
    where
        Self: Sized + 'static,
        A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
        A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
        A::Event: serde::Serialize + serde::de::DeserializeOwned,
    {
        EventSourcedRepository::new(Arc::clone(self))
    }

    /// Commits pending events from an aggregate root to the event store.
    ///
    /// This method:
    /// 1. Extracts pending events from the aggregate root
    /// 2. Converts them to event envelopes
    /// 3. Encrypts event data if the aggregate is encrypted or has encrypted fields
    /// 4. Appends them to the event store with optimistic concurrency control
    /// 5. Creates a snapshot if the strategy indicates it should (encrypted for encrypted aggregates)
    /// 6. Clears the pending events on success
    ///
    /// The pending events are only cleared once the write is durable: if
    /// `append` fails (e.g. `Error::ConcurrencyConflict`) or this call is
    /// cancelled while awaiting it, the aggregate keeps its pending events so
    /// a retried `commit()` persists them instead of silently doing nothing.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if another process modified the aggregate.
    /// Returns `Error::Encryption` if encryption fails for an encrypted aggregate.
    /// Returns `Error::InvalidState` if an encrypted aggregate lacks crypto configuration.
    /// Returns `Error::KeyNotFound` if the aggregate was crypto-shredded (its key was deleted).
    async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::Event: serde::Serialize,
    {
        if let Some(prepared) = prepare_commit(self, aggregate).await? {
            flush_prepared(self, prepared).await?;
            aggregate.clear_pending_events();
        }
        Ok(())
    }

    /// Commits pending events from a deleted aggregate root to the event store.
    ///
    /// This method works like [`commit()`](Self::commit), but operates on a
    /// `DeletedAggregateRoot<A>` instead of `AggregateRoot<A>`.
    ///
    /// The snapshot for a deleted aggregate stores the serialized `DeletedState`
    /// with `is_deleted = true`, so `load_any()` can deserialize it correctly.
    /// Snapshots are encrypted if the aggregate uses encryption.
    ///
    /// Like [`commit()`](Self::commit), the pending events are cleared only
    /// after the write succeeds, so a failed or cancelled call can be retried.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if another process modified the aggregate.
    /// Returns `Error::Encryption` if encryption fails for an encrypted aggregate.
    /// Returns `Error::InvalidState` if an encrypted aggregate lacks crypto configuration.
    /// Returns `Error::KeyNotFound` if the aggregate was crypto-shredded (its key was deleted).
    async fn commit_deleted<A>(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::DeletedState: serde::Serialize,
        A::Event: serde::Serialize,
    {
        if let Some(prepared) = prepare_commit_deleted(self, aggregate).await? {
            flush_prepared(self, prepared).await?;
            aggregate.clear_pending_events();
        }
        Ok(())
    }
}

/// Unwraps the crypto provider or returns an error.
fn require_crypto_provider<S: EventStore + ?Sized>(
    store: &S,
) -> Result<&dyn crate::CryptoProvider> {
    store
        .crypto_provider()
        .ok_or_else(|| crate::Error::invalid_state("Encrypted aggregate requires crypto_provider"))
}

/// Builds the AAD that binds a snapshot's ciphertext to its aggregate.
///
/// A snapshot has no per-event UUID, so it is bound to `aggregate_id || "snap"`.
/// This separates the snapshot domain from event ciphertext (an event blob cannot
/// be relocated into the snapshot slot, and vice versa) while staying stable
/// across the encrypt (write) and decrypt (load) sides.
fn snapshot_aad(aggregate_id: Uuid) -> Vec<u8> {
    let mut aad = Vec::with_capacity(20);
    aad.extend_from_slice(aggregate_id.as_bytes());
    aad.extend_from_slice(b"snap");
    aad
}

#[cfg(test)]
pub(crate) mod test_support {
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

    /// Deterministic provider that binds ciphertext to its AAD by prefixing it,
    /// so a wrong AAD on decrypt is detected instead of silently accepted (unlike
    /// a bare XOR mock).
    pub(crate) struct AadCheckingCryptoProvider;

    impl crate::CryptoProvider for AadCheckingCryptoProvider {
        fn encrypt(&self, key: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
            let mut out = Vec::new();
            let aad_len =
                u32::try_from(aad.len()).map_err(|_| crate::Error::encryption("aad too long"))?;
            out.extend_from_slice(&aad_len.to_le_bytes());
            out.extend_from_slice(aad);
            out.extend(
                plaintext
                    .iter()
                    .enumerate()
                    .map(|(i, b)| b ^ key[i % key.len()]),
            );
            Ok(out)
        }

        fn decrypt(&self, key: &[u8], ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
            if ciphertext.len() < 4 {
                return Err(crate::Error::encryption("ciphertext too short"));
            }
            let (len_bytes, rest) = ciphertext.split_at(4);
            let aad_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            if rest.len() < aad_len {
                return Err(crate::Error::encryption("ciphertext too short"));
            }
            let (bound_aad, body) = rest.split_at(aad_len);
            if bound_aad != aad {
                return Err(crate::Error::encryption("aad mismatch"));
            }
            Ok(body
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ key[i % key.len()])
                .collect())
        }

        fn generate_key(&self) -> Vec<u8> {
            vec![0x24; 32]
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

        async fn get_or_insert_key(
            &self,
            aggregate_id: Uuid,
            candidate: Vec<u8>,
        ) -> Result<Vec<u8>> {
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
        async fn get_or_insert_key(
            &self,
            _aggregate_id: Uuid,
            _candidate: Vec<u8>,
        ) -> Result<Vec<u8>> {
            Ok(self.winning_key.clone())
        }
        async fn is_shredded(&self, _aggregate_id: Uuid) -> Result<bool> {
            Ok(false)
        }
    }

    /// Builds a [`CommitTestStore`] with snapshots disabled and a fresh
    /// [`MockCryptoKeyStore`] plus [`AadCheckingCryptoProvider`] installed.
    pub(crate) fn crypto_test_store() -> (CommitTestStore, Arc<MockCryptoKeyStore>) {
        let key_store = Arc::new(MockCryptoKeyStore::default());
        let store = CommitTestStore::new(SnapshotConfig::disabled())
            .with_crypto(key_store.clone(), Arc::new(AadCheckingCryptoProvider));
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
        fn dispatch(
            &self,
            entity: &mut DeletableEntity,
        ) -> std::result::Result<(), DeletableError> {
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
}

#[cfg(test)]
#[allow(clippy::map_unwrap_or)]
mod tests {
    use super::*;
    use crate::test_fixtures::{SimpleTestEntity, SimpleTestEvent};
    use crate::Repository;
    use std::sync::Arc;

    #[test]
    fn test_stream_id_new() {
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        assert_eq!(stream_id.aggregate_type(), "User");
        assert_eq!(stream_id.aggregate_id(), aggregate_id);
    }

    #[test]
    fn test_stream_id_display() {
        let aggregate_id = Uuid::nil();
        let stream_id = StreamId::new("Order", aggregate_id);

        let display = stream_id.to_string();
        assert!(display.starts_with("Order:"));
        assert!(display.contains(&aggregate_id.to_string()));
    }

    #[test]
    fn test_stream_id_equality() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let stream1 = StreamId::new("User", id1);
        let stream2 = StreamId::new("User", id1);
        let stream3 = StreamId::new("User", id2);
        let stream4 = StreamId::new("Order", id1);

        assert_eq!(stream1, stream2);
        assert_ne!(stream1, stream3);
        assert_ne!(stream1, stream4);
    }

    #[test]
    fn test_position_new() {
        let pos = Position::new(42);
        assert_eq!(pos.as_i64(), 42);
    }

    #[test]
    fn test_position_start() {
        let pos = Position::start();
        assert_eq!(pos.as_i64(), 0);
    }

    #[test]
    fn test_position_ordering() {
        let pos1 = Position::new(1);
        let pos2 = Position::new(2);
        let pos3 = Position::new(3);

        assert!(pos1 < pos2);
        assert!(pos2 < pos3);
        assert!(pos1 < pos3);
    }

    #[test]
    fn test_position_from_i64() {
        let pos: Position = 100.into();
        assert_eq!(pos.as_i64(), 100);
    }

    #[test]
    fn test_position_into_i64() {
        let pos = Position::new(100);
        let value: i64 = pos.into();
        assert_eq!(value, 100);
    }

    #[test]
    fn test_snapshot_new() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"value": 42});

        let snapshot = Snapshot::new(
            aggregate_id,
            "Counter".to_string(),
            AggregateVersion::new(10),
            snapshot_data.clone(),
        );

        assert_eq!(snapshot.aggregate_id, aggregate_id);
        assert_eq!(snapshot.aggregate_type, "Counter");
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(10));
        assert_eq!(snapshot.snapshot_data, snapshot_data);
        assert!(!snapshot.is_deleted);
        assert_eq!(
            snapshot.snapshot_schema_version, 0,
            "Snapshot::new defaults the schema version to 0"
        );
    }

    #[test]
    fn test_snapshot_new_with_schema_version() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"value": 42});

        let snapshot = Snapshot::new_with_schema_version(
            aggregate_id,
            "Counter".to_string(),
            AggregateVersion::new(10),
            snapshot_data.clone(),
            3,
        );

        assert!(!snapshot.is_deleted);
        assert_eq!(snapshot.snapshot_data, snapshot_data);
        assert_eq!(snapshot.snapshot_schema_version, 3);
    }

    #[test]
    fn test_snapshot_new_deleted_with_schema_version() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"archived": true});

        let snapshot = Snapshot::new_deleted_with_schema_version(
            aggregate_id,
            "Counter".to_string(),
            AggregateVersion::new(5),
            snapshot_data.clone(),
            7,
        );

        assert!(snapshot.is_deleted);
        assert_eq!(snapshot.snapshot_data, snapshot_data);
        assert_eq!(snapshot.snapshot_schema_version, 7);
    }

    #[test]
    fn test_snapshot_new_deleted() {
        let aggregate_id = Uuid::new_v4();
        let snapshot_data = serde_json::json!({"value": -1, "archived": true});

        let snapshot = Snapshot::new_deleted(
            aggregate_id,
            "Counter".to_string(),
            AggregateVersion::new(5),
            snapshot_data.clone(),
        );

        assert_eq!(snapshot.aggregate_id, aggregate_id);
        assert_eq!(snapshot.aggregate_type, "Counter");
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(5));
        assert_eq!(snapshot.snapshot_data, snapshot_data);
        assert!(snapshot.is_deleted);
        assert_eq!(
            snapshot.snapshot_schema_version, 0,
            "Snapshot::new_deleted defaults the schema version to 0"
        );
    }

    // Tests for default trait implementations (including crypto accessors)
    use crate::test_fixtures::MockEventStore as SharedMockEventStore;

    #[tokio::test]
    async fn test_crypto_key_store_returns_none_by_default() {
        let store = SharedMockEventStore::new();
        assert!(store.crypto_key_store().is_none());
    }

    #[tokio::test]
    async fn test_crypto_provider_returns_none_by_default() {
        let store = SharedMockEventStore::new();
        assert!(store.crypto_provider().is_none());
    }

    #[tokio::test]
    async fn test_save_snapshot_default_implementation() {
        let store = SharedMockEventStore::new();
        let snapshot = Snapshot::new(
            Uuid::new_v4(),
            "TestAggregate".to_string(),
            AggregateVersion::new(10),
            serde_json::json!({"value": 42}),
        );

        let result = store.save_snapshot(snapshot).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_load_snapshot_default_implementation() {
        let store = SharedMockEventStore::new();
        let stream_id = StreamId::new("TestAggregate", Uuid::new_v4());

        let result = store.load_snapshot(stream_id).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_save_and_load_snapshot_default_implementations() {
        let store = SharedMockEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);

        let snapshot = Snapshot::new(
            aggregate_id,
            "TestAggregate".to_string(),
            AggregateVersion::new(100),
            serde_json::json!({"state": "active"}),
        );
        store.save_snapshot(snapshot).await.unwrap();

        let loaded = store.load_snapshot(stream_id).await.unwrap();
        assert!(loaded.is_none());
    }

    use test_support::MockEventStoreWithStreams;

    #[tokio::test]
    async fn test_stream_exists_returns_true_for_existing_stream() {
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let store = MockEventStoreWithStreams::new().with_stream(stream_id.clone(), 5);

        let exists = store.stream_exists(stream_id).await.unwrap();
        assert!(exists, "Stream with events should exist");
    }

    #[tokio::test]
    async fn test_stream_exists_returns_false_for_nonexistent_stream() {
        let store = MockEventStoreWithStreams::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let exists = store.stream_exists(stream_id).await.unwrap();
        assert!(!exists, "Nonexistent stream should not exist");
    }

    #[tokio::test]
    async fn test_repository_convenience_method() {
        let store = Arc::new(SharedMockEventStore::new());
        let repo = store.repository::<SimpleTestEntity>();

        let test_id = crate::EntityId::new();
        let mut aggregate = AggregateRoot::<SimpleTestEntity>::new(test_id);
        aggregate
            .apply(SimpleTestEvent::Created { value: 42 })
            .unwrap();

        repo.save(&mut aggregate).await.unwrap();
        let loaded = repo.load(test_id).await.unwrap();
        assert_eq!(loaded.value, 42);
    }
}
