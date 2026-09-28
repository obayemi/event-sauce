//! Event store trait for event persistence.
//!
//! Defines the `EventStore` trait for persisting and retrieving events with streaming support.

use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::commit_source::CommitSource;
use crate::{
    Aggregate, AggregateClaim, AggregateRoot, AggregateType, AggregateVersion,
    DeletedAggregateRoot, DomainEvent, EntityId, EventEnvelope, EventLogEntry,
    EventSourcedRepository, Loaded, Position, Result, SnapshotConfig, StreamId,
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

/// Encrypts event envelopes in place for an encrypted or field-encrypted
/// aggregate; a no-op for a plaintext one.
///
/// Full-value encryption replaces `event_data` outright; field-level
/// encryption only touches the event variant's declared `encrypted_fields()`.
/// Either way the ciphertext is bound to `aggregate_id || event_id` (see
/// [`crate::crypto::event_aad`]), so it cannot be relocated onto another event.
async fn encrypt_envelopes<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    aggregate_id: Uuid,
    envelopes: &mut [EventEnvelope],
    pending: &[crate::aggregate_root::PendingEvent<A::Event>],
) -> Result<()>
where
    A::Event: serde::Serialize,
{
    if A::is_encrypted() {
        let crypto_key = ensure_crypto_key(store, aggregate_id).await?;
        let provider = require_crypto_provider(store)?;

        for envelope in envelopes {
            let aad = crate::crypto::event_aad(aggregate_id, envelope.id);
            envelope.event_data =
                crate::crypto::encrypt_value(provider, &crypto_key, &envelope.event_data, &aad)?;
        }
    } else if A::Event::has_any_encrypted_fields() {
        let crypto_key = ensure_crypto_key(store, aggregate_id).await?;
        let provider = require_crypto_provider(store)?;

        for (envelope, pe) in envelopes.iter_mut().zip(pending.iter()) {
            let fields = pe.event.encrypted_fields();
            if !fields.is_empty() {
                let aad = crate::crypto::event_aad(aggregate_id, envelope.id);
                crate::crypto::encrypt_fields(
                    provider,
                    &crypto_key,
                    &mut envelope.event_data,
                    fields,
                    &aad,
                )?;
            }
        }
    }
    Ok(())
}

/// Prepares a commit without persisting it.
///
/// Extracts pending events from the aggregate root, serializes and encrypts
/// them and computes a snapshot if needed. Returns `None` if there are no
/// pending events.
///
/// This does **not** clear the aggregate's pending events — the caller must
/// do that only once the prepared commit has actually been persisted, so a
/// failed or cancelled write can be retried instead of silently losing the
/// events.
async fn prepare_commit_for<S: EventStore + ?Sized, A, R: CommitSource<A>>(
    store: &S,
    root: &R,
) -> Result<Option<PreparedCommit>>
where
    A: Aggregate,
    A::Event: serde::Serialize,
{
    if root.is_poisoned() {
        let what = if R::IS_DELETED {
            "poisoned deleted aggregate"
        } else {
            "poisoned aggregate"
        };
        return Err(crate::Error::invalid_state(format!(
            "cannot commit a {what}: a previous apply() failed, \
             leaving inconsistent state — discard and reload the aggregate"
        )));
    }

    let pending = root.pending_events_with_actors();
    if pending.is_empty() {
        return Ok(None);
    }

    let aggregate_id = root.entity_id().as_uuid();
    let aggregate_type = R::aggregate_type();
    let expected_version = root.committed_version();

    let mut envelopes =
        crate::aggregate_root::envelopes_from_pending::<A::Event>(pending, aggregate_id)?;
    encrypt_envelopes::<S, A>(store, aggregate_id, &mut envelopes, pending).await?;

    let config = store.snapshot_config();
    let strategy = config.strategy_for_type(aggregate_type.as_str());
    let current_version = root.version();

    let snapshot = if strategy.should_snapshot_range(expected_version, current_version) {
        build_snapshot::<S, A>(
            store,
            aggregate_id,
            &aggregate_type,
            current_version,
            R::IS_DELETED,
            || root.serialize_state(),
        )
        .await?
    } else {
        None
    };

    let stream_id = StreamId::new(aggregate_type, aggregate_id);

    Ok(Some(PreparedCommit {
        stream_id,
        events: envelopes,
        expected_version,
        snapshot,
        claims: root.claims(),
        clear_claims: R::IS_DELETED,
    }))
}

/// Prepares a commit without persisting it.
///
/// See [`prepare_commit_for`] for the shared implementation.
pub(crate) async fn prepare_commit<S: EventStore + ?Sized, A>(
    store: &S,
    aggregate: &AggregateRoot<A>,
) -> Result<Option<PreparedCommit>>
where
    A: Aggregate + serde::Serialize,
    A::Event: serde::Serialize,
{
    prepare_commit_for(store, aggregate).await
}

/// Prepares a commit for a deleted aggregate without persisting it.
///
/// Works like [`prepare_commit()`] but operates on a `DeletedAggregateRoot<A>`.
/// The snapshot stores the serialized `DeletedState` with `is_deleted = true`.
/// See [`prepare_commit_for`] for the shared implementation.
pub(crate) async fn prepare_commit_deleted<S: EventStore + ?Sized, A>(
    store: &S,
    aggregate: &DeletedAggregateRoot<A>,
) -> Result<Option<PreparedCommit>>
where
    A: Aggregate + serde::Serialize,
    A::DeletedState: serde::Serialize,
    A::Event: serde::Serialize,
{
    prepare_commit_for(store, aggregate).await
}

/// Flushes a prepared commit to the event store.
///
/// Appends events and saves the snapshot (best-effort for snapshot failures).
pub(crate) async fn flush_prepared<S: EventStore + ?Sized>(
    store: &S,
    prepared: PreparedCommit,
) -> Result<()> {
    store
        .append(
            prepared.stream_id.clone(),
            prepared.events,
            prepared.expected_version,
            prepared.claims,
            prepared.clear_claims,
        )
        .await?;

    if let Some(snapshot) = prepared.snapshot {
        if let Err(e) = store.save_snapshot(snapshot).await {
            tracing::warn!(
                stream_id = %prepared.stream_id,
                error = %e,
                "Failed to save snapshot"
            );
        }
    }

    Ok(())
}

/// Flushes several prepared commits as one logical write.
///
/// Appends every commit's events through a single
/// [`append_batch`](EventStore::append_batch) call — so on backends with real
/// transactions (e.g. `PostgreSQL`) the whole set is atomic. Snapshots are then
/// saved best-effort, after the events are durable.
///
/// Returns `Ok(())` immediately for an empty batch.
pub(crate) async fn flush_prepared_batch<S: EventStore + ?Sized>(
    store: &S,
    prepared: Vec<PreparedCommit>,
) -> Result<()> {
    if prepared.is_empty() {
        return Ok(());
    }

    let commits: Vec<StreamCommit> = prepared.iter().map(StreamCommit::from).collect();
    store.append_batch(commits).await?;

    for commit in prepared {
        if let Some(snapshot) = commit.snapshot {
            if let Err(e) = store.save_snapshot(snapshot).await {
                tracing::warn!(
                    stream_id = %commit.stream_id,
                    error = %e,
                    "Failed to save snapshot"
                );
            }
        }
    }

    Ok(())
}

/// Builds an aggregate's snapshot, encrypting if needed.
///
/// A serialization failure is best-effort (the events are already committed and
/// the snapshot is only an optimization) so it logs and yields `None`. A crypto
/// failure for an encryption-required aggregate is fatal: it propagates as an
/// error so the commit fails closed rather than persisting a plaintext snapshot.
///
/// `is_deleted` selects between an active-aggregate snapshot (`entity`,
/// [`Snapshot::new_with_schema_version`]) and a deleted-aggregate one
/// (`A::DeletedState`, [`Snapshot::new_deleted_with_schema_version`]).
///
/// # Errors
///
/// Returns an error if snapshot encryption fails for an encryption-required
/// aggregate (see [`encrypt_snapshot_data`]).
async fn build_snapshot<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    aggregate_id: uuid::Uuid,
    aggregate_type: &AggregateType,
    current_version: AggregateVersion,
    is_deleted: bool,
    serialize: impl FnOnce() -> std::result::Result<serde_json::Value, serde_json::Error>,
) -> Result<Option<Snapshot>> {
    match serialize() {
        Ok(mut snapshot_data) => {
            if A::is_encrypted() || A::Event::has_any_encrypted_fields() {
                encrypt_snapshot_data(store, aggregate_id, &mut snapshot_data).await?;
            }
            let new = if is_deleted {
                Snapshot::new_deleted_with_schema_version
            } else {
                Snapshot::new_with_schema_version
            };
            Ok(Some(new(
                aggregate_id,
                aggregate_type.clone(),
                current_version,
                snapshot_data,
                A::snapshot_version(),
            )))
        }
        Err(e) => {
            tracing::warn!(
                aggregate_type = %aggregate_type,
                aggregate_id = %aggregate_id,
                is_deleted,
                error = %e,
                "Failed to serialize state for snapshot"
            );
            Ok(None)
        }
    }
}

/// Encrypts snapshot data in-place, failing closed for an aggregate that
/// requires encryption.
///
/// Callers invoke this only when the aggregate is fully encrypted or has any
/// field-encrypted event variant, so a missing key store, a missing provider, a
/// key-store read error, a missing key, or an `encrypt_value` failure are all
/// HARD errors: the snapshot would otherwise be persisted in plaintext —
/// leaking full state and being un-shreddable. We never fall through to a
/// plaintext snapshot for an encryption-required aggregate.
///
/// # Errors
///
/// Returns `Error::InvalidState` if crypto configuration is missing,
/// `Error::KeyNotFound` if no key exists for the aggregate, or
/// `Error::Encryption` if encryption fails.
async fn encrypt_snapshot_data<S: EventStore + ?Sized>(
    store: &S,
    aggregate_id: uuid::Uuid,
    snapshot_data: &mut serde_json::Value,
) -> Result<()> {
    let key_store = store.crypto_key_store().ok_or_else(|| {
        crate::Error::invalid_state("Encrypted aggregate requires crypto_key_store")
    })?;
    let provider = require_crypto_provider(store)?;

    let crypto_key = Zeroizing::new(
        key_store
            .get_key(aggregate_id)
            .await?
            .ok_or_else(|| crate::Error::key_not_found(aggregate_id))?,
    );

    let aad = snapshot_aad(aggregate_id);
    *snapshot_data = crate::crypto::encrypt_value(provider, &crypto_key, snapshot_data, &aad)?;
    Ok(())
}

/// Gets or creates a crypto key for an aggregate.
///
/// If the key already exists, returns it. Otherwise, generates a candidate key
/// and atomically inserts it via
/// [`get_or_insert_key`](crate::CryptoKeyStore::get_or_insert_key), always
/// encrypting with whichever key won that race — never the locally generated
/// candidate blindly. This is what keeps two concurrent first commits of the
/// same aggregate from encrypting under two different keys.
///
/// The returned key is held in [`Zeroizing`] so the buffer is wiped when this
/// engine copy is dropped (after encryption), rather than lingering in freed
/// heap. [`CryptoKeyStore`](crate::CryptoKeyStore) still exchanges plain
/// `Vec<u8>`, so each call takes a short-lived copy bound straight to the
/// backend read/write.
async fn ensure_crypto_key<S: EventStore + ?Sized>(
    store: &S,
    aggregate_id: Uuid,
) -> Result<Zeroizing<Vec<u8>>> {
    let key_store = store.crypto_key_store().ok_or_else(|| {
        crate::Error::invalid_state("Encrypted aggregate requires crypto_key_store")
    })?;
    let provider = require_crypto_provider(store)?;

    if let Some(existing) = key_store.get_key(aggregate_id).await? {
        return Ok(Zeroizing::new(existing));
    }

    if key_store.is_shredded(aggregate_id).await? {
        return Err(crate::Error::key_not_found(aggregate_id));
    }

    let candidate = provider.generate_key();
    let winner = key_store.get_or_insert_key(aggregate_id, candidate).await?;
    Ok(Zeroizing::new(winner))
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

/// Decrypts event envelope data using either full-value or field-level decryption.
///
/// `aad` binds the ciphertext to its context (event or snapshot); see
/// [`event_aad`]/[`snapshot_aad`]. Legacy (v1) rows ignore the AAD via the
/// versioned envelope, so pre-existing ciphertext stays readable.
fn decrypt_event_data<S: EventStore + ?Sized>(
    store: &S,
    crypto_key: Option<&[u8]>,
    event_data: &mut serde_json::Value,
    aad: &[u8],
) -> Result<()> {
    if let Some(key) = crypto_key {
        if crate::crypto::is_encrypted(event_data) {
            let provider = require_crypto_provider(store)?;
            *event_data = crate::crypto::decrypt_value(provider, key, event_data, aad)?;
        } else if crate::crypto::has_encrypted_fields(event_data) {
            let provider = require_crypto_provider(store)?;
            crate::crypto::decrypt_encrypted_fields(provider, key, event_data, aad)?;
        }
    }
    Ok(())
}

/// Returns `true` if the aggregate has any committed data — a persisted
/// snapshot, or at least one event in its stream.
///
/// Used to distinguish a crypto-shredded aggregate (data exists but its key is
/// gone) from a genuinely never-committed one (nothing to read), so that a
/// missing field-encryption key only surfaces as `KeyNotFound` when there is
/// actually data that has become unreadable.
async fn has_committed_data<S: EventStore + ?Sized>(
    store: &S,
    stream_id: &StreamId,
) -> Result<bool> {
    use futures::StreamExt;

    if store.snapshot_config().use_snapshots_on_load()
        && store.load_snapshot(stream_id.clone()).await?.is_some()
    {
        return Ok(true);
    }

    let event_stream = store
        .load_stream(stream_id.clone(), AggregateVersion::initial())
        .await?;
    futures::pin_mut!(event_stream);
    Ok(event_stream.next().await.is_some())
}

/// Resolves the crypto key to use when loading an aggregate, or `None` for a
/// plaintext one.
///
/// For a fully encrypted aggregate the key must exist: `Error::KeyNotFound`
/// otherwise. For field-level encryption the key may legitimately be absent
/// when nothing has been committed yet — but if data already exists and the
/// key is gone, it was crypto-shredded out from under it, so that also
/// surfaces as `KeyNotFound`, uniformly with the fully-encrypted case.
///
/// # Errors
///
/// Returns `Error::InvalidState` if a fully encrypted aggregate has no
/// configured [`CryptoKeyStore`](crate::CryptoKeyStore), or `Error::KeyNotFound`
/// if the aggregate was crypto-shredded.
async fn resolve_crypto_key<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    uuid: Uuid,
    stream_id: &StreamId,
) -> Result<Option<Zeroizing<Vec<u8>>>> {
    if A::is_encrypted() {
        let key_store = store.crypto_key_store().ok_or_else(|| {
            crate::Error::invalid_state("Encrypted aggregate requires crypto_key_store")
        })?;
        let key = key_store
            .get_key(uuid)
            .await?
            .ok_or_else(|| crate::Error::key_not_found(uuid))?;
        return Ok(Some(Zeroizing::new(key)));
    }

    if !A::Event::has_any_encrypted_fields() {
        return Ok(None);
    }
    let Some(key_store) = store.crypto_key_store() else {
        return Ok(None);
    };
    if let Some(key) = key_store.get_key(uuid).await? {
        return Ok(Some(Zeroizing::new(key)));
    }
    if has_committed_data(store, stream_id).await? {
        return Err(crate::Error::key_not_found(uuid));
    }
    Ok(None)
}

/// Decrypts (if needed) and deserializes one event envelope.
fn decode_event<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    key: Option<&[u8]>,
    aggregate_id: Uuid,
    mut envelope: EventEnvelope,
) -> Result<A::Event>
where
    A::Event: serde::de::DeserializeOwned,
{
    let aad = crate::crypto::event_aad(aggregate_id, envelope.id);
    decrypt_event_data(store, key, &mut envelope.event_data, &aad)?;
    A::Event::from_envelope(&envelope)
}

/// Replays every remaining item of an event stream onto an in-progress
/// aggregate, stopping early (as [`Loaded::Deleted`]) if a delete event is
/// found — shared by [`try_load_from_snapshot`] (replaying the events after a
/// snapshot) and [`load_any`] (replaying the events after the first one).
async fn replay_onto<S, A, St>(
    store: &S,
    mut stream: St,
    key: Option<&[u8]>,
    mut aggregate: AggregateRoot<A>,
) -> Result<Loaded<A>>
where
    S: EventStore + ?Sized,
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned,
    St: Stream<Item = Result<EventEnvelope>> + Unpin,
{
    use crate::EventApplicator;
    use futures::StreamExt;

    let aggregate_id = aggregate.entity_id().as_uuid();

    while let Some(envelope) = stream.next().await {
        let event = decode_event::<S, A>(store, key, aggregate_id, envelope?)?;
        if EventApplicator::is_delete(&event) {
            return Ok(Loaded::Deleted(aggregate.apply_delete_unchecked(&event)));
        }
        aggregate.apply_unchecked(&event);
    }

    Ok(Loaded::Active(aggregate))
}

/// Attempts to reconstruct an aggregate from a (decrypted) snapshot plus its
/// post-snapshot events.
///
/// Returns `Ok(Some(loaded))` when the snapshot is a usable cache entry, and
/// `Ok(None)` when it is a CACHE MISS — i.e. the snapshot's type tag or schema
/// version no longer matches `A`, or its data can no longer be deserialized
/// into `A` / `A::DeletedState`. On a miss the caller falls through to full
/// event replay (snapshot is a cache, never the source of truth) and a fresh
/// snapshot is written at the current version on the next commit.
///
/// The `snapshot.snapshot_data` is expected to already be decrypted (the
/// crypto-shredding `KeyNotFound` case is handled by the caller before this is
/// invoked).
///
/// # Errors
///
/// Returns an error only if loading the post-snapshot event stream or
/// deserializing an event fails. A snapshot deserialization failure is a cache
/// miss (`Ok(None)`), not an error.
async fn try_load_from_snapshot<S, A>(
    store: &S,
    stream_id: StreamId,
    crypto_key: Option<&[u8]>,
    snapshot: Snapshot,
) -> Result<Option<Loaded<A>>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    // (i) Type-tag mismatch: a different aggregate's snapshot was stored under
    // this id, or the type name changed. Discard and replay.
    if snapshot.aggregate_type != A::aggregate_type() {
        tracing::warn!(
            aggregate_type = %A::aggregate_type(),
            snapshot_aggregate_type = %snapshot.aggregate_type,
            aggregate_id = %snapshot.aggregate_id,
            "Snapshot aggregate-type mismatch; discarding stale snapshot and replaying events"
        );
        return Ok(None);
    }

    // (ii) Schema-version mismatch: the aggregate's serialized shape changed
    // (its `snapshot_version()` was bumped). Discard and replay.
    if snapshot.snapshot_schema_version != A::snapshot_version() {
        tracing::warn!(
            aggregate_type = %A::aggregate_type(),
            aggregate_id = %snapshot.aggregate_id,
            snapshot_schema_version = snapshot.snapshot_schema_version,
            expected_schema_version = A::snapshot_version(),
            "Snapshot schema-version mismatch; discarding stale snapshot and replaying events"
        );
        return Ok(None);
    }

    let snapshot_version = snapshot.snapshot_version;
    let aggregate_id = snapshot.aggregate_id;

    // Deleted snapshot: deserialize as DeletedState and return immediately.
    if snapshot.is_deleted {
        let Ok(deleted_state) = serde_json::from_value::<A::DeletedState>(snapshot.snapshot_data)
        else {
            // (iii) Shape mismatch on the deleted state: cache miss, replay.
            tracing::warn!(
                aggregate_type = %A::aggregate_type(),
                aggregate_id = %aggregate_id,
                "Deleted snapshot no longer deserializes into current shape; \
                 discarding stale snapshot and replaying events"
            );
            return Ok(None);
        };
        let entity_id = EntityId::from(aggregate_id);
        return Ok(Some(Loaded::Deleted(DeletedAggregateRoot::restore(
            deleted_state,
            entity_id,
            snapshot_version,
        ))));
    }

    // (iii) Active snapshot: a deserialization failure is a cache miss, NOT a
    // hard error — the full correct state is still derivable from events.
    let Ok(entity) = serde_json::from_value::<A>(snapshot.snapshot_data) else {
        tracing::warn!(
            aggregate_type = %A::aggregate_type(),
            aggregate_id = %aggregate_id,
            "Snapshot no longer deserializes into current shape; \
             discarding stale snapshot and replaying events"
        );
        return Ok(None);
    };

    let aggregate = AggregateRoot::restore(snapshot_version, entity);

    let event_stream = store.load_stream(stream_id, snapshot_version).await?;
    futures::pin_mut!(event_stream);

    replay_onto(store, event_stream, crypto_key, aggregate)
        .await
        .map(Some)
}

/// Loads an aggregate from the event store, returning its lifecycle state.
///
/// Returns `Loaded::Active(AggregateRoot<A>)` for active aggregates, or
/// `Loaded::Deleted(DeletedAggregateRoot<A>)` for deleted ones.
///
/// This function handles `DefaultEntity` aggregates (legacy), init-event
/// aggregates, and delete events. It detects which pattern is used by
/// checking `is_init()` and `is_delete()` on events during replay.
///
/// # Errors
///
/// Returns an error if events cannot be deserialized or replay fails.
pub(crate) async fn load_any<S, A>(store: &S, id: EntityId) -> Result<Loaded<A>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    use crate::{EventApplicator, UninitAggregateRoot};
    use futures::StreamExt;

    let uuid = id.as_uuid();
    let aggregate_type = A::aggregate_type();
    let stream_id = StreamId::new(aggregate_type, uuid);

    let crypto_key = resolve_crypto_key::<S, A>(store, uuid, &stream_id).await?;
    let key = crypto_key.as_deref().map(Vec::as_slice);

    // Try to load snapshot if enabled.
    //
    // A snapshot is a CACHE, never the source of truth: the authoritative state
    // is always derivable by replaying events. So a stale or incompatible
    // snapshot must self-heal — we treat it as a cache miss and fall through to
    // the full-event-replay path below rather than corrupting state or
    // hard-failing. The next commit writes a fresh snapshot at the current
    // version. A cache miss is any of:
    //   (i)   the snapshot's `aggregate_type` no longer matches `A`,
    //   (ii)  the snapshot's `snapshot_schema_version` no longer matches
    //         `A::snapshot_version()` (the aggregate's serialized shape changed),
    //   (iii) the snapshot can no longer be deserialized into `A` /
    //         `A::DeletedState`.
    //
    // The encrypted-snapshot crypto-shredding path is NOT a cache miss: a
    // snapshot that is still ciphertext after decryption means the key was
    // shredded and the data is intentionally unrecoverable — that returns
    // `KeyNotFound`, never a silent fall-through to replay (replay would fail
    // the same way), so it is checked first.
    let config = store.snapshot_config();
    if config.use_snapshots_on_load() {
        if let Some(mut snapshot) = store.load_snapshot(stream_id.clone()).await? {
            let snap_aad = snapshot_aad(uuid);
            decrypt_event_data(store, key, &mut snapshot.snapshot_data, &snap_aad)?;

            // If the snapshot is still ciphertext after the decryption pass, we
            // lacked the key to read it — the aggregate was crypto-shredded.
            // Surface `KeyNotFound` instead of letting `from_value` fail with an
            // opaque deserialization error on the `__encrypted` blob.
            if crate::crypto::is_encrypted(&snapshot.snapshot_data) {
                return Err(crate::Error::key_not_found(uuid));
            }

            if let Some(loaded) =
                try_load_from_snapshot::<S, A>(store, stream_id.clone(), key, snapshot).await?
            {
                return Ok(loaded);
            }
            // Cache miss: fall through to full replay below.
        }
    }

    // No snapshot (or a snapshot that missed): load all events
    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;
    futures::pin_mut!(event_stream);

    let Some(first_envelope) = event_stream.next().await else {
        // No events and no snapshot: the aggregate does not exist. Return a
        // recoverable `NotFound` for ALL aggregate kinds. Synthesizing a
        // default-state root here only ever worked for `DefaultEntity` types
        // and was a foot-gun (callers could not distinguish a brand-new empty
        // aggregate from a genuinely missing one); for init-event aggregates
        // `Entity::new` panics by design, so this path used to crash on any
        // unknown ID. `load_any` is generic over `A`, so it cannot branch on
        // whether `A: DefaultEntity` at runtime — universal `NotFound` is the
        // clean, panic-free behavior.
        return Err(crate::Error::not_found(
            A::aggregate_type().as_str(),
            uuid.to_string(),
        ));
    };
    let first_event = decode_event::<S, A>(store, key, uuid, first_envelope?)?;

    // Detect init vs legacy from first event
    let aggregate = if EventApplicator::is_init(&first_event) {
        // Init-event aggregate: type-state transition
        UninitAggregateRoot::<A>::new(id).apply_init_unchecked(&first_event)
    } else {
        // Legacy aggregate: Entity::new(id) + apply first event
        let mut agg = AggregateRoot::<A>::new_for_replay(id);
        agg.apply_unchecked(&first_event);
        agg
    };

    // Replay remaining events
    replay_onto(store, event_stream, key, aggregate).await
}

/// Loads an aggregate from the event store by its ID.
///
/// Returns an `AggregateRoot<A>` reconstructed by replaying events,
/// optionally using a snapshot for optimization.
///
/// If the aggregate has been deleted, returns `Error::AggregateDeleted`.
/// Use [`load_any()`] to handle both active and deleted aggregates.
///
/// # Errors
///
/// Returns `Error::NotFound` if the aggregate does not exist (no events and no
/// snapshot). This is uniform for both legacy and `@init` aggregates: `load`
/// never synthesizes an empty default-state aggregate.
/// Returns `Error::AggregateDeleted` if the aggregate has been deleted.
/// Returns an error if events cannot be deserialized or replay fails.
pub(crate) async fn load<S, A>(store: &S, id: EntityId) -> Result<AggregateRoot<A>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    load_any(store, id).await?.into_active()
}

/// Loads a deleted aggregate from the event store by its ID.
///
/// Returns `DeletedAggregateRoot<A>` if the aggregate has been deleted.
/// Returns `Error::InvalidState` if the aggregate is still active.
///
/// # Errors
///
/// Returns `Error::InvalidState` if the aggregate is not deleted.
/// Returns an error if events cannot be deserialized or replay fails.
pub(crate) async fn load_deleted<S, A>(store: &S, id: EntityId) -> Result<DeletedAggregateRoot<A>>
where
    S: EventStore,
    A: Aggregate + serde::de::DeserializeOwned,
    A::DeletedState: serde::de::DeserializeOwned,
    A::Event: serde::de::DeserializeOwned,
{
    load_any(store, id).await?.into_deleted()
}

/// Counts the number of events in a stream.
///
/// # Errors
///
/// Returns an error if the event store fails to load the stream.
pub(crate) async fn count_events<S>(store: &S, stream_id: StreamId) -> Result<usize>
where
    S: EventStore,
{
    use futures::StreamExt;

    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;
    futures::pin_mut!(event_stream);

    let mut count = 0;
    while let Some(result) = event_stream.next().await {
        result?;
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
#[allow(clippy::map_unwrap_or)]
mod tests {
    use super::*;
    use crate::Repository;

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

    // Tests for stream_exists and count_events
    use async_trait::async_trait;
    use futures::stream;

    struct MockEventStoreWithStreams {
        streams: std::collections::HashMap<StreamId, Vec<EventEnvelope>>,
    }

    impl MockEventStoreWithStreams {
        fn new() -> Self {
            Self {
                streams: std::collections::HashMap::new(),
            }
        }

        fn with_stream(mut self, stream_id: StreamId, count: usize) -> Self {
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
    async fn test_count_events_returns_correct_count() {
        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let store = MockEventStoreWithStreams::new().with_stream(stream_id.clone(), 10);

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 10, "Should count all events in stream");
    }

    #[tokio::test]
    async fn test_count_events_returns_zero_for_empty_stream() {
        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let store = MockEventStoreWithStreams::new().with_stream(stream_id.clone(), 0);

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 0, "Empty stream should have zero events");
    }

    #[tokio::test]
    async fn test_count_events_returns_zero_for_nonexistent_stream() {
        let store = MockEventStoreWithStreams::new();
        let stream_id = StreamId::new("Order", Uuid::new_v4());

        let count = count_events(&store, stream_id).await.unwrap();
        assert_eq!(count, 0, "Nonexistent stream should have zero events");
    }

    // === Tests for commit() and load() ===

    use crate::test_fixtures::{SimpleTestEntity, SimpleTestEvent};
    use crate::{AggregateRoot, SnapshotConfig};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Mock event store that supports snapshots, tracks `append`/`save_snapshot` calls,
    /// and can be configured to fail on `save_snapshot`.
    struct CommitTestStore {
        streams: Arc<Mutex<HashMap<StreamId, Vec<EventEnvelope>>>>,
        snapshots: Arc<Mutex<HashMap<StreamId, Snapshot>>>,
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
        fn new(config: SnapshotConfig) -> Self {
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

        fn with_fail_save_snapshot(mut self) -> Self {
            self.fail_save_snapshot = true;
            self
        }

        fn with_fail_append_times(mut self, times: u32) -> Self {
            self.fail_append_times = Arc::new(Mutex::new(times));
            self
        }

        fn with_hang_append(mut self) -> Self {
            self.hang_append = true;
            self
        }

        fn with_crypto(
            mut self,
            key_store: Arc<dyn crate::CryptoKeyStore>,
            provider: Arc<dyn crate::CryptoProvider>,
        ) -> Self {
            self.key_store = Some(key_store);
            self.provider = Some(provider);
            self
        }

        fn append_count(&self) -> u32 {
            *self.append_count.lock().unwrap()
        }

        fn save_snapshot_count(&self) -> u32 {
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

    // -- commit() tests --

    #[tokio::test]
    async fn test_commit_with_pending_events() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 99 }).unwrap();

        assert_eq!(agg.pending_events().len(), 2);
        assert_eq!(agg.version(), AggregateVersion::new(2));

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            agg.pending_events().len(),
            0,
            "Pending events should be cleared after commit"
        );
        assert_eq!(
            store.append_count(),
            1,
            "Append should be called exactly once"
        );

        // Verify the events were stored
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();
        assert_eq!(stored.len(), 2, "Two events should be stored");
    }

    #[tokio::test]
    async fn test_commit_with_no_pending_events() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        // No events applied, so commit should be a no-op
        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.append_count(),
            0,
            "Append should not be called when there are no pending events"
        );
    }

    #[tokio::test]
    async fn test_commit_triggers_snapshot_when_strategy_says_yes() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.save_snapshot_count(),
            1,
            "save_snapshot should be called when strategy says yes"
        );

        // Verify snapshot was stored
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshots = store.snapshots.lock().unwrap();
        let snapshot = snapshots.get(&stream_id).expect("Snapshot should exist");
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(1));
        assert_eq!(snapshot.aggregate_type, "SimpleTestEntity");
    }

    #[tokio::test]
    async fn test_commit_does_not_snapshot_when_strategy_says_no() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        store.commit(&mut agg).await.unwrap();

        assert_eq!(
            store.save_snapshot_count(),
            0,
            "save_snapshot should not be called when strategy says no"
        );
    }

    #[tokio::test]
    async fn test_commit_handles_snapshot_save_failure_gracefully() {
        let store = CommitTestStore::new(SnapshotConfig::always()).with_fail_save_snapshot();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();

        // commit should still succeed even when save_snapshot fails
        let result = store.commit(&mut agg).await;
        assert!(
            result.is_ok(),
            "Commit should succeed even if snapshot save fails"
        );

        assert_eq!(store.append_count(), 1, "Events should still be appended");
        assert_eq!(
            store.save_snapshot_count(),
            1,
            "save_snapshot should have been attempted"
        );
        assert!(
            agg.pending_events().is_empty(),
            "Pending events should still be cleared"
        );
    }

    #[tokio::test]
    async fn test_commit_clears_pending_events_after_success() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 2 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 3 }).unwrap();

        assert_eq!(agg.pending_events().len(), 3);

        store.commit(&mut agg).await.unwrap();

        assert!(
            agg.pending_events().is_empty(),
            "All pending events should be cleared after successful commit"
        );
        // AggregateVersion should be preserved
        assert_eq!(agg.version(), AggregateVersion::new(3));
    }

    #[tokio::test]
    async fn test_commit_propagates_actor_id_to_created_by() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let actor_id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        // First event without actor
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        // Second event with actor
        agg.apply_with_actor(SimpleTestEvent::Updated { value: 2 }, actor_id)
            .unwrap();

        store.commit(&mut agg).await.unwrap();

        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();

        assert_eq!(stored.len(), 2);
        assert!(
            stored[0].created_by.is_none(),
            "First event should not have created_by"
        );
        assert_eq!(
            stored[1].created_by,
            Some(actor_id.as_uuid()),
            "Second event should have actor's UUID as created_by"
        );
    }

    // -- load() tests --

    #[tokio::test]
    async fn test_load_with_no_snapshot_replays_from_beginning() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // First, commit some events
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 10 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 20 }).unwrap();
        agg.apply(SimpleTestEvent::Updated { value: 30 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Load from store
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 30,
            "Entity state should reflect all replayed events"
        );
        assert_eq!(
            loaded.version(),
            AggregateVersion::new(3),
            "AggregateVersion should match number of events"
        );
        assert!(
            loaded.pending_events().is_empty(),
            "Loaded aggregate should have no pending events"
        );
    }

    #[tokio::test]
    async fn test_load_with_snapshot_resumes_from_snapshot_version() {
        let config = SnapshotConfig::always();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();

        // Pre-save a snapshot at version 2 with value=20
        let snapshot_entity = SimpleTestEntity { id, value: 20 };
        let snapshot_data = serde_json::to_value(&snapshot_entity).unwrap();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(2),
            snapshot_data,
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), snapshot);

        // Add all events (skip-based filtering requires full stream).
        // Events at index 0 and 1 are pre-snapshot, 2 and 3 are post-snapshot.
        let envelope1 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestCreated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Created { value: 10 }).unwrap(),
        );
        let envelope2 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 20 }).unwrap(),
        );
        let envelope3 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 30 }).unwrap(),
        );
        let envelope4 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 40 }).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .insert(stream_id, vec![envelope1, envelope2, envelope3, envelope4]);

        // Load from store — should start from snapshot and replay events 3 and 4
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 40,
            "Entity should reflect snapshot + replayed events"
        );
        // AggregateVersion should be snapshot (2) + replayed events (2) = 4
        assert_eq!(loaded.version(), AggregateVersion::new(4));
    }

    #[tokio::test]
    async fn test_load_with_snapshots_disabled_ignores_snapshot() {
        let config = SnapshotConfig::builder()
            .default_strategy(crate::AlwaysSnapshot)
            .use_snapshots_on_load(false)
            .build();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();

        // Save a snapshot that should be ignored
        let snapshot_entity = SimpleTestEntity { id, value: 999 };
        let snapshot_data = serde_json::to_value(&snapshot_entity).unwrap();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());
        let snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(5),
            snapshot_data,
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), snapshot);

        // Add events from the beginning
        let envelope1 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestCreated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Created { value: 10 }).unwrap(),
        );
        let envelope2 = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            "SimpleTestUpdated".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&SimpleTestEvent::Updated { value: 20 }).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .insert(stream_id, vec![envelope1, envelope2]);

        // Load — should ignore snapshot and replay all events from beginning
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(
            loaded.value, 20,
            "Should reflect full replay, not snapshot value of 999"
        );
        assert_eq!(loaded.version(), AggregateVersion::new(2));
    }

    // -- M4: snapshot self-heals on an undeserializable / stale snapshot --

    #[tokio::test]
    async fn test_load_falls_through_to_replay_when_snapshot_does_not_deserialize() {
        // M4 (TERMINAL -> REPLAY): a persisted snapshot whose `snapshot_data`
        // can no longer be deserialized into the current aggregate shape (here:
        // the required `value` field is absent — `SimpleTestEntity` has no
        // `#[serde(default)]`) must be treated as a CACHE MISS. The store should
        // fall through to full-event replay and rebuild the correct state,
        // NOT return an opaque "Failed to deserialize snapshot entity" error.
        //
        // TODAY: `serde_json::from_value::<SimpleTestEntity>` fails and
        // `load_any` propagates `Error::custom("Failed to deserialize snapshot
        // entity: ...")`, taking the aggregate offline even though the full
        // correct state is derivable from the events.
        let config = SnapshotConfig::always();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());

        // Seed an incompatible snapshot at version 1: object lacks `value`.
        let bad_snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(1),
            serde_json::json!({ "id": id }), // missing required `value`
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), bad_snapshot);

        // Seed the full event stream that produces the real state (value = 30).
        let make_env = |event: &SimpleTestEvent| {
            EventEnvelope::new(
                Uuid::new_v4(),
                id.as_uuid(),
                "SimpleTestEntity".to_string(),
                "SimpleTestEntity.Updated".to_string(),
                crate::EventVersion::new(1),
                serde_json::to_value(event).unwrap(),
            )
        };
        store.streams.lock().unwrap().insert(
            stream_id,
            vec![
                make_env(&SimpleTestEvent::Created { value: 10 }),
                make_env(&SimpleTestEvent::Updated { value: 20 }),
                make_env(&SimpleTestEvent::Updated { value: 30 }),
            ],
        );

        let result: Result<AggregateRoot<SimpleTestEntity>> = load(&store, id).await;

        let loaded = result.expect(
            "an undeserializable snapshot must self-heal via replay, not hard-fail \
             (snapshot is a cache, never the source of truth)",
        );
        assert_eq!(
            loaded.value, 30,
            "state must be rebuilt from full event replay"
        );
        assert_eq!(
            loaded.version(),
            AggregateVersion::new(3),
            "version must reflect all replayed events, not the stale snapshot"
        );
    }

    #[tokio::test]
    async fn test_load_uses_matching_snapshot_on_happy_path() {
        // M4 GUARD (must stay green): a correctly-shaped snapshot is still USED
        // (not replayed). We prove the snapshot path is taken by giving the
        // snapshot a DIFFERENT state than replaying from version 0 would
        // produce: the snapshot is at version 2 with value=20, and only the
        // post-snapshot events (index >= 2) are applied on top. If replay were
        // (wrongly) used instead, the pre-snapshot Created/Updated events would
        // re-run and the resulting version would not match.
        let config = SnapshotConfig::always();
        let store = CommitTestStore::new(config);
        let id = crate::EntityId::new();
        let stream_id = StreamId::new("SimpleTestEntity", id.as_uuid());

        let snapshot_entity = SimpleTestEntity { id, value: 20 };
        let snapshot = Snapshot::new(
            id.as_uuid(),
            "SimpleTestEntity".to_string(),
            AggregateVersion::new(2),
            serde_json::to_value(&snapshot_entity).unwrap(),
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), snapshot);

        let make_env = |event: &SimpleTestEvent| {
            EventEnvelope::new(
                Uuid::new_v4(),
                id.as_uuid(),
                "SimpleTestEntity".to_string(),
                "SimpleTestEntity.Updated".to_string(),
                crate::EventVersion::new(1),
                serde_json::to_value(event).unwrap(),
            )
        };
        // Four events; snapshot is at version 2 so only events at index 2,3 apply.
        store.streams.lock().unwrap().insert(
            stream_id,
            vec![
                make_env(&SimpleTestEvent::Created { value: 10 }),
                make_env(&SimpleTestEvent::Updated { value: 20 }),
                make_env(&SimpleTestEvent::Updated { value: 30 }),
                make_env(&SimpleTestEvent::Updated { value: 40 }),
            ],
        );

        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(loaded.value, 40, "snapshot + post-snapshot replay");
        assert_eq!(
            loaded.version(),
            AggregateVersion::new(4),
            "snapshot version (2) + 2 replayed events; proves snapshot path was taken"
        );
    }

    /// Aggregate whose snapshot schema version is `1` (state shape was
    /// "changed" relative to the default `0`), used to exercise the M4
    /// version-mismatch -> replay path without disturbing the shared
    /// `SimpleTestEntity` fixture.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct VersionedTestEntity {
        id: EntityId,
        value: i32,
    }

    impl crate::Entity for VersionedTestEntity {
        fn new(id: EntityId) -> Self {
            Self { id, value: 0 }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl crate::DefaultEntity for VersionedTestEntity {}

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum VersionedTestEvent {
        Created { value: i32 },
        Updated { value: i32 },
    }

    impl crate::DomainEvent for VersionedTestEvent {
        type Aggregate = VersionedTestEntity;
        fn event_type(&self) -> &'static str {
            match self {
                Self::Created { .. } => "VersionedTestEntity.Created",
                Self::Updated { .. } => "VersionedTestEntity.Updated",
            }
        }
        fn event_version(&self) -> crate::EventVersion {
            crate::EventVersion::new(1)
        }
        fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
    }

    impl crate::EventApplicator<VersionedTestEntity> for VersionedTestEvent {
        fn dispatch(
            &self,
            entity: &mut VersionedTestEntity,
        ) -> std::result::Result<(), crate::test_fixtures::SimpleTestError> {
            self.dispatch_unchecked(entity);
            Ok(())
        }
        fn dispatch_unchecked(&self, entity: &mut VersionedTestEntity) {
            match self {
                Self::Created { value } | Self::Updated { value } => entity.value = *value,
            }
        }
    }

    impl crate::Aggregate for VersionedTestEntity {
        type Event = VersionedTestEvent;
        type Error = crate::test_fixtures::SimpleTestError;
        type DeletedState = Self;

        fn snapshot_version() -> u32 {
            1
        }
    }

    #[tokio::test]
    async fn test_load_falls_through_to_replay_on_snapshot_schema_version_mismatch() {
        // M4 (VERSION-MISMATCH -> REPLAY): a snapshot stamped at schema version 0
        // is stale for an aggregate whose `snapshot_version()` is now 1. Even
        // though the snapshot data deserializes cleanly, its schema stamp no
        // longer matches, so it must be discarded as a cache miss and the state
        // rebuilt from full event replay. A subsequent commit must then persist a
        // fresh snapshot stamped at the current version (1).
        assert_eq!(VersionedTestEntity::snapshot_version(), 1);

        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();
        let stream_id = StreamId::new("VersionedTestEntity", id.as_uuid());

        // Seed a structurally-valid but schema-version-0 snapshot at version 2
        // (the full stream length) carrying a sentinel value (999) that replay
        // never produces. The version (2) is deliberately >= the event count:
        // `load_stream` skips `from_version` events, so if the stale snapshot
        // were (wrongly) USED, no post-snapshot events would replay over it and
        // the loaded value would stay 999 — whereas a correct cache-miss replays
        // from genesis and yields 20. The assertion on 20 therefore FAILS if the
        // schema-version check is removed (the snapshot would be used → 999).
        let stale_entity = VersionedTestEntity { id, value: 999 };
        let stale_snapshot = Snapshot::new_with_schema_version(
            id.as_uuid(),
            "VersionedTestEntity".to_string(),
            AggregateVersion::new(2),
            serde_json::to_value(&stale_entity).unwrap(),
            0, // old schema version
        );
        store
            .snapshots
            .lock()
            .unwrap()
            .insert(stream_id.clone(), stale_snapshot);

        let make_env = |event: &VersionedTestEvent| {
            EventEnvelope::new(
                Uuid::new_v4(),
                id.as_uuid(),
                "VersionedTestEntity".to_string(),
                "VersionedTestEntity.Updated".to_string(),
                crate::EventVersion::new(1),
                serde_json::to_value(event).unwrap(),
            )
        };
        store.streams.lock().unwrap().insert(
            stream_id.clone(),
            vec![
                make_env(&VersionedTestEvent::Created { value: 10 }),
                make_env(&VersionedTestEvent::Updated { value: 20 }),
            ],
        );

        let loaded: AggregateRoot<VersionedTestEntity> = load(&store, id).await.unwrap();
        assert_eq!(
            loaded.value, 20,
            "stale-version snapshot must be skipped; state rebuilt from replay"
        );
        assert_eq!(loaded.version(), AggregateVersion::new(2));

        // A subsequent commit must write a fresh snapshot stamped at version 1.
        let mut agg = loaded;
        agg.apply(VersionedTestEvent::Updated { value: 30 })
            .unwrap();
        store.commit(&mut agg).await.unwrap();

        let snapshots = store.snapshots.lock().unwrap();
        let fresh = snapshots.get(&stream_id).expect("fresh snapshot written");
        assert_eq!(
            fresh.snapshot_schema_version, 1,
            "fresh snapshot must be stamped at the current schema version"
        );
    }

    #[tokio::test]
    async fn test_load_with_no_events_returns_not_found() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Load an entity that has no events at all: an empty stream means the
        // aggregate does not exist, so `load` must return `NotFound` rather than
        // a synthetic default-state root (unified behavior across all aggregate
        // kinds — see the empty-stream branch of `load_any`).
        let result: Result<AggregateRoot<SimpleTestEntity>> = load(&store, id).await;

        let err = result.expect_err("loading an aggregate with no events must be NotFound");
        assert!(err.is_not_found(), "expected NotFound, got: {err:?}");
    }

    // -- repository() convenience method tests --

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

    // === Tests for load_any(), load_deleted(), commit_deleted() ===

    /// Entity that supports delete events for testing.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct DeletableEntity {
        id: EntityId,
        value: i32,
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
    enum DeletableEvent {
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
    struct DeletableError;

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

    #[tokio::test]
    async fn prepare_commit_carries_the_active_root_claims() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 7 }).unwrap();

        let prepared = prepare_commit(&store, &agg).await.unwrap().unwrap();
        let expected = agg.entity().claims();

        assert_eq!(prepared.claims.len(), expected.len());
        assert_eq!(prepared.claims[0].claim_type, expected[0].claim_type);
        assert_eq!(prepared.claims[0].claim_key, expected[0].claim_key);
        assert!(!prepared.clear_claims);
    }

    #[tokio::test]
    async fn prepare_commit_deleted_clears_claims() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 7 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "gone".to_string(),
            })
            .unwrap();

        let prepared = prepare_commit_deleted(&store, &deleted)
            .await
            .unwrap()
            .unwrap();

        assert!(prepared.claims.is_empty());
        assert!(prepared.clear_claims);
    }

    #[tokio::test]
    async fn test_load_any_returns_active_for_regular_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();
        agg.apply(DeletableEvent::Updated { value: 20 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_active());
        let active = loaded.into_active().unwrap();
        assert_eq!(active.value, 20);
    }

    #[tokio::test]
    async fn test_load_any_returns_deleted_when_delete_event_present() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Commit regular events
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Now commit a delete event via manual envelope
        let delete_event = DeletableEvent::Deleted {
            reason: "test".to_string(),
        };
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "DeletableEntity".to_string(),
            "DeletableEntity.Deleted".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&delete_event).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .get_mut(&StreamId::new("DeletableEntity", id.as_uuid()))
            .unwrap()
            .push(envelope);

        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_deleted());
        let deleted = loaded.into_deleted().unwrap();
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.entity_id(), id);
    }

    #[tokio::test]
    async fn test_load_errors_on_deleted_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Commit regular events + delete event
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let delete_event = DeletableEvent::Deleted {
            reason: "gone".to_string(),
        };
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "DeletableEntity".to_string(),
            "DeletableEntity.Deleted".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&delete_event).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .get_mut(&StreamId::new("DeletableEntity", id.as_uuid()))
            .unwrap()
            .push(envelope);

        let result = load::<_, DeletableEntity>(&store, id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_aggregate_deleted());
    }

    #[tokio::test]
    async fn test_load_deleted_succeeds_for_deleted_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 42 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let delete_event = DeletableEvent::Deleted {
            reason: "bye".to_string(),
        };
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            id.as_uuid(),
            "DeletableEntity".to_string(),
            "DeletableEntity.Deleted".to_string(),
            crate::EventVersion::new(1),
            serde_json::to_value(&delete_event).unwrap(),
        );
        store
            .streams
            .lock()
            .unwrap()
            .get_mut(&StreamId::new("DeletableEntity", id.as_uuid()))
            .unwrap()
            .push(envelope);

        let deleted = load_deleted::<_, DeletableEntity>(&store, id)
            .await
            .unwrap();
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.version(), AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_load_deleted_errors_for_active_aggregate() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let result = load_deleted::<_, DeletableEntity>(&store, id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_commit_deleted_persists_events() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();

        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "test".to_string(),
            })
            .unwrap();

        assert_eq!(deleted.pending_events().len(), 2);

        store.commit_deleted(&mut deleted).await.unwrap();

        assert!(
            deleted.pending_events().is_empty(),
            "Pending events should be cleared after commit"
        );

        // Verify events were stored
        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();
        assert_eq!(stored.len(), 2, "Both create and delete events stored");
    }

    #[tokio::test]
    async fn test_commit_deleted_with_no_pending_events_is_noop() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();

        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "test".to_string(),
            })
            .unwrap();

        // Clear pending to simulate already-committed
        deleted.clear_pending_events();
        store.commit_deleted(&mut deleted).await.unwrap();

        assert_eq!(store.append_count(), 0, "No append for empty pending");
    }

    #[tokio::test]
    async fn test_commit_deleted_saves_snapshot() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();

        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "test".to_string(),
            })
            .unwrap();

        store.commit_deleted(&mut deleted).await.unwrap();

        assert_eq!(store.save_snapshot_count(), 1);

        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        let snapshots = store.snapshots.lock().unwrap();
        let snapshot = snapshots.get(&stream_id).expect("Snapshot should exist");
        assert!(snapshot.is_deleted);
        assert_eq!(snapshot.snapshot_version, AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_commit_deleted_propagates_actor_id() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();
        let actor_id = crate::EntityId::new();

        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 10 }).unwrap();

        let mut deleted = agg
            .apply_delete_with_actor(
                DeletableEvent::Deleted {
                    reason: "test".to_string(),
                },
                actor_id,
            )
            .unwrap();

        store.commit_deleted(&mut deleted).await.unwrap();

        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        let streams = store.streams.lock().unwrap();
        let stored = streams.get(&stream_id).unwrap();
        // The delete event (last one) should have the actor ID
        let last = stored.last().unwrap();
        assert_eq!(last.created_by, Some(actor_id.as_uuid()));
    }

    #[tokio::test]
    async fn test_load_any_active_then_commit_delete_then_load_any_deleted() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Create and save
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 42 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Load — should be active
        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_active());
        let agg = loaded.into_active().unwrap();

        // Delete and save
        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "done".to_string(),
            })
            .unwrap();
        store.commit_deleted(&mut deleted).await.unwrap();

        // Load again — should be deleted
        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_deleted());
        let deleted = loaded.into_deleted().unwrap();
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.version(), AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_load_any_returns_deleted_from_deleted_snapshot() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();

        // Pre-save a deleted snapshot
        let deleted_entity = DeletableEntity { id, value: -1 };
        let snapshot_data = serde_json::to_value(&deleted_entity).unwrap();
        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        let snapshot = Snapshot::new_deleted(
            id.as_uuid(),
            "DeletableEntity".to_string(),
            AggregateVersion::new(3),
            snapshot_data,
        );
        store.snapshots.lock().unwrap().insert(stream_id, snapshot);

        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_deleted());
        let deleted = loaded.into_deleted().unwrap();
        assert_eq!(deleted.state().value, -1);
        assert_eq!(deleted.entity_id(), id);
        assert_eq!(deleted.version(), AggregateVersion::new(3));
    }

    #[tokio::test]
    async fn test_load_any_deleted_snapshot_round_trip() {
        let store = CommitTestStore::new(SnapshotConfig::always());
        let id = crate::EntityId::new();

        // Create, then delete
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 42 }).unwrap();

        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "done".to_string(),
            })
            .unwrap();

        store.commit_deleted(&mut deleted).await.unwrap();

        // Verify snapshot was saved as deleted
        let stream_id = StreamId::new("DeletableEntity", id.as_uuid());
        {
            let snapshots = store.snapshots.lock().unwrap();
            let snapshot = snapshots.get(&stream_id).expect("Snapshot should exist");
            assert!(snapshot.is_deleted);
        }

        // Load — should come from deleted snapshot
        let loaded = load_any::<_, DeletableEntity>(&store, id).await.unwrap();
        assert!(loaded.is_deleted());
        let loaded_deleted = loaded.into_deleted().unwrap();
        assert_eq!(loaded_deleted.state().value, -1);
        assert_eq!(loaded_deleted.version(), AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn commit_keeps_pending_events_when_append_fails() {
        let store = CommitTestStore::new(SnapshotConfig::disabled()).with_fail_append_times(1);
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 1 }).unwrap();

        let result = store.commit(&mut agg).await;

        assert!(result.is_err());
        assert_eq!(
            agg.pending_events().len(),
            1,
            "a failed commit must not drop the pending events"
        );

        store.commit(&mut agg).await.unwrap();
        assert_eq!(
            store.append_count(),
            2,
            "retrying must actually append again, not silently no-op"
        );
        assert!(agg.pending_events().is_empty());
    }

    #[tokio::test]
    async fn commit_deleted_keeps_pending_events_when_append_fails() {
        let store = CommitTestStore::new(SnapshotConfig::disabled()).with_fail_append_times(1);
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 1 }).unwrap();
        let mut deleted = agg
            .apply_delete(DeletableEvent::Deleted {
                reason: "done".to_string(),
            })
            .unwrap();

        let result = store.commit_deleted(&mut deleted).await;

        assert!(result.is_err());
        assert_eq!(
            deleted.pending_events().len(),
            2,
            "a failed commit_deleted must not drop the pending events"
        );

        store.commit_deleted(&mut deleted).await.unwrap();
        assert!(deleted.pending_events().is_empty());
    }

    #[tokio::test]
    async fn commit_keeps_pending_events_when_the_save_future_is_dropped() {
        let store = CommitTestStore::new(SnapshotConfig::disabled()).with_hang_append();
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<DeletableEntity>::new(id);
        agg.apply(DeletableEvent::Created { value: 1 }).unwrap();

        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(20), store.commit(&mut agg))
                .await
                .is_err();
        assert!(
            timed_out,
            "append must hang until the timeout, simulating a cancelled request"
        );

        assert_eq!(
            agg.pending_events().len(),
            1,
            "a dropped commit future must not drop the pending events"
        );
    }

    mod crypto_behavior {
        use super::*;
        use crate::CryptoKeyStore;
        use std::collections::{HashMap as Map, HashSet};
        use std::sync::Mutex;

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct SecretThing {
            id: crate::EntityId,
            email: String,
        }

        impl crate::Entity for SecretThing {
            fn new(id: crate::EntityId) -> Self {
                Self {
                    id,
                    email: String::new(),
                }
            }
            fn entity_id(&self) -> crate::EntityId {
                self.id
            }
        }
        impl crate::DefaultEntity for SecretThing {}

        #[derive(Debug, thiserror::Error)]
        #[error("secret thing error")]
        struct SecretThingError;
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
        struct SecretCreated {
            email: String,
        }

        impl DomainEvent for SecretCreated {
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
                entity.email = self.email.clone();
            }
        }

        impl crate::EventApplicator<SecretThing> for SecretCreated {
            fn dispatch(
                &self,
                entity: &mut SecretThing,
            ) -> std::result::Result<(), SecretThingError> {
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
        struct PartialSecretThing {
            id: crate::EntityId,
            public: String,
            secret: String,
        }

        impl crate::Entity for PartialSecretThing {
            fn new(id: crate::EntityId) -> Self {
                Self {
                    id,
                    public: String::new(),
                    secret: String::new(),
                }
            }
            fn entity_id(&self) -> crate::EntityId {
                self.id
            }
        }
        impl crate::DefaultEntity for PartialSecretThing {}

        #[derive(Debug, thiserror::Error)]
        #[error("partial secret thing error")]
        struct PartialSecretThingError;
        impl crate::AggregateError for PartialSecretThingError {}

        impl Aggregate for PartialSecretThing {
            type Event = PartialSecretCreated;
            type Error = PartialSecretThingError;
            type DeletedState = Self;
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct PartialSecretCreated {
            public: String,
            secret: String,
        }

        impl DomainEvent for PartialSecretCreated {
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
                entity.public = self.public.clone();
                entity.secret = self.secret.clone();
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

        /// Deterministic provider that binds ciphertext to its AAD by
        /// prefixing it, so a wrong AAD on decrypt is detected instead of
        /// silently accepted (unlike a bare XOR mock).
        struct AadCheckingCryptoProvider;

        impl crate::CryptoProvider for AadCheckingCryptoProvider {
            fn encrypt(&self, key: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
                let mut out = Vec::new();
                let aad_len = u32::try_from(aad.len())
                    .map_err(|_| crate::Error::encryption("aad too long"))?;
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

        /// In-memory [`CryptoKeyStore`](crate::CryptoKeyStore) with an atomic
        /// `get_or_insert_key`, so it can stand in for a real backend in
        /// concurrency-sensitive assertions.
        #[derive(Default)]
        struct MockCryptoKeyStore {
            keys: Mutex<Map<Uuid, Vec<u8>>>,
            shredded: Mutex<HashSet<Uuid>>,
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
        struct AlwaysWinsKeyStore {
            winning_key: Vec<u8>,
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
        fn crypto_store() -> (CommitTestStore, Arc<MockCryptoKeyStore>) {
            let key_store = Arc::new(MockCryptoKeyStore::default());
            let store = CommitTestStore::new(SnapshotConfig::disabled())
                .with_crypto(key_store.clone(), Arc::new(AadCheckingCryptoProvider));
            (store, key_store)
        }

        #[tokio::test]
        async fn commit_of_a_fully_encrypted_aggregate_replaces_event_data() {
            let (store, _key_store) = crypto_store();
            let id = crate::EntityId::new();
            let mut agg = AggregateRoot::<SecretThing>::new(id);
            agg.apply(SecretCreated {
                email: "alice@example.com".into(),
            })
            .unwrap();

            store.commit(&mut agg).await.unwrap();

            let stream_id = StreamId::new(SecretThing::aggregate_type(), id.as_uuid());
            let stored = store.streams.lock().unwrap().get(&stream_id).unwrap()[0].clone();
            assert!(
                crate::crypto::is_encrypted(&stored.event_data),
                "event_data must be replaced by its ciphertext envelope"
            );
        }

        #[tokio::test]
        async fn load_of_a_fully_encrypted_aggregate_decrypts_back_to_the_original() {
            let (store, _key_store) = crypto_store();
            let id = crate::EntityId::new();
            let mut agg = AggregateRoot::<SecretThing>::new(id);
            agg.apply(SecretCreated {
                email: "alice@example.com".into(),
            })
            .unwrap();
            store.commit(&mut agg).await.unwrap();

            let loaded = load_any::<CommitTestStore, SecretThing>(&store, id)
                .await
                .unwrap()
                .into_active()
                .unwrap();

            assert_eq!(loaded.entity().email, "alice@example.com");
        }

        #[tokio::test]
        async fn commit_of_a_field_encrypted_aggregate_only_touches_declared_fields() {
            let (store, _key_store) = crypto_store();
            let id = crate::EntityId::new();
            let mut agg = AggregateRoot::<PartialSecretThing>::new(id);
            agg.apply(PartialSecretCreated {
                public: "visible".into(),
                secret: "hidden".into(),
            })
            .unwrap();

            store.commit(&mut agg).await.unwrap();

            let stream_id = StreamId::new(PartialSecretThing::aggregate_type(), id.as_uuid());
            let stored = store.streams.lock().unwrap().get(&stream_id).unwrap()[0].clone();
            let obj = stored.event_data.as_object().unwrap();
            assert_eq!(obj.get("public").unwrap(), "visible");
            assert!(
                crate::crypto::is_encrypted(obj.get("secret").unwrap()),
                "the declared field must be encrypted"
            );
        }

        #[tokio::test]
        async fn a_snapshot_of_an_encrypted_aggregate_is_produced_encrypted() {
            let store = CommitTestStore::new(SnapshotConfig::always()).with_crypto(
                Arc::new(MockCryptoKeyStore::default()),
                Arc::new(AadCheckingCryptoProvider),
            );
            let id = crate::EntityId::new();
            let mut agg = AggregateRoot::<SecretThing>::new(id);
            agg.apply(SecretCreated {
                email: "shielded@example.com".into(),
            })
            .unwrap();

            store.commit(&mut agg).await.unwrap();

            let stream_id = StreamId::new(SecretThing::aggregate_type(), id.as_uuid());
            let snapshot = store
                .snapshots
                .lock()
                .unwrap()
                .get(&stream_id)
                .cloned()
                .expect("snapshot must have been saved");
            assert!(
                crate::crypto::is_encrypted(&snapshot.snapshot_data),
                "an encrypted aggregate's snapshot must be encrypted at rest"
            );

            let loaded = load_any::<CommitTestStore, SecretThing>(&store, id)
                .await
                .unwrap()
                .into_active()
                .unwrap();
            assert_eq!(loaded.entity().email, "shielded@example.com");
        }

        #[tokio::test]
        async fn ensure_crypto_key_returns_the_existing_key_unchanged() {
            let key_store = Arc::new(MockCryptoKeyStore::default());
            let id = Uuid::new_v4();
            key_store.upsert_key(id, vec![9; 32]).await.unwrap();
            let store = CommitTestStore::new(SnapshotConfig::disabled())
                .with_crypto(key_store, Arc::new(AadCheckingCryptoProvider));

            let key = ensure_crypto_key(&store, id).await.unwrap();

            assert_eq!(&*key, &[9; 32]);
        }

        #[tokio::test]
        async fn ensure_crypto_key_returns_whichever_key_won_the_race() {
            let winning_key = vec![7; 32];
            let store = CommitTestStore::new(SnapshotConfig::disabled()).with_crypto(
                Arc::new(AlwaysWinsKeyStore {
                    winning_key: winning_key.clone(),
                }),
                Arc::new(AadCheckingCryptoProvider),
            );

            let key = ensure_crypto_key(&store, Uuid::new_v4()).await.unwrap();

            assert_eq!(
                key.to_vec(),
                winning_key,
                "the caller's own candidate must never override the race's winner"
            );
        }

        #[tokio::test]
        async fn ensure_crypto_key_refuses_a_shredded_aggregate() {
            let key_store = Arc::new(MockCryptoKeyStore::default());
            let id = Uuid::new_v4();
            key_store.upsert_key(id, vec![1; 32]).await.unwrap();
            key_store.delete_key(id).await.unwrap();
            let store = CommitTestStore::new(SnapshotConfig::disabled())
                .with_crypto(key_store, Arc::new(AadCheckingCryptoProvider));

            let result = ensure_crypto_key(&store, id).await;

            assert!(result.unwrap_err().is_key_not_found());
        }

        #[tokio::test]
        async fn load_any_of_a_fully_encrypted_aggregate_needs_a_key_store() {
            let store = CommitTestStore::new(SnapshotConfig::disabled());

            let result =
                load_any::<CommitTestStore, SecretThing>(&store, crate::EntityId::new()).await;

            assert!(result.unwrap_err().is_invalid_state());
        }

        #[tokio::test]
        async fn load_any_of_a_fully_encrypted_aggregate_with_no_key_is_key_not_found() {
            let key_store = Arc::new(MockCryptoKeyStore::default());
            let store = CommitTestStore::new(SnapshotConfig::disabled())
                .with_crypto(key_store, Arc::new(AadCheckingCryptoProvider));

            let result =
                load_any::<CommitTestStore, SecretThing>(&store, crate::EntityId::new()).await;

            assert!(result.unwrap_err().is_key_not_found());
        }

        #[tokio::test]
        async fn load_any_of_a_field_encrypted_aggregate_with_no_store_is_not_found() {
            let store = CommitTestStore::new(SnapshotConfig::disabled());

            let result =
                load_any::<CommitTestStore, PartialSecretThing>(&store, crate::EntityId::new())
                    .await;

            assert!(
                result.unwrap_err().is_not_found(),
                "no key store and no committed data must fall through to NotFound, \
                 not surface a crypto error"
            );
        }

        #[tokio::test]
        async fn load_any_of_a_field_encrypted_aggregate_with_no_key_and_no_data_is_not_found() {
            let key_store = Arc::new(MockCryptoKeyStore::default());
            let store = CommitTestStore::new(SnapshotConfig::disabled())
                .with_crypto(key_store, Arc::new(AadCheckingCryptoProvider));

            let result =
                load_any::<CommitTestStore, PartialSecretThing>(&store, crate::EntityId::new())
                    .await;

            assert!(result.unwrap_err().is_not_found());
        }

        #[tokio::test]
        async fn load_any_of_a_field_encrypted_aggregate_with_committed_data_and_no_key_is_key_not_found(
        ) {
            let (store, key_store) = crypto_store();
            let id = crate::EntityId::new();
            let mut agg = AggregateRoot::<PartialSecretThing>::new(id);
            agg.apply(PartialSecretCreated {
                public: "visible".into(),
                secret: "hidden".into(),
            })
            .unwrap();
            store.commit(&mut agg).await.unwrap();
            key_store.delete_key(id.as_uuid()).await.unwrap();

            let result = load_any::<CommitTestStore, PartialSecretThing>(&store, id).await;

            assert!(result.unwrap_err().is_key_not_found());
        }
    }
}
