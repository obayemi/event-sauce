//! Event store trait for event persistence.
//!
//! Defines the `EventStore` trait for persisting and retrieving events with streaming support.

use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use uuid::Uuid;

use crate::{
    Aggregate, AggregateClaim, AggregateRoot, AggregateType, AggregateVersion,
    DeletedAggregateRoot, DomainEvent, EntityId, EventEnvelope, EventLogEntry, Loaded, Repository,
    Result, SnapshotConfig,
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

/// Stream ID uniquely identifying an event stream.
///
/// Combines aggregate type and aggregate ID to create a unique stream identifier.
///
/// # Examples
///
/// ```
/// use event_sauce_core::StreamId;
/// use uuid::Uuid;
///
/// let stream_id = StreamId::new("User", Uuid::new_v4());
/// assert_eq!(stream_id.aggregate_type(), "User");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamId {
    aggregate_type: AggregateType,
    aggregate_id: Uuid,
}

impl StreamId {
    /// Creates a new stream ID.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::StreamId;
    /// use uuid::Uuid;
    ///
    /// let stream_id = StreamId::new("Order", Uuid::new_v4());
    /// ```
    #[must_use]
    pub fn new(aggregate_type: impl Into<AggregateType>, aggregate_id: Uuid) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id,
        }
    }

    /// Returns the aggregate type.
    #[must_use]
    pub fn aggregate_type(&self) -> &AggregateType {
        &self.aggregate_type
    }

    /// Returns the aggregate ID.
    #[must_use]
    pub fn aggregate_id(&self) -> Uuid {
        self.aggregate_id
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.aggregate_type, self.aggregate_id)
    }
}

/// An opaque, store-issued, strictly-monotonic position in the global event log.
///
/// A position is a token the store assigns to each appended event in insertion
/// order — for the `PostgreSQL` backend it is the `BIGSERIAL` `events.id`. Treat
/// it as opaque: do not assume positions are dense (gaps can appear when an
/// append is rolled back, e.g. a unique-violation conflict), and do not
/// reconstruct it by counting events. To resume processing, checkpoint the
/// [`Position`] of the last entry you handled, then call
/// [`EventStore::stream_all`] with it; the store yields only entries whose
/// position is strictly greater.
///
/// [`Position::start`] is the position before any event — passing it streams the
/// whole log.
///
/// # Examples
///
/// ```
/// use event_sauce_core::Position;
///
/// let pos = Position::from(100);
/// assert_eq!(pos.as_i64(), 100);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(i64);

impl Position {
    /// Creates a new position.
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the position as an i64.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    /// Returns the start position (0).
    #[must_use]
    pub const fn start() -> Self {
        Self(0)
    }
}

impl From<i64> for Position {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<Position> for i64 {
    fn from(pos: Position) -> Self {
        pos.0
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
}

impl Snapshot {
    /// Creates a new snapshot for an active aggregate.
    #[must_use]
    pub fn new(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type: aggregate_type.into(),
            snapshot_version,
            snapshot_data,
            is_deleted: false,
        }
    }

    /// Creates a new snapshot for a deleted aggregate.
    ///
    /// The `snapshot_data` should contain the serialized `A::DeletedState`.
    #[must_use]
    pub fn new_deleted(
        aggregate_id: Uuid,
        aggregate_type: impl Into<AggregateType>,
        snapshot_version: AggregateVersion,
        snapshot_data: serde_json::Value,
    ) -> Self {
        Self {
            aggregate_id,
            aggregate_type: aggregate_type.into(),
            snapshot_version,
            snapshot_data,
            is_deleted: true,
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

    /// Creates a subscription builder pre-configured with this event store.
    fn subscription_builder(
        self: &std::sync::Arc<Self>,
        name: impl Into<String>,
    ) -> crate::SubscriptionBuilder<Self>
    where
        Self: Sized + 'static,
    {
        let mut builder = crate::SubscriptionBuilder::new(name, std::sync::Arc::clone(self));

        if let Some(checkpoint_store) = self.checkpoint_store() {
            builder = builder.checkpoint_store(checkpoint_store);
        }

        builder
    }

    /// Creates a [`Repository`] for the given aggregate type, wrapping this event store.
    ///
    /// This is a convenience method that avoids verbose turbofish syntax.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let store = Arc::new(MyEventStore::new());
    /// let user_repo = store.repository::<User>();
    /// ```
    fn repository<A>(self: &Arc<Self>) -> Repository<Self, A>
    where
        Self: Sized + 'static,
        A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
        A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
        A::Event: serde::Serialize + serde::de::DeserializeOwned,
    {
        Repository::new(Arc::clone(self))
    }

    /// Commits pending events from an aggregate root to the event store.
    ///
    /// This method:
    /// 1. Extracts pending events from the aggregate root
    /// 2. Converts them to event envelopes
    /// 3. Encrypts event data if the aggregate is private
    /// 4. Appends them to the event store with optimistic concurrency control
    /// 5. Creates a snapshot if the strategy indicates it should (encrypted for encrypted aggregates)
    /// 6. Clears the pending events on success
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if another process modified the aggregate.
    /// Returns `Error::Encryption` if encryption fails for a encrypted aggregate.
    /// Returns `Error::InvalidState` if a encrypted aggregate lacks crypto configuration.
    async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::Event: serde::Serialize,
    {
        if let Some(prepared) = prepare_commit(self, aggregate).await? {
            flush_prepared(self, prepared).await?;
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
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if another process modified the aggregate.
    /// Returns `Error::Encryption` if encryption fails for an encrypted aggregate.
    /// Returns `Error::InvalidState` if an encrypted aggregate lacks crypto configuration.
    async fn commit_deleted<A>(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::DeletedState: serde::Serialize,
        A::Event: serde::Serialize,
    {
        if let Some(prepared) = prepare_commit_deleted(self, aggregate).await? {
            flush_prepared(self, prepared).await?;
        }
        Ok(())
    }
}

/// Prepares a commit without persisting it.
///
/// Extracts pending events from the aggregate root, serializes and encrypts
/// them, computes a snapshot if needed, and clears the pending events.
/// Returns `None` if there are no pending events.
#[allow(clippy::too_many_lines)]
pub(crate) async fn prepare_commit<S: EventStore + ?Sized, A>(
    store: &S,
    aggregate: &mut AggregateRoot<A>,
) -> Result<Option<PreparedCommit>>
where
    A: Aggregate + serde::Serialize,
    A::Event: serde::Serialize,
{
    let pending = aggregate.pending_events_with_actors();
    if pending.is_empty() {
        return Ok(None);
    }

    let aggregate_id = aggregate.entity_id().as_uuid();
    let aggregate_type = AggregateRoot::<A>::aggregate_type();
    #[allow(clippy::cast_possible_wrap)]
    let pending_count = pending.len() as i64;
    let expected_version =
        AggregateVersion::new(aggregate.version().as_i64().saturating_sub(pending_count));

    let envelopes: Result<Vec<EventEnvelope>> = pending
        .iter()
        .map(|pe| {
            let mut envelope = pe.event.to_envelope(aggregate_id)?;
            if let Some(actor_id) = pe.actor_id {
                envelope = envelope.with_created_by(actor_id.as_uuid());
            }
            if let Some(metadata) = &pe.metadata {
                envelope = envelope.with_metadata(metadata.clone());
            }
            Ok(envelope)
        })
        .collect();
    let mut envelopes = envelopes?;

    // Encrypt event data for encrypted aggregates
    if A::is_encrypted() {
        let crypto_key = ensure_crypto_key(store, aggregate_id).await?;
        let provider = require_crypto_provider(store)?;

        for envelope in &mut envelopes {
            envelope.event_data =
                crate::crypto::encrypt_value(provider, &crypto_key, &envelope.event_data)?;
        }
    } else if A::Event::has_any_encrypted_fields() {
        // Field-level encryption: encrypt only specific fields per event
        let crypto_key = ensure_crypto_key(store, aggregate_id).await?;
        let provider = require_crypto_provider(store)?;

        for (envelope, pe) in envelopes.iter_mut().zip(pending.iter()) {
            let fields = pe.event.encrypted_fields();
            if !fields.is_empty() {
                crate::crypto::encrypt_fields(
                    provider,
                    &crypto_key,
                    &mut envelope.event_data,
                    fields,
                )?;
            }
        }
    }

    // Compute snapshot if strategy indicates we should
    let config = store.snapshot_config();
    let strategy = config.strategy_for_type(aggregate_type.as_str());
    let current_version = aggregate.version();

    let snapshot = if strategy.should_snapshot_range(expected_version, current_version) {
        build_snapshot::<S, A>(
            store,
            aggregate_id,
            &aggregate_type,
            current_version,
            || serde_json::to_value(aggregate.entity()),
        )
        .await
    } else {
        None
    };

    let claims = aggregate.entity().claims();
    let stream_id = StreamId::new(aggregate_type, aggregate_id);

    aggregate.clear_pending_events();

    Ok(Some(PreparedCommit {
        stream_id,
        events: envelopes,
        expected_version,
        snapshot,
        claims,
        clear_claims: false,
    }))
}

/// Prepares a commit for a deleted aggregate without persisting it.
///
/// Works like [`prepare_commit()`] but operates on a `DeletedAggregateRoot<A>`.
/// The snapshot stores the serialized `DeletedState` with `is_deleted = true`.
#[allow(clippy::too_many_lines)]
pub(crate) async fn prepare_commit_deleted<S: EventStore + ?Sized, A>(
    store: &S,
    aggregate: &mut DeletedAggregateRoot<A>,
) -> Result<Option<PreparedCommit>>
where
    A: Aggregate + serde::Serialize,
    A::DeletedState: serde::Serialize,
    A::Event: serde::Serialize,
{
    let pending = aggregate.pending_events_with_actors();
    if pending.is_empty() {
        return Ok(None);
    }

    let aggregate_id = aggregate.entity_id().as_uuid();
    let aggregate_type = DeletedAggregateRoot::<A>::aggregate_type();
    #[allow(clippy::cast_possible_wrap)]
    let pending_count = pending.len() as i64;
    let expected_version =
        AggregateVersion::new(aggregate.version().as_i64().saturating_sub(pending_count));

    let envelopes: Result<Vec<EventEnvelope>> = pending
        .iter()
        .map(|pe| {
            let mut envelope = pe.event.to_envelope(aggregate_id)?;
            if let Some(actor_id) = pe.actor_id {
                envelope = envelope.with_created_by(actor_id.as_uuid());
            }
            if let Some(metadata) = &pe.metadata {
                envelope = envelope.with_metadata(metadata.clone());
            }
            Ok(envelope)
        })
        .collect();
    let mut envelopes = envelopes?;

    // Encrypt event data for encrypted aggregates
    if A::is_encrypted() {
        let crypto_key = ensure_crypto_key(store, aggregate_id).await?;
        let provider = require_crypto_provider(store)?;

        for envelope in &mut envelopes {
            envelope.event_data =
                crate::crypto::encrypt_value(provider, &crypto_key, &envelope.event_data)?;
        }
    } else if A::Event::has_any_encrypted_fields() {
        let crypto_key = ensure_crypto_key(store, aggregate_id).await?;
        let provider = require_crypto_provider(store)?;

        for (envelope, pe) in envelopes.iter_mut().zip(pending.iter()) {
            let fields = pe.event.encrypted_fields();
            if !fields.is_empty() {
                crate::crypto::encrypt_fields(
                    provider,
                    &crypto_key,
                    &mut envelope.event_data,
                    fields,
                )?;
            }
        }
    }

    // Create snapshot for deleted aggregate
    let config = store.snapshot_config();
    let strategy = config.strategy_for_type(aggregate_type.as_str());
    let current_version = aggregate.version();

    let snapshot = if strategy.should_snapshot_range(expected_version, current_version) {
        build_deleted_snapshot::<S, A>(
            store,
            aggregate_id,
            &aggregate_type,
            current_version,
            || serde_json::to_value(aggregate.state()),
        )
        .await
    } else {
        None
    };

    let stream_id = StreamId::new(aggregate_type, aggregate_id);

    aggregate.clear_pending_events();

    Ok(Some(PreparedCommit {
        stream_id,
        events: envelopes,
        expected_version,
        snapshot,
        claims: vec![],
        clear_claims: true,
    }))
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

/// Builds an active-aggregate snapshot, encrypting if needed.
async fn build_snapshot<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    aggregate_id: uuid::Uuid,
    aggregate_type: &AggregateType,
    current_version: AggregateVersion,
    serialize: impl FnOnce() -> std::result::Result<serde_json::Value, serde_json::Error>,
) -> Option<Snapshot> {
    match serialize() {
        Ok(mut snapshot_data) => {
            if A::is_encrypted() || A::Event::has_any_encrypted_fields() {
                encrypt_snapshot_data(store, aggregate_id, aggregate_type, &mut snapshot_data)
                    .await;
            }
            Some(Snapshot::new(
                aggregate_id,
                aggregate_type.clone(),
                current_version,
                snapshot_data,
            ))
        }
        Err(e) => {
            tracing::warn!(
                aggregate_type = %aggregate_type,
                aggregate_id = %aggregate_id,
                error = %e,
                "Failed to serialize entity for snapshot"
            );
            None
        }
    }
}

/// Builds a deleted-aggregate snapshot, encrypting if needed.
async fn build_deleted_snapshot<S: EventStore + ?Sized, A: Aggregate>(
    store: &S,
    aggregate_id: uuid::Uuid,
    aggregate_type: &AggregateType,
    current_version: AggregateVersion,
    serialize: impl FnOnce() -> std::result::Result<serde_json::Value, serde_json::Error>,
) -> Option<Snapshot> {
    match serialize() {
        Ok(mut snapshot_data) => {
            if A::is_encrypted() || A::Event::has_any_encrypted_fields() {
                encrypt_snapshot_data(store, aggregate_id, aggregate_type, &mut snapshot_data)
                    .await;
            }
            Some(Snapshot::new_deleted(
                aggregate_id,
                aggregate_type.clone(),
                current_version,
                snapshot_data,
            ))
        }
        Err(e) => {
            tracing::warn!(
                aggregate_type = %aggregate_type,
                aggregate_id = %aggregate_id,
                error = %e,
                "Failed to serialize deleted state for snapshot"
            );
            None
        }
    }
}

/// Encrypts snapshot data in-place if crypto is available.
async fn encrypt_snapshot_data<S: EventStore + ?Sized>(
    store: &S,
    aggregate_id: uuid::Uuid,
    aggregate_type: &AggregateType,
    snapshot_data: &mut serde_json::Value,
) {
    if let (Some(key_store), Some(provider)) = (store.crypto_key_store(), store.crypto_provider()) {
        if let Ok(Some(crypto_key)) = key_store.get_key(aggregate_id).await {
            match crate::crypto::encrypt_value(provider, &crypto_key, snapshot_data) {
                Ok(encrypted) => *snapshot_data = encrypted,
                Err(e) => {
                    tracing::warn!(
                        aggregate_type = %aggregate_type,
                        aggregate_id = %aggregate_id,
                        error = %e,
                        "Failed to encrypt snapshot data"
                    );
                }
            }
        }
    }
}

/// Gets or creates a crypto key for an aggregate.
///
/// If the key already exists, returns it. Otherwise, generates a new key
/// and upserts it.
async fn ensure_crypto_key<S: EventStore + ?Sized>(
    store: &S,
    aggregate_id: Uuid,
) -> Result<Vec<u8>> {
    let key_store = store.crypto_key_store().ok_or_else(|| {
        crate::Error::invalid_state("Encrypted aggregate requires crypto_key_store")
    })?;
    let provider = require_crypto_provider(store)?;

    if let Some(existing) = key_store.get_key(aggregate_id).await? {
        return Ok(existing);
    }

    let new_key = provider.generate_key();
    key_store.upsert_key(aggregate_id, new_key.clone()).await?;
    Ok(new_key)
}

/// Unwraps the crypto provider or returns an error.
fn require_crypto_provider<S: EventStore + ?Sized>(
    store: &S,
) -> Result<&dyn crate::CryptoProvider> {
    store
        .crypto_provider()
        .ok_or_else(|| crate::Error::invalid_state("Encrypted aggregate requires crypto_provider"))
}

/// Decrypts event envelope data using either full-value or field-level decryption.
fn decrypt_event_data<S: EventStore + ?Sized>(
    store: &S,
    crypto_key: Option<&[u8]>,
    event_data: &mut serde_json::Value,
) -> Result<()> {
    if let Some(key) = crypto_key {
        if crate::crypto::is_encrypted(event_data) {
            let provider = require_crypto_provider(store)?;
            *event_data = crate::crypto::decrypt_value(provider, key, event_data)?;
        } else if crate::crypto::has_encrypted_fields(event_data) {
            let provider = require_crypto_provider(store)?;
            crate::crypto::decrypt_encrypted_fields(provider, key, event_data)?;
        }
    }
    Ok(())
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
#[allow(clippy::too_many_lines)]
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

    // Resolve crypto key for encrypted aggregates (required before any decryption)
    let crypto_key = if A::is_encrypted() {
        let key_store = store.crypto_key_store().ok_or_else(|| {
            crate::Error::invalid_state("Encrypted aggregate requires crypto_key_store")
        })?;
        let key = key_store
            .get_key(uuid)
            .await?
            .ok_or_else(|| crate::Error::key_not_found(uuid))?;
        Some(key)
    } else if A::Event::has_any_encrypted_fields() {
        // Field-level encryption: key may not exist yet (no events committed)
        if let Some(key_store) = store.crypto_key_store() {
            key_store.get_key(uuid).await?
        } else {
            None
        }
    } else {
        None
    };

    // Try to load snapshot if enabled
    let config = store.snapshot_config();
    if config.use_snapshots_on_load() {
        if let Some(mut snapshot) = store.load_snapshot(stream_id.clone()).await? {
            decrypt_event_data(store, crypto_key.as_deref(), &mut snapshot.snapshot_data)?;

            // Deleted snapshot: deserialize as DeletedState and return immediately
            if snapshot.is_deleted {
                let deleted_state: A::DeletedState = serde_json::from_value(snapshot.snapshot_data)
                    .map_err(|e| {
                        crate::Error::custom(format!("Failed to deserialize deleted snapshot: {e}"))
                    })?;
                let entity_id = EntityId::from(snapshot.aggregate_id);
                return Ok(Loaded::Deleted(DeletedAggregateRoot::from_snapshot(
                    deleted_state,
                    entity_id,
                    snapshot.snapshot_version,
                )));
            }

            let entity: A = serde_json::from_value(snapshot.snapshot_data).map_err(|e| {
                crate::Error::custom(format!("Failed to deserialize snapshot entity: {e}"))
            })?;

            let mut aggregate = AggregateRoot::from_snapshot(snapshot.snapshot_version, entity);
            let from_version = snapshot.snapshot_version;

            let event_stream = store.load_stream(stream_id, from_version).await?;
            futures::pin_mut!(event_stream);

            while let Some(envelope) = event_stream.next().await {
                let mut envelope = envelope?;
                decrypt_event_data(store, crypto_key.as_deref(), &mut envelope.event_data)?;
                let event = A::Event::from_envelope(&envelope)?;

                if EventApplicator::is_delete(&event) {
                    let deleted = aggregate.apply_delete_unchecked(&event);
                    return Ok(Loaded::Deleted(deleted));
                }

                aggregate.apply_unchecked(&event);
            }

            return Ok(Loaded::Active(aggregate));
        }
    }

    // No snapshot: load all events
    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;
    futures::pin_mut!(event_stream);

    let Some(first_envelope) = event_stream.next().await else {
        // No events: backward compat — returns default-state aggregate.
        // For init-event aggregates, Entity::new panics (data integrity issue).
        return Ok(Loaded::Active(AggregateRoot::new_for_replay(id)));
    };
    let mut first_envelope = first_envelope?;

    decrypt_event_data(store, crypto_key.as_deref(), &mut first_envelope.event_data)?;

    let first_event = A::Event::from_envelope(&first_envelope)?;

    // Detect init vs legacy from first event
    let mut aggregate = if EventApplicator::is_init(&first_event) {
        // Init-event aggregate: type-state transition
        UninitAggregateRoot::<A>::new(id).apply_init_unchecked(&first_event)
    } else {
        // Legacy aggregate: Entity::new(id) + apply first event
        let mut agg = AggregateRoot::<A>::new_for_replay(id);
        agg.apply_unchecked(&first_event);
        agg
    };

    // Replay remaining events
    while let Some(envelope) = event_stream.next().await {
        let mut envelope = envelope?;
        decrypt_event_data(store, crypto_key.as_deref(), &mut envelope.event_data)?;
        let event = A::Event::from_envelope(&envelope)?;

        if EventApplicator::is_delete(&event) {
            let deleted = aggregate.apply_delete_unchecked(&event);
            return Ok(Loaded::Deleted(deleted));
        }

        aggregate.apply_unchecked(&event);
    }

    Ok(Loaded::Active(aggregate))
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
            }
        }

        fn with_fail_save_snapshot(mut self) -> Self {
            self.fail_save_snapshot = true;
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
            _expected_version: AggregateVersion,
            _claims: Vec<AggregateClaim>,
            _clear_claims: bool,
        ) -> Result<()> {
            *self.append_count.lock().unwrap() += 1;
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

    #[tokio::test]
    async fn test_load_with_no_events_returns_default() {
        let store = CommitTestStore::new(SnapshotConfig::disabled());
        let id = crate::EntityId::new();

        // Load entity that has no events at all
        let loaded: AggregateRoot<SimpleTestEntity> = load(&store, id).await.unwrap();

        assert_eq!(loaded.value, 0, "Default entity should have value 0");
        assert_eq!(loaded.version(), AggregateVersion::initial());
        assert!(loaded.pending_events().is_empty());
        assert_eq!(loaded.entity_id(), id);
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
}
