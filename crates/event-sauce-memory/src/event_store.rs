//! In-memory event store implementation.
//!
//! Provides a fast, thread-safe in-memory implementation of `EventStore`
//! suitable for testing and development.

use crate::claims::ClaimsData;
use crate::expectations::check_expectations;
use async_trait::async_trait;
use event_sauce_core::{
    AggregateVersion, Error, EventEnvelope, EventLogEntry, EventStore, Position, Result, Snapshot,
    SnapshotConfig, StreamCommit, StreamId,
};
use futures::stream::{self, Stream};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

type Streams = HashMap<StreamId, Vec<Arc<EventEnvelope>>>;
type GlobalEvents = Vec<(Position, Arc<EventEnvelope>)>;

/// Validates every commit's `expected_version` against the stream's
/// current length, then every expectation's version against the same
/// state, without mutating anything, so a whole batch is checked before
/// anything in it is applied.
///
/// Commits to the same stream within the batch chain: a later commit's
/// `expected_version` is checked against the version the previous commit
/// to that stream in this batch leaves it at, not against the stream's
/// currently committed version — mirroring the PostgreSQL backend's
/// `plan_version_chain_checks`. An empty-events commit still has its
/// version checked and still occupies its slot in the chain (it just
/// does not advance it), matching `append`'s existing behaviour for a
/// single empty commit.
///
/// Expectations follow the load-time rule of [`check_expectations`]: the
/// batch never mutates `streams`, so the committed version is the load-time
/// version.
///
/// # Errors
///
/// Returns `Error::ConcurrencyConflict` on the first commit or
/// expectation whose version doesn't match.
fn check(
    streams: &Streams,
    commits: &[StreamCommit],
    expectations: &[(StreamId, AggregateVersion)],
) -> Result<()> {
    let mut next_required: HashMap<&StreamId, AggregateVersion> = HashMap::new();

    for commit in commits {
        let current_version = next_required
            .get(&commit.stream_id)
            .copied()
            .unwrap_or_else(|| committed_version(streams, &commit.stream_id));

        if current_version != commit.expected_version {
            return Err(Error::concurrency_conflict(
                commit.expected_version,
                current_version,
            ));
        }

        #[allow(clippy::cast_possible_wrap)]
        let leaves_at =
            AggregateVersion::new(commit.expected_version.as_i64() + commit.events.len() as i64);
        next_required.insert(&commit.stream_id, leaves_at);
    }

    check_expectations(expectations, |stream_id| {
        committed_version(streams, stream_id)
    })
}

/// Applies every commit's claims to a copy of `claims`, in batch order, so a
/// later commit sees the claims an earlier one took or released and a
/// conflict leaves the committed index untouched.
///
/// # Errors
///
/// Returns a claim conflict on the first claim held by another aggregate.
fn stage_claims(claims: &ClaimsData, commits: &[StreamCommit]) -> Result<ClaimsData> {
    let mut staged = claims.clone();
    for commit in commits {
        staged.stage(
            commit.stream_id.aggregate_id(),
            &commit.claims,
            commit.clear_claims,
        )?;
    }
    Ok(staged)
}

/// Appends every commit's events, assigning global positions in batch
/// order. Must only run after [`check`] accepted the same batch — it does
/// not re-validate versions.
fn apply(streams: &mut Streams, global_events: &mut GlobalEvents, commits: Vec<StreamCommit>) {
    for commit in commits {
        let stream = streams.entry(commit.stream_id).or_default();
        for event in commit.events {
            let shared = Arc::new(event);
            stream.push(Arc::clone(&shared));
            #[allow(clippy::cast_possible_wrap)]
            let position = Position::new(global_events.len() as i64 + 1);
            global_events.push((position, shared));
        }
    }
}

#[allow(clippy::cast_possible_wrap)]
fn committed_version(streams: &Streams, stream_id: &StreamId) -> AggregateVersion {
    streams
        .get(stream_id)
        .map_or(AggregateVersion::initial(), |stream| {
            AggregateVersion::new(stream.len() as i64)
        })
}

/// In-memory event store for testing and development.
///
/// This implementation stores all events and snapshots in memory using
/// thread-safe data structures. It provides full `EventStore` functionality
/// including optimistic concurrency control and snapshots.
///
/// # Thread Safety
///
/// This store is thread-safe and can be cloned cheaply (uses `Arc` internally).
///
/// # Examples
///
/// ```
/// use event_sauce_memory::InMemoryEventStore;
/// use event_sauce_core::{EventStore, StreamId, AggregateVersion};
/// use uuid::Uuid;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let store = InMemoryEventStore::new();
///     let stream_id = StreamId::new("User", Uuid::new_v4());
///
///     // Store is ready to use
///     let version = store.get_version(stream_id).await?;
///     assert_eq!(version, AggregateVersion::initial());
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryEventStore {
    inner: Arc<InMemoryEventStoreInner>,
    checkpoint_store: Option<Arc<dyn event_sauce_core::CheckpointStore>>,
    crypto_key_store: Option<Arc<dyn event_sauce_core::CryptoKeyStore>>,
    crypto_provider: Option<Arc<dyn event_sauce_core::CryptoProvider>>,
}

struct InMemoryEventStoreInner {
    /// Stores events by stream ID. Events are stored as `Arc` so that the
    /// same envelope shared between per-stream and global views is allocated
    /// once on append.
    streams: RwLock<HashMap<StreamId, Vec<Arc<EventEnvelope>>>>,
    /// Stores snapshots by stream ID
    snapshots: RwLock<HashMap<StreamId, Snapshot>>,
    /// All events in global order for `stream_all`, each paired with its
    /// store-issued global [`Position`]. Positions are assigned inside the
    /// append write lock (so position order matches insertion order) and start
    /// at 1. Shares `Arc`s with `streams`, avoiding a second copy on append.
    global_events: RwLock<Vec<(Position, Arc<EventEnvelope>)>>,
    /// Uniqueness claims held by stream aggregates.
    claims: Mutex<ClaimsData>,
    /// Snapshot configuration
    snapshot_config: SnapshotConfig,
}

/// Builder for configuring `InMemoryEventStore`.
///
/// Provides a flexible way to configure the event store with:
/// - Snapshot configuration
/// - Checkpoint store for subscriptions
///
/// # Examples
///
/// ```
/// use event_sauce_memory::InMemoryEventStore;
/// use event_sauce_core::SnapshotConfig;
///
/// let store = InMemoryEventStore::builder()
///     .snapshot_config(SnapshotConfig::builder().build())
///     .build();
/// ```
#[derive(Clone, Default)]
pub struct InMemoryEventStoreBuilder {
    snapshot_config: Option<SnapshotConfig>,
    checkpoint_store: Option<Arc<dyn event_sauce_core::CheckpointStore>>,
    crypto_key_store: Option<Arc<dyn event_sauce_core::CryptoKeyStore>>,
    crypto_provider: Option<Arc<dyn event_sauce_core::CryptoProvider>>,
}

impl InMemoryEventStore {
    /// Creates a new empty in-memory event store with default snapshot configuration.
    ///
    /// Default configuration: Snapshots every 100 events, enabled on load.
    ///
    /// Equivalent to `InMemoryEventStore::builder().build()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    ///
    /// // Default: snapshots every 100 events
    /// let store = InMemoryEventStore::new();
    ///
    /// // Or use builder for custom configuration
    /// use event_sauce_core::SnapshotConfig;
    /// let store = InMemoryEventStore::builder()
    ///     .snapshot_config(SnapshotConfig::disabled())
    ///     .build();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Creates a new empty in-memory event store with the given snapshot configuration.
    ///
    /// Equivalent to `InMemoryEventStore::builder().snapshot_config(config).build()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents};
    ///
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents::try_new(100).unwrap())
    ///     .build();
    ///
    /// let store = InMemoryEventStore::with_config(config);
    /// ```
    #[must_use]
    pub fn with_config(snapshot_config: SnapshotConfig) -> Self {
        Self::builder().snapshot_config(snapshot_config).build()
    }

    /// Creates a new empty in-memory event store with checkpoint store.
    ///
    /// This is a convenience method that configures both snapshot and checkpoint storage.
    ///
    /// Equivalent to `InMemoryEventStore::builder().snapshot_config(config).checkpoint_store(store).build()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::{InMemoryEventStore, InMemoryCheckpointStore};
    /// use event_sauce_core::{EventStore, SnapshotConfig};
    /// use std::sync::Arc;
    ///
    /// let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
    /// let store = Arc::new(InMemoryEventStore::with_checkpoint_store(
    ///     SnapshotConfig::builder().build(),
    ///     checkpoint_store,
    /// ));
    ///
    /// // The checkpoint store is now wired in, so `policy_runner()` and
    /// // projection runners can track their position automatically.
    /// ```
    #[must_use]
    pub fn with_checkpoint_store(
        snapshot_config: SnapshotConfig,
        checkpoint_store: Arc<dyn event_sauce_core::CheckpointStore>,
    ) -> Self {
        Self::builder()
            .snapshot_config(snapshot_config)
            .checkpoint_store(checkpoint_store)
            .build()
    }

    /// Returns a snapshot of all events in global insertion order.
    ///
    /// This is useful for testing and for the [`InMemoryEventLogQuery`](super::InMemoryEventLogQuery).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    ///
    /// let store = InMemoryEventStore::new();
    /// assert!(store.all_events().is_empty());
    /// ```
    #[must_use]
    pub fn all_events(&self) -> Vec<EventEnvelope> {
        self.inner
            .global_events
            .read()
            .iter()
            .map(|(_, e)| (**e).clone())
            .collect()
    }

    /// Creates a builder for configuring the event store.
    ///
    /// This is the recommended way to create an `InMemoryEventStore` when you need
    /// to customize the configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    /// use event_sauce_core::SnapshotConfig;
    ///
    /// let store = InMemoryEventStore::builder()
    ///     .snapshot_config(SnapshotConfig::disabled())
    ///     .build();
    /// ```
    #[must_use]
    pub fn builder() -> InMemoryEventStoreBuilder {
        InMemoryEventStoreBuilder::new()
    }

    /// Creates a fully-wired in-memory store for tests.
    ///
    /// Wraps the store in `Arc` and pre-installs an
    /// [`InMemoryCheckpointStore`](super::InMemoryCheckpointStore) so
    /// projections and policies work without additional setup. A crypto key
    /// store is installed automatically, plus the AES-256-GCM provider when
    /// the `crypto` feature is enabled.
    ///
    /// Intended for tests and quick demos — production code should use
    /// [`builder()`](Self::builder) and pass dependencies explicitly.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    ///
    /// // One line to get a store ready for subscriptions, policies, projections.
    /// let store = InMemoryEventStore::for_testing();
    /// assert!(store.all_events().is_empty());
    /// ```
    #[must_use]
    pub fn for_testing() -> Arc<Self> {
        Arc::new(
            Self::builder()
                .checkpoint_store(Arc::new(super::InMemoryCheckpointStore::new()))
                .build(),
        )
    }

    /// Checks and applies `commits` together with `expectations` — read-only
    /// stream version assertions for streams this call does not itself write
    /// — under one pair of write locks, atomically.
    ///
    /// [`EventStore::append_batch`] calls this with no expectations. The
    /// `unit-of-work` feature's cross-store atomic commit calls it with the
    /// stream expectations collected by a unit of work, from inside the
    /// state store's own commit critical section, so a stream write or
    /// expectation failure and a state write or expectation failure roll
    /// back together.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if any commit's or any
    /// expectation's version does not match what is currently stored, or a
    /// claim conflict if a commit claims a value another aggregate holds.
    pub(crate) fn commit_with_expectations(
        &self,
        commits: Vec<StreamCommit>,
        expectations: &[(StreamId, AggregateVersion)],
    ) -> Result<()> {
        let mut streams = self.inner.streams.write();
        let mut global_events = self.inner.global_events.write();

        let mut claims = self.inner.claims.lock();

        check(&streams, &commits, expectations)?;
        let staged_claims = stage_claims(&claims, &commits)?;
        apply(&mut streams, &mut global_events, commits);
        *claims = staged_claims;

        Ok(())
    }
}

impl InMemoryEventStoreBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot_config: None,
            checkpoint_store: None,
            crypto_key_store: None,
            crypto_provider: None,
        }
    }

    /// Sets the snapshot configuration.
    ///
    /// Defaults to `SnapshotConfig::builder().build()` (every 100 events).
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    /// use event_sauce_core::SnapshotConfig;
    ///
    /// let builder = InMemoryEventStore::builder()
    ///     .snapshot_config(SnapshotConfig::disabled());
    /// ```
    #[must_use]
    pub fn snapshot_config(mut self, config: SnapshotConfig) -> Self {
        self.snapshot_config = Some(config);
        self
    }

    /// Sets the checkpoint store for subscription tracking.
    ///
    /// When a checkpoint store is configured, the event store can create
    /// subscriptions with automatic checkpoint management.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::{InMemoryEventStore, InMemoryCheckpointStore};
    /// use std::sync::Arc;
    ///
    /// let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
    /// let builder = InMemoryEventStore::builder()
    ///     .checkpoint_store(checkpoint_store);
    /// ```
    #[must_use]
    pub fn checkpoint_store(mut self, store: Arc<dyn event_sauce_core::CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Sets the crypto key store for encrypted aggregate encryption.
    ///
    /// Required when using encrypted aggregates. Stores per-aggregate encryption keys.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::{InMemoryEventStore, InMemoryCryptoKeyStore};
    /// use std::sync::Arc;
    ///
    /// let key_store = Arc::new(InMemoryCryptoKeyStore::new());
    /// let builder = InMemoryEventStore::builder()
    ///     .crypto_key_store(key_store);
    /// ```
    #[must_use]
    pub fn crypto_key_store(mut self, store: Arc<dyn event_sauce_core::CryptoKeyStore>) -> Self {
        self.crypto_key_store = Some(store);
        self
    }

    /// Sets the crypto provider for encrypted aggregate encryption.
    ///
    /// Required when using encrypted aggregates. Provides encrypt/decrypt operations.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_memory::InMemoryEventStore;
    /// use event_sauce_crypto::Aes256GcmProvider;
    /// use std::sync::Arc;
    ///
    /// let provider = Arc::new(Aes256GcmProvider);
    /// let builder = InMemoryEventStore::builder()
    ///     .crypto_provider(provider);
    /// ```
    #[must_use]
    pub fn crypto_provider(mut self, provider: Arc<dyn event_sauce_core::CryptoProvider>) -> Self {
        self.crypto_provider = Some(provider);
        self
    }

    /// Builds the `InMemoryEventStore` with the configured settings.
    ///
    /// # Defaults
    ///
    /// - **Snapshot config**: Every 100 events
    /// - **Checkpoint store**: None
    /// - **Crypto key store**: [`InMemoryCryptoKeyStore`](super::InMemoryCryptoKeyStore)
    /// - **Crypto provider**: `Aes256GcmProvider` when the `crypto` feature is
    ///   enabled; without it (the default), none is installed and encrypted
    ///   aggregates require an explicit [`crypto_provider()`](Self::crypto_provider)
    ///
    /// # Crypto auto-install
    ///
    /// A key store is always installed. With the opt-in `crypto` feature, a
    /// provider is installed too, even if your aggregates are not encrypted —
    /// it remains dormant until an encrypted aggregate (one whose
    /// `Aggregate::is_encrypted()` returns true, or one with `@encrypted_fields`)
    /// is committed or loaded. Override either via
    /// [`crypto_key_store()`](Self::crypto_key_store) /
    /// [`crypto_provider()`](Self::crypto_provider) if you need a custom backend.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryEventStore;
    ///
    /// let store = InMemoryEventStore::builder().build();
    /// ```
    #[must_use]
    pub fn build(self) -> InMemoryEventStore {
        InMemoryEventStore {
            inner: Arc::new(InMemoryEventStoreInner {
                streams: RwLock::new(HashMap::new()),
                snapshots: RwLock::new(HashMap::new()),
                global_events: RwLock::new(Vec::new()),
                claims: Mutex::new(ClaimsData::default()),
                snapshot_config: self
                    .snapshot_config
                    .unwrap_or_else(|| SnapshotConfig::builder().build()),
            }),
            checkpoint_store: self.checkpoint_store,
            crypto_key_store: Some(
                self.crypto_key_store
                    .unwrap_or_else(|| Arc::new(super::InMemoryCryptoKeyStore::new())),
            ),
            #[cfg(feature = "crypto")]
            crypto_provider: Some(
                self.crypto_provider
                    .unwrap_or_else(|| Arc::new(event_sauce_crypto::Aes256GcmProvider)),
            ),
            #[cfg(not(feature = "crypto"))]
            crypto_provider: self.crypto_provider,
        }
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        claims: Vec<event_sauce_core::AggregateClaim>,
        clear_claims: bool,
    ) -> Result<()> {
        self.append_batch(vec![StreamCommit {
            stream_id,
            events,
            expected_version,
            claims,
            clear_claims,
        }])
        .await
    }

    /// Checks and applies every commit under one pair of write locks, so the
    /// batch is atomic: either every commit's version checks out and all of
    /// them are applied, or the first conflict is returned and the store is
    /// left exactly as it was — no earlier commit in the batch survives a
    /// later one's rejection.
    ///
    /// Claims are enforced in the same critical section: a claim held by
    /// another aggregate rejects the whole batch with a claim conflict, and
    /// no claim from an earlier commit in it survives.
    async fn append_batch(&self, commits: Vec<StreamCommit>) -> Result<()> {
        self.commit_with_expectations(commits, &[])
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        // Clone Arcs under the lock (cheap), then drop the lock before
        // unwrapping each envelope — keeps the lock window small.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let arcs: Vec<Arc<EventEnvelope>> = {
            let streams = self.inner.streams.read();
            streams
                .get(&stream_id)
                .map(|stream| {
                    stream
                        .iter()
                        .skip(from_version.as_i64() as usize)
                        .map(Arc::clone)
                        .collect()
                })
                .unwrap_or_default()
        };

        Ok(stream::iter(arcs.into_iter().map(|arc| Ok((*arc).clone()))))
    }

    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventLogEntry>> + Send> {
        let entries: Vec<(Position, Arc<EventEnvelope>)> = {
            let global_events = self.inner.global_events.read();
            global_events
                .iter()
                .filter(|(position, _)| *position > from_position)
                .map(|(position, arc)| (*position, Arc::clone(arc)))
                .collect()
        };

        Ok(stream::iter(entries.into_iter().map(|(position, arc)| {
            Ok(EventLogEntry {
                position,
                envelope: (*arc).clone(),
            })
        })))
    }

    async fn max_position(&self) -> Result<Position> {
        let global_events = self.inner.global_events.read();
        Ok(global_events
            .last()
            .map_or_else(Position::start, |(position, _)| *position))
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        let streams = self.inner.streams.read();

        #[allow(clippy::cast_possible_wrap)]
        let version = streams
            .get(&stream_id)
            .map_or(AggregateVersion::initial(), |stream| {
                AggregateVersion::new(stream.len() as i64)
            });

        Ok(version)
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let mut snapshots = self.inner.snapshots.write();
        let stream_id = StreamId::new(snapshot.aggregate_type.clone(), snapshot.aggregate_id);
        snapshots.insert(stream_id, snapshot);
        Ok(())
    }

    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        let snapshots = self.inner.snapshots.read();
        Ok(snapshots.get(&stream_id).cloned())
    }

    fn snapshot_config(&self) -> &SnapshotConfig {
        &self.inner.snapshot_config
    }

    fn checkpoint_store(&self) -> Option<std::sync::Arc<dyn event_sauce_core::CheckpointStore>> {
        self.checkpoint_store.clone()
    }

    fn crypto_key_store(&self) -> Option<&dyn event_sauce_core::CryptoKeyStore> {
        self.crypto_key_store.as_deref()
    }

    fn crypto_provider(&self) -> Option<&dyn event_sauce_core::CryptoProvider> {
        self.crypto_provider.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use event_sauce_core::{
        AggregateVersion, Error, EventEnvelope, EventStore, Position, Snapshot, StreamCommit,
        StreamId,
    };
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    fn commit(
        stream_id: &StreamId,
        aggregate_id: Uuid,
        event_type: &str,
        expected_version: AggregateVersion,
    ) -> StreamCommit {
        StreamCommit {
            stream_id: stream_id.clone(),
            events: vec![create_test_envelope(event_type, aggregate_id)],
            expected_version,
            claims: vec![],
            clear_claims: false,
        }
    }

    fn create_test_envelope(event_type: &str, aggregate_id: Uuid) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            "TestAggregate".to_string(),
            event_type.to_string(),
            event_sauce_core::EventVersion::new(1),
            json!({"data": "test"}),
        )
    }

    use super::{InMemoryEventStore, InMemoryEventStoreBuilder};

    // GREEN: Test that we can create an InMemoryEventStore
    #[tokio::test]
    async fn test_create_store() {
        let _store = InMemoryEventStore::new();
        // Store is created successfully
    }

    // GREEN: Test appending events to a new stream
    #[tokio::test]
    async fn test_append_to_new_stream() {
        let store = InMemoryEventStore::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let event = create_test_envelope("UserCreated", stream_id.aggregate_id());

        let result = store
            .append(
                stream_id,
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await;
        assert!(result.is_ok());
    }

    // GREEN: Test loading events from a stream
    #[tokio::test]
    async fn test_load_stream() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let mut stream = store
            .load_stream(stream_id, AggregateVersion::initial())
            .await
            .unwrap();
        let loaded = stream.next().await.unwrap().unwrap();

        assert_eq!(loaded.event_type, "UserCreated");
    }

    // GREEN: Test getting stream version
    #[tokio::test]
    async fn test_get_version() {
        let store = InMemoryEventStore::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        // New stream should have initial version
        let version = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(version, AggregateVersion::initial());
    }

    // GREEN: Test concurrency conflict detection
    #[tokio::test]
    async fn test_concurrency_conflict() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event1 = create_test_envelope("UserCreated", aggregate_id);
        let event2 = create_test_envelope("UserUpdated", aggregate_id);

        // Append first event
        store
            .append(
                stream_id.clone(),
                vec![event1],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // Try to append with wrong version - should fail
        let result = store
            .append(
                stream_id,
                vec![event2],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_concurrency_conflict());
    }

    // GREEN: Test streaming all events
    #[tokio::test]
    async fn test_stream_all() {
        let store = InMemoryEventStore::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let stream1 = StreamId::new("User", id1);
        let stream2 = StreamId::new("Order", id2);

        store
            .append(
                stream1,
                vec![create_test_envelope("UserCreated", id1)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
        store
            .append(
                stream2,
                vec![create_test_envelope("OrderPlaced", id2)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let all_events = store.stream_all(Position::start()).await.unwrap();
        let events: Vec<_> = all_events.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 2);
    }

    // GREEN: Test snapshot save and load
    #[tokio::test]
    async fn test_save_and_load_snapshot() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let snapshot = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(10),
            json!({"name": "Alice"}),
        );

        store.save_snapshot(snapshot.clone()).await.unwrap();
        let loaded = store.load_snapshot(stream_id).await.unwrap();

        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().snapshot_version, AggregateVersion::new(10));
    }

    // Additional tests for better coverage

    #[tokio::test]
    async fn test_append_multiple_events() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let events = vec![
            create_test_envelope("UserCreated", aggregate_id),
            create_test_envelope("UserUpdated", aggregate_id),
            create_test_envelope("UserVerified", aggregate_id),
        ];

        store
            .append(
                stream_id.clone(),
                events,
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(3));
    }

    #[tokio::test]
    async fn test_load_stream_from_version() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Add 5 events
        for i in 0..5 {
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);
            store
                .append(
                    stream_id.clone(),
                    vec![event],
                    AggregateVersion::new(i),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        // Load from version 2
        let stream = store
            .load_stream(stream_id, AggregateVersion::new(2))
            .await
            .unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 3); // Should get events 2, 3, 4
    }

    #[tokio::test]
    async fn test_stream_all_from_position() {
        let store = InMemoryEventStore::new();

        for i in 0..5 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("User", aggregate_id);
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);

            store
                .append(
                    stream_id,
                    vec![event],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        // Stream events whose position is strictly greater than 2.
        // With five dense positions (1..=5), that leaves 3, 4, 5.
        let stream = store.stream_all(Position::new(2)).await.unwrap();
        let entries: Vec<_> = stream.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq!(entries.len(), 3);
        let positions: Vec<i64> = entries.iter().map(|e| e.position.as_i64()).collect();
        assert_eq!(positions, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn test_max_position_tracks_last_appended_event() {
        let store = InMemoryEventStore::new();

        // Empty store reports the start sentinel.
        assert_eq!(store.max_position().await.unwrap(), Position::start());

        for i in 0..3 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("User", aggregate_id);
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);
            store
                .append(
                    stream_id,
                    vec![event],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        // Positions are 1-based and dense, so the max is the count.
        assert_eq!(store.max_position().await.unwrap(), Position::new(3));
    }

    #[tokio::test]
    async fn test_load_nonexistent_stream() {
        let store = InMemoryEventStore::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let stream = store
            .load_stream(stream_id, AggregateVersion::initial())
            .await
            .unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_load_nonexistent_snapshot() {
        let store = InMemoryEventStore::new();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let snapshot = store.load_snapshot(stream_id).await.unwrap();
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_store_is_cloneable() {
        let store = InMemoryEventStore::new();
        let store_clone = store.clone();

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        // Append via original store
        store
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // Read via clone - should see the same data (Arc semantics)
        let version = store_clone.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));
    }

    #[tokio::test]
    async fn test_default_trait() {
        let store = InMemoryEventStore::default();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::initial());
    }

    #[tokio::test]
    async fn test_version_increments_correctly() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Initial version
        let v0 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v0, AggregateVersion::initial());

        // After first append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event1", aggregate_id)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
        let v1 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v1, AggregateVersion::new(1));

        // After second append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event2", aggregate_id)],
                AggregateVersion::new(1),
                vec![],
                false,
            )
            .await
            .unwrap();
        let v2 = store.get_version(stream_id).await.unwrap();
        assert_eq!(v2, AggregateVersion::new(2));
    }

    #[tokio::test]
    async fn test_snapshot_overwrites() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Save first snapshot
        let snapshot1 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(5),
            json!({"version": 1}),
        );
        store.save_snapshot(snapshot1).await.unwrap();

        // Save second snapshot (should overwrite)
        let snapshot2 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(10),
            json!({"version": 2}),
        );
        store.save_snapshot(snapshot2).await.unwrap();

        // Should get the latest snapshot
        let loaded = store.load_snapshot(stream_id).await.unwrap().unwrap();
        assert_eq!(loaded.snapshot_version, AggregateVersion::new(10));
    }

    #[tokio::test]
    async fn test_append_without_event_bus_still_works() {
        let store = InMemoryEventStore::new(); // No event bus configured

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        // Should work fine without event bus
        let result = store
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await;
        assert!(result.is_ok());

        // Events should still be stored
        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));
    }

    // === Checkpoint Store Integration Tests ===

    use super::super::checkpoint_store::InMemoryCheckpointStore;
    use event_sauce_core::SnapshotConfig;

    #[tokio::test]
    async fn test_checkpoint_store_returns_none_by_default() {
        let store = InMemoryEventStore::new();
        assert!(store.checkpoint_store().is_none());
    }

    #[tokio::test]
    async fn test_checkpoint_store_returns_configured_store() {
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let store = InMemoryEventStore::with_checkpoint_store(
            SnapshotConfig::builder().build(),
            checkpoint_store.clone(),
        );

        let retrieved = store.checkpoint_store();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_checkpoint_store_can_save_and_load() {
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let store = InMemoryEventStore::with_checkpoint_store(
            SnapshotConfig::builder().build(),
            checkpoint_store.clone(),
        );

        // Save checkpoint through the store's checkpoint store
        if let Some(cs) = store.checkpoint_store() {
            cs.save_checkpoint("test-sub", Position::new(42))
                .await
                .unwrap();

            let loaded = cs.load_checkpoint("test-sub").await.unwrap();
            assert_eq!(loaded, Some(Position::new(42)));
        } else {
            panic!("Checkpoint store should be configured");
        }
    }

    // === Builder Pattern Tests ===

    #[tokio::test]
    async fn test_builder_with_defaults() {
        let store = InMemoryEventStore::builder().build();

        // Should work with defaults
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));
    }

    #[tokio::test]
    async fn test_builder_with_snapshot_config() {
        let config = SnapshotConfig::disabled();
        let store = InMemoryEventStore::builder()
            .snapshot_config(config.clone())
            .build();

        // Verify snapshot config is set - just check it's accessible
        let _config = store.snapshot_config();
        // Config is accessible - test passes if no panic occurs
    }

    #[tokio::test]
    async fn test_builder_with_checkpoint_store() {
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let store = InMemoryEventStore::builder()
            .checkpoint_store(checkpoint_store.clone())
            .build();

        // Verify checkpoint store is set
        let retrieved = store.checkpoint_store();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_builder_full_configuration() {
        let config = SnapshotConfig::builder()
            .default_strategy(event_sauce_core::EveryNEvents::try_new(50).unwrap())
            .build();
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());

        let store = InMemoryEventStore::builder()
            .snapshot_config(config)
            .checkpoint_store(checkpoint_store.clone())
            .build();

        // Verify both are configured
        assert!(store.checkpoint_store().is_some());
        let _config = store.snapshot_config();
        // Config is accessible - test passes if no panic occurs
    }

    #[tokio::test]
    async fn test_builder_new_equals_default() {
        let builder1 = InMemoryEventStoreBuilder::new();
        let builder2 = InMemoryEventStoreBuilder::default();

        let store1 = builder1.build();
        let store2 = builder2.build();

        // Both should work identically
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store1
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
        store2
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let v1 = store1.get_version(stream_id.clone()).await.unwrap();
        let v2 = store2.get_version(stream_id).await.unwrap();
        assert_eq!(v1, v2);
    }

    #[tokio::test]
    async fn test_new_uses_builder() {
        // Verify that new() produces the same result as builder().build()
        let store1 = InMemoryEventStore::new();
        let store2 = InMemoryEventStore::builder().build();

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store1
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
        store2
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let v1 = store1.get_version(stream_id.clone()).await.unwrap();
        let v2 = store2.get_version(stream_id).await.unwrap();
        assert_eq!(v1, v2);
    }

    #[tokio::test]
    async fn test_with_config_uses_builder() {
        let config = SnapshotConfig::disabled();
        let store1 = InMemoryEventStore::with_config(config.clone());
        let store2 = InMemoryEventStore::builder()
            .snapshot_config(config)
            .build();

        // Both should work identically
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store1
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
        store2
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let v1 = store1.get_version(stream_id.clone()).await.unwrap();
        let v2 = store2.get_version(stream_id).await.unwrap();
        assert_eq!(v1, v2);
    }

    // === Crypto Builder Tests ===

    #[tokio::test]
    async fn test_crypto_key_store_available_by_default() {
        let store = InMemoryEventStore::new();
        assert!(
            store.crypto_key_store().is_some(),
            "crypto key store should be provided by default"
        );
    }

    #[cfg(feature = "crypto")]
    #[tokio::test]
    async fn test_crypto_provider_available_by_default() {
        let store = InMemoryEventStore::new();
        assert!(
            store.crypto_provider().is_some(),
            "crypto provider should be provided by default"
        );
    }

    #[tokio::test]
    async fn test_builder_with_custom_crypto_key_store() {
        use super::super::crypto_key_store::InMemoryCryptoKeyStore;
        let key_store = Arc::new(InMemoryCryptoKeyStore::new());
        let store = InMemoryEventStore::builder()
            .crypto_key_store(key_store)
            .build();

        assert!(store.crypto_key_store().is_some());
    }

    #[tokio::test]
    async fn test_builder_with_custom_crypto_provider() {
        /// Minimal mock provider for builder testing.
        struct MockProvider;
        impl event_sauce_core::CryptoProvider for MockProvider {
            fn encrypt(
                &self,
                _key: &[u8],
                _plaintext: &[u8],
                _aad: &[u8],
            ) -> event_sauce_core::Result<Vec<u8>> {
                Ok(vec![])
            }
            fn decrypt(
                &self,
                _key: &[u8],
                _ciphertext: &[u8],
                _aad: &[u8],
            ) -> event_sauce_core::Result<Vec<u8>> {
                Ok(vec![])
            }
            fn generate_key(&self) -> Vec<u8> {
                vec![0; 32]
            }
        }

        let provider = Arc::new(MockProvider);
        let store = InMemoryEventStore::builder()
            .crypto_provider(provider)
            .build();

        assert!(store.crypto_provider().is_some());
    }

    #[tokio::test]
    async fn test_with_checkpoint_store_uses_builder() {
        let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
        let config = SnapshotConfig::builder().build();

        let store1 =
            InMemoryEventStore::with_checkpoint_store(config.clone(), checkpoint_store.clone());
        let store2 = InMemoryEventStore::builder()
            .snapshot_config(config)
            .checkpoint_store(checkpoint_store)
            .build();

        // Both should have checkpoint store configured
        assert!(store1.checkpoint_store().is_some());
        assert!(store2.checkpoint_store().is_some());
    }

    /// `append_batch` must be atomic: when a later commit in the batch
    /// conflicts, no earlier commit may be left persisted. Mirrors the
    /// PostgreSQL backend's `test_append_batch_is_atomic`.
    #[tokio::test]
    async fn test_append_batch_is_atomic_across_streams() {
        let store = InMemoryEventStore::new();
        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();
        let stream_a = StreamId::new("Acct", id_a);
        let stream_b = StreamId::new("Acct", id_b);

        let commits = vec![
            commit(
                &stream_a.clone(),
                id_a,
                "Credited",
                AggregateVersion::initial(),
            ),
            commit(
                &stream_b.clone(),
                id_b,
                "Credited",
                AggregateVersion::new(5),
            ),
        ];

        let error = store
            .append_batch(commits)
            .await
            .expect_err("append_batch must fail when the second commit conflicts");
        assert!(
            error.is_concurrency_conflict(),
            "expected a concurrency conflict, got {error:?}"
        );

        assert_eq!(
            store.get_version(stream_a.clone()).await.unwrap(),
            AggregateVersion::initial(),
            "first stream must roll back with the failed batch"
        );
        assert_eq!(
            store.get_version(stream_b).await.unwrap(),
            AggregateVersion::initial(),
            "second stream must not be partially written"
        );

        let mut first_stream = store
            .load_stream(stream_a, AggregateVersion::initial())
            .await
            .unwrap();
        assert!(
            first_stream.next().await.is_none(),
            "no events may survive an atomic batch that failed"
        );

        assert_eq!(
            store.max_position().await.unwrap(),
            Position::start(),
            "global position must not advance for a rejected batch"
        );
    }

    /// A batch with two consecutive commits to the SAME stream (v0 then v1)
    /// must succeed, the second commit targeting the version the first
    /// commit leaves the stream at rather than its currently committed
    /// version. Mirrors the PostgreSQL backend's
    /// `test_append_batch_allows_consecutive_commits_to_same_stream`.
    #[tokio::test]
    async fn test_append_batch_allows_consecutive_commits_to_same_stream() {
        let store = InMemoryEventStore::new();
        let id = Uuid::new_v4();
        let stream = StreamId::new("Acct", id);

        let commits = vec![
            commit(&stream.clone(), id, "Opened", AggregateVersion::initial()),
            commit(&stream.clone(), id, "Credited", AggregateVersion::new(1)),
        ];

        store
            .append_batch(commits)
            .await
            .expect("consecutive commits to the same stream must succeed");

        assert_eq!(
            store.get_version(stream.clone()).await.unwrap(),
            AggregateVersion::new(2),
            "both commits must be stored, advancing the stream by two versions"
        );

        let events = store
            .load_stream(stream, AggregateVersion::initial())
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(events.len(), 2, "both events must be persisted");
    }

    /// A later commit to the same stream that skips a version, rather than
    /// extending the previous commit in the batch by exactly its event
    /// count, must be rejected and leave no trace — not even the first
    /// commit's events. Mirrors the PostgreSQL backend's
    /// `test_append_batch_rejects_a_skipped_version_in_the_same_stream`.
    #[tokio::test]
    async fn test_append_batch_rejects_a_skipped_version_in_the_same_stream() {
        let store = InMemoryEventStore::new();
        let id = Uuid::new_v4();
        let stream = StreamId::new("Acct", id);

        let commits = vec![
            commit(&stream.clone(), id, "Opened", AggregateVersion::initial()),
            commit(&stream.clone(), id, "Credited", AggregateVersion::new(5)),
        ];

        let error = store
            .append_batch(commits)
            .await
            .expect_err("a batch whose second commit skips a version must be rejected");
        assert!(
            matches!(error, Error::ConcurrencyConflict { .. }),
            "expected a concurrency conflict, got {error:?}"
        );

        assert_eq!(
            store.get_version(stream.clone()).await.unwrap(),
            AggregateVersion::initial(),
            "a rejected batch must leave no events behind, not even the first commit's"
        );
        assert_eq!(
            store.max_position().await.unwrap(),
            Position::start(),
            "global position must not advance for a rejected batch"
        );
    }

    /// An expectation for a stream this call does not write is satisfied
    /// when the stream is already at that version, and has no effect on what
    /// gets applied.
    #[tokio::test]
    async fn test_commit_with_expectations_accepts_a_matching_read_only_expectation() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("Acct", aggregate_id);
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Opened", aggregate_id)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        store
            .commit_with_expectations(vec![], &[(stream_id.clone(), AggregateVersion::new(1))])
            .expect("a matching expectation must be accepted");

        assert_eq!(
            store.get_version(stream_id).await.unwrap(),
            AggregateVersion::new(1),
            "an expectation-only call must not write anything"
        );
    }

    /// A stale expectation for a stream this call does not write is a
    /// conflict, even though nothing in the call touches that stream.
    #[tokio::test]
    async fn test_commit_with_expectations_rejects_a_stale_read_only_expectation() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("Acct", aggregate_id);
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Opened", aggregate_id)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let error = store
            .commit_with_expectations(vec![], &[(stream_id, AggregateVersion::initial())])
            .expect_err("a stale expectation must be rejected");
        assert!(error.is_concurrency_conflict());
    }

    /// An expectation for a stream this call also writes, agreeing with the
    /// write's own `expected_version`, is redundant but harmless.
    #[tokio::test]
    async fn test_commit_with_expectations_accepts_an_expectation_agreeing_with_its_own_write() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("Acct", aggregate_id);

        store
            .commit_with_expectations(
                vec![commit(
                    &stream_id.clone(),
                    aggregate_id,
                    "Opened",
                    AggregateVersion::initial(),
                )],
                &[(stream_id.clone(), AggregateVersion::initial())],
            )
            .expect("an expectation agreeing with the write must be accepted");

        assert_eq!(
            store.get_version(stream_id).await.unwrap(),
            AggregateVersion::new(1)
        );
    }

    /// An expectation for a stream this call also writes, disagreeing with
    /// the write's own `expected_version`, is a conflict — the two were
    /// prepared from different reads of the same stream.
    #[tokio::test]
    async fn test_commit_with_expectations_rejects_disagreement_with_its_own_write() {
        let store = InMemoryEventStore::new();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("Acct", aggregate_id);

        let error = store
            .commit_with_expectations(
                vec![commit(
                    &stream_id.clone(),
                    aggregate_id,
                    "Opened",
                    AggregateVersion::initial(),
                )],
                &[(stream_id.clone(), AggregateVersion::new(1))],
            )
            .expect_err("a write and an expectation that disagree must conflict");
        assert!(error.is_concurrency_conflict());

        assert_eq!(
            store.get_version(stream_id).await.unwrap(),
            AggregateVersion::initial(),
            "the write must not apply when its own expectation disagrees"
        );
    }

    /// Model-based property test: random batches of appends (correctly or
    /// incorrectly versioned) against a handful of streams must accept and
    /// reject in lockstep with a trivial reference model, must leave
    /// `load_stream`/`stream_all` reporting exactly the model's contents from
    /// every offset (not just the start), and must assign dense, strictly
    /// increasing global positions. Every op runs through
    /// [`EventStore::append_batch`], which checks every commit in the batch
    /// before applying any of them — a batch of one sub-commit exercises
    /// `append` itself, and a longer batch exercises that atomic-rollback
    /// semantics: a batch condemned by any of its sub-commits leaves the
    /// model, and the store, exactly as they were before it ran.
    mod proptests {
        use super::*;
        use proptest::prelude::*;
        use proptest::test_runner::TestCaseError;
        use std::sync::LazyLock;

        static RUNTIME: LazyLock<tokio::runtime::Runtime> =
            LazyLock::new(|| tokio::runtime::Runtime::new().unwrap());

        const NUM_STREAMS: usize = 3;

        #[derive(Debug, Clone, Default)]
        struct Model {
            streams: Vec<Vec<Uuid>>,
            global: Vec<Uuid>,
        }

        impl Model {
            fn new() -> Self {
                Self {
                    streams: vec![Vec::new(); NUM_STREAMS],
                    global: Vec::new(),
                }
            }

            fn version(&self, stream: usize) -> i64 {
                i64::try_from(self.streams[stream].len()).unwrap()
            }

            fn record(&mut self, stream: usize, ids: &[Uuid]) {
                self.streams[stream].extend_from_slice(ids);
                self.global.extend_from_slice(ids);
            }
        }

        fn sub_commit_strategy() -> impl Strategy<Value = (usize, i64, u8)> {
            (
                0..NUM_STREAMS,
                prop::sample::select(&[-2i64, -1, 0, 1, 2][..]),
                1u8..=3,
            )
        }

        fn make_envelope(aggregate_id: Uuid) -> (Uuid, EventEnvelope) {
            let id = Uuid::new_v4();
            let envelope = EventEnvelope::new(
                id,
                aggregate_id,
                "Model".to_string(),
                "Model.Event".to_string(),
                event_sauce_core::EventVersion::new(1),
                json!({}),
            );
            (id, envelope)
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(48))]

            #[test]
            fn matches_reference_model(
                batches in prop::collection::vec(
                    prop::collection::vec(sub_commit_strategy(), 1..=3),
                    1..15,
                )
            ) {
                let outcome: Result<(), TestCaseError> = RUNTIME.block_on(async {
                    let store = InMemoryEventStore::new();
                    let aggregate_ids: Vec<Uuid> = (0..NUM_STREAMS).map(|_| Uuid::new_v4()).collect();
                    let stream_ids: Vec<StreamId> = aggregate_ids
                        .iter()
                        .map(|id| StreamId::new("Model", *id))
                        .collect();
                    let mut model = Model::new();

                    for sub_commits in &batches {
                        let mut tentative = model.clone();
                        let mut rejected = false;
                        let mut commits = Vec::new();

                        for (stream, offset, count) in sub_commits {
                            let expected = (tentative.version(*stream) + offset).max(0);
                            let accepts = !rejected && expected == tentative.version(*stream);

                            let mut ids = Vec::new();
                            let mut events = Vec::new();
                            for _ in 0..*count {
                                let (id, event) = make_envelope(aggregate_ids[*stream]);
                                ids.push(id);
                                events.push(event);
                            }
                            commits.push(StreamCommit {
                                stream_id: stream_ids[*stream].clone(),
                                events,
                                expected_version: AggregateVersion::new(expected),
                                claims: vec![],
                                clear_claims: false,
                            });

                            if accepts {
                                tentative.record(*stream, &ids);
                            } else {
                                rejected = true;
                            }
                        }

                        let result = store.append_batch(commits).await;
                        if rejected {
                            let Err(error) = result else {
                                return Err(TestCaseError::fail(
                                    "expected a concurrency conflict but the batch was accepted",
                                ));
                            };
                            if !error.is_concurrency_conflict() {
                                return Err(TestCaseError::fail(format!(
                                    "expected a concurrency conflict, got {error:?}"
                                )));
                            }
                            // Atomic rollback: a condemned batch applies NONE of
                            // its sub-commits, so the model stays as it was.
                        } else {
                            if let Err(error) = result {
                                return Err(TestCaseError::fail(format!(
                                    "expected the batch to be accepted, got {error:?}"
                                )));
                            }
                            model = tentative;
                        }
                    }

                    for (stream, stream_id) in stream_ids.iter().enumerate() {
                        let version = store.get_version(stream_id.clone()).await.unwrap();
                        let stream_len = model.version(stream);
                        prop_assert_eq!(version, AggregateVersion::new(stream_len));

                        for from in 0..=stream_len {
                            let loaded = store
                                .load_stream(stream_id.clone(), AggregateVersion::new(from))
                                .await
                                .unwrap();
                            let ids: Vec<Uuid> = loaded
                                .map(|event| event.unwrap().id)
                                .collect::<Vec<_>>()
                                .await;
                            let from = usize::try_from(from).unwrap();
                            prop_assert_eq!(ids, model.streams[stream][from..].to_vec());
                        }
                    }

                    let global_len = i64::try_from(model.global.len()).unwrap();
                    for from in 0..=global_len {
                        let all = store.stream_all(Position::new(from)).await.unwrap();
                        let entries: Vec<_> = all.map(Result::unwrap).collect().await;

                        let positions: Vec<i64> =
                            entries.iter().map(|e| e.position.as_i64()).collect();
                        let expected_positions: Vec<i64> = (from + 1..=global_len).collect();
                        prop_assert_eq!(positions, expected_positions);

                        let ids: Vec<Uuid> = entries.iter().map(|e| e.envelope.id).collect();
                        let from = usize::try_from(from).unwrap();
                        prop_assert_eq!(ids, model.global[from..].to_vec());
                    }

                    Ok(())
                });
                outcome?;
            }
        }
    }

    mod claims {
        use super::*;
        use event_sauce_core::AggregateClaim;

        fn claiming(
            aggregate_id: Uuid,
            expected_version: AggregateVersion,
            claims: Vec<AggregateClaim>,
        ) -> StreamCommit {
            let stream_id = StreamId::new("Account", aggregate_id);
            StreamCommit {
                claims,
                ..commit(&stream_id, aggregate_id, "Registered", expected_version)
            }
        }

        fn email(value: &str) -> Vec<AggregateClaim> {
            vec![AggregateClaim::new("Account.email", json!(value))]
        }

        async fn append_one(store: &InMemoryEventStore, commit: StreamCommit) -> Result<(), Error> {
            store.append_batch(vec![commit]).await
        }

        #[tokio::test]
        async fn test_claim_held_by_another_aggregate_is_refused_and_nothing_written() {
            let store = InMemoryEventStore::new();
            append_one(
                &store,
                claiming(Uuid::new_v4(), AggregateVersion::initial(), email("a@x")),
            )
            .await
            .unwrap();

            let intruder = Uuid::new_v4();
            let err = append_one(
                &store,
                claiming(intruder, AggregateVersion::initial(), email("a@x")),
            )
            .await
            .unwrap_err();

            assert!(err.is_claim_conflict());
            assert_eq!(store.all_events().len(), 1);
            assert_eq!(
                store
                    .get_version(StreamId::new("Account", intruder))
                    .await
                    .unwrap(),
                AggregateVersion::initial()
            );
        }

        #[tokio::test]
        async fn test_conflicting_commit_rejects_the_whole_batch() {
            let store = InMemoryEventStore::new();
            append_one(
                &store,
                claiming(Uuid::new_v4(), AggregateVersion::initial(), email("taken")),
            )
            .await
            .unwrap();

            let first = Uuid::new_v4();
            let err = store
                .append_batch(vec![
                    claiming(first, AggregateVersion::initial(), email("fresh")),
                    claiming(Uuid::new_v4(), AggregateVersion::initial(), email("taken")),
                ])
                .await
                .unwrap_err();

            assert!(err.is_claim_conflict());
            assert_eq!(store.all_events().len(), 1);
            append_one(
                &store,
                claiming(Uuid::new_v4(), AggregateVersion::initial(), email("fresh")),
            )
            .await
            .expect("an earlier commit's claim must not survive the rejected batch");
        }

        #[tokio::test]
        async fn test_two_commits_in_one_batch_cannot_claim_the_same_value() {
            let store = InMemoryEventStore::new();
            let err = store
                .append_batch(vec![
                    claiming(Uuid::new_v4(), AggregateVersion::initial(), email("dup")),
                    claiming(Uuid::new_v4(), AggregateVersion::initial(), email("dup")),
                ])
                .await
                .unwrap_err();

            assert!(err.is_claim_conflict());
            assert!(store.all_events().is_empty());
        }

        #[tokio::test]
        async fn test_same_aggregate_may_reclaim_its_own_value() {
            let store = InMemoryEventStore::new();
            let id = Uuid::new_v4();
            append_one(
                &store,
                claiming(id, AggregateVersion::initial(), email("a@x")),
            )
            .await
            .unwrap();

            append_one(&store, claiming(id, AggregateVersion::new(1), email("a@x")))
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn test_changing_a_claim_releases_the_old_value() {
            let store = InMemoryEventStore::new();
            let id = Uuid::new_v4();
            append_one(
                &store,
                claiming(id, AggregateVersion::initial(), email("old")),
            )
            .await
            .unwrap();
            append_one(&store, claiming(id, AggregateVersion::new(1), email("new")))
                .await
                .unwrap();

            append_one(
                &store,
                claiming(Uuid::new_v4(), AggregateVersion::initial(), email("old")),
            )
            .await
            .expect("the released value is free for another aggregate");
            let err = append_one(
                &store,
                claiming(Uuid::new_v4(), AggregateVersion::initial(), email("new")),
            )
            .await
            .unwrap_err();
            assert!(err.is_claim_conflict());
        }

        #[tokio::test]
        async fn test_clear_claims_releases_every_claim() {
            let store = InMemoryEventStore::new();
            let id = Uuid::new_v4();
            append_one(
                &store,
                claiming(id, AggregateVersion::initial(), email("gone")),
            )
            .await
            .unwrap();
            let mut tombstone = claiming(id, AggregateVersion::new(1), vec![]);
            tombstone.clear_claims = true;
            append_one(&store, tombstone).await.unwrap();

            append_one(
                &store,
                claiming(Uuid::new_v4(), AggregateVersion::initial(), email("gone")),
            )
            .await
            .expect("cleared claims are free for another aggregate");
        }

        #[tokio::test]
        async fn test_version_conflict_does_not_record_claims() {
            let store = InMemoryEventStore::new();
            let err = append_one(
                &store,
                claiming(Uuid::new_v4(), AggregateVersion::new(3), email("ghost")),
            )
            .await
            .unwrap_err();
            assert!(err.is_concurrency_conflict());

            append_one(
                &store,
                claiming(Uuid::new_v4(), AggregateVersion::initial(), email("ghost")),
            )
            .await
            .unwrap();
        }
    }
}
