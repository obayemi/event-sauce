//! Event store trait for event persistence.
//!
//! Defines the `EventStore` trait for persisting and retrieving events with streaming support.
//!
//! The commit pipeline (preparing and flushing writes) lives in [`commit`];
//! loading and replay live in [`load`]; their shared crypto helpers live in
//! [`encryption`]; the snapshot type lives in [`snapshot`].

mod commit;
mod encryption;
mod load;
mod snapshot;

pub(crate) use commit::{flush_prepared, flush_prepared_batch, prepare_commit, PreparedCommit};
pub(crate) use load::{count_events, load, load_any, load_deleted};
pub use snapshot::Snapshot;

use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
#[cfg(test)]
use uuid::Uuid;

use crate::{
    Aggregate, AggregateClaim, AggregateRoot, AggregateVersion, DeletedAggregateRoot,
    EventEnvelope, EventLogEntry, EventSourcedRepository, Position, Result, SnapshotConfig,
    StreamId,
};

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
        if let Some(prepared) = prepare_commit(self, aggregate).await? {
            flush_prepared(self, prepared).await?;
            aggregate.clear_pending_events();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test_support;

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
