//! Current-state persistence primitives (the non-event-sourced storage style).
//!
//! A [`StateStore`] persists each aggregate as a single versioned row of
//! serialized state instead of an event stream. Events still drive every
//! in-memory state transition — they are handed to the store inside each
//! [`StateCommit`] so backends can feed in-transaction projections and a
//! transactional outbox — but they are **not** stored as a log and cannot be
//! replayed later.
//!
//! Application code never talks to a `StateStore` directly; it uses the
//! [`Repository`](crate::Repository) trait via
//! [`StateStoredRepository`](crate::StateStoredRepository).

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Aggregate, AggregateClaim, AggregateType, AggregateVersion, EventEnvelope, Result,
    StateStoredRepository, StreamId,
};

/// A persisted aggregate state row.
///
/// This is the unit a [`StateStore`] reads and writes: the aggregate's
/// serialized current state plus the bookkeeping needed for optimistic
/// concurrency ([`version`](Self::version)), the deletion lifecycle
/// ([`is_deleted`](Self::is_deleted)), and schema evolution
/// ([`schema_version`](Self::schema_version)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredState {
    /// The aggregate's unique identifier.
    pub aggregate_id: Uuid,
    /// The aggregate type name.
    pub aggregate_type: AggregateType,
    /// The serialized aggregate state (`A` for active rows,
    /// `A::DeletedState` for tombstones).
    pub state_data: serde_json::Value,
    /// The aggregate version this state reflects (the optimistic-lock value).
    pub version: AggregateVersion,
    /// Whether this row is a deletion tombstone holding `A::DeletedState`.
    pub is_deleted: bool,
    /// The state schema version, from
    /// [`Aggregate::snapshot_version()`](crate::Aggregate::snapshot_version).
    pub schema_version: u32,
}

impl StoredState {
    /// Returns the stream identifier (`aggregate_type` + `aggregate_id`) for this row.
    #[must_use]
    pub fn stream_id(&self) -> StreamId {
        StreamId::new(self.aggregate_type.clone(), self.aggregate_id)
    }
}

/// One aggregate's state write (the state-path analog of
/// [`StreamCommit`](crate::StreamCommit)).
///
/// Carries the new state row, the optimistic-concurrency expectation, the
/// events that produced the transition, and the claims to enforce. The
/// `events` are consumed transactionally by backends (in-transaction
/// projections, outbox enqueue) and then discarded — they are never stored as
/// a replayable log.
#[derive(Debug, Clone)]
pub struct StateCommit {
    /// The new state row to persist.
    pub state: StoredState,
    /// The version the stored row must currently have
    /// ([`AggregateVersion::initial()`] for a new aggregate).
    pub expected_version: AggregateVersion,
    /// The events that produced this state transition, in application order.
    pub events: Vec<EventEnvelope>,
    /// Uniqueness claims to enforce transactionally with the write.
    pub claims: Vec<AggregateClaim>,
    /// When `true`, all existing claims for the aggregate are removed
    /// (used for deleted aggregates).
    pub clear_claims: bool,
}

/// Trait for current-state store implementations.
///
/// The state store persists each aggregate as one versioned row and enforces
/// optimistic concurrency and uniqueness claims on write. Backends with real
/// transactions additionally run registered in-transaction projections and
/// outbox enqueues inside the same transaction as the state write — a
/// projection failure rolls the whole command back. There are deliberately
/// **no after-commit callbacks**: anything that must outlive the transaction
/// goes through the backend's transactional outbox.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Loads the state row for a stream, or `None` if the aggregate was never
    /// saved.
    ///
    /// Deletion tombstones are returned as rows with
    /// [`is_deleted`](StoredState::is_deleted) set — interpreting them is the
    /// repository's job.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails.
    async fn load(&self, stream_id: StreamId) -> Result<Option<StoredState>>;

    /// Persists one state commit with optimistic concurrency control.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if the stored version doesn't
    /// match [`expected_version`](StateCommit::expected_version).
    /// Returns `Error::ClaimConflict` if a claim is already held by another
    /// aggregate.
    async fn save(&self, commit: StateCommit) -> Result<()>;

    /// Persists several state commits as one logical write.
    ///
    /// The default implementation loops [`save`](Self::save) over each commit,
    /// so it is **not** atomic: a partial failure leaves earlier commits
    /// persisted. Backends with real transactions override this to run the
    /// whole batch in a single transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if any commit fails; on the looping default, commits
    /// before the failing one are already persisted.
    async fn save_batch(&self, commits: Vec<StateCommit>) -> Result<()> {
        for commit in commits {
            self.save(commit).await?;
        }
        Ok(())
    }

    /// Gets the current version of a stream without deserializing its state.
    ///
    /// Returns [`AggregateVersion::initial()`] if the aggregate was never saved.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails.
    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        Ok(self
            .load(stream_id)
            .await?
            .map_or_else(AggregateVersion::initial, |state| state.version))
    }

    /// Checks if a state row exists for the stream (tombstones included).
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store fails.
    async fn exists(&self, stream_id: StreamId) -> Result<bool> {
        Ok(self.load(stream_id).await?.is_some())
    }

    /// Creates a [`StateStoredRepository`] for the given aggregate type,
    /// wrapping this state store.
    ///
    /// This is a convenience method that avoids verbose turbofish syntax.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let store = Arc::new(MyStateStore::new());
    /// let user_repo = store.repository::<User>();
    /// ```
    fn repository<A>(self: &Arc<Self>) -> StateStoredRepository<Self, A>
    where
        Self: Sized + 'static,
        A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
        A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
        A::Event: serde::Serialize + serde::de::DeserializeOwned,
    {
        StateStoredRepository::new(Arc::clone(self))
    }
}
