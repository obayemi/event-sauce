//! `PostgreSQL` [`TransactionalStore`] implementation: commits event-sourced
//! and state-stored writes in one Postgres transaction.
//!
//! [`PostgresTransactionalStore`] pairs a [`PostgresEventStore`] and a
//! [`PostgresStateStore`] that share one database, and implements
//! [`commit_atomic`](TransactionalStore::commit_atomic) by running every
//! stream write, every state write, and every version expectation on a
//! single connection from the event store's pool, inside one transaction.

use std::sync::Arc;

use async_trait::async_trait;
use event_sauce_core::{
    AggregateVersion, AtomicCommit, Error, Result, StreamId, TransactionalStore,
};
use uuid::Uuid;

use crate::event_store::{PlannedBatch, PostgresEventStore};
use crate::state_store::PostgresStateStore;

/// Pairs a [`PostgresEventStore`] and a [`PostgresStateStore`] that share one
/// database, and commits [`AtomicCommit`]s across both atomically.
///
/// Both stores must point at the same Postgres database — [`Self::new`]
/// refuses the pair otherwise, since [`commit_atomic`](TransactionalStore::commit_atomic)
/// runs every write on a single connection from the event store's pool, and a
/// state-store write on a different database could never share that
/// connection's transaction. The two stores may use different schemas, but
/// sharing one (the default `"event_sauce"`) is recommended: both create the
/// same `aggregate_claims` table idempotently on `migrate()`, so uniqueness
/// claims stay visible to both stores either way.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::{PostgresEventStore, PostgresStateStore, PostgresTransactionalStore};
/// use std::sync::Arc;
///
/// let events = Arc::new(PostgresEventStore::new(pool.clone()));
/// let states = Arc::new(PostgresStateStore::new(pool));
/// let store = PostgresTransactionalStore::new(events, states)?;
/// ```
#[derive(Clone)]
pub struct PostgresTransactionalStore {
    events: Arc<PostgresEventStore>,
    states: Arc<PostgresStateStore>,
}

impl std::fmt::Debug for PostgresTransactionalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresTransactionalStore")
            .field("events_schema", &self.events.schema())
            .field("states_schema", &self.states.schema())
            .finish_non_exhaustive()
    }
}

impl PostgresTransactionalStore {
    /// Pairs an event store and a state store into one transactional store.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidState` if the two stores' pools do not point at
    /// the same host, port and database — `commit_atomic` could not make
    /// their writes atomic in that case.
    pub fn new(events: Arc<PostgresEventStore>, states: Arc<PostgresStateStore>) -> Result<Self> {
        let events_options = events.pool().connect_options();
        let states_options = states.pool().connect_options();

        let same_database = events_options.get_host() == states_options.get_host()
            && events_options.get_port() == states_options.get_port()
            && events_options.get_database() == states_options.get_database();

        if !same_database {
            return Err(Error::invalid_state(
                "PostgresTransactionalStore requires both stores' pools to point \
                 at the same host, port and database",
            ));
        }

        Ok(Self { events, states })
    }
}

/// Sorts a stream expectation, state expectation or state write by its
/// `(aggregate_type, aggregate_id)` key, so [`PostgresTransactionalStore::commit_atomic`]
/// always locks state rows in the same order regardless of the order its
/// caller built them in. Two commits racing to lock the same rows in
/// opposite orders is exactly how two transactions deadlock; a fixed order
/// rules that out.
fn sort_key(stream_id: &StreamId) -> (String, Uuid) {
    (
        stream_id.aggregate_type().as_str().to_string(),
        stream_id.aggregate_id(),
    )
}

/// Maps a deadlock (`40P01`) or serialization failure (`40001`) raised
/// anywhere inside [`PostgresTransactionalStore::commit_atomic`]'s
/// transaction to `Error::ConcurrencyConflict`, so callers retry instead of
/// treating it as a hard failure. Neither SQLSTATE names one specific stream
/// or row, so both `expected` and `actual` are reported as
/// [`AggregateVersion::initial`] — a retry signal, not a real version
/// mismatch.
fn as_retryable_conflict(error: Error) -> Error {
    let sqlstate = match &error {
        Error::Backend { source, .. } => source
            .downcast_ref::<sqlx::Error>()
            .and_then(sqlx::Error::as_database_error)
            .and_then(|db_error| db_error.code().map(std::borrow::Cow::into_owned)),
        _ => None,
    };

    match sqlstate.as_deref() {
        Some("40001" | "40P01") => {
            Error::concurrency_conflict(AggregateVersion::initial(), AggregateVersion::initial())
        }
        _ => error,
    }
}

#[async_trait]
impl TransactionalStore for PostgresTransactionalStore {
    type Events = PostgresEventStore;
    type States = PostgresStateStore;

    fn event_store(&self) -> &Self::Events {
        &self.events
    }

    fn state_store(&self) -> &Self::States {
        &self.states
    }

    /// Commits every write and checks every expectation in `commit` in one
    /// Postgres transaction on a single connection from the event store's
    /// pool: either all of it becomes durable, or none of it does.
    ///
    /// Stream expectations and state expectations are checked first — under
    /// the append serialization lock, and under a `FOR SHARE` row lock
    /// respectively — before any write runs, so a stale read never slips a
    /// write past a caller who only meant to assert, not change, an
    /// aggregate's version. State rows (writes and expectations alike) are
    /// locked in a fixed `(aggregate_type, aggregate_id)` order to rule out
    /// a lock-ordering deadlock between two concurrent commits.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if any write's or expectation's
    /// version does not match what is currently stored, including when
    /// Postgres reports a deadlock or serialization failure instead (mapped
    /// from SQLSTATE `40001`/`40P01`). Returns `Error::ClaimConflict` if a
    /// claim in a write is already held by another aggregate. Any other
    /// failure — including a failing state projection — rolls the whole
    /// transaction back.
    async fn commit_atomic(&self, commit: AtomicCommit) -> Result<()> {
        if commit.is_empty() {
            return Ok(());
        }
        self.run_commit_atomic(commit)
            .await
            .map_err(as_retryable_conflict)
    }
}

impl PostgresTransactionalStore {
    async fn run_commit_atomic(&self, commit: AtomicCommit) -> Result<()> {
        let AtomicCommit {
            stream_writes,
            mut state_writes,
            mut stream_expectations,
            mut state_expectations,
        } = commit;

        let batch = PlannedBatch::plan(stream_writes)?;

        state_writes.sort_by_key(|write| sort_key(&write.state.stream_id()));
        stream_expectations.sort_by_key(|(stream_id, _)| sort_key(stream_id));
        state_expectations.sort_by_key(|(stream_id, _)| sort_key(stream_id));

        let mut tx = self
            .events
            .pool()
            .begin()
            .await
            .map_err(|e| Error::backend("Failed to start transaction", e))?;

        if !batch.is_empty() || !stream_expectations.is_empty() {
            self.events.acquire_append_lock(&mut tx).await?;
        }

        for (stream_id, version) in &stream_expectations {
            self.events
                .check_stream_expectation_on(&mut tx, stream_id, *version)
                .await?;
        }

        for (stream_id, version) in &state_expectations {
            self.states
                .check_state_expectation_on(&mut tx, stream_id, *version)
                .await?;
        }

        self.events.write_streams_on(&mut tx, &batch).await?;

        for commit in state_writes {
            self.states.write_commit_on(&mut tx, commit).await?;
        }

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit transaction", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests;
