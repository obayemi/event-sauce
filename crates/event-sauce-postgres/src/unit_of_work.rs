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

use crate::event_store::PostgresEventStore;
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
        self.commit_atomic_in_tx(commit)
            .await
            .map_err(as_retryable_conflict)
    }
}

impl PostgresTransactionalStore {
    async fn commit_atomic_in_tx(&self, commit: AtomicCommit) -> Result<()> {
        let AtomicCommit {
            stream_writes,
            mut state_writes,
            mut stream_expectations,
            mut state_expectations,
        } = commit;

        let stream_commits = stream_writes
            .into_iter()
            .map(|write| write.commit)
            .collect();
        let (stream_commits, precheck_indices) = PostgresEventStore::prepare_batch(stream_commits)?;

        state_writes.sort_by_key(|write| sort_key(&write.state.stream_id()));
        stream_expectations.sort_by_key(|(stream_id, _)| sort_key(stream_id));
        state_expectations.sort_by_key(|(stream_id, _)| sort_key(stream_id));

        let mut tx = self
            .events
            .pool()
            .begin()
            .await
            .map_err(|e| Error::backend("Failed to start transaction", e))?;

        if !stream_commits.is_empty() || !stream_expectations.is_empty() {
            self.events.acquire_append_lock_for(&mut tx).await?;
        }

        for (stream_id, version) in &stream_expectations {
            self.events
                .check_stream_expectation_in(&mut tx, stream_id, *version)
                .await?;
        }

        for (stream_id, version) in &state_expectations {
            self.states
                .check_expectation_in(&mut tx, stream_id, *version)
                .await?;
        }

        self.events
            .write_streams_in(&mut tx, &stream_commits, &precheck_indices)
            .await?;

        for commit in state_writes {
            self.states.write_commit_in_tx(&mut tx, commit).await?;
        }

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit transaction", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use event_sauce_core::{
        AggregateType, EventEnvelope, EventFilter, EventStore, EventVersion, Expectation,
        PreparedWrite, StateCommit, StateProjection, StateStore, StoredState, StreamCommit,
        StreamWrite,
    };
    use serde_json::json;
    use sqlx::{PgConnection, PgPool};
    use std::time::Duration;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::ContainerAsync;

    use crate::event_store::PostgresEventStoreBuilder;
    use crate::state_store::PostgresStateStoreBuilder;

    struct TestDb {
        pool: PgPool,
        url: String,
        #[allow(dead_code)]
        container: ContainerAsync<Postgres>,
    }

    impl TestDb {
        async fn new() -> Self {
            let (container, url) = crate::test_support::start_postgres_url().await;
            let pool = PgPool::connect(&url).await.expect("connect");
            Self {
                pool,
                url,
                container,
            }
        }

        async fn stores_with(
            &self,
            state_builder: PostgresStateStoreBuilder,
        ) -> (Arc<PostgresEventStore>, Arc<PostgresStateStore>) {
            let events = PostgresEventStoreBuilder::new()
                .pool(self.pool.clone())
                .schema("event_sauce")
                .build()
                .expect("pool was set");
            events.migrate().await.expect("migrate events");

            let states = state_builder.build().expect("pool was set");
            states.migrate().await.expect("migrate states");

            (Arc::new(events), Arc::new(states))
        }

        async fn stores(&self) -> (Arc<PostgresEventStore>, Arc<PostgresStateStore>) {
            self.stores_with(
                PostgresStateStore::builder()
                    .pool(self.pool.clone())
                    .schema("event_sauce"),
            )
            .await
        }

        async fn transactional(
            &self,
        ) -> (
            Arc<PostgresEventStore>,
            Arc<PostgresStateStore>,
            PostgresTransactionalStore,
        ) {
            let (events, states) = self.stores().await;
            let store = PostgresTransactionalStore::new(Arc::clone(&events), Arc::clone(&states))
                .expect("same database");
            (events, states, store)
        }
    }

    fn manual_event(aggregate_id: Uuid, aggregate_type: &str, event_type: &str) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            aggregate_type.to_string(),
            event_type.to_string(),
            EventVersion::new(1),
            json!({}),
        )
    }

    fn manual_stream_commit(
        stream_id: StreamId,
        expected_version: AggregateVersion,
        events: Vec<EventEnvelope>,
    ) -> StreamWrite {
        StreamWrite {
            commit: StreamCommit {
                stream_id,
                events,
                expected_version,
                claims: vec![],
                clear_claims: false,
            },
            snapshot: None,
        }
    }

    fn manual_state_commit(
        aggregate_type: &str,
        aggregate_id: Uuid,
        version: i64,
        expected: i64,
        events: Vec<EventEnvelope>,
    ) -> StateCommit {
        StateCommit {
            state: StoredState {
                aggregate_id,
                aggregate_type: AggregateType::from_owned(aggregate_type.to_string()),
                state_data: json!({ "n": version }),
                version: AggregateVersion::new(version),
                is_deleted: false,
                schema_version: 0,
            },
            expected_version: AggregateVersion::new(expected),
            events,
            claims: vec![],
            clear_claims: false,
        }
    }

    #[tokio::test]
    async fn test_new_refuses_stores_on_different_databases() {
        let db = TestDb::new().await;
        let events = Arc::new(PostgresEventStore::new(db.pool.clone()));

        sqlx::query("CREATE DATABASE other_db")
            .execute(&db.pool)
            .await
            .expect("create other database");
        let cut = db.url.rfind('/').expect("url has a database segment");
        let other_url = format!("{}other_db", &db.url[..=cut]);
        let other_pool = PgPool::connect(&other_url).await.expect("connect");
        let states = Arc::new(PostgresStateStore::new(other_pool));

        let err = PostgresTransactionalStore::new(events, states).unwrap_err();
        assert!(err.is_invalid_state());
    }

    #[tokio::test]
    async fn test_commit_atomic_writes_stream_and_state() {
        let db = TestDb::new().await;
        let (events, states, store) = db.transactional().await;

        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let state_id = Uuid::new_v4();

        let mut commit = AtomicCommit::new();
        commit.push_write(PreparedWrite::Stream(manual_stream_commit(
            stream_id.clone(),
            AggregateVersion::initial(),
            vec![manual_event(
                stream_id.aggregate_id(),
                "Order",
                "Order.Placed",
            )],
        )));
        commit.push_write(PreparedWrite::State(manual_state_commit(
            "Invoice",
            state_id,
            1,
            0,
            vec![],
        )));

        store.commit_atomic(commit).await.expect("commit_atomic");

        assert_eq!(events.get_version(stream_id).await.unwrap().as_i64(), 1);
        assert!(states
            .exists(StreamId::new("Invoice", state_id))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_stale_stream_expectation_rolls_back_state_write() {
        let db = TestDb::new().await;
        let (events, states, store) = db.transactional().await;

        let stream_id = StreamId::new("Order", Uuid::new_v4());
        events
            .append(
                stream_id.clone(),
                vec![manual_event(
                    stream_id.aggregate_id(),
                    "Order",
                    "Order.Placed",
                )],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .expect("seed stream");

        let state_id = Uuid::new_v4();
        let mut commit = AtomicCommit::new();
        commit.push_expectation(Expectation::Stream {
            stream_id: stream_id.clone(),
            version: AggregateVersion::initial(),
        });
        commit.push_write(PreparedWrite::State(manual_state_commit(
            "Invoice",
            state_id,
            1,
            0,
            vec![],
        )));

        let err = store.commit_atomic(commit).await.unwrap_err();
        assert!(err.is_concurrency_conflict());
        assert!(!states
            .exists(StreamId::new("Invoice", state_id))
            .await
            .unwrap());
        assert_eq!(
            events.get_version(stream_id).await.unwrap().as_i64(),
            1,
            "the stream must be unaffected by the failed commit"
        );
    }

    #[tokio::test]
    async fn test_stale_state_expectation_rolls_back_stream_write() {
        let db = TestDb::new().await;
        let (events, states, store) = db.transactional().await;

        let state_id = Uuid::new_v4();
        states
            .save(manual_state_commit("Invoice", state_id, 1, 0, vec![]))
            .await
            .expect("seed state");

        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let mut commit = AtomicCommit::new();
        commit.push_expectation(Expectation::State {
            stream_id: StreamId::new("Invoice", state_id),
            version: AggregateVersion::initial(),
        });
        commit.push_write(PreparedWrite::Stream(manual_stream_commit(
            stream_id.clone(),
            AggregateVersion::initial(),
            vec![manual_event(
                stream_id.aggregate_id(),
                "Order",
                "Order.Placed",
            )],
        )));

        let err = store.commit_atomic(commit).await.unwrap_err();
        assert!(err.is_concurrency_conflict());
        assert_eq!(events.get_version(stream_id).await.unwrap().as_i64(), 0);
        assert_eq!(
            states
                .get_version(StreamId::new("Invoice", state_id))
                .await
                .unwrap()
                .as_i64(),
            1,
            "the state row must be unaffected by the failed commit"
        );
    }

    #[tokio::test]
    async fn test_stale_stream_write_rolls_back_state_write() {
        let db = TestDb::new().await;
        let (events, states, store) = db.transactional().await;

        let stream_id = StreamId::new("Order", Uuid::new_v4());
        events
            .append(
                stream_id.clone(),
                vec![manual_event(
                    stream_id.aggregate_id(),
                    "Order",
                    "Order.Placed",
                )],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .expect("seed stream");

        let state_id = Uuid::new_v4();
        let mut commit = AtomicCommit::new();
        commit.push_write(PreparedWrite::Stream(manual_stream_commit(
            stream_id.clone(),
            AggregateVersion::initial(),
            vec![manual_event(
                stream_id.aggregate_id(),
                "Order",
                "Order.Stale",
            )],
        )));
        commit.push_write(PreparedWrite::State(manual_state_commit(
            "Invoice",
            state_id,
            1,
            0,
            vec![],
        )));

        let err = store.commit_atomic(commit).await.unwrap_err();
        assert!(err.is_concurrency_conflict());
        assert!(!states
            .exists(StreamId::new("Invoice", state_id))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_stale_state_write_rolls_back_stream_write() {
        let db = TestDb::new().await;
        let (events, states, store) = db.transactional().await;

        let state_id = Uuid::new_v4();
        states
            .save(manual_state_commit("Invoice", state_id, 1, 0, vec![]))
            .await
            .expect("seed state");

        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let mut commit = AtomicCommit::new();
        commit.push_write(PreparedWrite::State(manual_state_commit(
            "Invoice",
            state_id,
            2,
            0,
            vec![],
        )));
        commit.push_write(PreparedWrite::Stream(manual_stream_commit(
            stream_id.clone(),
            AggregateVersion::initial(),
            vec![manual_event(
                stream_id.aggregate_id(),
                "Order",
                "Order.Placed",
            )],
        )));

        let err = store.commit_atomic(commit).await.unwrap_err();
        assert!(err.is_concurrency_conflict());
        assert_eq!(events.get_version(stream_id).await.unwrap().as_i64(), 0);
        assert_eq!(
            states
                .get_version(StreamId::new("Invoice", state_id))
                .await
                .unwrap()
                .as_i64(),
            1,
            "the stale write must leave the state row unchanged"
        );
    }

    struct BoomProjection;

    #[async_trait]
    impl StateProjection<PgConnection> for BoomProjection {
        fn name(&self) -> &'static str {
            "boom"
        }

        fn filter(&self) -> EventFilter {
            EventFilter::by_event_type("Invoice.Boom")
        }

        async fn project(&self, _ctx: &mut PgConnection, _events: &[EventEnvelope]) -> Result<()> {
            Err(Error::custom("projection boom"))
        }
    }

    #[tokio::test]
    async fn test_failing_projection_rolls_back_stream_write_too() {
        let db = TestDb::new().await;
        let (events, states) = db
            .stores_with(
                PostgresStateStore::builder()
                    .pool(db.pool.clone())
                    .schema("event_sauce")
                    .with_projection(Arc::new(BoomProjection)),
            )
            .await;
        let store = PostgresTransactionalStore::new(Arc::clone(&events), Arc::clone(&states))
            .expect("same database");

        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let state_id = Uuid::new_v4();

        let mut commit = AtomicCommit::new();
        commit.push_write(PreparedWrite::Stream(manual_stream_commit(
            stream_id.clone(),
            AggregateVersion::initial(),
            vec![manual_event(
                stream_id.aggregate_id(),
                "Order",
                "Order.Placed",
            )],
        )));
        commit.push_write(PreparedWrite::State(manual_state_commit(
            "Invoice",
            state_id,
            1,
            0,
            vec![manual_event(state_id, "Invoice", "Invoice.Boom")],
        )));

        let err = store.commit_atomic(commit).await.unwrap_err();
        assert_eq!(err.to_string(), "projection boom");
        assert_eq!(
            events.get_version(stream_id).await.unwrap().as_i64(),
            0,
            "the paired stream write must roll back when the projection fails"
        );
        assert!(!states
            .exists(StreamId::new("Invoice", state_id))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_concurrent_commits_on_same_stream_only_one_wins() {
        let db = TestDb::new().await;
        let (events, _states, store) = db.transactional().await;
        let stream_id = StreamId::new("Order", Uuid::new_v4());

        let mut commit_a = AtomicCommit::new();
        commit_a.push_write(PreparedWrite::Stream(manual_stream_commit(
            stream_id.clone(),
            AggregateVersion::initial(),
            vec![manual_event(stream_id.aggregate_id(), "Order", "Order.A")],
        )));
        let mut commit_b = AtomicCommit::new();
        commit_b.push_write(PreparedWrite::Stream(manual_stream_commit(
            stream_id.clone(),
            AggregateVersion::initial(),
            vec![manual_event(stream_id.aggregate_id(), "Order", "Order.B")],
        )));

        let store_a = store.clone();
        let store_b = store.clone();
        let task_a = tokio::spawn(async move { store_a.commit_atomic(commit_a).await });
        let task_b = tokio::spawn(async move { store_b.commit_atomic(commit_b).await });
        let result_a = task_a.await.unwrap();
        let result_b = task_b.await.unwrap();

        let ok_count = usize::from(result_a.is_ok()) + usize::from(result_b.is_ok());
        assert_eq!(ok_count, 1, "exactly one commit must win the race");

        let loser = if result_a.is_err() {
            result_a
        } else {
            result_b
        };
        assert!(
            loser.unwrap_err().is_concurrency_conflict(),
            "the loser must surface a typed concurrency conflict, not a raw backend error"
        );
        assert_eq!(events.get_version(stream_id).await.unwrap().as_i64(), 1);
    }

    #[tokio::test]
    async fn test_state_expectation_sees_a_concurrently_committed_update() {
        let db = TestDb::new().await;
        let (_events, states, store) = db.transactional().await;

        let state_id = Uuid::new_v4();
        states
            .save(manual_state_commit("Invoice", state_id, 1, 0, vec![]))
            .await
            .expect("seed state");

        let mut racer = db.pool.begin().await.expect("begin racer tx");
        sqlx::query(
            "UPDATE event_sauce.aggregate_states SET version = 2
             WHERE aggregate_id = $1 AND aggregate_type = $2",
        )
        .bind(state_id)
        .bind("Invoice")
        .execute(&mut *racer)
        .await
        .expect("racer update");

        let mut commit = AtomicCommit::new();
        commit.push_expectation(Expectation::State {
            stream_id: StreamId::new("Invoice", state_id),
            version: AggregateVersion::new(1),
        });
        let checker_store = store.clone();
        let checker = tokio::spawn(async move { checker_store.commit_atomic(commit).await });

        tokio::time::sleep(Duration::from_millis(200)).await;
        racer.commit().await.expect("racer commit");

        let err = checker
            .await
            .unwrap()
            .expect_err("the expectation must see the concurrently committed update");
        assert!(err.is_concurrency_conflict());
    }
}
