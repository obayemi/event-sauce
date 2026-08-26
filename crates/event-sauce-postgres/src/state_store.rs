//! `PostgreSQL` state store implementation.
//!
//! Provides a production-ready `PostgreSQL` implementation of
//! [`StateStore`](event_sauce_core::StateStore): each aggregate is persisted
//! as one versioned `aggregate_states` row instead of an event stream, with
//! the same optimistic-concurrency and uniqueness-claim guarantees as the
//! event store.
//!
//! Every [`save`](event_sauce_core::StateStore::save) runs in a single
//! transaction that also feeds two explicitly opt-in channels:
//!
//! - **In-transaction projections** — [`StateProjection`] handlers registered
//!   via [`PostgresStateStoreBuilder::with_projection`] run on the save
//!   transaction's connection with the commit's matching events.
//!   Exactly-once, read-your-writes; a failing projection rolls the whole
//!   command back.
//! - **Transactional outbox** — with
//!   [`PostgresStateStoreBuilder::with_outbox`], the commit's events are
//!   inserted into the `state_outbox` table in the same transaction, for a
//!   background [`StateOutboxDispatcher`](crate::StateOutboxDispatcher) to
//!   deliver later. This is the only mechanism for effects that outlive the
//!   transaction — there are deliberately **no after-commit callbacks**.

use std::sync::Arc;

use async_trait::async_trait;
use event_sauce_core::{
    AggregateVersion, Error, EventEnvelope, Result, StateCommit, StateProjection, StateStore,
    StoredState, StreamId,
};
use sqlx::{PgConnection, PgPool};

/// `PostgreSQL` state store implementation.
///
/// Persists each aggregate as a single versioned row in `aggregate_states`
/// with:
/// - Optimistic concurrency control (version-checked upsert)
/// - Transactional uniqueness claims (shared `aggregate_claims` table)
/// - Opt-in in-transaction projections
/// - Opt-in transactional outbox
/// - Schema isolation to avoid conflicts with application migrations
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresStateStore;
/// use sqlx::PgPool;
/// use std::sync::Arc;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let pool = PgPool::connect("postgresql://localhost/app").await?;
///
///     let store = Arc::new(
///         PostgresStateStore::builder()
///             .pool(pool)
///             .schema("event_sauce")
///             .build()?,
///     );
///     store.migrate().await?;
///
///     let repo = store.repository::<Ticket>();
///     let mut ticket = Ticket::open("Broken build".to_string())?;
///     repo.save(&mut ticket).await?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct PostgresStateStore {
    pool: PgPool,
    schema: String,
    projections: Vec<Arc<dyn StateProjection<PgConnection>>>,
    outbox_enabled: bool,
}

/// Builder for configuring [`PostgresStateStore`].
///
/// # Examples
///
/// ```ignore
/// let store = PostgresStateStore::builder()
///     .pool(pool)
///     .schema("event_sauce")
///     .with_projection(Arc::new(VoteTotals))
///     .with_outbox()
///     .build()?;
/// ```
#[derive(Clone)]
pub struct PostgresStateStoreBuilder {
    pool: Option<PgPool>,
    schema: Option<String>,
    projections: Vec<Arc<dyn StateProjection<PgConnection>>>,
    outbox_enabled: bool,
}

impl std::fmt::Debug for PostgresStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStateStore")
            .field("schema", &self.schema)
            .field(
                "projections",
                &self
                    .projections
                    .iter()
                    .map(|p| p.name().to_string())
                    .collect::<Vec<_>>(),
            )
            .field("outbox_enabled", &self.outbox_enabled)
            .finish_non_exhaustive()
    }
}

impl PostgresStateStore {
    /// Creates a new `PostgreSQL` state store with default configuration
    /// (schema "`event_sauce`", no projections, outbox disabled).
    ///
    /// Equivalent to `PostgresStateStore::builder().pool(pool).build()`.
    ///
    /// # Panics
    ///
    /// Cannot panic — the pool is always set before calling `build()`.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self::builder().pool(pool).build().expect("pool was set")
    }

    /// Creates a builder for configuring the state store.
    #[must_use]
    pub fn builder() -> PostgresStateStoreBuilder {
        PostgresStateStoreBuilder::new()
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns the schema name used by this state store.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns an outbox handle bound to this store's pool and schema.
    ///
    /// Rows only appear in the outbox when the store was built with
    /// [`PostgresStateStoreBuilder::with_outbox`]; the handle itself is always
    /// available so a dispatcher can be constructed independently of the
    /// store that enqueues.
    #[must_use]
    pub fn outbox(&self) -> crate::PostgresStateOutbox {
        crate::PostgresStateOutbox::new(self.pool.clone(), self.schema.clone())
    }

    fn qualify_table(&self, table: &str) -> String {
        crate::migrations::qualify(&self.schema, table)
    }

    /// Runs database migrations to set up the state store schema.
    ///
    /// Creates the `aggregate_states` and `state_outbox` tables plus the
    /// shared `aggregate_claims` table (the same table the event store uses —
    /// claims are persistence-style-agnostic, so creation is idempotent
    /// across both stores). Safe to call multiple times.
    ///
    /// # Errors
    ///
    /// Returns an error if the database connection fails or the migration
    /// statements cannot be applied.
    pub async fn migrate(&self) -> Result<()> {
        crate::migrations::ensure_schema(&self.pool, &self.schema).await?;

        let migrations_table = self.qualify_table("_state_store_migrations");
        crate::migrations::ensure_migrations_table(&self.pool, &migrations_table).await?;

        let states_table = self.qualify_table("aggregate_states");
        crate::migrations::apply_once(
            &self.pool,
            &migrations_table,
            20_260_825_000_001_i64,
            "create_aggregate_states_table",
            move |pool| async move {
                let create_states = format!(
                    "CREATE TABLE IF NOT EXISTS {states_table} (
                        aggregate_type VARCHAR(255) NOT NULL,
                        aggregate_id UUID NOT NULL,
                        state JSONB NOT NULL,
                        version BIGINT NOT NULL,
                        is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                        schema_version INT NOT NULL DEFAULT 0,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (aggregate_type, aggregate_id)
                    )"
                );
                sqlx::query(&create_states)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create aggregate_states table", e))?;
                Ok(())
            },
        )
        .await?;

        let outbox_table = self.qualify_table("state_outbox");
        crate::migrations::apply_once(
            &self.pool,
            &migrations_table,
            20_260_825_000_002_i64,
            "create_state_outbox_table",
            move |pool| async move {
                let create_outbox = format!(
                    "CREATE TABLE IF NOT EXISTS {outbox_table} (
                        id BIGSERIAL PRIMARY KEY,
                        event_id UUID NOT NULL UNIQUE,
                        envelope JSONB NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'pending',
                        attempts INT NOT NULL DEFAULT 0,
                        failures INT NOT NULL DEFAULT 0,
                        last_error TEXT,
                        locked_by VARCHAR(255),
                        locked_until TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    )"
                );
                sqlx::query(&create_outbox)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create state_outbox table", e))?;

                let claim_index = format!(
                    "CREATE INDEX IF NOT EXISTS idx_state_outbox_claim
                     ON {outbox_table} (id)
                     WHERE status = 'pending'"
                );
                sqlx::query(&claim_index)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create state_outbox index", e))?;
                Ok(())
            },
        )
        .await?;

        let claims_table = self.qualify_table("aggregate_claims");
        crate::migrations::apply_once(
            &self.pool,
            &migrations_table,
            20_260_825_000_003_i64,
            "create_aggregate_claims_table",
            move |pool| async move {
                crate::migrations::create_aggregate_claims_table(pool, &claims_table).await
            },
        )
        .await
    }

    async fn stored_version_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        states_table: &str,
        stream_id: &StreamId,
    ) -> Result<AggregateVersion> {
        let query = format!(
            "SELECT version FROM {states_table} WHERE aggregate_id = $1 AND aggregate_type = $2"
        );
        let version: Option<i64> = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type().as_str())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to read stored state version", e))?;
        Ok(version.map_or_else(AggregateVersion::initial, AggregateVersion::new))
    }

    async fn upsert_state(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        states_table: &str,
        state: &StoredState,
        expected_version: AggregateVersion,
    ) -> Result<()> {
        let affected = if expected_version == AggregateVersion::initial() {
            let query = format!(
                "INSERT INTO {states_table} AS s
                     (aggregate_id, aggregate_type, state, version, is_deleted, schema_version)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE
                 SET state = EXCLUDED.state,
                     version = EXCLUDED.version,
                     is_deleted = EXCLUDED.is_deleted,
                     schema_version = EXCLUDED.schema_version,
                     updated_at = NOW()
                 WHERE s.version = $7"
            );
            sqlx::query(&query)
                .bind(state.aggregate_id)
                .bind(state.aggregate_type.as_str())
                .bind(&state.state_data)
                .bind(state.version.as_i64())
                .bind(state.is_deleted)
                .bind(i64::from(state.schema_version))
                .bind(expected_version.as_i64())
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::backend("Failed to insert aggregate state", e))?
                .rows_affected()
        } else {
            let query = format!(
                "UPDATE {states_table}
                 SET state = $3, version = $4, is_deleted = $5, schema_version = $6,
                     updated_at = NOW()
                 WHERE aggregate_id = $1 AND aggregate_type = $2 AND version = $7"
            );
            sqlx::query(&query)
                .bind(state.aggregate_id)
                .bind(state.aggregate_type.as_str())
                .bind(&state.state_data)
                .bind(state.version.as_i64())
                .bind(state.is_deleted)
                .bind(i64::from(state.schema_version))
                .bind(expected_version.as_i64())
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::backend("Failed to update aggregate state", e))?
                .rows_affected()
        };

        if affected == 0 {
            let actual = Self::stored_version_in_tx(tx, states_table, &state.stream_id()).await?;
            return Err(Error::concurrency_conflict(expected_version, actual));
        }
        Ok(())
    }

    async fn run_projections(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        events: &[EventEnvelope],
    ) -> Result<()> {
        for projection in &self.projections {
            let filter = projection.filter();
            let matching: Vec<EventEnvelope> = events
                .iter()
                .filter(|event| filter.matches(event))
                .cloned()
                .collect();
            if !matching.is_empty() {
                projection.project(&mut **tx, &matching).await?;
            }
        }
        Ok(())
    }

    async fn write_commit_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        commit: StateCommit,
    ) -> Result<()> {
        let StateCommit {
            state,
            expected_version,
            events,
            claims,
            clear_claims,
        } = commit;
        let states_table = self.qualify_table("aggregate_states");
        let stream_id = state.stream_id();

        Self::upsert_state(tx, &states_table, &state, expected_version).await?;

        crate::PostgresEventStore::handle_claims(
            tx,
            &self.qualify_table("aggregate_claims"),
            &stream_id,
            claims,
            clear_claims,
        )
        .await?;

        self.run_projections(tx, &events).await?;

        if self.outbox_enabled {
            let outbox = self.outbox();
            for event in &events {
                outbox.enqueue_tx(tx, event).await?;
            }
        }

        Ok(())
    }
}

impl PostgresStateStoreBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pool: None,
            schema: None,
            projections: Vec::new(),
            outbox_enabled: false,
        }
    }

    /// Sets the database connection pool.
    ///
    /// This is required — calling `build()` without a pool returns an error.
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Sets the schema name for state store tables.
    ///
    /// Defaults to "`event_sauce`" to isolate event-sauce migrations from your
    /// application's migration system.
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Registers an in-transaction projection.
    ///
    /// The handler runs inside every save transaction with the commit's
    /// events matching its [`filter`](StateProjection::filter), on the
    /// transaction's connection (`Ctx = sqlx::PgConnection`). A projection
    /// error rolls the whole save back. Projections must stay DB-local —
    /// effects that outlive the transaction belong in the outbox.
    #[must_use]
    pub fn with_projection(mut self, projection: Arc<dyn StateProjection<PgConnection>>) -> Self {
        self.projections.push(projection);
        self
    }

    /// Enables the transactional outbox.
    ///
    /// Every saved commit's events are inserted into the `state_outbox`
    /// table inside the save transaction, to be delivered by a
    /// [`StateOutboxDispatcher`](crate::StateOutboxDispatcher). Disabled by
    /// default.
    #[must_use]
    pub fn with_outbox(mut self) -> Self {
        self.outbox_enabled = true;
        self
    }

    /// Builds the [`PostgresStateStore`] with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool has not been set via [`pool()`](Self::pool).
    pub fn build(self) -> Result<PostgresStateStore> {
        let pool = self
            .pool
            .ok_or_else(|| Error::invalid_state("pool is required"))?;
        Ok(PostgresStateStore {
            pool,
            schema: self.schema.unwrap_or_else(|| "event_sauce".to_string()),
            projections: self.projections,
            outbox_enabled: self.outbox_enabled,
        })
    }
}

impl Default for PostgresStateStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(sqlx::FromRow)]
struct StateRow {
    aggregate_id: uuid::Uuid,
    aggregate_type: String,
    state: serde_json::Value,
    version: i64,
    is_deleted: bool,
    schema_version: i32,
}

impl From<StateRow> for StoredState {
    fn from(row: StateRow) -> Self {
        StoredState {
            aggregate_id: row.aggregate_id,
            aggregate_type: event_sauce_core::AggregateType::from_owned(row.aggregate_type),
            state_data: row.state,
            version: AggregateVersion::new(row.version),
            is_deleted: row.is_deleted,
            schema_version: u32::try_from(row.schema_version).unwrap_or(0),
        }
    }
}

#[async_trait]
impl StateStore for PostgresStateStore {
    async fn load(&self, stream_id: StreamId) -> Result<Option<StoredState>> {
        let states_table = self.qualify_table("aggregate_states");
        let query = format!(
            "SELECT aggregate_id, aggregate_type, state, version, is_deleted, schema_version
             FROM {states_table}
             WHERE aggregate_id = $1 AND aggregate_type = $2"
        );
        let row: Option<StateRow> = sqlx::query_as(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type().as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to load aggregate state", e))?;
        Ok(row.map(Into::into))
    }

    async fn save(&self, commit: StateCommit) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::backend("Failed to start transaction", e))?;

        self.write_commit_in_tx(&mut tx, commit).await?;

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit transaction", e))?;
        Ok(())
    }

    async fn save_batch(&self, commits: Vec<StateCommit>) -> Result<()> {
        if commits.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::backend("Failed to start transaction", e))?;

        for commit in commits {
            self.write_commit_in_tx(&mut tx, commit).await?;
        }

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit transaction", e))?;
        Ok(())
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        let states_table = self.qualify_table("aggregate_states");
        let query = format!(
            "SELECT version FROM {states_table} WHERE aggregate_id = $1 AND aggregate_type = $2"
        );
        let version: Option<i64> = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type().as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to get state version", e))?;
        Ok(version.map_or_else(AggregateVersion::initial, AggregateVersion::new))
    }

    async fn exists(&self, stream_id: StreamId) -> Result<bool> {
        let states_table = self.qualify_table("aggregate_states");
        let query = format!(
            "SELECT EXISTS(SELECT 1 FROM {states_table} WHERE aggregate_id = $1 AND aggregate_type = $2)"
        );
        let exists: bool = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type().as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to check state existence", e))?;
        Ok(exists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StateOutboxHandler;
    use event_sauce_core::{
        command_handler, define_events, Aggregate, AggregateClaim, AggregateError, AggregateType,
        Entity, EntityId, EventFilter, Repository,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    struct TestDb {
        pool: PgPool,
        #[allow(dead_code)]
        container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDb {
        async fn new() -> Self {
            let container = crate::test_support::start_postgres()
                .await
                .expect("start postgres");
            let host = container.get_host().await.expect("get host");
            let port = container.get_host_port_ipv4(5432).await.expect("get port");
            let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
            let pool = PgPool::connect(&url).await.expect("connect");
            Self { pool, container }
        }

        fn store_builder(&self) -> PostgresStateStoreBuilder {
            PostgresStateStore::builder()
                .pool(self.pool.clone())
                .schema("event_sauce")
        }

        async fn store(&self) -> Arc<PostgresStateStore> {
            self.migrated(self.store_builder()).await
        }

        async fn migrated(&self, builder: PostgresStateStoreBuilder) -> Arc<PostgresStateStore> {
            let store = builder.build().expect("pool was set");
            store.migrate().await.expect("migrate");
            Arc::new(store)
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum TicketError {}

    impl AggregateError for TicketError {}

    #[derive(Debug, Serialize, Deserialize)]
    struct Ticket {
        id: EntityId,
        title: String,
        votes: i64,
        closed: bool,
    }

    impl Entity for Ticket {
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl Aggregate for Ticket {
        type Event = TicketEvent;
        type Error = TicketError;
        type DeletedState = Self;

        fn claims(&self) -> Vec<AggregateClaim> {
            vec![AggregateClaim::new("Ticket.title", json!(self.title))]
        }
    }

    define_events! {
        enum TicketEvent for Ticket {
            Opened {
                title: String,
            }
            @init
            => |id, event| {
                Ticket {
                    id,
                    title: event.title.clone(),
                    votes: 0,
                    closed: false,
                }
            },

            Voted {
                amount: i64,
            } => |ticket, event| {
                ticket.votes += event.amount;
            },

            Closed {
                reason: String,
            }
            @delete
            => |mut ticket, _event| {
                ticket.closed = true;
                ticket
            },
        }
    }

    command_handler! {
        impl Ticket {
            @init fn open(title: String) -> OpenedEvent { title };
            fn vote(amount: i64) -> VotedEvent { amount };
            @delete fn close(reason: String) -> ClosedEvent { reason };
        }
    }

    fn manual_commit(aggregate_id: Uuid, version: i64, expected: i64) -> StateCommit {
        StateCommit {
            state: StoredState {
                aggregate_id,
                aggregate_type: AggregateType::from_owned("Manual".to_string()),
                state_data: json!({"n": version}),
                version: AggregateVersion::new(version),
                is_deleted: false,
                schema_version: 0,
            },
            expected_version: AggregateVersion::new(expected),
            events: vec![],
            claims: vec![],
            clear_claims: false,
        }
    }

    struct VoteTotals;

    #[async_trait]
    impl StateProjection<PgConnection> for VoteTotals {
        fn name(&self) -> &'static str {
            "vote_totals"
        }

        fn filter(&self) -> EventFilter {
            EventFilter::by_event_type("Ticket.Voted")
        }

        async fn project(&self, ctx: &mut PgConnection, events: &[EventEnvelope]) -> Result<()> {
            for event in events {
                let amount = event
                    .event_data
                    .get("amount")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                sqlx::query(
                    "INSERT INTO vote_totals (aggregate_id, votes) VALUES ($1, $2)
                     ON CONFLICT (aggregate_id) DO UPDATE SET votes = vote_totals.votes + $2",
                )
                .bind(event.aggregate_id)
                .bind(amount)
                .execute(&mut *ctx)
                .await
                .map_err(|e| Error::backend("Failed to project vote totals", e))?;
            }
            Ok(())
        }
    }

    async fn create_vote_totals_table(pool: &PgPool) {
        sqlx::query(
            "CREATE TABLE vote_totals (aggregate_id UUID PRIMARY KEY, votes BIGINT NOT NULL)",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    struct FailingProjection;

    #[async_trait]
    impl StateProjection<PgConnection> for FailingProjection {
        fn name(&self) -> &'static str {
            "failing"
        }

        fn filter(&self) -> EventFilter {
            EventFilter::by_event_type("Ticket.Voted")
        }

        async fn project(&self, _ctx: &mut PgConnection, _events: &[EventEnvelope]) -> Result<()> {
            Err(Error::custom("projection boom"))
        }
    }

    struct Recorder {
        name: &'static str,
        filter: EventFilter,
        seen: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl StateProjection<PgConnection> for Recorder {
        fn name(&self) -> &'static str {
            self.name
        }

        fn filter(&self) -> EventFilter {
            self.filter.clone()
        }

        async fn project(&self, _ctx: &mut PgConnection, events: &[EventEnvelope]) -> Result<()> {
            self.seen
                .lock()
                .unwrap()
                .extend(events.iter().map(|e| e.event_type.clone()));
            Ok(())
        }
    }

    struct CollectingHandler {
        seen: Mutex<Vec<EventEnvelope>>,
    }

    #[async_trait]
    impl StateOutboxHandler for CollectingHandler {
        async fn handle(&self, envelope: &EventEnvelope) -> Result<()> {
            self.seen.lock().unwrap().push(envelope.clone());
            Ok(())
        }
    }

    struct AlwaysFailingHandler;

    #[async_trait]
    impl StateOutboxHandler for AlwaysFailingHandler {
        async fn handle(&self, _envelope: &EventEnvelope) -> Result<()> {
            Err(Error::custom("handler boom"))
        }
    }

    struct FailOnceHandler {
        failed: AtomicBool,
    }

    #[async_trait]
    impl StateOutboxHandler for FailOnceHandler {
        async fn handle(&self, _envelope: &EventEnvelope) -> Result<()> {
            if self.failed.swap(true, Ordering::SeqCst) {
                Ok(())
            } else {
                Err(Error::custom("transient boom"))
            }
        }
    }

    async fn outbox_row_count(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM event_sauce.state_outbox")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[test]
    fn test_builder_requires_pool() {
        let err = PostgresStateStore::builder().build().unwrap_err();
        assert!(matches!(err, Error::InvalidState(_)));
    }

    #[tokio::test]
    async fn test_builder_defaults() {
        let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
        let store = PostgresStateStore::new(pool);
        assert_eq!(store.schema(), "event_sauce");
        assert_eq!(store.outbox().schema(), "event_sauce");
        assert!(!store.pool().is_closed());
    }

    #[tokio::test]
    async fn test_migrate_creates_tables_idempotently() {
        let db = TestDb::new().await;
        let store = db.store().await;
        store.migrate().await.unwrap();

        for table in ["aggregate_states", "state_outbox", "aggregate_claims"] {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT FROM information_schema.tables
                 WHERE table_schema = 'event_sauce' AND table_name = $1)",
            )
            .bind(table)
            .fetch_one(&db.pool)
            .await
            .unwrap();
            assert!(exists, "{table} should exist");
        }

        let missing = StreamId::new("Ticket", Uuid::new_v4());
        assert!(store.load(missing.clone()).await.unwrap().is_none());
        assert!(!store.exists(missing.clone()).await.unwrap());
        assert_eq!(
            store.get_version(missing).await.unwrap(),
            AggregateVersion::initial()
        );
    }

    async fn ticket_lifecycle<R: Repository<Ticket>>(repo: &R) {
        let mut ticket = Ticket::open("Broken build".to_string()).unwrap();
        let id = ticket.entity_id();
        repo.save(&mut ticket).await.unwrap();

        assert!(repo.exists(id).await.unwrap());
        assert_eq!(repo.get_version(id).await.unwrap().as_i64(), 1);

        let loaded = repo.load(id).await.unwrap();
        assert_eq!(loaded.title, "Broken build");
        assert_eq!(loaded.votes, 0);

        let votes = repo
            .modify(id, |ticket| {
                ticket.vote(3)?;
                Ok(ticket.votes)
            })
            .await
            .unwrap();
        assert_eq!(votes, 3);
        assert_eq!(repo.get_version(id).await.unwrap().as_i64(), 2);
        assert_eq!(repo.load(id).await.unwrap().votes, 3);

        let loaded = repo.load(id).await.unwrap();
        let mut deleted = loaded.close("resolved".to_string()).unwrap();
        repo.save_deleted(&mut deleted).await.unwrap();

        assert!(repo.load(id).await.unwrap_err().is_aggregate_deleted());
        assert!(repo.load_any(id).await.unwrap().is_deleted());
        let tombstone = repo.load_deleted(id).await.unwrap();
        assert_eq!(tombstone.state().title, "Broken build");
        assert!(tombstone.state().closed);
        assert!(repo.exists(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_repository_parity_lifecycle() {
        let db = TestDb::new().await;
        let store = db.store().await;
        let repo = store.repository::<Ticket>();
        ticket_lifecycle(&repo).await;
    }

    #[tokio::test]
    async fn test_stale_save_is_concurrency_conflict() {
        let db = TestDb::new().await;
        let store = db.store().await;
        let repo = store.repository::<Ticket>();

        let mut ticket = Ticket::open("Race".to_string()).unwrap();
        let id = ticket.entity_id();
        repo.save(&mut ticket).await.unwrap();

        let mut first = repo.load(id).await.unwrap();
        let mut second = repo.load(id).await.unwrap();

        first.vote(1).unwrap();
        repo.save(&mut first).await.unwrap();

        second.vote(2).unwrap();
        let err = repo.save(&mut second).await.unwrap_err();
        assert!(err.is_concurrency_conflict());
        assert_eq!(repo.load(id).await.unwrap().votes, 1);
    }

    #[tokio::test]
    async fn test_claims_conflict_and_clear_on_delete() {
        let db = TestDb::new().await;
        let store = db.store().await;
        let repo = store.repository::<Ticket>();

        let mut first = Ticket::open("duplicate".to_string()).unwrap();
        let first_id = first.entity_id();
        repo.save(&mut first).await.unwrap();

        let mut second = Ticket::open("duplicate".to_string()).unwrap();
        let second_id = second.entity_id();
        let err = repo.save(&mut second).await.unwrap_err();
        assert!(err.is_claim_conflict());
        assert!(
            !repo.exists(second_id).await.unwrap(),
            "a claim conflict must roll back the state row"
        );

        let loaded = repo.load(first_id).await.unwrap();
        let mut deleted = loaded.close("making room".to_string()).unwrap();
        repo.save_deleted(&mut deleted).await.unwrap();

        repo.save(&mut second).await.unwrap();
        assert_eq!(repo.load(second_id).await.unwrap().title, "duplicate");
    }

    #[tokio::test]
    async fn test_projection_commits_with_save() {
        let db = TestDb::new().await;
        create_vote_totals_table(&db.pool).await;
        let store = db
            .migrated(db.store_builder().with_projection(Arc::new(VoteTotals)))
            .await;
        let repo = store.repository::<Ticket>();

        let mut ticket = Ticket::open("Projected".to_string()).unwrap();
        ticket.vote(2).unwrap();
        ticket.vote(5).unwrap();
        repo.save(&mut ticket).await.unwrap();

        let votes: i64 =
            sqlx::query_scalar("SELECT votes FROM vote_totals WHERE aggregate_id = $1")
                .bind(ticket.entity_id().as_uuid())
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(votes, 7);

        ticket.vote(1).unwrap();
        repo.save(&mut ticket).await.unwrap();
        let votes: i64 =
            sqlx::query_scalar("SELECT votes FROM vote_totals WHERE aggregate_id = $1")
                .bind(ticket.entity_id().as_uuid())
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(votes, 8, "each commit's events project exactly once");
    }

    #[tokio::test]
    async fn test_projection_error_rolls_back_state_row() {
        let db = TestDb::new().await;
        let store = db
            .migrated(
                db.store_builder()
                    .with_projection(Arc::new(FailingProjection)),
            )
            .await;
        let repo = store.repository::<Ticket>();

        let mut ticket = Ticket::open("Fragile".to_string()).unwrap();
        let id = ticket.entity_id();
        repo.save(&mut ticket).await.unwrap();

        let mut loaded = repo.load(id).await.unwrap();
        loaded.vote(9).unwrap();
        let err = repo.save(&mut loaded).await.unwrap_err();
        assert_eq!(err.to_string(), "projection boom");

        assert_eq!(repo.get_version(id).await.unwrap().as_i64(), 1);
        assert_eq!(
            repo.load(id).await.unwrap().votes,
            0,
            "the failed save must leave the state row unchanged"
        );
    }

    #[tokio::test]
    async fn test_event_filter_routes_events_to_projections() {
        let db = TestDb::new().await;
        let voted_seen = Arc::new(Mutex::new(Vec::new()));
        let all_seen = Arc::new(Mutex::new(Vec::new()));
        let other_seen = Arc::new(Mutex::new(Vec::new()));
        let store = db
            .migrated(
                db.store_builder()
                    .with_projection(Arc::new(Recorder {
                        name: "voted-only",
                        filter: EventFilter::by_event_type("Ticket.Voted"),
                        seen: Arc::clone(&voted_seen),
                    }))
                    .with_projection(Arc::new(Recorder {
                        name: "all",
                        filter: EventFilter::all(),
                        seen: Arc::clone(&all_seen),
                    }))
                    .with_projection(Arc::new(Recorder {
                        name: "other",
                        filter: EventFilter::by_event_type("Ticket.Nope"),
                        seen: Arc::clone(&other_seen),
                    })),
            )
            .await;
        let repo = store.repository::<Ticket>();

        let mut ticket = Ticket::open("Routed".to_string()).unwrap();
        ticket.vote(1).unwrap();
        repo.save(&mut ticket).await.unwrap();

        assert_eq!(*voted_seen.lock().unwrap(), vec!["Ticket.Voted"]);
        assert_eq!(
            *all_seen.lock().unwrap(),
            vec!["Ticket.Opened", "Ticket.Voted"]
        );
        assert!(
            other_seen.lock().unwrap().is_empty(),
            "a projection with no matching events must not be invoked"
        );
    }

    #[tokio::test]
    async fn test_save_batch_is_atomic() {
        let db = TestDb::new().await;
        let store = db.store().await;

        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        store
            .save_batch(vec![
                manual_commit(first, 1, 0),
                manual_commit(second, 1, 0),
            ])
            .await
            .unwrap();
        assert_eq!(
            store
                .get_version(StreamId::new("Manual", second))
                .await
                .unwrap()
                .as_i64(),
            1
        );

        let third = Uuid::new_v4();
        let err = store
            .save_batch(vec![
                manual_commit(third, 1, 0),
                manual_commit(second, 2, 5),
            ])
            .await
            .unwrap_err();
        assert!(err.is_concurrency_conflict());

        assert!(
            store
                .load(StreamId::new("Manual", third))
                .await
                .unwrap()
                .is_none(),
            "the conflicting second commit must roll back the first"
        );
        assert_eq!(
            store
                .get_version(StreamId::new("Manual", second))
                .await
                .unwrap()
                .as_i64(),
            1
        );
    }

    #[tokio::test]
    async fn test_outbox_enqueues_transactionally() {
        let db = TestDb::new().await;
        let store = db.migrated(db.store_builder().with_outbox()).await;
        let repo = store.repository::<Ticket>();
        let outbox = store.outbox();

        let mut ticket = Ticket::open("Outboxed".to_string()).unwrap();
        ticket.vote(4).unwrap();
        repo.save(&mut ticket).await.unwrap();
        assert_eq!(outbox.pending_count().await.unwrap(), 2);

        let id = ticket.entity_id();
        let mut stale = repo.load(id).await.unwrap();
        let mut winner = repo.load(id).await.unwrap();
        winner.vote(1).unwrap();
        repo.save(&mut winner).await.unwrap();
        stale.vote(2).unwrap();
        repo.save(&mut stale).await.unwrap_err();

        assert_eq!(
            outbox.pending_count().await.unwrap(),
            3,
            "a failed save must enqueue nothing"
        );
    }

    #[tokio::test]
    async fn test_dispatcher_delivers_and_prunes() {
        let db = TestDb::new().await;
        let store = db.migrated(db.store_builder().with_outbox()).await;
        let repo = store.repository::<Ticket>();

        let mut ticket = Ticket::open("Dispatched".to_string()).unwrap();
        ticket.vote(6).unwrap();
        repo.save(&mut ticket).await.unwrap();

        let handler = Arc::new(CollectingHandler {
            seen: Mutex::new(Vec::new()),
        });
        let dispatcher = store
            .outbox()
            .dispatcher(Arc::clone(&handler) as Arc<dyn StateOutboxHandler>)
            .with_worker_id("test-worker")
            .with_batch_size(10)
            .with_lock_duration(Duration::from_secs(60));

        assert_eq!(dispatcher.run_once().await.unwrap(), 2);
        assert_eq!(dispatcher.run_once().await.unwrap(), 0);

        let types: Vec<String> = {
            let seen = handler.seen.lock().unwrap();
            assert_eq!(seen[0].aggregate_id, ticket.entity_id().as_uuid());
            seen.iter().map(|e| e.event_type.clone()).collect()
        };
        assert_eq!(types, vec!["Ticket.Opened", "Ticket.Voted"]);

        assert_eq!(
            outbox_row_count(&db.pool).await,
            0,
            "dispatched rows must be pruned, not kept"
        );
    }

    #[tokio::test]
    async fn test_failing_handler_retries_then_parks_in_dlq() {
        let db = TestDb::new().await;
        let store = db.migrated(db.store_builder().with_outbox()).await;
        let repo = store.repository::<Ticket>();

        let mut ticket = Ticket::open("Doomed".to_string()).unwrap();
        repo.save(&mut ticket).await.unwrap();

        let dispatcher = store
            .outbox()
            .dispatcher(Arc::new(AlwaysFailingHandler))
            .with_max_failures(Some(2));

        assert_eq!(dispatcher.run_once().await.unwrap(), 0);
        let (status, failures): (String, i32) =
            sqlx::query_as("SELECT status, failures FROM event_sauce.state_outbox")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(status, "pending", "first failure must return to pending");
        assert_eq!(failures, 1);

        assert_eq!(dispatcher.run_once().await.unwrap(), 0);
        let (status, failures): (String, i32) =
            sqlx::query_as("SELECT status, failures FROM event_sauce.state_outbox")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(status, "failed", "second failure must park the row");
        assert_eq!(failures, 2);

        assert_eq!(
            dispatcher.run_once().await.unwrap(),
            0,
            "a parked row must not be claimed again"
        );
        assert_eq!(store.outbox().pending_count().await.unwrap(), 0);
        assert_eq!(outbox_row_count(&db.pool).await, 1, "DLQ rows are kept");
    }

    #[tokio::test]
    async fn test_transient_handler_failure_retries_to_success() {
        let db = TestDb::new().await;
        let store = db.migrated(db.store_builder().with_outbox()).await;
        let repo = store.repository::<Ticket>();

        let mut ticket = Ticket::open("Flaky".to_string()).unwrap();
        repo.save(&mut ticket).await.unwrap();

        let dispatcher = store
            .outbox()
            .dispatcher(Arc::new(FailOnceHandler {
                failed: AtomicBool::new(false),
            }))
            .with_max_failures(Some(3));

        assert_eq!(dispatcher.run_once().await.unwrap(), 0);
        assert_eq!(dispatcher.run_once().await.unwrap(), 1);
        assert_eq!(outbox_row_count(&db.pool).await, 0);
    }
}
