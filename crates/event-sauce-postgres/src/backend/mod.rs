//! Unified `PostgreSQL` backend setup.
//!
//! Provides a single entry point for setting up all `PostgreSQL` stores
//! (event store + checkpoint store) with migrations.

use std::sync::Arc;
use std::time::Duration;

use event_sauce_core::{CheckpointStore, Error, Position, Result, SnapshotConfig};
use sqlx::PgPool;

use crate::{PostgresCheckpointStore, PostgresEventStore};

mod policy_dispatch;
mod projection_runner;

pub use policy_dispatch::PolicyDispatch;

/// A fully configured `PostgreSQL` backend with event store and checkpoint store.
///
/// `PostgresBackend` simplifies setup by creating and migrating all required stores
/// from a database URL and optional configuration. This replaces the manual 6-step
/// process of creating pools, stores, running migrations, and wiring `Arc` references.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresBackend;
///
/// // One-liner setup with defaults
/// let backend = PostgresBackend::setup("postgresql://localhost/events", "event_sauce").await?;
///
/// // Access components
/// let event_store = backend.event_store();
/// let checkpoint_store = backend.checkpoint_store();
/// let pool = backend.pool();
/// ```
pub struct PostgresBackend {
    pool: PgPool,
    event_store: Arc<PostgresEventStore>,
    checkpoint_store: Arc<PostgresCheckpointStore>,
}

impl PostgresBackend {
    /// Sets up a fully configured `PostgreSQL` backend with the given database URL and schema.
    ///
    /// This is a convenience method that connects to the database, creates both stores
    /// in the specified schema, runs all migrations, and wires the checkpoint store
    /// into the event store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresBackend;
    ///
    /// let backend = PostgresBackend::setup("postgresql://localhost/events", "event_sauce").await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the database connection fails or migrations cannot be applied.
    pub async fn setup(database_url: &str, schema: &str) -> Result<Self> {
        Self::builder()
            .database_url(database_url)
            .schema(schema)
            .build()
            .await
    }

    /// Creates a builder for configuring the backend.
    ///
    /// Use the builder for custom snapshot configuration or when you want
    /// to set options incrementally.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresBackend;
    /// use event_sauce_core::SnapshotConfig;
    ///
    /// let backend = PostgresBackend::builder()
    ///     .database_url("postgresql://localhost/events")
    ///     .schema("my_schema")
    ///     .snapshot_config(SnapshotConfig::disabled())
    ///     .build()
    ///     .await?;
    /// ```
    #[must_use]
    pub fn builder() -> PostgresBackendBuilder {
        PostgresBackendBuilder::new()
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a shared reference to the event store.
    #[must_use]
    pub fn event_store(&self) -> Arc<PostgresEventStore> {
        Arc::clone(&self.event_store)
    }

    /// Returns a shared reference to the checkpoint store.
    #[must_use]
    pub fn checkpoint_store(&self) -> Arc<PostgresCheckpointStore> {
        Arc::clone(&self.checkpoint_store)
    }

    /// Creates a [`PostgresEventLogQuery`](crate::PostgresEventLogQuery) for this backend.
    ///
    /// The returned query object uses the same connection pool and schema as the event store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::EventLogQuery;
    ///
    /// let log_query = backend.event_log_query();
    /// let page = log_query.query_events(Default::default()).await?;
    /// ```
    #[must_use]
    pub fn event_log_query(&self) -> crate::PostgresEventLogQuery {
        self.event_store.event_log_query()
    }

    /// Starts a transaction on the pool, mapping a failure to
    /// `Failed to start {what} transaction`.
    async fn begin(&self, what: &str) -> Result<sqlx::Transaction<'_, sqlx::Postgres>> {
        self.pool
            .begin()
            .await
            .map_err(|e| Error::backend(format!("Failed to start {what} transaction"), e))
    }

    /// Applies every `(name, position)` fenced checkpoint write inside `tx`;
    /// if all land, commits and returns `Ok(())`. If any is rejected by the
    /// fence — the caller no longer holds that lease, or the position does
    /// not strictly advance — rolls back the whole transaction and returns
    /// [`Error::LeaseLost`](Error::LeaseLost) for that
    /// name, so a partially-applied batch never lands.
    ///
    /// Shared by every fenced commit site: the per-event and tail advances
    /// of [`run_under_lease`](Self::run_under_lease), and every held
    /// policy's advance in
    /// [`dispatch_under_lease`](Self::dispatch_under_lease).
    async fn commit_fenced(
        &self,
        mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
        worker_id: &str,
        checkpoints: &[(&str, Position)],
    ) -> Result<()> {
        for (name, position) in checkpoints {
            let landed = self
                .checkpoint_store
                .save_checkpoint_fenced_tx(&mut tx, name, worker_id, *position)
                .await?;
            if !landed {
                tx.rollback()
                    .await
                    .map_err(|e| Error::backend("Failed to roll back transaction", e))?;
                return Err(Error::lease_lost(*name, worker_id));
            }
        }

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit transaction", e))
    }
}

/// Maximum number of events fetched per batch by the projection runners and
/// the policy dispatcher.
///
/// Bounds memory and ensures the streaming connection is released between
/// batches, so the connection pool isn't held captive while a projection or
/// a dispatch call catches up. The lease is renewed inside each batch loop,
/// so this also bounds how long a projection runner goes between pool
/// connections, and, for the dispatcher, the longest stretch between lease
/// renewals.
const PROJECTION_BATCH_SIZE: i64 = 500;

/// Tracks when a held lease is next due for renewal.
///
/// Every long-running holder — the drain loop under a fenced lease, and the
/// policy dispatcher's own leases — renews on the same cadence, a third of
/// the lease's duration, so this is the only place that divides by 3.
struct LeaseRenewal {
    interval: Duration,
    last: std::time::Instant,
}

impl LeaseRenewal {
    /// The renewal cadence for a lease held for `lease_duration`.
    fn interval_for(lease_duration: Duration) -> Duration {
        lease_duration / 3
    }

    fn new(lease_duration: Duration) -> Self {
        Self {
            interval: Self::interval_for(lease_duration),
            last: std::time::Instant::now(),
        }
    }

    /// Whether at least one renewal interval has passed since the last
    /// renewal (or since this was created, if none yet).
    fn due(&self) -> bool {
        self.last.elapsed() >= self.interval
    }

    /// Resets the clock after a successful renewal.
    fn renewed(&mut self) {
        self.last = std::time::Instant::now();
    }
}

/// Builder for configuring a [`PostgresBackend`].
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresBackend;
///
/// let backend = PostgresBackend::builder()
///     .database_url("postgresql://localhost/events")
///     .build()
///     .await?;
/// ```
pub struct PostgresBackendBuilder {
    database_url: Option<String>,
    pool: Option<PgPool>,
    schema: Option<String>,
    snapshot_config: Option<SnapshotConfig>,
    crypto_key_store: Option<Arc<dyn event_sauce_core::CryptoKeyStore>>,
    crypto_provider: Option<Arc<dyn event_sauce_core::CryptoProvider>>,
}

impl PostgresBackendBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            database_url: None,
            pool: None,
            schema: None,
            snapshot_config: None,
            crypto_key_store: None,
            crypto_provider: None,
        }
    }

    /// Sets the database connection URL.
    ///
    /// Either this or [`pool`](Self::pool) must be set before calling `build()`.
    /// When `database_url` is set, `build()` opens a default `PgPool::connect`
    /// to the URL. Use [`pool`](Self::pool) instead to share a pre-configured
    /// pool with the rest of the application (e.g. with a custom
    /// `max_connections`).
    #[must_use]
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
        self
    }

    /// Sets a pre-built `PgPool` to use for all stores.
    ///
    /// Use this when the application already owns a connection pool (e.g.
    /// configured with `PgPoolOptions::max_connections(...)`) so the event
    /// store, checkpoint store, and the rest of the app share a single pool.
    ///
    /// Either this or [`database_url`](Self::database_url) must be set; if both
    /// are provided, `pool` wins.
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Sets the schema name for all stores.
    ///
    /// Defaults to `"event_sauce"`.
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Sets the snapshot configuration for the event store.
    ///
    /// Defaults to `SnapshotConfig::builder().build()` (`EveryNEvents(100)` with
    /// snapshots-on-load enabled).
    #[must_use]
    pub fn snapshot_config(mut self, config: SnapshotConfig) -> Self {
        self.snapshot_config = Some(config);
        self
    }

    /// Sets the crypto key store for encrypted aggregate encryption.
    #[must_use]
    pub fn crypto_key_store(mut self, store: Arc<dyn event_sauce_core::CryptoKeyStore>) -> Self {
        self.crypto_key_store = Some(store);
        self
    }

    /// Sets the crypto provider for encrypted aggregate encryption.
    #[must_use]
    pub fn crypto_provider(mut self, provider: Arc<dyn event_sauce_core::CryptoProvider>) -> Self {
        self.crypto_provider = Some(provider);
        self
    }

    /// Builds the `PostgresBackend` by connecting to the database and running migrations.
    ///
    /// # Errors
    ///
    /// Returns an error if neither `database_url` nor `pool` has been set, or
    /// if the database connection fails or migrations cannot be applied.
    pub async fn build(self) -> Result<PostgresBackend> {
        let schema = self.schema.unwrap_or_else(|| "event_sauce".to_string());
        let snapshot_config = self
            .snapshot_config
            .unwrap_or_else(|| SnapshotConfig::builder().build());

        let pool = match (self.pool, self.database_url) {
            (Some(pool), _) => pool,
            (None, Some(url)) => PgPool::connect(&url)
                .await
                .map_err(|e| Error::backend("Failed to connect", e))?,
            (None, None) => {
                return Err(Error::invalid_state(
                    "either database_url or pool is required",
                ));
            }
        };

        let checkpoint_store = PostgresCheckpointStore::builder()
            .pool(pool.clone())
            .schema(&schema)
            .build()?;
        checkpoint_store.migrate().await?;

        let checkpoint_store = Arc::new(checkpoint_store);

        let mut event_store_builder = PostgresEventStore::builder()
            .pool(pool.clone())
            .schema(&schema)
            .snapshot_config(snapshot_config)
            .checkpoint_store(Arc::clone(&checkpoint_store) as Arc<dyn CheckpointStore>);

        if let Some(key_store) = self.crypto_key_store {
            event_store_builder = event_store_builder.crypto_key_store(key_store);
        }
        if let Some(provider) = self.crypto_provider {
            event_store_builder = event_store_builder.crypto_provider(provider);
        }

        let event_store = event_store_builder.build()?;
        event_store.migrate().await?;

        Ok(PostgresBackend {
            pool,
            event_store: Arc::new(event_store),
            checkpoint_store,
        })
    }
}

impl Default for PostgresBackendBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{AggregateVersion, CheckpointStore, EventStore, StreamId};
    use futures::StreamExt;
    use uuid::Uuid;

    pub(crate) mod fixtures {
        use crate::PostgresEventStore;
        use event_sauce_core::{
            AggregateVersion, EventEnvelope, EventStore, EventVersion, StreamId,
        };
        use serde_json::json;
        use testcontainers_modules::postgres::Postgres;
        use uuid::Uuid;

        /// Starts a `PostgreSQL` testcontainer and returns a connection URL.
        pub(crate) async fn start_test_db() -> (
            String,
            testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
        ) {
            let (container, url) = crate::test_support::start_postgres_url().await;
            (url, container)
        }

        pub(crate) fn create_test_envelope(aggregate_id: Uuid) -> EventEnvelope {
            EventEnvelope::new(
                Uuid::new_v4(),
                aggregate_id,
                "TestAggregate".to_string(),
                "TestEvent".to_string(),
                EventVersion::new(1),
                json!({"data": "test"}),
            )
        }

        /// Appends one event of `event_type` on a fresh aggregate of
        /// `aggregate_type` to its own stream.
        pub(crate) async fn append_typed(
            store: &PostgresEventStore,
            aggregate_type: String,
            event_type: String,
        ) {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new(aggregate_type.clone(), aggregate_id);
            let envelope = EventEnvelope::new(
                Uuid::new_v4(),
                aggregate_id,
                aggregate_type,
                event_type,
                EventVersion::new(1),
                json!({}),
            );
            store
                .append(
                    stream_id,
                    vec![envelope],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }
    }
    use fixtures::{create_test_envelope, start_test_db};

    #[tokio::test]
    async fn test_setup_creates_working_backend() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        // Verify stores are functional by checking schema
        assert_eq!(backend.event_store().schema(), "event_sauce");
    }

    #[tokio::test]
    async fn test_builder_with_defaults() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::builder()
            .database_url(&url)
            .build()
            .await
            .expect("builder with defaults should succeed");

        assert_eq!(backend.event_store().schema(), "event_sauce");
    }

    #[tokio::test]
    async fn test_builder_with_custom_schema() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::builder()
            .database_url(&url)
            .schema("custom_schema")
            .build()
            .await
            .expect("builder with custom schema should succeed");

        assert_eq!(backend.event_store().schema(), "custom_schema");
    }

    #[tokio::test]
    async fn test_builder_with_snapshot_config() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::builder()
            .database_url(&url)
            .snapshot_config(SnapshotConfig::disabled())
            .build()
            .await
            .expect("builder with snapshot config should succeed");

        assert!(!backend
            .event_store()
            .snapshot_config()
            .use_snapshots_on_load());
    }

    #[tokio::test]
    async fn test_event_store_has_checkpoint_store() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        assert!(backend.event_store().checkpoint_store().is_some());
    }

    #[tokio::test]
    async fn test_migrations_run_automatically() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "test_migrations")
            .await
            .expect("setup should succeed");

        // Verify schema and tables exist by querying them
        let result: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1",
        )
        .bind("test_migrations")
        .fetch_one(backend.pool())
        .await
        .expect("should be able to query information_schema");

        // events, snapshots, checkpoints, + 2 migration tracking tables
        assert!(
            result.0 >= 3,
            "expected at least 3 tables, got {}",
            result.0
        );
    }

    #[tokio::test]
    async fn test_event_store_is_functional() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);
        let event = create_test_envelope(aggregate_id);

        let store = backend.event_store();

        store
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .expect("append should succeed");

        let mut stream = store
            .load_stream(stream_id, AggregateVersion::initial())
            .await
            .expect("load_stream should succeed");

        let loaded = stream.next().await;
        assert!(loaded.is_some(), "should have loaded one event");
        let envelope = loaded.unwrap().expect("event should be Ok");
        assert_eq!(envelope.event_type, "TestEvent");
    }

    #[tokio::test]
    async fn test_checkpoint_store_is_functional() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        backend
            .checkpoint_store()
            .save_checkpoint("test-sub", Position::new(42))
            .await
            .expect("save_checkpoint should succeed");

        let loaded = backend
            .checkpoint_store()
            .load_checkpoint("test-sub")
            .await
            .expect("load_checkpoint should succeed");

        assert_eq!(loaded, Some(Position::new(42)));
    }

    #[tokio::test]
    async fn test_pool_is_accessible() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        let result: (i32,) = sqlx::query_as("SELECT 1")
            .fetch_one(backend.pool())
            .await
            .expect("pool should be functional");

        assert_eq!(result.0, 1);
    }

    #[tokio::test]
    async fn test_builder_errors_without_url_or_pool() {
        let result = PostgresBackend::builder().build().await;
        let err = result.err().expect("should be an error");
        let msg = err.to_string();
        assert!(
            msg.contains("either database_url or pool is required"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn test_builder_with_pool() {
        let (url, _container) = start_test_db().await;

        let pool = PgPool::connect(&url)
            .await
            .expect("pool connect should succeed");

        let backend = PostgresBackend::builder()
            .pool(pool.clone())
            .schema("pool_backend")
            .build()
            .await
            .expect("builder with pre-built pool should succeed");

        assert_eq!(backend.event_store().schema(), "pool_backend");

        // Both backend and the original pool handle should reference the
        // same underlying pool — confirm by querying through both.
        let count: (i64,) = sqlx::query_as("SELECT 1::bigint")
            .fetch_one(backend.pool())
            .await
            .expect("backend pool should be functional");
        assert_eq!(count.0, 1);
        let count: (i64,) = sqlx::query_as("SELECT 2::bigint")
            .fetch_one(&pool)
            .await
            .expect("original pool should still work");
        assert_eq!(count.0, 2);
    }

    /// `commit_fenced` applies every checkpoint in one transaction together
    /// with whatever else that transaction did — here, an enqueue. One of
    /// the two checkpoints belongs to another worker: the whole transaction
    /// must roll back, so neither checkpoint moves and the enqueue never
    /// lands, not just the checkpoint whose fence was rejected.
    #[tokio::test]
    async fn test_commit_fenced_rolls_back_the_whole_transaction_on_one_rejection() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        backend
            .checkpoint_store()
            .try_acquire_lease("cp-held", "worker-a", Duration::from_secs(30))
            .await
            .unwrap();
        backend
            .checkpoint_store()
            .try_acquire_lease("cp-foreign", "worker-b", Duration::from_secs(30))
            .await
            .unwrap();

        let mut tx = backend.pool().begin().await.unwrap();
        outbox
            .enqueue_tx(&mut tx, "policy-a", Uuid::new_v4(), 1)
            .await
            .unwrap();

        let err = backend
            .commit_fenced(
                tx,
                "worker-a",
                &[
                    ("cp-held", Position::new(1)),
                    ("cp-foreign", Position::new(1)),
                ],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::LeaseLost { .. }),
            "the name worker-a does not hold must be reported as LeaseLost: {err:?}"
        );

        assert_eq!(
            outbox.pending_count("policy-a").await.unwrap(),
            0,
            "the enqueue sharing the rolled-back transaction must not land"
        );
        assert_eq!(
            backend
                .checkpoint_store()
                .load_checkpoint("cp-held")
                .await
                .unwrap(),
            Some(Position::start()),
            "even the name whose fence would have passed must not move, \
             since it shared the rolled-back transaction"
        );
        assert_eq!(
            backend
                .checkpoint_store()
                .load_checkpoint("cp-foreign")
                .await
                .unwrap(),
            Some(Position::start()),
            "the foreign checkpoint must not move either"
        );
    }
}
