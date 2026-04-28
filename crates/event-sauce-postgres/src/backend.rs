//! Unified `PostgreSQL` backend setup.
//!
//! Provides a single entry point for setting up all `PostgreSQL` stores
//! (event store + checkpoint store) with migrations.

use std::sync::Arc;

use event_sauce_core::{EventStore, SnapshotConfig};
use sqlx::PgPool;

use crate::{PostgresCheckpointStore, PostgresEventStore};

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
    pub async fn setup(database_url: &str, schema: &str) -> event_sauce_core::Result<Self> {
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

    /// Creates a subscription builder with the event store and checkpoint store pre-wired.
    ///
    /// This is a convenience method that avoids extracting the event store and checkpoint
    /// store separately.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let subscription = backend
    ///     .subscription_builder("my-projection")
    ///     .filter(EventFilter::by_aggregate_type("User"))
    ///     .build()?;
    /// ```
    pub fn subscription_builder(
        &self,
        name: impl Into<String>,
    ) -> event_sauce_core::SubscriptionBuilder<PostgresEventStore> {
        self.event_store.subscription_builder(name)
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

    /// Runs a [`PostgresProjection`](crate::PostgresProjection) atomically.
    ///
    /// For every matched event the runner opens a transaction, calls
    /// [`PostgresProjection::handle`](crate::PostgresProjection::handle) with
    /// it, advances the subscription checkpoint inside the same transaction,
    /// and commits. If any step fails the transaction is dropped (rolled back)
    /// and the error propagates — the checkpoint never advances past an event
    /// whose materialization didn't commit, so a re-run picks up exactly where
    /// the failure occurred.
    ///
    /// # Multi-instance safety
    ///
    /// This method does **not** acquire a lease. Running it from two
    /// processes (or two tasks) at the same time will race on the checkpoint
    /// and double-apply events. Use
    /// [`run_leased_projection`](Self::run_leased_projection) when more than
    /// one instance might run the same projection.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut projection = OrderTotalsProjection;
    /// backend.run_postgres_projection(&mut projection).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint loading fails, the event stream errors,
    /// the projection's `handle` returns an error, or the per-event
    /// transaction cannot be started or committed.
    pub async fn run_postgres_projection<P: crate::PostgresProjection>(
        &self,
        projection: &mut P,
    ) -> event_sauce_core::Result<()> {
        use event_sauce_core::{CheckpointStore, EventStore, Position};
        use futures::StreamExt;

        let start_position = self
            .checkpoint_store
            .load_checkpoint(P::NAME)
            .await?
            .unwrap_or_else(Position::start);

        let event_stream = self.event_store.stream_all(start_position).await?;
        futures::pin_mut!(event_stream);

        let filter = P::event_filter();
        let mut current_position = start_position;

        while let Some(event_result) = event_stream.next().await {
            let event = event_result?;
            current_position = Position::new(current_position.as_i64() + 1);

            if !filter.matches(&event) {
                continue;
            }

            let mut tx = self.pool.begin().await.map_err(|e| {
                event_sauce_core::Error::backend("Failed to start projection transaction", e)
            })?;

            projection.handle(&event, &mut tx).await?;
            self.checkpoint_store
                .save_checkpoint_tx(&mut tx, P::NAME, current_position)
                .await?;

            tx.commit().await.map_err(|e| {
                event_sauce_core::Error::backend("Failed to commit projection transaction", e)
            })?;
        }

        Ok(())
    }

    /// Runs a [`PostgresProjection`](crate::PostgresProjection) under a lease.
    ///
    /// Acquires the lease for `P::NAME` on behalf of `worker_id`, drains all
    /// currently-available events (atomically per event, identically to
    /// [`run_postgres_projection`](Self::run_postgres_projection)), then
    /// releases the lease and returns. If another worker holds an active
    /// lease, this is a no-op that returns
    /// [`LeaseOutcome::Busy`](crate::LeaseOutcome::Busy) — the caller can
    /// retry later.
    ///
    /// While processing, the lease is renewed roughly every
    /// `lease_duration / 3` to keep ownership. If renewal fails — typically
    /// because the lease expired and was taken by another worker — the run
    /// stops with an error rather than risk concurrent processing.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let worker_id = format!("{}-{}", hostname()?, std::process::id());
    /// let outcome = backend
    ///     .run_leased_projection(&mut OrderTotalsProjection, &worker_id, Duration::from_secs(30))
    ///     .await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint operations fail, the event stream
    /// errors, the projection's `handle` returns an error, the per-event
    /// transaction cannot be started or committed, or the lease is lost
    /// during the run.
    pub async fn run_leased_projection<P: crate::PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: std::time::Duration,
    ) -> event_sauce_core::Result<crate::LeaseOutcome> {
        use event_sauce_core::CheckpointStore;

        let Some(start_position) = self
            .checkpoint_store
            .try_acquire_lease(P::NAME, worker_id, lease_duration)
            .await?
        else {
            return Ok(crate::LeaseOutcome::Busy);
        };

        let result = self
            .run_under_lease(projection, worker_id, lease_duration, start_position)
            .await;

        // Always try to release on exit, including on error. Releasing a
        // lease we no longer hold is a no-op.
        let _ = self
            .checkpoint_store
            .release_lease(P::NAME, worker_id)
            .await;

        result.map(|()| crate::LeaseOutcome::Completed)
    }

    async fn run_under_lease<P: crate::PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: std::time::Duration,
        start_position: event_sauce_core::Position,
    ) -> event_sauce_core::Result<()> {
        use event_sauce_core::{CheckpointStore, EventStore, Position};
        use futures::StreamExt;

        let event_stream = self.event_store.stream_all(start_position).await?;
        futures::pin_mut!(event_stream);

        let filter = P::event_filter();
        let mut current_position = start_position;
        let renew_interval = lease_duration / 3;
        let mut last_renew = std::time::Instant::now();

        while let Some(event_result) = event_stream.next().await {
            let event = event_result?;
            current_position = Position::new(current_position.as_i64() + 1);

            if last_renew.elapsed() >= renew_interval {
                self.checkpoint_store
                    .renew_lease(P::NAME, worker_id, lease_duration)
                    .await?;
                last_renew = std::time::Instant::now();
            }

            if !filter.matches(&event) {
                continue;
            }

            let mut tx = self.pool.begin().await.map_err(|e| {
                event_sauce_core::Error::backend("Failed to start projection transaction", e)
            })?;

            projection.handle(&event, &mut tx).await?;
            self.checkpoint_store
                .save_checkpoint_tx(&mut tx, P::NAME, current_position)
                .await?;

            tx.commit().await.map_err(|e| {
                event_sauce_core::Error::backend("Failed to commit projection transaction", e)
            })?;
        }

        Ok(())
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
            schema: None,
            snapshot_config: None,
            crypto_key_store: None,
            crypto_provider: None,
        }
    }

    /// Sets the database connection URL.
    ///
    /// This is required — calling `build()` without setting a URL will panic.
    #[must_use]
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
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
    /// Returns an error if `database_url` has not been set, or if the database
    /// connection fails or migrations cannot be applied.
    pub async fn build(self) -> event_sauce_core::Result<PostgresBackend> {
        let database_url = self
            .database_url
            .ok_or_else(|| event_sauce_core::Error::invalid_state("database_url is required"))?;
        let schema = self.schema.unwrap_or_else(|| "event_sauce".to_string());
        let snapshot_config = self
            .snapshot_config
            .unwrap_or_else(|| SnapshotConfig::builder().build());

        let pool = PgPool::connect(&database_url)
            .await
            .map_err(|e| event_sauce_core::Error::backend("Failed to connect", e))?;

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
            .checkpoint_store(
                Arc::clone(&checkpoint_store) as Arc<dyn event_sauce_core::CheckpointStore>
            );

        // Pass through user-provided crypto overrides; otherwise
        // the event store builder defaults to PostgresCryptoKeyStore + Aes256GcmProvider.
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
    use event_sauce_core::{
        AggregateVersion, CheckpointStore, EventEnvelope, EventStore, EventVersion, Position,
        StreamId,
    };
    use futures::StreamExt;
    use serde_json::json;
    use testcontainers::ImageExt;
    use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
    use uuid::Uuid;

    /// Starts a `PostgreSQL` testcontainer and returns a connection URL.
    async fn start_postgres() -> (
        String,
        testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    ) {
        let container = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .expect("Failed to start PostgreSQL container");

        let host = container.get_host().await.expect("Failed to get host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get port");

        let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
        (url, container)
    }

    fn create_test_envelope(aggregate_id: Uuid) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            "TestAggregate".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            json!({"data": "test"}),
        )
    }

    #[tokio::test]
    async fn test_setup_creates_working_backend() {
        let (url, _container) = start_postgres().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        // Verify stores are functional by checking schema
        assert_eq!(backend.event_store().schema(), "event_sauce");
    }

    #[tokio::test]
    async fn test_builder_with_defaults() {
        let (url, _container) = start_postgres().await;

        let backend = PostgresBackend::builder()
            .database_url(&url)
            .build()
            .await
            .expect("builder with defaults should succeed");

        assert_eq!(backend.event_store().schema(), "event_sauce");
    }

    #[tokio::test]
    async fn test_builder_with_custom_schema() {
        let (url, _container) = start_postgres().await;

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
        let (url, _container) = start_postgres().await;

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
        let (url, _container) = start_postgres().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        assert!(backend.event_store().checkpoint_store().is_some());
    }

    #[tokio::test]
    async fn test_migrations_run_automatically() {
        let (url, _container) = start_postgres().await;

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
        let (url, _container) = start_postgres().await;

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
        let (url, _container) = start_postgres().await;

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
        let (url, _container) = start_postgres().await;

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
    async fn test_builder_errors_without_url() {
        let result = PostgresBackend::builder().build().await;
        let err = result.err().expect("should be an error");
        let msg = err.to_string();
        assert!(
            msg.contains("database_url is required"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn test_subscription_builder_convenience() {
        let (url, _container) = start_postgres().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);
        let event = create_test_envelope(aggregate_id);

        let store = backend.event_store();
        store
            .append(
                stream_id,
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .expect("append should succeed");

        // Use the convenience method
        let mut subscription = backend
            .subscription_builder("test-sub")
            .build()
            .expect("build should succeed");

        let mut count = 0;
        subscription
            .run(|_event| {
                count += 1;
                Ok(())
            })
            .await
            .expect("run should succeed");

        assert_eq!(count, 1);
    }

    /// Counts every matched event in a postgres-backed table.
    struct CountingProjection {
        fail_after: Option<u64>,
        seen: u64,
    }

    impl CountingProjection {
        async fn migrate(pool: &PgPool, schema: &str) {
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {schema}.counting_projection (
                    id INTEGER PRIMARY KEY,
                    n BIGINT NOT NULL
                )"
            ))
            .execute(pool)
            .await
            .unwrap();
            sqlx::query(&format!(
                "INSERT INTO {schema}.counting_projection (id, n) VALUES (1, 0)
                 ON CONFLICT (id) DO NOTHING"
            ))
            .execute(pool)
            .await
            .unwrap();
        }

        async fn read(pool: &PgPool, schema: &str) -> i64 {
            sqlx::query_scalar(&format!(
                "SELECT n FROM {schema}.counting_projection WHERE id = 1"
            ))
            .fetch_one(pool)
            .await
            .unwrap()
        }
    }

    #[async_trait::async_trait]
    impl crate::PostgresProjection for CountingProjection {
        const NAME: &'static str = "CountingProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            _envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> event_sauce_core::Result<()> {
            self.seen += 1;
            if let Some(limit) = self.fail_after {
                if self.seen > limit {
                    return Err(event_sauce_core::Error::custom("forced failure"));
                }
            }
            sqlx::query("UPDATE event_sauce.counting_projection SET n = n + 1 WHERE id = 1")
                .execute(&mut **tx)
                .await
                .map_err(|e| event_sauce_core::Error::custom(format!("update failed: {e}")))?;
            Ok(())
        }
    }

    async fn append_test_event(store: &PostgresEventStore, version: AggregateVersion) -> Uuid {
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);
        let event = create_test_envelope(aggregate_id);
        store
            .append(stream_id, vec![event], version, vec![], false)
            .await
            .expect("append should succeed");
        aggregate_id
    }

    #[tokio::test]
    async fn test_run_postgres_projection_atomic_success() {
        let (url, _container) = start_postgres().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_postgres_projection(&mut projection)
            .await
            .unwrap();

        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            3
        );

        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(<CountingProjection as crate::PostgresProjection>::NAME)
            .await
            .unwrap();
        assert_eq!(checkpoint, Some(Position::new(3)));
    }

    #[tokio::test]
    async fn test_run_postgres_projection_rolls_back_on_failure() {
        let (url, _container) = start_postgres().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = CountingProjection {
            fail_after: Some(2),
            seen: 0,
        };
        let err = backend.run_postgres_projection(&mut projection).await;
        assert!(err.is_err(), "expected forced failure");

        // First two events committed; the third was rolled back.
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            2
        );

        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(<CountingProjection as crate::PostgresProjection>::NAME)
            .await
            .unwrap();
        assert_eq!(checkpoint, Some(Position::new(2)));
    }

    #[tokio::test]
    async fn test_run_postgres_projection_resumes_from_checkpoint() {
        let (url, _container) = start_postgres().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..2 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_postgres_projection(&mut projection)
            .await
            .unwrap();
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            2
        );

        // Append two more events and re-run; only the new ones should be applied.
        for _ in 0..2 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }
        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_postgres_projection(&mut projection)
            .await
            .unwrap();
        assert_eq!(projection.seen, 2);
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            4
        );
    }

    #[tokio::test]
    async fn test_run_leased_projection_completes_under_lease() {
        let (url, _container) = start_postgres().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(
                &mut projection,
                "worker-1",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, crate::LeaseOutcome::Completed);
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            3
        );
    }

    #[tokio::test]
    async fn test_run_leased_projection_busy_when_lease_held() {
        let (url, _container) = start_postgres().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        // Worker A grabs the lease and holds it.
        backend
            .checkpoint_store()
            .try_acquire_lease(
                <CountingProjection as crate::PostgresProjection>::NAME,
                "worker-a",
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();

        // Worker B should see Busy.
        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(
                &mut projection,
                "worker-b",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, crate::LeaseOutcome::Busy);
        // No work was done.
        assert_eq!(projection.seen, 0);
    }

    #[tokio::test]
    async fn test_run_leased_projection_releases_on_exit() {
        let (url, _container) = start_postgres().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_leased_projection(
                &mut projection,
                "worker-a",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();

        // Worker B should now be able to acquire the lease (worker A released it).
        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(
                &mut projection,
                "worker-b",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, crate::LeaseOutcome::Completed);
    }

    #[tokio::test]
    async fn test_run_leased_projection_releases_on_failure() {
        let (url, _container) = start_postgres().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        // Worker A fails partway through.
        let mut projection = CountingProjection {
            fail_after: Some(2),
            seen: 0,
        };
        let result = backend
            .run_leased_projection(
                &mut projection,
                "worker-a",
                std::time::Duration::from_secs(30),
            )
            .await;
        assert!(result.is_err());

        // Lease should have been released even on failure.
        let acquired = backend
            .checkpoint_store()
            .try_acquire_lease(
                <CountingProjection as crate::PostgresProjection>::NAME,
                "worker-b",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert!(acquired.is_some(), "lease should be released after failure");
    }
}
