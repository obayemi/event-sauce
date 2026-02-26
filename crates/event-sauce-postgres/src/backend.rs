//! Unified `PostgreSQL` backend setup.
//!
//! Provides a single entry point for setting up all `PostgreSQL` stores
//! (event store + checkpoint store) with migrations.

use std::sync::Arc;

use event_sauce_core::SnapshotConfig;
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
}

impl PostgresBackendBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            database_url: None,
            schema: None,
            snapshot_config: None,
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

    /// Builds the `PostgresBackend` by connecting to the database and running migrations.
    ///
    /// # Panics
    ///
    /// Panics if `database_url` has not been set.
    ///
    /// # Errors
    ///
    /// Returns an error if the database connection fails or migrations cannot be applied.
    pub async fn build(self) -> event_sauce_core::Result<PostgresBackend> {
        let database_url = self.database_url.expect("database_url is required");
        let schema = self.schema.unwrap_or_else(|| "event_sauce".to_string());
        let snapshot_config = self
            .snapshot_config
            .unwrap_or_else(|| SnapshotConfig::builder().build());

        let pool = PgPool::connect(&database_url)
            .await
            .map_err(|e| event_sauce_core::Error::custom(format!("Failed to connect: {e}")))?;

        let checkpoint_store = PostgresCheckpointStore::builder()
            .pool(pool.clone())
            .schema(&schema)
            .build();
        checkpoint_store.migrate().await?;

        let checkpoint_store = Arc::new(checkpoint_store);

        let event_store = PostgresEventStore::builder()
            .pool(pool.clone())
            .schema(&schema)
            .snapshot_config(snapshot_config)
            .checkpoint_store(
                Arc::clone(&checkpoint_store) as Arc<dyn event_sauce_core::CheckpointStore>
            )
            .build();
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
            .append(stream_id.clone(), vec![event], AggregateVersion::initial())
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
    #[should_panic(expected = "database_url is required")]
    async fn test_builder_panics_without_url() {
        let _ = PostgresBackend::builder().build().await;
    }
}
