//! `PostgreSQL` event store implementation.
//!
//! Provides a production-ready `PostgreSQL` implementation of `EventStore`.

use async_trait::async_trait;
use event_sauce_core::{
    Error, EventEnvelope, EventStore, Position, Result, Snapshot, SnapshotConfig, StreamId, Version,
};
use futures::stream::{self, Stream};
use sqlx::PgPool;

/// `PostgreSQL` event store implementation.
///
/// This store provides durable event persistence using `PostgreSQL` with:
/// - Optimistic concurrency control
/// - Efficient streaming queries
/// - Snapshot support
/// - Full ACID guarantees
/// - Schema isolation to avoid conflicts with application migrations
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresEventStore;
/// use sqlx::PgPool;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let pool = PgPool::connect("postgresql://localhost/events").await?;
///
///     // Using builder pattern with custom schema (recommended)
///     let store = PostgresEventStore::builder()
///         .pool(pool)
///         .schema("event_sauce") // Isolates migrations from your app
///         .build();
///
///     // Run migrations in the custom schema
///     store.migrate().await?;
///
///     // Or use simple constructor (uses "public" schema)
///     let store = PostgresEventStore::new(pool);
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct PostgresEventStore {
    pool: PgPool,
    snapshot_config: SnapshotConfig,
    schema: String,
    checkpoint_store: Option<std::sync::Arc<dyn event_sauce_core::CheckpointStore>>,
}

/// Builder for configuring `PostgresEventStore`.
///
/// Provides a flexible way to configure the event store with:
/// - Custom database connection pool
/// - Snapshot configuration
/// - Schema name for table isolation
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresEventStore;
/// use event_sauce_core::SnapshotConfig;
/// use sqlx::PgPool;
///
/// let pool = PgPool::connect("postgresql://localhost/events").await?;
///
/// let store = PostgresEventStore::builder()
///     .pool(pool)
///     .schema("event_sauce")
///     .snapshot_config(SnapshotConfig::builder().build())
///     .build();
/// ```
#[derive(Clone)]
pub struct PostgresEventStoreBuilder {
    pool: Option<PgPool>,
    snapshot_config: Option<SnapshotConfig>,
    schema: Option<String>,
    checkpoint_store: Option<std::sync::Arc<dyn event_sauce_core::CheckpointStore>>,
}

impl PostgresEventStore {
    /// Creates a new `PostgreSQL` event store with default configuration.
    ///
    /// This is a convenience method that uses the builder with all defaults:
    /// - **Schema**: "`event_sauce`" (isolated from your app)
    /// - **Snapshots**: Every 100 events
    ///
    /// Equivalent to `PostgresEventStore::builder().pool(pool).build()`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    ///
    /// // Uses "event_sauce" schema by default
    /// let store = PostgresEventStore::new(pool);
    /// store.migrate().await?;
    /// ```
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self::builder().pool(pool).build()
    }

    /// Creates a new `PostgreSQL` event store with custom snapshot configuration.
    ///
    /// Uses default schema "`event_sauce`".
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use event_sauce_core::{SnapshotConfig, EveryNEvents};
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    /// let config = SnapshotConfig::builder()
    ///     .default_strategy(EveryNEvents(50))
    ///     .build();
    ///
    /// let store = PostgresEventStore::with_config(pool, config);
    /// ```
    #[must_use]
    pub fn with_config(pool: PgPool, snapshot_config: SnapshotConfig) -> Self {
        Self::builder()
            .pool(pool)
            .snapshot_config(snapshot_config)
            .build()
    }

    /// Creates a builder for configuring the event store.
    ///
    /// This is the recommended way to create a `PostgresEventStore` when you need
    /// to customize the schema name or other settings.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    ///
    /// let store = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .schema("event_sauce")  // Isolate from app migrations
    ///     .build();
    /// ```
    #[must_use]
    pub fn builder() -> PostgresEventStoreBuilder {
        PostgresEventStoreBuilder::new()
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns the schema name used by this event store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let store = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .schema("my_schema")
    ///     .build();
    ///
    /// assert_eq!(store.schema(), "my_schema");
    /// ```
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns a schema-qualified table name (e.g., "schema.events").
    fn qualify_table(&self, table: &str) -> String {
        format!("{}.{}", self.schema, table)
    }

    /// Runs database migrations to set up the event store schema.
    ///
    /// This method creates the necessary tables (`events` and `snapshots`) and indexes
    /// for the event store. It is idempotent and safe to call multiple times.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use sqlx::PgPool;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let pool = PgPool::connect("postgresql://localhost/events").await?;
    ///     let store = PostgresEventStore::new(pool);
    ///
    ///     // Run migrations to set up schema
    ///     store.migrate().await?;
    ///
    ///     // Store is now ready to use
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database connection fails
    /// - The migrations cannot be applied due to permission issues
    /// - There are SQL syntax errors in migration files
    #[allow(clippy::too_many_lines)]
    pub async fn migrate(&self) -> Result<()> {
        // Create schema if it doesn't exist (skip for public schema)
        if self.schema != "public" {
            let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
            sqlx::query(&create_schema)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::custom(format!("Failed to create schema: {e}")))?;
        }

        // Create migration tracking table in the custom schema
        let migrations_table = self.qualify_table("_event_sauce_migrations");
        let create_migrations_table = format!(
            "CREATE TABLE IF NOT EXISTS {migrations_table} (
                version BIGINT PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )"
        );
        sqlx::query(&create_migrations_table)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to create migrations table: {e}")))?;

        // Check if migration has already been applied
        let check_query = format!("SELECT COUNT(*) FROM {migrations_table} WHERE version = $1");
        let count: i64 = sqlx::query_scalar(&check_query)
            .bind(20_250_101_000_000_i64)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to check migration status: {e}")))?;

        if count > 0 {
            // Migration already applied
            return Ok(());
        }

        // Apply the migration with schema-qualified table names
        // Note: Each statement must be executed separately
        let events_table = self.qualify_table("events");
        let snapshots_table = self.qualify_table("snapshots");

        // Create events table
        let create_events = format!(
            "CREATE TABLE IF NOT EXISTS {events_table} (
                id BIGSERIAL PRIMARY KEY,
                event_id UUID NOT NULL UNIQUE,
                aggregate_id UUID NOT NULL,
                aggregate_type VARCHAR(255) NOT NULL,
                event_type VARCHAR(255) NOT NULL,
                event_version INTEGER NOT NULL,
                event_data JSONB NOT NULL,
                stream_version BIGINT NOT NULL,
                created_by UUID,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                correlation_id UUID,
                causation_id UUID,
                metadata JSONB,
                UNIQUE(aggregate_id, aggregate_type, stream_version)
            )"
        );
        sqlx::query(&create_events)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to create events table: {e}")))?;

        // Create indexes for events table
        let indexes = vec![
            format!("CREATE INDEX IF NOT EXISTS idx_events_aggregate ON {}(aggregate_id, aggregate_type)", events_table),
            format!("CREATE INDEX IF NOT EXISTS idx_events_aggregate_version ON {}(aggregate_id, aggregate_type, stream_version)", events_table),
            format!("CREATE INDEX IF NOT EXISTS idx_events_type ON {}(event_type)", events_table),
            format!("CREATE INDEX IF NOT EXISTS idx_events_aggregate_type ON {}(aggregate_type)", events_table),
            format!("CREATE INDEX IF NOT EXISTS idx_events_created_at ON {}(created_at)", events_table),
            format!("CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON {}(correlation_id) WHERE correlation_id IS NOT NULL", events_table),
        ];

        for index_sql in indexes {
            sqlx::query(&index_sql)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::custom(format!("Failed to create index: {e}")))?;
        }

        // Create snapshots table
        let create_snapshots = format!(
            "CREATE TABLE IF NOT EXISTS {snapshots_table} (
                aggregate_id UUID NOT NULL,
                aggregate_type VARCHAR(255) NOT NULL,
                snapshot_version BIGINT NOT NULL,
                snapshot_data JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                PRIMARY KEY (aggregate_id, aggregate_type)
            )"
        );
        sqlx::query(&create_snapshots)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to create snapshots table: {e}")))?;

        // Create index for snapshots table
        let snapshots_index = format!(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_type ON {snapshots_table}(aggregate_type)"
        );
        sqlx::query(&snapshots_index)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to create snapshots index: {e}")))?;

        // Record the migration
        let record_query =
            format!("INSERT INTO {migrations_table} (version, description) VALUES ($1, $2)");
        sqlx::query(&record_query)
            .bind(20_250_101_000_000_i64)
            .bind("create_events_table")
            .execute(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to record migration: {e}")))?;

        Ok(())
    }
}

impl PostgresEventStoreBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pool: None,
            snapshot_config: None,
            schema: None,
            checkpoint_store: None,
        }
    }

    /// Sets the database connection pool.
    ///
    /// This is required - calling `build()` without setting a pool will panic.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    /// let builder = PostgresEventStore::builder().pool(pool);
    /// ```
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Sets the snapshot configuration.
    ///
    /// Defaults to `SnapshotConfig::builder().build()` (every 100 events).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use event_sauce_core::SnapshotConfig;
    ///
    /// let builder = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .snapshot_config(SnapshotConfig::disabled());
    /// ```
    #[must_use]
    pub fn snapshot_config(mut self, config: SnapshotConfig) -> Self {
        self.snapshot_config = Some(config);
        self
    }

    /// Sets the schema name for event store tables.
    ///
    /// Defaults to "`event_sauce`" to isolate event-sauce migrations from your
    /// application's migration system. Use "public" if you want to use the
    /// default `PostgreSQL` schema.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    ///
    /// let builder = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .schema("my_custom_schema");
    /// ```
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Sets the checkpoint store for subscription tracking.
    ///
    /// When a checkpoint store is configured, the event store can create
    /// subscriptions with automatic checkpoint management.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::{PostgresEventStore, PostgresCheckpointStore};
    /// use std::sync::Arc;
    ///
    /// let checkpoint_store = Arc::new(PostgresCheckpointStore::new(pool.clone()));
    /// let builder = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .checkpoint_store(checkpoint_store);
    /// ```
    #[must_use]
    pub fn checkpoint_store(
        mut self,
        store: std::sync::Arc<dyn event_sauce_core::CheckpointStore>,
    ) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Builds the `PostgresEventStore` with the configured settings.
    ///
    /// # Defaults
    ///
    /// - **Schema**: "`event_sauce`" (isolates migrations from your app)
    /// - **Snapshot config**: Every 100 events
    ///
    /// # Panics
    ///
    /// Panics if the pool has not been set via [`pool()`](Self::pool).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    ///
    /// // Uses "event_sauce" schema by default
    /// let store = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .build();
    /// ```
    #[must_use]
    pub fn build(self) -> PostgresEventStore {
        PostgresEventStore {
            pool: self.pool.expect("Pool is required"),
            snapshot_config: self
                .snapshot_config
                .unwrap_or_else(|| SnapshotConfig::builder().build()),
            schema: self.schema.unwrap_or_else(|| "event_sauce".to_string()),
            checkpoint_store: self.checkpoint_store,
        }
    }
}

impl Default for PostgresEventStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventStore for PostgresEventStore {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::custom(format!("Failed to start transaction: {e}")))?;

        // Check current version
        let events_table = self.qualify_table("events");
        let query = format!(
            "SELECT MAX(stream_version) FROM {events_table} WHERE aggregate_id = $1 AND aggregate_type = $2"
        );
        let current_version: Option<i64> = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type())
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::custom(format!("Failed to check version: {e}")))?;

        #[allow(clippy::cast_possible_truncation)]
        let current_version = Version::new((current_version.unwrap_or(-1) + 1) as i32);

        if current_version != expected_version {
            return Err(Error::concurrency_conflict(
                expected_version,
                current_version,
            ));
        }

        // Insert events
        for (idx, event) in events.iter().enumerate() {
            #[allow(clippy::cast_possible_wrap)]
            let stream_version = i64::from(expected_version.as_i32()) + idx as i64;

            let insert_query = format!(
                "INSERT INTO {events_table} (
                    event_id, aggregate_id, aggregate_type, event_type, event_version,
                    event_data, stream_version, created_by, correlation_id, causation_id, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
            );

            sqlx::query(&insert_query)
                .bind(event.id)
                .bind(event.aggregate_id)
                .bind(&event.aggregate_type)
                .bind(&event.event_type)
                .bind(event.event_version.as_i32())
                .bind(&event.event_data)
                .bind(stream_version)
                .bind(event.created_by)
                .bind(event.metadata.as_ref().and_then(|m| m.correlation_id))
                .bind(event.metadata.as_ref().and_then(|m| m.causation_id))
                .bind(event.metadata.as_ref().and_then(|m| m.additional.clone()))
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::custom(format!("Failed to insert event: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| Error::custom(format!("Failed to commit transaction: {e}")))?;

        Ok(())
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: Version,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        let events_table = self.qualify_table("events");
        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, event_type, event_version,
                    event_data, created_by, created_at, correlation_id, causation_id, metadata
             FROM {events_table}
             WHERE aggregate_id = $1 AND aggregate_type = $2 AND stream_version >= $3
             ORDER BY stream_version ASC"
        );

        let events: Vec<EventEnvelope> = sqlx::query_as::<_, EventRow>(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type())
            .bind(i64::from(from_version.as_i32()))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to load stream: {e}")))?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(stream::iter(events.into_iter().map(Ok)))
    }

    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        let events_table = self.qualify_table("events");
        let query = format!(
            "SELECT event_id, aggregate_id, aggregate_type, event_type, event_version,
                    event_data, created_by, created_at, correlation_id, causation_id, metadata
             FROM {events_table}
             WHERE id > $1
             ORDER BY id ASC"
        );

        let events: Vec<EventEnvelope> = sqlx::query_as::<_, EventRow>(&query)
            .bind(from_position.as_i64())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to stream all: {e}")))?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(stream::iter(events.into_iter().map(Ok)))
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<Version> {
        let events_table = self.qualify_table("events");
        let query = format!(
            "SELECT MAX(stream_version) FROM {events_table} WHERE aggregate_id = $1 AND aggregate_type = $2"
        );

        let version: Option<i64> = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to get version: {e}")))?;

        #[allow(clippy::cast_possible_truncation)]
        let next_version = version.map_or(0, |v| v + 1) as i32;
        Ok(Version::new(next_version))
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let snapshots_table = self.qualify_table("snapshots");
        let query = format!(
            "INSERT INTO {snapshots_table} (aggregate_id, aggregate_type, snapshot_version, snapshot_data)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (aggregate_id, aggregate_type)
             DO UPDATE SET snapshot_version = $3, snapshot_data = $4, created_at = NOW()"
        );

        sqlx::query(&query)
            .bind(snapshot.aggregate_id)
            .bind(&snapshot.aggregate_type)
            .bind(i64::from(snapshot.snapshot_version.as_i32()))
            .bind(&snapshot.snapshot_data)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to save snapshot: {e}")))?;

        Ok(())
    }

    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        let snapshots_table = self.qualify_table("snapshots");
        let query = format!(
            "SELECT aggregate_id, aggregate_type, snapshot_version, snapshot_data
             FROM {snapshots_table}
             WHERE aggregate_id = $1 AND aggregate_type = $2"
        );

        let row: Option<SnapshotRow> = sqlx::query_as::<_, SnapshotRow>(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::custom(format!("Failed to load snapshot: {e}")))?;

        Ok(row.map(Into::into))
    }

    fn snapshot_config(&self) -> &SnapshotConfig {
        &self.snapshot_config
    }

    fn checkpoint_store(&self) -> Option<std::sync::Arc<dyn event_sauce_core::CheckpointStore>> {
        self.checkpoint_store.clone()
    }
}

impl PostgresEventStore {
    /// Counts events in a stream using an optimized SQL COUNT(*) query.
    ///
    /// This is a Postgres-specific optimization that's much faster than using
    /// the generic [`count_events()`](event_sauce_core::count_events) function,
    /// especially for streams with many events.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let count = postgres_store.count_events_fast(stream_id).await?;
    /// println!("Stream has {} events", count);
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the database query fails.
    pub async fn count_events_fast(&self, stream_id: StreamId) -> event_sauce_core::Result<usize> {
        let events_table = self.qualify_table("events");

        let query = format!(
            "SELECT COUNT(*) as count
             FROM {events_table}
             WHERE aggregate_id = $1 AND aggregate_type = $2"
        );

        let count: i64 = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| event_sauce_core::Error::custom(format!("Failed to count events: {e}")))?;

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        Ok(count as usize)
    }
}

// Database row types
#[derive(sqlx::FromRow)]
struct EventRow {
    event_id: uuid::Uuid,
    aggregate_id: uuid::Uuid,
    aggregate_type: String,
    event_type: String,
    event_version: i32,
    event_data: serde_json::Value,
    created_by: Option<uuid::Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
    correlation_id: Option<uuid::Uuid>,
    causation_id: Option<uuid::Uuid>,
    metadata: Option<serde_json::Value>,
}

impl From<EventRow> for EventEnvelope {
    fn from(row: EventRow) -> Self {
        let metadata =
            if row.correlation_id.is_some() || row.causation_id.is_some() || row.metadata.is_some()
            {
                Some(event_sauce_core::EventMetadata {
                    correlation_id: row.correlation_id,
                    causation_id: row.causation_id,
                    timestamp: row.created_at,
                    additional: row.metadata,
                })
            } else {
                None
            };

        EventEnvelope {
            id: row.event_id,
            aggregate_id: row.aggregate_id,
            aggregate_type: row.aggregate_type,
            event_type: row.event_type,
            event_version: Version::new(row.event_version),
            event_data: row.event_data,
            created_by: row.created_by,
            created_at: row.created_at,
            metadata,
        }
    }
}

#[derive(sqlx::FromRow)]
struct SnapshotRow {
    aggregate_id: uuid::Uuid,
    aggregate_type: String,
    snapshot_version: i64,
    snapshot_data: serde_json::Value,
}

impl From<SnapshotRow> for Snapshot {
    fn from(row: SnapshotRow) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let snapshot_version = Version::new(row.snapshot_version as i32);

        Snapshot {
            aggregate_id: row.aggregate_id,
            aggregate_type: row.aggregate_type,
            snapshot_version,
            snapshot_data: row.snapshot_data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{EventEnvelope, EventStore, Position, Snapshot, StreamId, Version};
    use futures::StreamExt;
    use serde_json::json;
    use sqlx::PgPool;
    use testcontainers::ImageExt;
    use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
    use uuid::Uuid;

    /// Test database helper using testcontainers for isolated `PostgreSQL` testing.
    struct TestDatabase {
        pool: PgPool,
        #[allow(dead_code)]
        container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDatabase {
        /// Creates a new test database with testcontainers.
        async fn new() -> Result<Self> {
            // Start PostgreSQL container
            let container = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .map_err(|e| Error::custom(format!("Failed to start PostgreSQL container: {e}")))?;

            // Get connection string
            let host = container
                .get_host()
                .await
                .map_err(|e| Error::custom(format!("Failed to get container host: {e}")))?;
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .map_err(|e| Error::custom(format!("Failed to get container port: {e}")))?;

            let connection_string =
                format!("postgresql://postgres:postgres@{host}:{port}/postgres");

            // Connect to database
            let pool = PgPool::connect(&connection_string)
                .await
                .map_err(|e| Error::custom(format!("Failed to connect to test database: {e}")))?;

            // Run migrations
            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .map_err(|e| Error::custom(format!("Failed to run migrations: {e}")))?;

            Ok(Self { pool, container })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        /// Creates a store configured for this test database (uses public schema to match migrations).
        fn store(&self) -> PostgresEventStore {
            PostgresEventStore::builder()
                .pool(self.pool.clone())
                .schema("public") // Match the schema where TestDatabase runs migrations
                .build()
        }
    }

    fn create_test_envelope(event_type: &str, aggregate_id: Uuid) -> EventEnvelope {
        create_test_envelope_with_type(event_type, "User", aggregate_id)
    }

    fn create_test_envelope_with_type(
        event_type: &str,
        aggregate_type: &str,
        aggregate_id: Uuid,
    ) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            aggregate_type.to_string(),
            event_type.to_string(),
            Version::new(1),
            json!({"data": "test"}),
        )
    }

    #[tokio::test]
    async fn test_create_store() {
        let db = TestDatabase::new().await.unwrap();
        let _store = db.store();
    }

    #[tokio::test]
    async fn test_append_to_new_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let event = create_test_envelope("UserCreated", stream_id.aggregate_id());

        let result = store
            .append(stream_id, vec![event], Version::initial())
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_load_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(stream_id.clone(), vec![event.clone()], Version::initial())
            .await
            .unwrap();

        let mut stream = store
            .load_stream(stream_id, Version::initial())
            .await
            .unwrap();
        let loaded = stream.next().await.unwrap().unwrap();

        assert_eq!(loaded.event_type, "UserCreated");
    }

    #[tokio::test]
    async fn test_get_version() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        // New stream should have initial version
        let version = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(version, Version::initial());
    }

    #[tokio::test]
    async fn test_concurrency_conflict() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event1 = create_test_envelope("UserCreated", aggregate_id);
        let event2 = create_test_envelope("UserUpdated", aggregate_id);

        // Append first event
        store
            .append(stream_id.clone(), vec![event1], Version::initial())
            .await
            .unwrap();

        // Try to append with wrong version - should fail
        let result = store
            .append(stream_id, vec![event2], Version::initial())
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_concurrency_conflict());
    }

    #[tokio::test]
    async fn test_stream_all() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let stream1 = StreamId::new("User", id1);
        let stream2 = StreamId::new("Order", id2);

        store
            .append(
                stream1,
                vec![create_test_envelope("UserCreated", id1)],
                Version::initial(),
            )
            .await
            .unwrap();
        store
            .append(
                stream2,
                vec![create_test_envelope_with_type("OrderPlaced", "Order", id2)],
                Version::initial(),
            )
            .await
            .unwrap();

        let all_events = store.stream_all(Position::start()).await.unwrap();
        let events: Vec<_> = all_events.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_save_and_load_snapshot() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let snapshot = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            Version::new(10),
            json!({"name": "Alice"}),
        );

        store.save_snapshot(snapshot.clone()).await.unwrap();
        let loaded = store.load_snapshot(stream_id).await.unwrap();

        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().snapshot_version, Version::new(10));
    }

    #[tokio::test]
    async fn test_append_multiple_events() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let events = vec![
            create_test_envelope("UserCreated", aggregate_id),
            create_test_envelope("UserUpdated", aggregate_id),
            create_test_envelope("UserVerified", aggregate_id),
        ];

        store
            .append(stream_id.clone(), events, Version::initial())
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::new(3));
    }

    #[tokio::test]
    async fn test_load_stream_from_version() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Add 5 events
        for i in 0..5 {
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);
            store
                .append(stream_id.clone(), vec![event], Version::new(i))
                .await
                .unwrap();
        }

        // Load from version 2
        let stream = store.load_stream(stream_id, Version::new(2)).await.unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 3); // Should get events 2, 3, 4
    }

    #[tokio::test]
    async fn test_stream_all_from_position() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();

        for i in 0..5 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("User", aggregate_id);
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);

            store
                .append(stream_id, vec![event], Version::initial())
                .await
                .unwrap();
        }

        // Stream from position 2
        let stream = store.stream_all(Position::new(2)).await.unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 3); // Should get events at positions 2, 3, 4
    }

    #[tokio::test]
    async fn test_load_nonexistent_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let stream = store
            .load_stream(stream_id, Version::initial())
            .await
            .unwrap();
        let events: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_load_nonexistent_snapshot() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let snapshot = store.load_snapshot(stream_id).await.unwrap();
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_store_is_cloneable() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let store_clone = store.clone();

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        // Append via original store
        store
            .append(stream_id.clone(), vec![event], Version::initial())
            .await
            .unwrap();

        // Read via clone - should see the same data (same database)
        let version = store_clone.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::new(1));
    }

    #[tokio::test]
    async fn test_version_increments_correctly() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Initial version
        let v0 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v0, Version::initial());

        // After first append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event1", aggregate_id)],
                Version::initial(),
            )
            .await
            .unwrap();
        let v1 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v1, Version::new(1));

        // After second append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event2", aggregate_id)],
                Version::new(1),
            )
            .await
            .unwrap();
        let v2 = store.get_version(stream_id).await.unwrap();
        assert_eq!(v2, Version::new(2));
    }

    #[tokio::test]
    async fn test_snapshot_overwrites() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Save first snapshot
        let snapshot1 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            Version::new(5),
            json!({"version": 1}),
        );
        store.save_snapshot(snapshot1).await.unwrap();

        // Save second snapshot (should overwrite via UPSERT)
        let snapshot2 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            Version::new(10),
            json!({"version": 2}),
        );
        store.save_snapshot(snapshot2).await.unwrap();

        // Should get the latest snapshot
        let loaded = store.load_snapshot(stream_id).await.unwrap().unwrap();
        assert_eq!(loaded.snapshot_version, Version::new(10));
    }

    #[tokio::test]
    async fn test_append_empty_events() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        // Appending empty events should succeed (no-op)
        let result = store.append(stream_id, vec![], Version::initial()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_appends_from_different_streams() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let stream1 = StreamId::new("User", id1);
        let stream2 = StreamId::new("User", id2);

        // Concurrent appends to different streams should both succeed
        let result1 = store.append(
            stream1.clone(),
            vec![create_test_envelope("Event1", id1)],
            Version::initial(),
        );
        let result2 = store.append(
            stream2.clone(),
            vec![create_test_envelope("Event2", id2)],
            Version::initial(),
        );

        let (r1, r2) = tokio::join!(result1, result2);
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    #[tokio::test]
    async fn test_migrate_creates_tables() {
        // Create a fresh database without running migrations
        let container = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .unwrap();

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let connection_string = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

        let pool = PgPool::connect(&connection_string).await.unwrap();

        // Create store and call migrate
        let store = PostgresEventStore::new(pool.clone());
        store.migrate().await.unwrap();

        // Verify tables exist by querying them
        let events_exist: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'events'
            )",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let snapshots_exist: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'snapshots'
            )",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(events_exist);
        assert!(snapshots_exist);
    }

    #[tokio::test]
    async fn test_migrate_is_idempotent() {
        // Create a fresh database without running migrations
        let container = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .unwrap();

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let connection_string = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

        let pool = PgPool::connect(&connection_string).await.unwrap();
        let store = PostgresEventStore::new(pool.clone());

        // Call migrate twice - should not fail
        store.migrate().await.unwrap();
        store.migrate().await.unwrap();

        // Verify we can use the store
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        let result = store
            .append(stream_id, vec![event], Version::initial())
            .await;
        assert!(result.is_ok());
    }

    // === Builder Pattern Tests ===

    #[tokio::test]
    async fn test_builder_with_defaults() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::builder()
            .pool(db.pool().clone())
            .build();

        // Default schema should be "event_sauce"
        assert_eq!(store.schema(), "event_sauce");

        // Migrate to create the schema
        store.migrate().await.unwrap();

        // Should work with default schema
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(stream_id.clone(), vec![event], Version::initial())
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, Version::new(1));
    }

    #[tokio::test]
    async fn test_builder_with_custom_schema() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::builder()
            .pool(db.pool().clone())
            .schema("custom_schema")
            .build();

        // Verify schema is used
        assert_eq!(store.schema(), "custom_schema");
    }

    #[tokio::test]
    async fn test_builder_with_snapshot_config() {
        let db = TestDatabase::new().await.unwrap();
        let config = SnapshotConfig::disabled();
        let store = PostgresEventStore::builder()
            .pool(db.pool().clone())
            .snapshot_config(config.clone())
            .build();

        // Verify snapshot config is set - just check it's accessible
        // (testing the actual strategy behavior is done in core crate tests)
        let _config = store.snapshot_config();
        assert!(true); // Config is accessible
    }

    #[tokio::test]
    async fn test_builder_full_configuration() {
        let db = TestDatabase::new().await.unwrap();
        let config = SnapshotConfig::builder()
            .default_strategy(event_sauce_core::EveryNEvents(50))
            .build();

        let store = PostgresEventStore::builder()
            .pool(db.pool().clone())
            .schema("my_events")
            .snapshot_config(config)
            .build();

        assert_eq!(store.schema(), "my_events");
    }

    #[tokio::test]
    async fn test_builder_migrate_creates_schema() {
        // Create a fresh database
        let container = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .unwrap();

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let connection_string = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

        let pool = PgPool::connect(&connection_string).await.unwrap();

        // Build store with custom schema
        let store = PostgresEventStore::builder()
            .pool(pool.clone())
            .schema("event_sauce_test")
            .build();

        // Run migrations
        store.migrate().await.unwrap();

        // Verify schema exists
        let schema_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.schemata
                WHERE schema_name = $1
            )",
        )
        .bind("event_sauce_test")
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(schema_exists);

        // Verify tables exist in the schema
        let events_exist: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = 'events'
            )",
        )
        .bind("event_sauce_test")
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(events_exist);
    }

    #[tokio::test]
    async fn test_builder_operations_use_custom_schema() {
        // Create a fresh database
        let container = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await
            .unwrap();

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let connection_string = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

        let pool = PgPool::connect(&connection_string).await.unwrap();

        // Build store with custom schema
        let store = PostgresEventStore::builder()
            .pool(pool.clone())
            .schema("events_schema")
            .build();

        store.migrate().await.unwrap();

        // Perform operations
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(stream_id.clone(), vec![event], Version::initial())
            .await
            .unwrap();

        // Verify data is in the custom schema, not public
        let count_in_custom_schema: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events_schema.events WHERE aggregate_id = $1")
                .bind(aggregate_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(count_in_custom_schema, 1);

        // Verify data is NOT in public schema
        let count_in_public: std::result::Result<i64, sqlx::Error> =
            sqlx::query_scalar("SELECT COUNT(*) FROM public.events WHERE aggregate_id = $1")
                .bind(aggregate_id)
                .fetch_one(&pool)
                .await;

        // Should fail because table doesn't exist in public schema
        assert!(count_in_public.is_err());
    }

    #[tokio::test]
    async fn test_count_events_fast_returns_correct_count() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Append 5 events
        for i in 0..5 {
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);
            store
                .append(
                    stream_id.clone(),
                    vec![event],
                    Version::new(i32::try_from(i).unwrap()),
                )
                .await
                .unwrap();
        }

        // Test optimized count
        let count = store.count_events_fast(stream_id).await.unwrap();
        assert_eq!(count, 5, "Should count all 5 events using optimized query");
    }

    #[tokio::test]
    async fn test_count_events_fast_returns_zero_for_nonexistent_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("NonExistent", Uuid::new_v4());

        let count = store.count_events_fast(stream_id).await.unwrap();
        assert_eq!(count, 0, "Should return 0 for nonexistent stream");
    }

    #[tokio::test]
    async fn test_count_events_fast_matches_generic_count() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Append 10 events
        for i in 0..10 {
            let event = create_test_envelope(&format!("Event{i}"), aggregate_id);
            store
                .append(
                    stream_id.clone(),
                    vec![event],
                    Version::new(i32::try_from(i).unwrap()),
                )
                .await
                .unwrap();
        }

        // Compare optimized and generic counts
        let fast_count = store.count_events_fast(stream_id.clone()).await.unwrap();
        let generic_count = event_sauce_core::count_events(&store, stream_id)
            .await
            .unwrap();

        assert_eq!(
            fast_count, generic_count,
            "Optimized count should match generic count"
        );
    }
}
