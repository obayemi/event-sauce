//! `PostgreSQL` checkpoint store implementation.
//!
//! Provides a production-ready `PostgreSQL` implementation of `CheckpointStore`.

use async_trait::async_trait;
use event_sauce_core::{CheckpointStore, Error, Position, Result};
use sqlx::PgPool;
use std::time::Duration;

/// `PostgreSQL` checkpoint store implementation.
///
/// This store provides durable checkpoint persistence using `PostgreSQL` with:
/// - Full ACID guarantees
/// - Schema isolation to avoid conflicts with application migrations
/// - Efficient upsert operations
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresCheckpointStore;
/// use sqlx::PgPool;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let pool = PgPool::connect("postgresql://localhost/events").await?;
///
///     // Using builder pattern with custom schema (recommended)
///     let store = PostgresCheckpointStore::builder()
///         .pool(pool)
///         .schema("event_sauce") // Isolates migrations from your app
///         .build()?;
///
///     // Run migrations in the custom schema
///     store.migrate().await?;
///
///     // Or use simple constructor (uses "event_sauce" schema)
///     let store = PostgresCheckpointStore::new(pool);
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct PostgresCheckpointStore {
    pool: PgPool,
    schema: String,
}

/// Builder for configuring `PostgresCheckpointStore`.
///
/// Provides a flexible way to configure the checkpoint store with:
/// - Custom database connection pool
/// - Schema name for table isolation
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresCheckpointStore;
/// use sqlx::PgPool;
///
/// let pool = PgPool::connect("postgresql://localhost/events").await?;
///
/// let store = PostgresCheckpointStore::builder()
///     .pool(pool)
///     .schema("event_sauce")
///     .build()?;
/// ```
#[derive(Clone)]
pub struct PostgresCheckpointStoreBuilder {
    pool: Option<PgPool>,
    schema: Option<String>,
}

impl PostgresCheckpointStore {
    /// Creates a new `PostgreSQL` checkpoint store with default configuration.
    ///
    /// This is a convenience method that uses the builder with all defaults:
    /// - **Schema**: "`event_sauce`" (isolated from your app)
    ///
    /// Equivalent to `PostgresCheckpointStore::builder().pool(pool).build()`.
    ///
    /// # Panics
    ///
    /// Cannot panic — the pool is always set before calling `build()`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresCheckpointStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    ///
    /// // Uses "event_sauce" schema by default
    /// let store = PostgresCheckpointStore::new(pool);
    /// store.migrate().await?;
    /// ```
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self::builder().pool(pool).build().expect("pool was set")
    }

    /// Creates a builder for configuring the checkpoint store.
    ///
    /// This is the recommended way to create a `PostgresCheckpointStore` when you need
    /// to customize the schema name.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresCheckpointStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    ///
    /// let store = PostgresCheckpointStore::builder()
    ///     .pool(pool)
    ///     .schema("event_sauce")  // Isolate from app migrations
    ///     .build()?;
    /// ```
    #[must_use]
    pub fn builder() -> PostgresCheckpointStoreBuilder {
        PostgresCheckpointStoreBuilder::new()
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns the schema name used by this checkpoint store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let store = PostgresCheckpointStore::builder()
    ///     .pool(pool)
    ///     .schema("my_schema")
    ///     .build()?;
    ///
    /// assert_eq!(store.schema(), "my_schema");
    /// ```
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns a schema-qualified table name (e.g., "schema.checkpoints").
    fn qualify_table(&self, table: &str) -> String {
        crate::migrations::qualify(&self.schema, table)
    }

    /// Builds the upsert SQL used by both pool- and transaction-based saves.
    fn checkpoint_upsert_sql(&self) -> String {
        let checkpoints_table = self.qualify_table("checkpoints");
        format!(
            "INSERT INTO {checkpoints_table} (subscription_name, position, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (subscription_name)
             DO UPDATE SET position = $2, updated_at = NOW()"
        )
    }

    /// Saves a checkpoint inside an existing transaction.
    ///
    /// This is the building block used by transactional projection runners that
    /// need the checkpoint update to commit atomically with the projection's
    /// data writes. Use [`save_checkpoint`](Self::save_checkpoint) when no outer
    /// transaction is in play.
    ///
    /// # Errors
    ///
    /// Returns an error if the upsert query fails (e.g. connection lost,
    /// unique constraint violation — neither expected under normal use).
    pub async fn save_checkpoint_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        subscription_name: &str,
        position: Position,
    ) -> Result<()> {
        let query = self.checkpoint_upsert_sql();
        sqlx::query(&query)
            .bind(subscription_name)
            .bind(position.as_i64())
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to save checkpoint", e))?;
        Ok(())
    }

    /// Saves a checkpoint inside an existing transaction, fenced on lease
    /// ownership and monotonicity.
    ///
    /// Unlike [`save_checkpoint_tx`](Self::save_checkpoint_tx), the write only
    /// lands when **all** of the following hold for the existing row:
    /// - it is currently leased by `worker_id`,
    /// - the lease has not expired (`leased_until > NOW()`), and
    /// - the new `position` strictly advances the stored position.
    ///
    /// This is the fence that stops a stalled worker — one that lost its lease
    /// to another worker while a per-event transaction was in flight — from
    /// double-applying events or regressing the checkpoint to a lower
    /// position. The caller (a leased projection or dispatcher runner) should
    /// roll back the transaction and surface
    /// [`Error::LeaseLost`](event_sauce_core::Error::LeaseLost) when this
    /// returns `Ok(false)`.
    ///
    /// Returns `Ok(true)` when the fenced write landed, `Ok(false)` when the
    /// fence rejected it (lease no longer owned/active, or position not
    /// strictly increasing).
    ///
    /// # Errors
    ///
    /// Returns an error if the upsert query fails (e.g. connection lost).
    pub async fn save_checkpoint_fenced_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        subscription_name: &str,
        worker_id: &str,
        position: Position,
    ) -> Result<bool> {
        let checkpoints_table = self.qualify_table("checkpoints");
        // The fence lives entirely in the conditional UPDATE: only advance
        // the position when this worker still owns an active lease and the
        // new position is strictly greater than the stored one. RETURNING
        // yields a row exactly when the write landed.
        //
        // The bare INSERT branch (no existing row) is intentionally NOT fenced
        // and is unreachable in practice: every caller is a leased runner that
        // has already `try_acquire_lease`d, which inserts the checkpoint row
        // (with this worker's lease) before any fenced save runs — so a fenced
        // save always hits the conditional DO UPDATE. Do not call this without
        // first holding the lease, or the first write would bypass the fence.
        let query = format!(
            "INSERT INTO {checkpoints_table} (subscription_name, position, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (subscription_name)
             DO UPDATE SET position = EXCLUDED.position, updated_at = NOW()
             WHERE {checkpoints_table}.worker_id = $3
               AND {checkpoints_table}.leased_until > NOW()
               AND EXCLUDED.position > {checkpoints_table}.position
             RETURNING position"
        );
        let landed: Option<i64> = sqlx::query_scalar(&query)
            .bind(subscription_name)
            .bind(position.as_i64())
            .bind(worker_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to save fenced checkpoint", e))?;
        Ok(landed.is_some())
    }

    /// Runs database migrations to set up the checkpoint store schema.
    ///
    /// This method creates the necessary tables (`checkpoints`) for the checkpoint store.
    /// It is idempotent and safe to call multiple times.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresCheckpointStore;
    /// use sqlx::PgPool;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let pool = PgPool::connect("postgresql://localhost/events").await?;
    ///     let store = PostgresCheckpointStore::new(pool);
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
    /// - There are SQL syntax errors in migration statements
    pub async fn migrate(&self) -> Result<()> {
        crate::migrations::ensure_schema(&self.pool, &self.schema).await?;

        let migrations_table = self.qualify_table("_checkpoint_migrations");
        crate::migrations::ensure_migrations_table(&self.pool, &migrations_table).await?;

        self.migrate_checkpoints_table(&migrations_table).await?;
        self.migrate_lease_columns(&migrations_table).await?;
        Ok(())
    }

    async fn migrate_checkpoints_table(&self, migrations_table: &str) -> Result<()> {
        let checkpoints_table = self.qualify_table("checkpoints");
        crate::migrations::apply_once(
            &self.pool,
            migrations_table,
            20_250_101_000_001_i64,
            "create_checkpoints_table",
            |pool| async move {
                let create_checkpoints = format!(
                    "CREATE TABLE IF NOT EXISTS {checkpoints_table} (
                        subscription_name VARCHAR(255) PRIMARY KEY,
                        position BIGINT NOT NULL,
                        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    )"
                );
                sqlx::query(&create_checkpoints)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create checkpoints table", e))?;

                let index = format!(
                    "CREATE INDEX IF NOT EXISTS idx_checkpoints_updated_at ON {checkpoints_table}(updated_at)"
                );
                sqlx::query(&index)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create index", e))?;
                Ok(())
            },
        )
        .await
    }

    async fn migrate_lease_columns(&self, migrations_table: &str) -> Result<()> {
        let checkpoints_table = self.qualify_table("checkpoints");
        crate::migrations::apply_once(
            &self.pool,
            migrations_table,
            20_260_428_000_000_i64,
            "add_lease_columns_to_checkpoints",
            |pool| async move {
                let stmts = vec![
                    format!(
                        "ALTER TABLE {checkpoints_table} ADD COLUMN IF NOT EXISTS worker_id VARCHAR(255)"
                    ),
                    format!(
                        "ALTER TABLE {checkpoints_table} ADD COLUMN IF NOT EXISTS leased_until TIMESTAMP WITH TIME ZONE"
                    ),
                    format!(
                        "ALTER TABLE {checkpoints_table} ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP WITH TIME ZONE"
                    ),
                    format!(
                        "CREATE INDEX IF NOT EXISTS idx_checkpoints_leased_until ON {checkpoints_table}(leased_until)"
                    ),
                ];
                for sql in stmts {
                    sqlx::query(&sql)
                        .execute(pool)
                        .await
                        .map_err(|e| Error::backend("Failed to apply lease columns migration", e))?;
                }
                Ok(())
            },
        )
        .await
    }
}

impl PostgresCheckpointStoreBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pool: None,
            schema: None,
        }
    }

    /// Sets the database connection pool.
    ///
    /// This is required - calling `build()` without setting a pool will panic.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresCheckpointStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    /// let builder = PostgresCheckpointStore::builder().pool(pool);
    /// ```
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Sets the schema name for checkpoint store tables.
    ///
    /// Defaults to "`event_sauce`" to isolate event-sauce migrations from your
    /// application's migration system. Use "public" if you want to use the
    /// default `PostgreSQL` schema.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresCheckpointStore;
    ///
    /// let builder = PostgresCheckpointStore::builder()
    ///     .pool(pool)
    ///     .schema("my_custom_schema");
    /// ```
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Builds the `PostgresCheckpointStore` with the configured settings.
    ///
    /// # Defaults
    ///
    /// - **Schema**: "`event_sauce`" (isolates migrations from your app)
    ///
    /// # Errors
    ///
    /// Returns an error if the pool has not been set via [`pool()`](Self::pool).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresCheckpointStore;
    /// use sqlx::PgPool;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    ///
    /// // Uses "event_sauce" schema by default
    /// let store = PostgresCheckpointStore::builder()
    ///     .pool(pool)
    ///     .build()?;
    /// ```
    pub fn build(self) -> event_sauce_core::Result<PostgresCheckpointStore> {
        Ok(PostgresCheckpointStore {
            pool: self
                .pool
                .ok_or_else(|| Error::invalid_state("pool is required"))?,
            schema: self.schema.unwrap_or_else(|| "event_sauce".to_string()),
        })
    }
}

impl Default for PostgresCheckpointStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CheckpointStore for PostgresCheckpointStore {
    async fn save_checkpoint(&self, subscription_name: &str, position: Position) -> Result<()> {
        let query = self.checkpoint_upsert_sql();
        sqlx::query(&query)
            .bind(subscription_name)
            .bind(position.as_i64())
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to save checkpoint", e))?;
        Ok(())
    }

    async fn load_checkpoint(&self, subscription_name: &str) -> Result<Option<Position>> {
        let checkpoints_table = self.qualify_table("checkpoints");
        let query =
            format!("SELECT position FROM {checkpoints_table} WHERE subscription_name = $1");

        let position: Option<i64> = sqlx::query_scalar(&query)
            .bind(subscription_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to load checkpoint", e))?;

        Ok(position.map(Position::new))
    }

    async fn delete_checkpoint(&self, subscription_name: &str) -> Result<()> {
        let checkpoints_table = self.qualify_table("checkpoints");
        let query = format!("DELETE FROM {checkpoints_table} WHERE subscription_name = $1");

        sqlx::query(&query)
            .bind(subscription_name)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to delete checkpoint", e))?;

        Ok(())
    }

    async fn try_acquire_lease(
        &self,
        subscription_name: &str,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<Position>> {
        let checkpoints_table = self.qualify_table("checkpoints");
        // Atomically insert-or-update only if the existing lease is free,
        // expired, or already ours. The conditional UPDATE returns the
        // current position; if the WHERE blocks the update, RETURNING
        // returns no rows and we report the lease as held by someone else.
        let query = format!(
            "INSERT INTO {checkpoints_table}
                (subscription_name, position, worker_id, leased_until, heartbeat_at, updated_at)
             VALUES ($1, 0, $2, NOW() + ($3 * INTERVAL '1 second'), NOW(), NOW())
             ON CONFLICT (subscription_name) DO UPDATE
                SET worker_id = EXCLUDED.worker_id,
                    leased_until = EXCLUDED.leased_until,
                    heartbeat_at = EXCLUDED.heartbeat_at,
                    updated_at = NOW()
                WHERE {checkpoints_table}.worker_id IS NULL
                   OR {checkpoints_table}.leased_until IS NULL
                   OR {checkpoints_table}.leased_until < NOW()
                   OR {checkpoints_table}.worker_id = EXCLUDED.worker_id
             RETURNING position"
        );

        #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
        let secs = lease_duration.as_secs() as i64;
        let position: Option<i64> = sqlx::query_scalar(&query)
            .bind(subscription_name)
            .bind(worker_id)
            .bind(secs)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to acquire lease", e))?;

        Ok(position.map(Position::new))
    }

    async fn renew_lease(
        &self,
        subscription_name: &str,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<()> {
        let checkpoints_table = self.qualify_table("checkpoints");
        let query = format!(
            "UPDATE {checkpoints_table}
             SET leased_until = NOW() + ($3 * INTERVAL '1 second'),
                 heartbeat_at = NOW(),
                 updated_at = NOW()
             WHERE subscription_name = $1
               AND worker_id = $2
               AND leased_until > NOW()
             RETURNING position"
        );

        #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
        let secs = lease_duration.as_secs() as i64;
        let renewed: Option<i64> = sqlx::query_scalar(&query)
            .bind(subscription_name)
            .bind(worker_id)
            .bind(secs)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to renew lease", e))?;

        if renewed.is_none() {
            return Err(Error::custom(format!(
                "lease lost for {subscription_name} (worker {worker_id})"
            )));
        }
        Ok(())
    }

    async fn release_lease(&self, subscription_name: &str, worker_id: &str) -> Result<()> {
        let checkpoints_table = self.qualify_table("checkpoints");
        let query = format!(
            "UPDATE {checkpoints_table}
             SET worker_id = NULL,
                 leased_until = NULL
             WHERE subscription_name = $1 AND worker_id = $2"
        );
        sqlx::query(&query)
            .bind(subscription_name)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to release lease", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{CheckpointStore, Position};
    use sqlx::PgPool;
    use testcontainers_modules::postgres::Postgres;

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
            let container = crate::test_support::start_postgres()
                .await
                .map_err(|e| Error::backend("Failed to start PostgreSQL container", e))?;

            // Get connection string
            let host = container
                .get_host()
                .await
                .map_err(|e| Error::backend("Failed to get container host", e))?;
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .map_err(|e| Error::backend("Failed to get container port", e))?;

            let connection_string =
                format!("postgresql://postgres:postgres@{host}:{port}/postgres");

            // Connect to database
            let pool = PgPool::connect(&connection_string)
                .await
                .map_err(|e| Error::backend("Failed to connect to test database", e))?;

            Ok(Self { pool, container })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }
    }

    #[tokio::test]
    async fn test_create_store() {
        let db = TestDatabase::new().await.unwrap();
        let _store = PostgresCheckpointStore::new(db.pool().clone());
    }

    #[tokio::test]
    async fn test_migrate_creates_tables() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());

        // Run migrations
        store.migrate().await.unwrap();

        // Verify tables exist
        let checkpoints_exist: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'event_sauce' AND table_name = 'checkpoints'
            )",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();

        assert!(checkpoints_exist);
    }

    #[tokio::test]
    async fn test_migrate_is_idempotent() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());

        // Call migrate twice - should not fail
        store.migrate().await.unwrap();
        store.migrate().await.unwrap();

        // Verify we can use the store
        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(42)));
    }

    #[tokio::test]
    async fn test_save_and_load_checkpoint() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(42)));
    }

    #[tokio::test]
    async fn test_load_nonexistent_checkpoint() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let position = store
            .load_checkpoint("nonexistent-subscription")
            .await
            .unwrap();
        assert_eq!(position, None);
    }

    #[tokio::test]
    async fn test_delete_checkpoint() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        // Save checkpoint
        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        // Verify it exists
        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(42)));

        // Delete it
        store.delete_checkpoint("test-subscription").await.unwrap();

        // Verify it's gone
        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, None);
    }

    #[tokio::test]
    async fn test_overwrite_checkpoint() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        // Save initial checkpoint
        store
            .save_checkpoint("test-subscription", Position::new(10))
            .await
            .unwrap();

        // Overwrite with new position
        store
            .save_checkpoint("test-subscription", Position::new(20))
            .await
            .unwrap();

        // Should get the latest position
        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(20)));
    }

    #[tokio::test]
    async fn test_multiple_subscriptions() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        // Save checkpoints for multiple subscriptions
        store
            .save_checkpoint("subscription-1", Position::new(10))
            .await
            .unwrap();
        store
            .save_checkpoint("subscription-2", Position::new(20))
            .await
            .unwrap();
        store
            .save_checkpoint("subscription-3", Position::new(30))
            .await
            .unwrap();

        // Load each checkpoint
        let pos1 = store.load_checkpoint("subscription-1").await.unwrap();
        let pos2 = store.load_checkpoint("subscription-2").await.unwrap();
        let pos3 = store.load_checkpoint("subscription-3").await.unwrap();

        assert_eq!(pos1, Some(Position::new(10)));
        assert_eq!(pos2, Some(Position::new(20)));
        assert_eq!(pos3, Some(Position::new(30)));
    }

    #[tokio::test]
    async fn test_delete_nonexistent_checkpoint() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        // Deleting a nonexistent checkpoint should succeed (no-op)
        let result = store.delete_checkpoint("nonexistent-subscription").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_store_is_cloneable() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let store_clone = store.clone();

        // Save via original store
        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        // Read via clone - should see the same data
        let position = store_clone
            .load_checkpoint("test-subscription")
            .await
            .unwrap();
        assert_eq!(position, Some(Position::new(42)));
    }

    // === Builder Pattern Tests ===

    #[tokio::test]
    async fn test_builder_with_defaults() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::builder()
            .pool(db.pool().clone())
            .build()
            .unwrap();

        // Default schema should be "event_sauce"
        assert_eq!(store.schema(), "event_sauce");

        // Migrate to create the schema
        store.migrate().await.unwrap();

        // Should work with default schema
        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(42)));
    }

    #[tokio::test]
    async fn test_builder_with_custom_schema() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::builder()
            .pool(db.pool().clone())
            .schema("custom_schema")
            .build()
            .unwrap();

        // Verify schema is used
        assert_eq!(store.schema(), "custom_schema");
    }

    #[tokio::test]
    async fn test_builder_migrate_creates_schema() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::builder()
            .pool(db.pool().clone())
            .schema("checkpoint_test")
            .build()
            .unwrap();

        // Run migrations
        store.migrate().await.unwrap();

        // Verify schema exists
        let schema_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.schemata
                WHERE schema_name = $1
            )",
        )
        .bind("checkpoint_test")
        .fetch_one(db.pool())
        .await
        .unwrap();

        assert!(schema_exists);

        // Verify tables exist in the schema
        let checkpoints_exist: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = 'checkpoints'
            )",
        )
        .bind("checkpoint_test")
        .fetch_one(db.pool())
        .await
        .unwrap();

        assert!(checkpoints_exist);
    }

    #[tokio::test]
    async fn test_builder_operations_use_custom_schema() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::builder()
            .pool(db.pool().clone())
            .schema("checkpoints_schema")
            .build()
            .unwrap();

        store.migrate().await.unwrap();

        // Perform operations
        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        // Verify data is in the custom schema, not public
        let count_in_custom_schema: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM checkpoints_schema.checkpoints WHERE subscription_name = $1",
        )
        .bind("test-subscription")
        .fetch_one(db.pool())
        .await
        .unwrap();

        assert_eq!(count_in_custom_schema, 1);

        // Verify data is NOT in public schema
        let count_in_public: std::result::Result<i64, sqlx::Error> = sqlx::query_scalar(
            "SELECT COUNT(*) FROM public.checkpoints WHERE subscription_name = $1",
        )
        .bind("test-subscription")
        .fetch_one(db.pool())
        .await;

        // Should fail because table doesn't exist in public schema
        assert!(count_in_public.is_err());
    }

    #[tokio::test]
    async fn test_save_checkpoint_tx_commits_with_transaction() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let mut tx = db.pool().begin().await.unwrap();
        store
            .save_checkpoint_tx(&mut tx, "tx-sub", Position::new(7))
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let position = store.load_checkpoint("tx-sub").await.unwrap();
        assert_eq!(position, Some(Position::new(7)));
    }

    #[tokio::test]
    async fn test_save_checkpoint_tx_rolls_back_with_transaction() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        // Establish a baseline so rollback has something to revert to.
        store
            .save_checkpoint("tx-sub", Position::new(3))
            .await
            .unwrap();

        let mut tx = db.pool().begin().await.unwrap();
        store
            .save_checkpoint_tx(&mut tx, "tx-sub", Position::new(99))
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        // Position must remain at the pre-tx value.
        let position = store.load_checkpoint("tx-sub").await.unwrap();
        assert_eq!(position, Some(Position::new(3)));
    }

    #[tokio::test]
    async fn test_lease_acquire_creates_entry() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let pos = store
            .try_acquire_lease("sub-a", "worker-1", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(pos, Some(Position::start()));
    }

    #[tokio::test]
    async fn test_lease_blocks_second_worker() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let lease_dur = std::time::Duration::from_secs(60);
        store
            .try_acquire_lease("sub-b", "worker-1", lease_dur)
            .await
            .unwrap();
        let blocked = store
            .try_acquire_lease("sub-b", "worker-2", lease_dur)
            .await
            .unwrap();
        assert!(blocked.is_none());
    }

    #[tokio::test]
    async fn test_lease_same_worker_can_re_acquire() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let lease_dur = std::time::Duration::from_secs(60);
        store
            .try_acquire_lease("sub-c", "worker-1", lease_dur)
            .await
            .unwrap();
        let again = store
            .try_acquire_lease("sub-c", "worker-1", lease_dur)
            .await
            .unwrap();
        assert!(again.is_some());
    }

    #[tokio::test]
    async fn test_lease_expired_can_be_taken() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        // Acquire then manually expire it via DB write
        store
            .try_acquire_lease("sub-d", "worker-1", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        sqlx::query(
            "UPDATE event_sauce.checkpoints SET leased_until = NOW() - INTERVAL '1 minute' WHERE subscription_name = $1",
        )
        .bind("sub-d")
        .execute(db.pool())
        .await
        .unwrap();

        let taken = store
            .try_acquire_lease("sub-d", "worker-2", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert!(taken.is_some(), "expired lease should be reclaimable");
    }

    #[tokio::test]
    async fn test_lease_renew_extends_expiry() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        store
            .try_acquire_lease("sub-e", "worker-1", std::time::Duration::from_secs(1))
            .await
            .unwrap();

        // Renew with a long duration
        store
            .renew_lease("sub-e", "worker-1", std::time::Duration::from_secs(60))
            .await
            .unwrap();

        // Sleep past the original expiry
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        // Another worker should still be blocked
        let blocked = store
            .try_acquire_lease("sub-e", "worker-2", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert!(blocked.is_none());
    }

    #[tokio::test]
    async fn test_lease_renew_errors_when_not_held() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let result = store
            .renew_lease("sub-f", "worker-1", std::time::Duration::from_secs(60))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_lease_release_allows_other_worker() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let lease_dur = std::time::Duration::from_secs(60);
        store
            .try_acquire_lease("sub-g", "worker-1", lease_dur)
            .await
            .unwrap();
        store.release_lease("sub-g", "worker-1").await.unwrap();
        let taken = store
            .try_acquire_lease("sub-g", "worker-2", lease_dur)
            .await
            .unwrap();
        assert!(taken.is_some());
    }

    #[tokio::test]
    async fn test_lease_returns_existing_position() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        store
            .save_checkpoint("sub-h", Position::new(42))
            .await
            .unwrap();
        let pos = store
            .try_acquire_lease("sub-h", "worker-1", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(pos, Some(Position::new(42)));
    }

    // === Fenced checkpoint save tests (lease fencing, H6) ===

    #[tokio::test]
    async fn test_fenced_checkpoint_happy_path() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let lease_dur = std::time::Duration::from_secs(60);
        // Worker holds the lease.
        store
            .try_acquire_lease("fenced-sub", "worker-1", lease_dur)
            .await
            .unwrap();

        // Strictly-increasing position while holding the lease persists.
        let mut tx = db.pool().begin().await.unwrap();
        let landed = store
            .save_checkpoint_fenced_tx(&mut tx, "fenced-sub", "worker-1", Position::new(10))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(landed, "increasing position under held lease must persist");
        assert_eq!(
            store.load_checkpoint("fenced-sub").await.unwrap(),
            Some(Position::new(10))
        );

        // Equal position is not strictly increasing: fence rejects, position unchanged.
        let mut tx = db.pool().begin().await.unwrap();
        let landed_equal = store
            .save_checkpoint_fenced_tx(&mut tx, "fenced-sub", "worker-1", Position::new(10))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            !landed_equal,
            "equal position must not persist (monotonicity)"
        );
        assert_eq!(
            store.load_checkpoint("fenced-sub").await.unwrap(),
            Some(Position::new(10))
        );

        // Lower position is rejected and leaves the stored position unchanged.
        let mut tx = db.pool().begin().await.unwrap();
        let landed_lower = store
            .save_checkpoint_fenced_tx(&mut tx, "fenced-sub", "worker-1", Position::new(5))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            !landed_lower,
            "lower position must not persist (monotonicity)"
        );
        assert_eq!(
            store.load_checkpoint("fenced-sub").await.unwrap(),
            Some(Position::new(10))
        );
    }

    #[tokio::test]
    async fn test_stalled_worker_cannot_move_checkpoint_backward() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCheckpointStore::new(db.pool().clone());
        store.migrate().await.unwrap();

        let lease_dur = std::time::Duration::from_secs(60);

        // Worker-1 acquires the lease and (fenced) saves position 50.
        store
            .try_acquire_lease("stall-sub", "worker-1", lease_dur)
            .await
            .unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        let landed = store
            .save_checkpoint_fenced_tx(&mut tx, "stall-sub", "worker-1", Position::new(50))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            landed,
            "worker-1 should persist position 50 while holding lease"
        );

        // Simulate lease handover: worker-1 stalls past its lease expiry.
        sqlx::query(
            "UPDATE event_sauce.checkpoints SET leased_until = NOW() - INTERVAL '1 minute' WHERE subscription_name = $1",
        )
        .bind("stall-sub")
        .execute(db.pool())
        .await
        .unwrap();

        // Worker-2 takes over the now-expired lease and (fenced) saves position 60.
        let taken = store
            .try_acquire_lease("stall-sub", "worker-2", lease_dur)
            .await
            .unwrap();
        assert!(taken.is_some(), "worker-2 should reclaim the expired lease");
        let mut tx = db.pool().begin().await.unwrap();
        let landed2 = store
            .save_checkpoint_fenced_tx(&mut tx, "stall-sub", "worker-2", Position::new(60))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            landed2,
            "worker-2 should persist position 60 while holding lease"
        );

        // Worker-1, now stale (no longer holds the lease), tries to commit an
        // in-flight save at position 51. The fence must reject it: it no longer
        // owns the lease, so the write does not land and the checkpoint stays 60.
        let mut tx = db.pool().begin().await.unwrap();
        let landed_stale = store
            .save_checkpoint_fenced_tx(&mut tx, "stall-sub", "worker-1", Position::new(51))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            !landed_stale,
            "a stalled worker that lost the lease must not move the checkpoint"
        );
        assert_eq!(
            store.load_checkpoint("stall-sub").await.unwrap(),
            Some(Position::new(60)),
            "checkpoint must not regress below the value written by the current lease holder"
        );
    }
}
