//! `PostgreSQL` checkpoint store implementation.
//!
//! Provides a production-ready `PostgreSQL` implementation of `CheckpointStore`.

use async_trait::async_trait;
use event_sauce_core::{CheckpointStore, Error, Position, Result};
use sqlx::PgPool;

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
        format!("{}.{}", self.schema, table)
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
        // Create schema if it doesn't exist (skip for public schema)
        if self.schema != "public" {
            let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
            sqlx::query(&create_schema)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::backend("Failed to create schema", e))?;
        }

        // Create migration tracking table in the custom schema
        let migrations_table = self.qualify_table("_checkpoint_migrations");
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
            .map_err(|e| Error::backend("Failed to create migrations table", e))?;

        // Check if migration has already been applied
        let check_query = format!("SELECT COUNT(*) FROM {migrations_table} WHERE version = $1");
        let count: i64 = sqlx::query_scalar(&check_query)
            .bind(20_250_101_000_001_i64)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to check migration status", e))?;

        if count > 0 {
            // Migration already applied
            return Ok(());
        }

        // Apply the migration with schema-qualified table names
        let checkpoints_table = self.qualify_table("checkpoints");

        // Create checkpoints table
        let create_checkpoints = format!(
            "CREATE TABLE IF NOT EXISTS {checkpoints_table} (
                subscription_name VARCHAR(255) PRIMARY KEY,
                position BIGINT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )"
        );
        sqlx::query(&create_checkpoints)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to create checkpoints table", e))?;

        // Create index on updated_at
        let index = format!(
            "CREATE INDEX IF NOT EXISTS idx_checkpoints_updated_at ON {checkpoints_table}(updated_at)"
        );
        sqlx::query(&index)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to create index", e))?;

        // Record the migration
        let record_query =
            format!("INSERT INTO {migrations_table} (version, description) VALUES ($1, $2)");
        sqlx::query(&record_query)
            .bind(20_250_101_000_001_i64)
            .bind("create_checkpoints_table")
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to record migration", e))?;

        Ok(())
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
        let checkpoints_table = self.qualify_table("checkpoints");
        let query = format!(
            "INSERT INTO {checkpoints_table} (subscription_name, position, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (subscription_name)
             DO UPDATE SET position = $2, updated_at = NOW()"
        );

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{CheckpointStore, Position};
    use sqlx::PgPool;
    use testcontainers::ImageExt;
    use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};

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
}
