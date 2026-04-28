//! `PostgreSQL` crypto key store implementation.
//!
//! Provides a production-ready `PostgreSQL` implementation of `CryptoKeyStore`
//! for managing per-aggregate encryption keys used in crypto-shredding.

use async_trait::async_trait;
use event_sauce_core::{CryptoKeyStore, Error, Result};
use sqlx::PgPool;
use uuid::Uuid;

/// `PostgreSQL` crypto key store for per-aggregate encryption keys.
///
/// Manages the lifecycle of encryption keys used for encrypted aggregates.
/// Deleting a key effectively "crypto-shreds" the aggregate's data,
/// making it permanently unreadable (GDPR right-to-be-forgotten).
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresCryptoKeyStore;
/// use sqlx::PgPool;
///
/// let pool = PgPool::connect("postgresql://localhost/events").await?;
/// let store = PostgresCryptoKeyStore::builder()
///     .pool(pool)
///     .schema("event_sauce")
///     .build()?;
///
/// store.migrate().await?;
/// ```
#[derive(Clone)]
pub struct PostgresCryptoKeyStore {
    pool: PgPool,
    schema: String,
}

/// Builder for configuring `PostgresCryptoKeyStore`.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresCryptoKeyStore;
/// use sqlx::PgPool;
///
/// let pool = PgPool::connect("postgresql://localhost/events").await?;
/// let store = PostgresCryptoKeyStore::builder()
///     .pool(pool)
///     .schema("event_sauce")
///     .build()?;
/// ```
#[derive(Clone)]
pub struct PostgresCryptoKeyStoreBuilder {
    pool: Option<PgPool>,
    schema: Option<String>,
}

impl PostgresCryptoKeyStore {
    /// Creates a new `PostgreSQL` crypto key store with default configuration.
    ///
    /// Uses the "`event_sauce`" schema by default.
    ///
    /// # Panics
    ///
    /// Cannot panic — the pool is always set before calling `build()`.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self::builder().pool(pool).build().expect("pool was set")
    }

    /// Creates a builder for configuring the crypto key store.
    #[must_use]
    pub fn builder() -> PostgresCryptoKeyStoreBuilder {
        PostgresCryptoKeyStoreBuilder::new()
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns the schema name used by this crypto key store.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns a schema-qualified table name.
    fn qualify_table(&self, table: &str) -> String {
        format!("{}.{}", self.schema, table)
    }

    /// Runs database migrations to set up the crypto keys table.
    ///
    /// Creates the `crypto_keys` table for storing per-aggregate encryption keys.
    /// This method is idempotent and safe to call multiple times.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database connection fails
    /// - The migrations cannot be applied due to permission issues
    pub async fn migrate(&self) -> Result<()> {
        crate::migrations::ensure_schema(&self.pool, &self.schema).await?;

        let migrations_table = self.qualify_table("_crypto_key_migrations");
        crate::migrations::ensure_migrations_table(&self.pool, &migrations_table).await?;

        let crypto_keys_table = self.qualify_table("crypto_keys");
        crate::migrations::apply_once(
            &self.pool,
            &migrations_table,
            20_250_301_000_000_i64,
            "create_crypto_keys_table",
            |pool| async move {
                let create_crypto_keys = format!(
                    "CREATE TABLE IF NOT EXISTS {crypto_keys_table} (
                        aggregate_id UUID PRIMARY KEY,
                        key_data BYTEA NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    )"
                );
                sqlx::query(&create_crypto_keys)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create crypto_keys table", e))?;
                Ok(())
            },
        )
        .await
    }
}

impl PostgresCryptoKeyStoreBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pool: None,
            schema: None,
        }
    }

    /// Sets the database connection pool.
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Sets the schema name for the crypto keys table.
    ///
    /// Defaults to "`event_sauce`".
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Builds the `PostgresCryptoKeyStore` with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool has not been set via [`pool()`](Self::pool).
    pub fn build(self) -> Result<PostgresCryptoKeyStore> {
        Ok(PostgresCryptoKeyStore {
            pool: self
                .pool
                .ok_or_else(|| Error::invalid_state("pool is required"))?,
            schema: self.schema.unwrap_or_else(|| "event_sauce".to_string()),
        })
    }
}

impl Default for PostgresCryptoKeyStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CryptoKeyStore for PostgresCryptoKeyStore {
    async fn get_key(&self, aggregate_id: Uuid) -> Result<Option<Vec<u8>>> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        let query = format!("SELECT key_data FROM {crypto_keys_table} WHERE aggregate_id = $1");

        let key_data: Option<Vec<u8>> = sqlx::query_scalar(&query)
            .bind(aggregate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to get crypto key", e))?;

        Ok(key_data)
    }

    async fn upsert_key(&self, aggregate_id: Uuid, key: Vec<u8>) -> Result<()> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        let query = format!(
            "INSERT INTO {crypto_keys_table} (aggregate_id, key_data)
             VALUES ($1, $2)
             ON CONFLICT (aggregate_id)
             DO UPDATE SET key_data = $2"
        );

        sqlx::query(&query)
            .bind(aggregate_id)
            .bind(&key)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to upsert crypto key", e))?;

        Ok(())
    }

    async fn delete_key(&self, aggregate_id: Uuid) -> Result<()> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        let query = format!("DELETE FROM {crypto_keys_table} WHERE aggregate_id = $1");

        sqlx::query(&query)
            .bind(aggregate_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to delete crypto key", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::CryptoKeyStore;
    use sqlx::PgPool;
    use testcontainers::ImageExt;
    use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
    use uuid::Uuid;

    /// Test database helper using testcontainers.
    struct TestDatabase {
        pool: PgPool,
        #[allow(dead_code)]
        container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let container = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .map_err(|e| Error::backend("Failed to start PostgreSQL container", e))?;

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

            let pool = PgPool::connect(&connection_string)
                .await
                .map_err(|e| Error::backend("Failed to connect", e))?;

            Ok(Self { pool, container })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        fn store(&self) -> PostgresCryptoKeyStore {
            PostgresCryptoKeyStore::new(self.pool.clone())
        }
    }

    #[tokio::test]
    async fn test_create_store() {
        let db = TestDatabase::new().await.unwrap();
        let _store = db.store();
    }

    #[tokio::test]
    async fn test_migrate_creates_table() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let table_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'event_sauce' AND table_name = 'crypto_keys'
            )",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();

        assert!(table_exists);
    }

    #[tokio::test]
    async fn test_migrate_is_idempotent() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();
        store.migrate().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_key_returns_none_for_unknown() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let key = store.get_key(Uuid::new_v4()).await.unwrap();
        assert!(key.is_none());
    }

    #[tokio::test]
    async fn test_upsert_and_get_key() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let id = Uuid::new_v4();
        let key_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

        store.upsert_key(id, key_data.clone()).await.unwrap();
        let loaded = store.get_key(id).await.unwrap();

        assert_eq!(loaded, Some(key_data));
    }

    #[tokio::test]
    async fn test_upsert_overwrites_existing_key() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let id = Uuid::new_v4();
        let key1 = vec![1, 2, 3];
        let key2 = vec![4, 5, 6];

        store.upsert_key(id, key1).await.unwrap();
        store.upsert_key(id, key2.clone()).await.unwrap();

        let loaded = store.get_key(id).await.unwrap();
        assert_eq!(loaded, Some(key2));
    }

    #[tokio::test]
    async fn test_delete_key() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let id = Uuid::new_v4();
        let key_data = vec![1, 2, 3];

        store.upsert_key(id, key_data).await.unwrap();
        assert!(store.get_key(id).await.unwrap().is_some());

        store.delete_key(id).await.unwrap();
        assert!(store.get_key(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_key_is_idempotent() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let result = store.delete_key(Uuid::new_v4()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_multiple_aggregates_have_separate_keys() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let key1 = vec![1, 1, 1];
        let key2 = vec![2, 2, 2];

        store.upsert_key(id1, key1.clone()).await.unwrap();
        store.upsert_key(id2, key2.clone()).await.unwrap();

        assert_eq!(store.get_key(id1).await.unwrap(), Some(key1));
        assert_eq!(store.get_key(id2).await.unwrap(), Some(key2));
    }

    #[tokio::test]
    async fn test_custom_schema() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresCryptoKeyStore::builder()
            .pool(db.pool().clone())
            .schema("custom_crypto")
            .build()
            .unwrap();

        store.migrate().await.unwrap();

        let schema_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.schemata
                WHERE schema_name = $1
            )",
        )
        .bind("custom_crypto")
        .fetch_one(db.pool())
        .await
        .unwrap();

        assert!(schema_exists);

        // Verify table exists in custom schema
        let table_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = 'crypto_keys'
            )",
        )
        .bind("custom_crypto")
        .fetch_one(db.pool())
        .await
        .unwrap();

        assert!(table_exists);
    }

    #[tokio::test]
    async fn test_builder_errors_without_pool() {
        let result = PostgresCryptoKeyStore::builder().build();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_store_is_cloneable() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let store_clone = store.clone();
        let id = Uuid::new_v4();
        let key_data = vec![1, 2, 3];

        store.upsert_key(id, key_data.clone()).await.unwrap();
        let loaded = store_clone.get_key(id).await.unwrap();

        assert_eq!(loaded, Some(key_data));
    }
}
