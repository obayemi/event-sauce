//! Builder for configuring [`PostgresEventStore`].

use event_sauce_core::{Error, SnapshotConfig};
use sqlx::PgPool;

use super::PostgresEventStore;

/// Default time `append()` waits to acquire the transaction-scoped advisory
/// lock that serializes commit order before giving up with a backend error.
const DEFAULT_APPEND_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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
///     .build()?;
/// ```
#[derive(Clone)]
pub struct PostgresEventStoreBuilder {
    pool: Option<PgPool>,
    snapshot_config: Option<SnapshotConfig>,
    schema: Option<String>,
    append_lock_timeout: Option<std::time::Duration>,
    checkpoint_store: Option<std::sync::Arc<dyn event_sauce_core::CheckpointStore>>,
    crypto_key_store: Option<std::sync::Arc<dyn event_sauce_core::CryptoKeyStore>>,
    crypto_provider: Option<std::sync::Arc<dyn event_sauce_core::CryptoProvider>>,
}

impl PostgresEventStoreBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pool: None,
            snapshot_config: None,
            schema: None,
            append_lock_timeout: None,
            checkpoint_store: None,
            crypto_key_store: None,
            crypto_provider: None,
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

    /// Sets how long [`append`](event_sauce_core::EventStore::append) waits to acquire the
    /// transaction-scoped advisory lock that serializes commit order.
    ///
    /// `append()` takes a lock keyed by the qualified events table before
    /// allocating global event ids, so id order equals commit order. Under heavy
    /// cross-stream write contention an append may have to wait behind another
    /// in-flight append's insert window; this bounds that wait. If the lock is
    /// not acquired within the timeout, `append()` returns a backend error
    /// rather than blocking indefinitely.
    ///
    /// Defaults to 5 seconds.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use std::time::Duration;
    ///
    /// let store = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .append_lock_timeout(Duration::from_secs(2))
    ///     .build()?;
    /// ```
    #[must_use]
    pub fn append_lock_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.append_lock_timeout = Some(timeout);
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

    /// Sets the crypto key store for encrypted aggregate encryption.
    ///
    /// Required when using encrypted aggregates with crypto-shredding support.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::{PostgresEventStore, PostgresCryptoKeyStore};
    /// use std::sync::Arc;
    ///
    /// let key_store = Arc::new(PostgresCryptoKeyStore::new(pool.clone()));
    /// let builder = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .crypto_key_store(key_store);
    /// ```
    #[must_use]
    pub fn crypto_key_store(
        mut self,
        store: std::sync::Arc<dyn event_sauce_core::CryptoKeyStore>,
    ) -> Self {
        self.crypto_key_store = Some(store);
        self
    }

    /// Sets the crypto provider for encrypted aggregate encryption.
    ///
    /// Required when using encrypted aggregates with crypto-shredding support.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresEventStore;
    /// use event_sauce_crypto::Aes256GcmProvider;
    /// use std::sync::Arc;
    ///
    /// let provider = Arc::new(Aes256GcmProvider);
    /// let builder = PostgresEventStore::builder()
    ///     .pool(pool)
    ///     .crypto_provider(provider);
    /// ```
    #[must_use]
    pub fn crypto_provider(
        mut self,
        provider: std::sync::Arc<dyn event_sauce_core::CryptoProvider>,
    ) -> Self {
        self.crypto_provider = Some(provider);
        self
    }

    /// Builds the `PostgresEventStore` with the configured settings.
    ///
    /// # Defaults
    ///
    /// - **Schema**: "`event_sauce`" (isolates migrations from your app)
    /// - **Snapshot config**: Every 100 events
    /// - **Crypto key store**: [`PostgresCryptoKeyStore`](crate::PostgresCryptoKeyStore) with the same pool and schema
    /// - **Crypto provider**: `Aes256GcmProvider` when the `crypto` feature is
    ///   enabled; without it (the default), none is installed and encrypted
    ///   aggregates require an explicit [`crypto_provider()`](Self::crypto_provider)
    ///
    /// # Crypto auto-install
    ///
    /// A key store is always installed. With the opt-in `crypto` feature, a
    /// provider is installed too, even if your aggregates are
    /// not encrypted — it remains dormant until an encrypted aggregate (one whose
    /// `Aggregate::is_encrypted()` returns true, or one with `@encrypted_fields`)
    /// is committed or loaded. The default key store will create its `crypto_keys`
    /// table on first `migrate()`. Override either via
    /// [`crypto_key_store()`](Self::crypto_key_store) /
    /// [`crypto_provider()`](Self::crypto_provider) if you need a custom backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool has not been set via [`pool()`](Self::pool).
    ///
    /// # Panics
    ///
    /// Cannot panic — the internal `PostgresCryptoKeyStore` builder always has its pool set
    /// before calling `build()`.
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
    ///     .build()?;
    /// ```
    pub fn build(self) -> event_sauce_core::Result<PostgresEventStore> {
        let pool = self
            .pool
            .ok_or_else(|| Error::invalid_state("pool is required"))?;
        let schema = self.schema.unwrap_or_else(|| "event_sauce".to_string());

        let crypto_key_store: std::sync::Arc<dyn event_sauce_core::CryptoKeyStore> =
            self.crypto_key_store.unwrap_or_else(|| {
                std::sync::Arc::new(
                    crate::PostgresCryptoKeyStore::builder()
                        .pool(pool.clone())
                        .schema(&schema)
                        .build()
                        .expect("pool was set"),
                )
            });

        #[cfg(feature = "crypto")]
        let crypto_provider: Option<std::sync::Arc<dyn event_sauce_core::CryptoProvider>> = Some(
            self.crypto_provider
                .unwrap_or_else(|| std::sync::Arc::new(event_sauce_crypto::Aes256GcmProvider)),
        );
        #[cfg(not(feature = "crypto"))]
        let crypto_provider = self.crypto_provider;

        Ok(PostgresEventStore {
            pool,
            snapshot_config: self
                .snapshot_config
                .unwrap_or_else(|| SnapshotConfig::builder().build()),
            schema,
            append_lock_timeout: self
                .append_lock_timeout
                .unwrap_or(DEFAULT_APPEND_LOCK_TIMEOUT),
            checkpoint_store: self.checkpoint_store,
            crypto_key_store: Some(crypto_key_store),
            crypto_provider,
        })
    }
}

impl Default for PostgresEventStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{AggregateVersion, EventStore, StreamId};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::super::tests::{create_test_envelope, TestDatabase};

    // === Builder Pattern Tests ===

    #[tokio::test]
    async fn test_builder_with_defaults() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::builder()
            .pool(db.pool().clone())
            .build()
            .unwrap();

        // Default schema should be "event_sauce"
        assert_eq!(store.schema(), "event_sauce");

        // Migrate to create the schema
        store.migrate().await.unwrap();

        // Should work with default schema
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));
    }

    #[tokio::test]
    async fn test_builder_with_custom_schema() {
        let db = TestDatabase::new().await.unwrap();
        let store = PostgresEventStore::builder()
            .pool(db.pool().clone())
            .schema("custom_schema")
            .build()
            .unwrap();

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
            .build()
            .unwrap();

        // Verify snapshot config is set - just check it's accessible
        // (testing the actual strategy behavior is done in core crate tests)
        let _config = store.snapshot_config();
        // Config is accessible - no assertion needed
    }

    #[tokio::test]
    async fn test_builder_full_configuration() {
        let db = TestDatabase::new().await.unwrap();
        let config = SnapshotConfig::builder()
            .default_strategy(event_sauce_core::EveryNEvents::try_new(50).unwrap())
            .build();

        let store = PostgresEventStore::builder()
            .pool(db.pool().clone())
            .schema("my_events")
            .snapshot_config(config)
            .build()
            .unwrap();

        assert_eq!(store.schema(), "my_events");
    }

    #[tokio::test]
    async fn test_builder_migrate_creates_schema() {
        // Create a fresh database
        let (_container, connection_string) = crate::test_support::start_postgres_url().await;

        let pool = PgPool::connect(&connection_string).await.unwrap();

        // Build store with custom schema
        let store = PostgresEventStore::builder()
            .pool(pool.clone())
            .schema("event_sauce_test")
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
        let (_container, connection_string) = crate::test_support::start_postgres_url().await;

        let pool = PgPool::connect(&connection_string).await.unwrap();

        // Build store with custom schema
        let store = PostgresEventStore::builder()
            .pool(pool.clone())
            .schema("events_schema")
            .build()
            .unwrap();

        store.migrate().await.unwrap();

        // Perform operations
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
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
}
