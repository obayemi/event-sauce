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
        crate::migrations::qualify(&self.schema, table)
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
        crate::migrations::with_migration_lock(&self.pool, &self.schema, || self.apply_migrations())
            .await
    }

    /// Applies every crypto-key-store migration step, run by [`Self::migrate`]
    /// while it holds the cross-store migration lock.
    async fn apply_migrations(&self) -> Result<()> {
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
        .await?;

        let crypto_keys_table = self.qualify_table("crypto_keys");
        crate::migrations::apply_once(
            &self.pool,
            &migrations_table,
            20_250_301_000_001_i64,
            "add_crypto_keys_shredded_at",
            |pool| async move { add_shredded_at_column(pool, &crypto_keys_table).await },
        )
        .await
    }
}

/// Relaxes `key_data` to nullable and adds the `shredded_at` marker column.
///
/// A shredded key row keeps its `aggregate_id` with `key_data` cleared and
/// `shredded_at` stamped, so a shred is remembered even after the key
/// itself is gone, instead of looking like a row that was never created.
///
/// Both [`PostgresCryptoKeyStore::migrate`] and `PostgresEventStore::migrate`
/// run this step against the same `crypto_keys` table, since a caller who
/// only ever runs the event store's `migrate()` — the default key store's
/// own `migrate()` is never called for them — still needs this column to
/// exist before the first encrypted commit.
pub(crate) async fn add_shredded_at_column(pool: &PgPool, crypto_keys_table: &str) -> Result<()> {
    let alter = format!("ALTER TABLE {crypto_keys_table} ALTER COLUMN key_data DROP NOT NULL");
    sqlx::query(&alter)
        .execute(pool)
        .await
        .map_err(|e| Error::backend("Failed to relax crypto_keys.key_data", e))?;

    let add_column = format!(
        "ALTER TABLE {crypto_keys_table} \
         ADD COLUMN IF NOT EXISTS shredded_at TIMESTAMP WITH TIME ZONE"
    );
    sqlx::query(&add_column)
        .execute(pool)
        .await
        .map_err(|e| Error::backend("Failed to add crypto_keys.shredded_at", e))?;
    Ok(())
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
        let query = format!(
            "SELECT key_data FROM {crypto_keys_table}
             WHERE aggregate_id = $1 AND shredded_at IS NULL"
        );

        let key_data: Option<Vec<u8>> = sqlx::query_scalar(&query)
            .bind(aggregate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to get crypto key", e))?;

        Ok(key_data)
    }

    /// An explicit upsert (key rotation) always wins and un-shreds the
    /// aggregate — only the automatic
    /// [`get_or_insert_key`](CryptoKeyStore::get_or_insert_key) path must
    /// respect a previous shred.
    async fn upsert_key(&self, aggregate_id: Uuid, key: Vec<u8>) -> Result<()> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        let query = format!(
            "INSERT INTO {crypto_keys_table} (aggregate_id, key_data)
             VALUES ($1, $2)
             ON CONFLICT (aggregate_id)
             DO UPDATE SET key_data = $2, shredded_at = NULL"
        );

        sqlx::query(&query)
            .bind(aggregate_id)
            .bind(&key)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to upsert crypto key", e))?;

        Ok(())
    }

    /// Leaves a tombstone rather than deleting the row, so a later
    /// [`get_or_insert_key`](CryptoKeyStore::get_or_insert_key) can see the
    /// aggregate was deliberately shredded instead of never having had a key.
    async fn delete_key(&self, aggregate_id: Uuid) -> Result<()> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        let query = format!(
            "INSERT INTO {crypto_keys_table} (aggregate_id, key_data, shredded_at)
             VALUES ($1, NULL, NOW())
             ON CONFLICT (aggregate_id)
             DO UPDATE SET key_data = NULL, shredded_at = NOW()"
        );

        sqlx::query(&query)
            .bind(aggregate_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to delete crypto key", e))?;

        Ok(())
    }

    /// `DO UPDATE SET key_data = <table>.key_data` is a no-op write that lets
    /// the conflicting row be returned by the same statement, so the insert
    /// and the "someone already won" read happen as one atomic operation.
    /// The conflicting row's `key_data` comes back `NULL` when the aggregate
    /// was shredded, which is reported as `KeyNotFound` rather than handed
    /// back as a bogus empty key.
    async fn get_or_insert_key(&self, aggregate_id: Uuid, candidate: Vec<u8>) -> Result<Vec<u8>> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        let query = format!(
            "INSERT INTO {crypto_keys_table} (aggregate_id, key_data)
             VALUES ($1, $2)
             ON CONFLICT (aggregate_id)
             DO UPDATE SET key_data = {crypto_keys_table}.key_data
             RETURNING key_data"
        );

        let key_data: Option<Vec<u8>> = sqlx::query_scalar(&query)
            .bind(aggregate_id)
            .bind(&candidate)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to get or insert crypto key", e))?;

        key_data.ok_or_else(|| Error::key_not_found(aggregate_id))
    }

    async fn is_shredded(&self, aggregate_id: Uuid) -> Result<bool> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        let query = format!(
            "SELECT EXISTS(
                SELECT 1 FROM {crypto_keys_table}
                WHERE aggregate_id = $1 AND shredded_at IS NOT NULL
             )"
        );

        sqlx::query_scalar(&query)
            .bind(aggregate_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to check shredded status", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::CryptoKeyStore;
    use sqlx::PgPool;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    /// Test database helper using testcontainers.
    struct TestDatabase {
        pool: PgPool,
        #[allow(dead_code)]
        container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let container = crate::test_support::start_postgres()
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
    async fn test_get_or_insert_key_inserts_when_absent() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let id = Uuid::new_v4();
        let candidate = vec![1, 2, 3];

        let winner = store
            .get_or_insert_key(id, candidate.clone())
            .await
            .unwrap();

        assert_eq!(winner, candidate);
        assert_eq!(store.get_key(id).await.unwrap(), Some(candidate));
    }

    #[tokio::test]
    async fn test_get_or_insert_key_returns_existing_key_unchanged() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        let id = Uuid::new_v4();
        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();

        let winner = store.get_or_insert_key(id, vec![9, 9, 9]).await.unwrap();

        assert_eq!(
            winner,
            vec![1, 2, 3],
            "existing key must win over candidate"
        );
        assert_eq!(store.get_key(id).await.unwrap(), Some(vec![1, 2, 3]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_get_or_insert_key_agree_on_one_winner() {
        let db = TestDatabase::new().await.unwrap();
        let store = std::sync::Arc::new(db.store());
        store.migrate().await.unwrap();
        let id = Uuid::new_v4();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(8));

        let handles = (0_u8..8).map(|n| {
            let store = store.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                store.get_or_insert_key(id, vec![n; 16]).await.unwrap()
            })
        });

        let winners: Vec<Vec<u8>> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|joined| joined.unwrap())
            .collect();

        let first = winners[0].clone();
        assert!(
            winners.iter().all(|w| *w == first),
            "every racer must agree on the same winning key, got {winners:?}"
        );
        assert_eq!(store.get_key(id).await.unwrap().unwrap(), first);
    }

    #[tokio::test]
    async fn test_is_shredded_is_false_before_any_delete() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();

        assert!(!store.is_shredded(Uuid::new_v4()).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_key_marks_the_aggregate_shredded() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();
        let id = Uuid::new_v4();
        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();

        store.delete_key(id).await.unwrap();

        assert!(store.is_shredded(id).await.unwrap());
        assert!(store.get_key(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_key_shreds_an_aggregate_with_no_prior_key() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();
        let id = Uuid::new_v4();

        store.delete_key(id).await.unwrap();

        assert!(store.is_shredded(id).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_or_insert_key_fails_for_a_shredded_aggregate() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();
        let id = Uuid::new_v4();
        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();
        store.delete_key(id).await.unwrap();

        let result = store.get_or_insert_key(id, vec![9, 9, 9]).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_key_not_found());
        assert!(store.get_key(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_upsert_key_clears_a_shredded_marker() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        store.migrate().await.unwrap();
        let id = Uuid::new_v4();
        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();
        store.delete_key(id).await.unwrap();

        store.upsert_key(id, vec![4, 5, 6]).await.unwrap();

        assert!(!store.is_shredded(id).await.unwrap());
        assert_eq!(store.get_key(id).await.unwrap(), Some(vec![4, 5, 6]));
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

    mod encrypted_aggregate_through_event_store {
        use super::*;
        use event_sauce_core::{
            Aggregate, AggregateError, AggregateRoot, ApplyEvent, DomainEvent, Entity, EntityId,
            EventApplicator, EventStore, EventVersion, Repository,
        };
        use serde::{Deserialize, Serialize};
        use std::sync::Arc;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct SecretUser {
            id: EntityId,
            email: String,
        }

        impl Entity for SecretUser {
            fn new(id: EntityId) -> Self {
                Self {
                    id,
                    email: String::new(),
                }
            }
            fn entity_id(&self) -> EntityId {
                self.id
            }
        }

        impl event_sauce_core::DefaultEntity for SecretUser {}

        impl Aggregate for SecretUser {
            type Event = SecretUserEvent;
            type Error = SecretUserError;
            type DeletedState = Self;

            fn is_encrypted() -> bool {
                true
            }
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        enum SecretUserEvent {
            Created { email: String },
        }

        impl DomainEvent for SecretUserEvent {
            type Aggregate = SecretUser;
            fn event_type(&self) -> &'static str {
                "SecretUser.Created"
            }
            fn event_version(&self) -> EventVersion {
                EventVersion::new(1)
            }
            fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
                chrono::Utc::now()
            }
        }

        impl ApplyEvent<SecretUser> for SecretUserEvent {
            fn apply(&self, entity: &mut SecretUser) {
                let Self::Created { email } = self;
                entity.email = email.clone();
            }
        }

        impl EventApplicator<SecretUser> for SecretUserEvent {
            fn dispatch(
                &self,
                entity: &mut SecretUser,
            ) -> std::result::Result<(), SecretUserError> {
                ApplyEvent::apply(self, entity);
                Ok(())
            }
            fn dispatch_unchecked(&self, entity: &mut SecretUser) {
                ApplyEvent::apply(self, entity);
            }
        }

        #[derive(Debug, thiserror::Error)]
        #[error("secret user error")]
        struct SecretUserError;
        impl AggregateError for SecretUserError {}

        /// The default key store `PostgresEventStore::builder().build()` installs
        /// must share the same `crypto_keys` table shape that
        /// `PostgresEventStore::migrate()` creates, since callers only ever run
        /// the event store's own `migrate()`.
        #[tokio::test]
        async fn shredding_through_the_default_key_store_round_trips() {
            let db = TestDatabase::new().await.unwrap();
            let store = crate::PostgresEventStore::builder()
                .pool(db.pool().clone())
                .build()
                .unwrap();
            store.migrate().await.unwrap();

            let id = EntityId::new();
            let mut agg = AggregateRoot::<SecretUser>::new(id);
            agg.apply(SecretUserEvent::Created {
                email: "shred-me@example.com".into(),
            })
            .unwrap();
            store.commit(&mut agg).await.unwrap();

            let store = Arc::new(store);
            let loaded = store.repository::<SecretUser>().load(id).await.unwrap();
            assert_eq!(loaded.entity().email, "shred-me@example.com");

            let key_store = crate::PostgresCryptoKeyStore::new(db.pool().clone());
            key_store.delete_key(id.as_uuid()).await.unwrap();

            let mut agg = AggregateRoot::<SecretUser>::new(id);
            agg.apply(SecretUserEvent::Created {
                email: "still-shredded@example.com".into(),
            })
            .unwrap();
            let result = store.commit(&mut agg).await;

            assert!(
                result.unwrap_err().is_key_not_found(),
                "committing after the key is gone must fail instead of re-keying"
            );
        }

        /// A fresh deployment whose replicas only ever run
        /// `PostgresEventStore::migrate()`, all at once, must end up with the
        /// whole schema the default key store relies on, `shredded_at`
        /// included, so its first encrypted commit succeeds and reloads.
        #[tokio::test]
        async fn concurrent_event_store_migrates_install_a_working_crypto_schema() {
            let db = TestDatabase::new().await.unwrap();
            let store = crate::PostgresEventStore::builder()
                .pool(db.pool().clone())
                .build()
                .unwrap();
            let migrations: Vec<_> = (0..4)
                .map(|_| {
                    let replica = store.clone();
                    tokio::spawn(async move { replica.migrate().await })
                })
                .collect();
            for migration in migrations {
                migration.await.unwrap().unwrap();
            }

            let tables: Vec<String> = sqlx::query_scalar(
                "SELECT table_name::text FROM information_schema.tables
                 WHERE table_schema = $1 ORDER BY table_name",
            )
            .bind(store.schema())
            .fetch_all(db.pool())
            .await
            .unwrap();
            assert_eq!(
                tables,
                [
                    "_event_sauce_migrations",
                    "aggregate_claims",
                    "crypto_keys",
                    "events",
                    "snapshots"
                ]
            );
            let has_shredded_at: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema = $1 AND table_name = 'crypto_keys'
                   AND column_name = 'shredded_at')",
            )
            .bind(store.schema())
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert!(has_shredded_at, "crypto_keys must carry shredded_at");

            let store = Arc::new(store);
            let id = EntityId::new();
            let mut agg = AggregateRoot::<SecretUser>::new(id);
            agg.apply(SecretUserEvent::Created {
                email: "fresh-deploy@example.com".into(),
            })
            .unwrap();
            store.commit(&mut agg).await.unwrap();

            let loaded = store.repository::<SecretUser>().load(id).await.unwrap();
            assert_eq!(loaded.entity().email, "fresh-deploy@example.com");
        }
    }
}
