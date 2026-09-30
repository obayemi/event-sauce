//! Event-store migration steps, run under the cross-store migration lock
//! from [`PostgresEventStore::migrate`].

use event_sauce_core::{Error, Result};

use super::PostgresEventStore;

impl PostgresEventStore {
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
    pub async fn migrate(&self) -> Result<()> {
        crate::migrations::with_migration_lock(
            &self.pool,
            &self.schema,
            "_event_sauce_migrations",
            |migrations_table| async move { self.apply_migrations(&migrations_table).await },
        )
        .await
    }

    /// Applies every event-store migration step, run by [`Self::migrate`]
    /// while it holds the cross-store migration lock.
    async fn apply_migrations(&self, migrations_table: &str) -> Result<()> {
        self.migrate_events_and_snapshots(migrations_table).await?;
        self.migrate_crypto_keys(migrations_table).await?;
        self.migrate_aggregate_claims(migrations_table).await?;
        self.migrate_snapshot_schema_version(migrations_table)
            .await?;
        self.migrate_crypto_keys_shredded_at(migrations_table)
            .await?;
        Ok(())
    }

    /// Migration 1: Creates the events and snapshots tables.
    async fn migrate_events_and_snapshots(&self, migrations_table: &str) -> Result<()> {
        let events_table = self.qualify_table("events");
        let snapshots_table = self.qualify_table("snapshots");
        crate::migrations::apply_once(
            &self.pool,
            migrations_table,
            20_250_101_000_000_i64,
            "create_events_table",
            move |pool| async move {
                let create_events = format!(
                    "CREATE TABLE IF NOT EXISTS {events_table} (
                        id BIGSERIAL PRIMARY KEY,
                        event_id UUID NOT NULL UNIQUE,
                        aggregate_id UUID NOT NULL,
                        aggregate_type VARCHAR(255) NOT NULL,
                        event_type VARCHAR(255) NOT NULL,
                        event_version BIGINT NOT NULL,
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
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create events table", e))?;

                let indexes = vec![
                    format!("CREATE INDEX IF NOT EXISTS idx_events_aggregate ON {events_table}(aggregate_id, aggregate_type)"),
                    format!("CREATE INDEX IF NOT EXISTS idx_events_aggregate_version ON {events_table}(aggregate_id, aggregate_type, stream_version)"),
                    format!("CREATE INDEX IF NOT EXISTS idx_events_type ON {events_table}(event_type)"),
                    format!("CREATE INDEX IF NOT EXISTS idx_events_aggregate_type ON {events_table}(aggregate_type)"),
                    format!("CREATE INDEX IF NOT EXISTS idx_events_created_at ON {events_table}(created_at)"),
                    format!("CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON {events_table}(correlation_id) WHERE correlation_id IS NOT NULL"),
                ];
                for index_sql in indexes {
                    sqlx::query(&index_sql)
                        .execute(pool)
                        .await
                        .map_err(|e| Error::backend("Failed to create index", e))?;
                }

                let create_snapshots = format!(
                    "CREATE TABLE IF NOT EXISTS {snapshots_table} (
                        aggregate_id UUID NOT NULL,
                        aggregate_type VARCHAR(255) NOT NULL,
                        snapshot_version BIGINT NOT NULL,
                        snapshot_data JSONB NOT NULL,
                        is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (aggregate_id, aggregate_type)
                    )"
                );
                sqlx::query(&create_snapshots)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create snapshots table", e))?;

                let snapshots_index = format!(
                    "CREATE INDEX IF NOT EXISTS idx_snapshots_type ON {snapshots_table}(aggregate_type)"
                );
                sqlx::query(&snapshots_index)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create snapshots index", e))?;

                Ok(())
            },
        )
        .await
    }

    /// Migration 2: Creates the `crypto_keys` table for per-aggregate encryption keys.
    async fn migrate_crypto_keys(&self, migrations_table: &str) -> Result<()> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        crate::migrations::apply_once(
            &self.pool,
            migrations_table,
            20_250_303_000_000_i64,
            "create_crypto_keys_table",
            move |pool| async move {
                crate::crypto_key_store::create_table(pool, &crypto_keys_table).await
            },
        )
        .await
    }

    /// Migration 3: Creates the `aggregate_claims` table for cross-aggregate uniqueness.
    async fn migrate_aggregate_claims(&self, migrations_table: &str) -> Result<()> {
        let claims_table = self.qualify_table("aggregate_claims");
        crate::migrations::apply_once(
            &self.pool,
            migrations_table,
            20_250_315_000_000_i64,
            "create_aggregate_claims_table",
            move |pool| async move { crate::claims::create_table(pool, &claims_table).await },
        )
        .await
    }

    /// Migration 4: Adds the `snapshot_schema_version` column to `snapshots`.
    ///
    /// Snapshots are a cache, never the source of truth. Stamping each snapshot
    /// with [`Aggregate::snapshot_version()`](event_sauce_core::Aggregate::snapshot_version)
    /// lets a stale snapshot (written before an incompatible state-shape change)
    /// be detected on load and transparently discarded in favour of full event
    /// replay. The column is nullable so existing rows read back as `NULL`,
    /// which the [`SnapshotRow`] mapper interprets as schema version `0`.
    async fn migrate_snapshot_schema_version(&self, migrations_table: &str) -> Result<()> {
        let snapshots_table = self.qualify_table("snapshots");
        crate::migrations::apply_once(
            &self.pool,
            migrations_table,
            20_260_605_000_000_i64,
            "add_snapshot_schema_version_column",
            move |pool| async move {
                let alter = format!(
                    "ALTER TABLE {snapshots_table} ADD COLUMN IF NOT EXISTS snapshot_schema_version BIGINT"
                );
                sqlx::query(&alter)
                    .execute(pool)
                    .await
                    .map_err(|e| {
                        Error::backend("Failed to add snapshot_schema_version column", e)
                    })?;
                Ok(())
            },
        )
        .await
    }

    /// Migration 5: adds the `shredded_at` marker column to `crypto_keys`.
    ///
    /// Callers who never construct their own [`PostgresCryptoKeyStore`] and
    /// call its `migrate()` still get the default key store installed by
    /// [`PostgresEventStoreBuilder::build`](crate::PostgresEventStoreBuilder::build),
    /// so this store's `migrate()` must bring that same table up to date
    /// rather than assume the key store's own migration ran.
    async fn migrate_crypto_keys_shredded_at(&self, migrations_table: &str) -> Result<()> {
        let crypto_keys_table = self.qualify_table("crypto_keys");
        crate::migrations::apply_once(
            &self.pool,
            migrations_table,
            20_260_930_000_000_i64,
            "add_crypto_keys_shredded_at",
            move |pool| async move {
                crate::crypto_key_store::add_shred_state_columns(pool, &crypto_keys_table).await
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{AggregateVersion, EventStore, StreamId};
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::super::tests::create_test_envelope;

    #[tokio::test]
    async fn test_migrate_creates_tables() {
        // Create a fresh database without running migrations
        let (_container, connection_string) = crate::test_support::start_postgres_url().await;

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
        let (_container, connection_string) = crate::test_support::start_postgres_url().await;

        let pool = PgPool::connect(&connection_string).await.unwrap();
        let store = PostgresEventStore::new(pool.clone());

        // Call migrate twice - should not fail (idempotent, including the
        // M4 snapshot_schema_version ADD COLUMN IF NOT EXISTS migration).
        store.migrate().await.unwrap();
        store.migrate().await.unwrap();

        // M4: the snapshots table must carry the schema-version column.
        let column_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_name = 'snapshots'
                  AND column_name = 'snapshot_schema_version'
            )",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            column_exists,
            "snapshots table must have snapshot_schema_version column after migrate()"
        );

        // M4: an old row whose snapshot_schema_version is NULL (as written by a
        // pre-migration store) must load back as schema version 0.
        let legacy_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO event_sauce.snapshots
                (aggregate_id, aggregate_type, snapshot_version, snapshot_data, is_deleted, snapshot_schema_version)
             VALUES ($1, 'User', 1, '{}'::jsonb, FALSE, NULL)",
        )
        .bind(legacy_id)
        .execute(&pool)
        .await
        .unwrap();

        let loaded = store
            .load_snapshot(StreamId::new("User", legacy_id))
            .await
            .unwrap()
            .expect("legacy snapshot row should load");
        assert_eq!(
            loaded.snapshot_schema_version, 0,
            "a NULL snapshot_schema_version must map to schema version 0"
        );

        // Verify we can use the store
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        let result = store
            .append(
                stream_id,
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await;
        assert!(result.is_ok());
    }
}
