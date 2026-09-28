//! `PostgreSQL` event store implementation.
//!
//! Provides a production-ready `PostgreSQL` implementation of `EventStore`.

use async_trait::async_trait;
use event_sauce_core::{
    AggregateVersion, Error, EventEnvelope, EventStore, EventVersion, Position, Result, Snapshot,
    SnapshotConfig, StreamId,
};
use futures::stream::Stream;
use futures::TryStreamExt;
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
///         .build()?;
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
    append_lock_timeout: std::time::Duration,
    checkpoint_store: Option<std::sync::Arc<dyn event_sauce_core::CheckpointStore>>,
    crypto_key_store: Option<std::sync::Arc<dyn event_sauce_core::CryptoKeyStore>>,
    crypto_provider: Option<std::sync::Arc<dyn event_sauce_core::CryptoProvider>>,
}

/// Default time `append()` waits to acquire the transaction-scoped advisory
/// lock that serializes commit order before giving up with a backend error.
const DEFAULT_APPEND_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Computes the stable advisory-lock key for an event log, derived from the
/// schema-qualified events table name (e.g. `"public.events"`).
///
/// [`PostgresEventStore::append`] takes a transaction-scoped advisory lock on
/// this key before allocating any global event ids, so that id order equals
/// commit order across all streams sharing the table (see
/// [`PostgresEventStore::append`] for the resulting guarantee). Keying on the
/// qualified table name isolates different schemas / test databases — they
/// take distinct keys and never serialize against one another.
///
/// The hash is a 64-bit [FNV-1a] over the UTF-8 bytes of `qualified_events_table`,
/// reinterpreted as the signed `bigint` that `pg_advisory_xact_lock` expects.
/// FNV-1a is used deliberately rather than [`std::hash::DefaultHasher`]: the
/// latter seeds `SipHash` randomly per process, so two processes would compute
/// *different* keys and the cross-process serialization guarantee would
/// silently not hold. The algorithm and input string are therefore part of the
/// store's wire contract and must remain stable.
///
/// [FNV-1a]: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
pub(crate) fn append_lock_key(qualified_events_table: &str) -> i64 {
    crate::migrations::advisory_lock_key(qualified_events_table)
}

/// A commit with nothing to persist: no events, no claims, and no claim
/// clearing. `append`/`append_batch` drop these rather than opening a
/// transaction for them.
fn is_noop_commit(commit: &event_sauce_core::StreamCommit) -> bool {
    commit.events.is_empty() && commit.claims.is_empty() && !commit.clear_claims
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

impl PostgresEventStore {
    /// Creates a new `PostgreSQL` event store with default configuration.
    ///
    /// This is a convenience method that uses the builder with all defaults:
    /// - **Schema**: "`event_sauce`" (isolated from your app)
    /// - **Snapshots**: Every 100 events
    ///
    /// Equivalent to `PostgresEventStore::builder().pool(pool).build()`.
    ///
    /// # Panics
    ///
    /// Cannot panic — the pool is always set before calling `build()`.
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
        Self::builder().pool(pool).build().expect("pool was set")
    }

    /// Creates a new `PostgreSQL` event store with custom snapshot configuration.
    ///
    /// Uses default schema "`event_sauce`".
    ///
    /// # Panics
    ///
    /// Cannot panic — the pool is always set before calling `build()`.
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
    ///     .default_strategy(EveryNEvents::try_new(50).unwrap())
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
            .expect("pool was set")
    }

    /// Creates a `PostgreSQL` event store wired with both snapshot and checkpoint storage.
    ///
    /// Mirrors [`InMemoryEventStore::with_checkpoint_store`](https://docs.rs/event-sauce-memory).
    /// Equivalent to
    /// `PostgresEventStore::builder().pool(pool).snapshot_config(config).checkpoint_store(store).build()`.
    ///
    /// # Panics
    ///
    /// Cannot panic — the pool is always set before calling `build()`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::{PostgresEventStore, PostgresCheckpointStore};
    /// use event_sauce_core::SnapshotConfig;
    /// use sqlx::PgPool;
    /// use std::sync::Arc;
    ///
    /// let pool = PgPool::connect("postgresql://localhost/events").await?;
    /// let checkpoint_store = Arc::new(PostgresCheckpointStore::new(pool.clone()));
    /// let store = PostgresEventStore::with_checkpoint_store(
    ///     pool,
    ///     SnapshotConfig::builder().build(),
    ///     checkpoint_store,
    /// );
    /// ```
    #[must_use]
    pub fn with_checkpoint_store(
        pool: PgPool,
        snapshot_config: SnapshotConfig,
        checkpoint_store: std::sync::Arc<dyn event_sauce_core::CheckpointStore>,
    ) -> Self {
        Self::builder()
            .pool(pool)
            .snapshot_config(snapshot_config)
            .checkpoint_store(checkpoint_store)
            .build()
            .expect("pool was set")
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
    ///     .build()?;
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
    ///     .build()?;
    ///
    /// assert_eq!(store.schema(), "my_schema");
    /// ```
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns a schema-qualified table name (e.g., "schema.events").
    fn qualify_table(&self, table: &str) -> String {
        crate::migrations::qualify(&self.schema, table)
    }

    /// Returns the `LISTEN`/`NOTIFY` channel name used by this store.
    ///
    /// Derived from the schema (`event_sauce_events_<schema>`) so multiple
    /// event-sauce instances sharing a database but using different schemas
    /// stay isolated.
    #[must_use]
    pub fn notify_channel(&self) -> String {
        format!("event_sauce_events_{}", self.schema)
    }

    /// Fetches a bounded batch of events with `id > from_position`, ordered by `id`.
    ///
    /// Unlike [`stream_all`](EventStore::stream_all), this method runs as a
    /// single query that releases its pooled connection as soon as the
    /// returned vector is materialized — there is no long-lived cursor that
    /// holds a connection for the duration of consumption. Projection runners
    /// drive a loop of these to drain new events without occupying a
    /// connection between transactions.
    ///
    /// Each returned [`EventLogEntry`](event_sauce_core::EventLogEntry) pairs
    /// the event's global [`Position`] (its `id`) with the envelope. `limit`
    /// caps the batch size; pass a small value (e.g. 500) to keep memory
    /// bounded. The caller advances `from_position` to the last entry's
    /// position after each batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying SQL query fails.
    pub async fn fetch_events_batch(
        &self,
        from_position: Position,
        limit: i64,
    ) -> Result<Vec<event_sauce_core::EventLogEntry>> {
        let events_table = self.qualify_table("events");
        let query = format!(
            "SELECT {EVENT_COLUMNS}
             FROM {events_table}
             WHERE id > $1
             ORDER BY id ASC
             LIMIT $2"
        );

        let rows: Vec<EventRow> = sqlx::query_as::<_, EventRow>(&query)
            .bind(from_position.as_i64())
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to fetch event batch", e))?;

        Ok(rows.into_iter().map(EventRow::log_entry).collect())
    }

    /// Subscribes to `NOTIFY` wake-ups for new event commits.
    ///
    /// Returns a stream that yields a [`Position`] each time a transaction
    /// commits at least one event. The yielded position is the global `id`
    /// of the last event written by that transaction, intended as a hint:
    /// readers can skip waking up if they have already streamed past it.
    ///
    /// The stream owns its own pooled connection (sqlx [`PgListener`]) — it
    /// does not return rows from the connection pool until dropped, so prefer
    /// holding it for the lifetime of a worker rather than acquiring it per
    /// poll. Combine with [`stream_all`](EventStore::stream_all) to drain
    /// missed events on each wake-up.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener cannot connect or fails to subscribe
    /// to the notification channel.
    ///
    /// [`PgListener`]: sqlx::postgres::PgListener
    pub async fn listen_for_events(
        &self,
    ) -> Result<impl futures::Stream<Item = Result<Position>> + Send> {
        use sqlx::postgres::PgListener;

        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to connect NOTIFY listener", e))?;

        let channel = self.notify_channel();
        listener
            .listen(&channel)
            .await
            .map_err(|e| Error::backend("Failed to LISTEN on NOTIFY channel", e))?;

        Ok(async_stream::stream! {
            loop {
                match listener.recv().await {
                    Ok(notification) => {
                        let payload = notification.payload();
                        match payload.parse::<i64>() {
                            Ok(pos) => yield Ok(Position::new(pos)),
                            Err(parse_err) => {
                                yield Err(Error::backend(
                                    format!("Invalid NOTIFY payload: {payload}"),
                                    parse_err,
                                ));
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(Error::backend("NOTIFY listener error", e));
                        return;
                    }
                }
            }
        })
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
    pub async fn migrate(&self) -> Result<()> {
        crate::migrations::with_migration_lock(&self.pool, &self.schema, || self.apply_migrations())
            .await
    }

    /// Applies every event-store migration step, run by [`Self::migrate`]
    /// while it holds the cross-store migration lock.
    async fn apply_migrations(&self) -> Result<()> {
        crate::migrations::ensure_schema(&self.pool, &self.schema).await?;

        let migrations_table = self.qualify_table("_event_sauce_migrations");
        crate::migrations::ensure_migrations_table(&self.pool, &migrations_table).await?;

        self.migrate_events_and_snapshots(&migrations_table).await?;
        self.migrate_crypto_keys(&migrations_table).await?;
        self.migrate_aggregate_claims(&migrations_table).await?;
        self.migrate_snapshot_schema_version(&migrations_table)
            .await?;
        self.migrate_crypto_keys_shredded_at(&migrations_table)
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
                crate::crypto_key_store::add_shredded_at_column(pool, &crypto_keys_table).await
            },
        )
        .await
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

    /// Sets how long [`append`](PostgresEventStore::append) waits to acquire the
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

impl PostgresEventStore {
    /// Reads the current committed stream version inside the given transaction.
    ///
    /// Computes `MAX(stream_version) + 1` for the stream, treating an empty
    /// stream (`NULL`) as [`AggregateVersion::initial`]. Runs against the
    /// transaction so callers observe their own uncommitted writes and any
    /// committed concurrent writes under READ COMMITTED.
    async fn current_stream_version(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        events_table: &str,
        stream_id: &StreamId,
    ) -> Result<AggregateVersion> {
        let query = format!(
            "SELECT MAX(stream_version) FROM {events_table} WHERE aggregate_id = $1 AND aggregate_type = $2"
        );
        let current_version: Option<i64> = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type().as_str())
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to check version", e))?;

        Ok(AggregateVersion::new(current_version.unwrap_or(-1) + 1))
    }
}

impl PostgresEventStore {
    /// Takes the per-log append serialization advisory lock inside the given
    /// transaction.
    ///
    /// Serializes id-assignment-to-commit across ALL streams sharing this log.
    /// The `events.id` BIGSERIAL is allocated at INSERT but the row only becomes
    /// visible at COMMIT, so under READ COMMITTED two concurrent appends to
    /// different streams can take ids N and N+1 yet commit in the opposite order.
    /// A checkpoint reader scanning `WHERE id > checkpoint ORDER BY id ASC` could
    /// then observe N+1, advance its checkpoint past it, and never see N once it
    /// commits — silent, permanent event loss. Taking a transaction-scoped
    /// advisory lock here, before allocating any id, forces insert order to equal
    /// commit order. `pg_advisory_xact_lock` auto-releases at commit/rollback (no
    /// manual unlock). Readers stay fully concurrent: the advisory lock does not
    /// block SELECT. Only id-allocating appends take it — claims-only /
    /// clear-only writes allocate no ids and must not serialize on it.
    async fn acquire_append_lock(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        events_table: &str,
    ) -> Result<()> {
        #[allow(clippy::cast_possible_truncation)]
        let lock_timeout_ms = self.append_lock_timeout.as_millis() as i64;
        sqlx::query(&format!("SET LOCAL lock_timeout = {lock_timeout_ms}"))
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to set append lock timeout", e))?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(append_lock_key(events_table))
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to acquire append serialization lock", e))?;
        Ok(())
    }

    /// Persists one stream's events and claims inside an already-open
    /// transaction that already holds the append lock.
    ///
    /// Runs the version precheck, inserts each event (translating a unique
    /// violation into a typed `ConcurrencyConflict`), and applies claims. Returns
    /// the global id of the last inserted event (0 if this commit had no events),
    /// so the caller can emit a single NOTIFY for the whole transaction.
    ///
    /// Does NOT begin/commit the transaction or emit NOTIFY — those are the
    /// caller's responsibility so that `append` and `append_batch` can share this
    /// body while controlling transaction and notification scope.
    async fn write_commit_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        events_table: &str,
        commit: event_sauce_core::StreamCommit,
    ) -> Result<i64> {
        let event_sauce_core::StreamCommit {
            stream_id,
            events,
            expected_version,
            claims,
            clear_claims,
        } = commit;
        let mut last_inserted_id: i64 = 0;

        if !events.is_empty() {
            let current_version =
                Self::current_stream_version(tx, events_table, &stream_id).await?;

            if current_version != expected_version {
                return Err(Error::concurrency_conflict(
                    expected_version,
                    current_version,
                ));
            }

            for (idx, event) in events.iter().enumerate() {
                #[allow(clippy::cast_possible_wrap)]
                let stream_version = expected_version.as_i64() + idx as i64;
                let event_version_i64 = event.event_version.as_i64();

                let insert_query = format!(
                    "INSERT INTO {events_table} (
                        event_id, aggregate_id, aggregate_type, event_type, event_version,
                        event_data, stream_version, created_by, correlation_id, causation_id, metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    RETURNING id"
                );

                let insert_result = sqlx::query_scalar(&insert_query)
                    .bind(event.id)
                    .bind(event.aggregate_id)
                    .bind(event.aggregate_type.as_str())
                    .bind(&event.event_type)
                    .bind(event_version_i64)
                    .bind(&event.event_data)
                    .bind(stream_version)
                    .bind(event.created_by)
                    .bind(event.metadata.as_ref().and_then(|m| m.correlation_id))
                    .bind(event.metadata.as_ref().and_then(|m| m.causation_id))
                    .bind(event.metadata.as_ref().and_then(|m| m.additional.clone()))
                    .fetch_one(&mut **tx)
                    .await;

                last_inserted_id = match insert_result {
                    Ok(id) => id,
                    // Two concurrent appends can both pass the MAX(stream_version)
                    // precheck under READ COMMITTED (each sees an empty committed
                    // stream), then the loser violates the
                    // UNIQUE(aggregate_id, aggregate_type, stream_version) index.
                    // Surface that as a typed conflict rather than a generic
                    // backend error, matching the in-memory backend and letting
                    // callers drive an optimistic-retry loop. The violation aborts
                    // this transaction (any further query in it would fail with
                    // 25P02), so we cannot re-read the committed version here. The
                    // winner committed at `expected_version`, so the true current
                    // version is at least one beyond it — report that lower bound.
                    Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
                        let actual = AggregateVersion::new(expected_version.as_i64() + 1);
                        return Err(Error::concurrency_conflict(expected_version, actual));
                    }
                    Err(e) => return Err(Error::backend("Failed to insert event", e)),
                };
            }
        }

        // Handle claims
        crate::claims::enforce(
            tx,
            &self.qualify_table("aggregate_claims"),
            &stream_id,
            claims,
            clear_claims,
        )
        .await?;

        Ok(last_inserted_id)
    }

    /// Emits a single NOTIFY carrying the highest inserted id for the
    /// transaction. Postgres holds notifications until commit, so this fires only
    /// if the transaction succeeds. A `last_inserted_id` of 0 means no events
    /// were inserted (claims-only / clear-only) — nothing to notify.
    async fn notify_inserted(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        last_inserted_id: i64,
    ) -> Result<()> {
        if last_inserted_id == 0 {
            return Ok(());
        }
        let channel = self.notify_channel();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&channel)
            .bind(last_inserted_id.to_string())
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to issue NOTIFY", e))?;
        Ok(())
    }
}

#[async_trait]
impl EventStore for PostgresEventStore {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        claims: Vec<event_sauce_core::AggregateClaim>,
        clear_claims: bool,
    ) -> Result<()> {
        self.append_batch(vec![event_sauce_core::StreamCommit {
            stream_id,
            events,
            expected_version,
            claims,
            clear_claims,
        }])
        .await
    }

    async fn append_batch(&self, commits: Vec<event_sauce_core::StreamCommit>) -> Result<()> {
        let commits: Vec<event_sauce_core::StreamCommit> =
            commits.into_iter().filter(|c| !is_noop_commit(c)).collect();
        if commits.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::backend("Failed to start transaction", e))?;

        let events_table = self.qualify_table("events");

        // Take the append serialization lock ONCE for the whole batch if any
        // commit allocates ids. Holding it across all streams keeps the
        // insert-order = commit-order invariant for the batch.
        if commits.iter().any(|c| !c.events.is_empty()) {
            self.acquire_append_lock(&mut tx, &events_table).await?;
        }

        // Process every commit in the same transaction; the first conflict (or
        // any error) propagates and rolls the whole batch back. Track the highest
        // inserted id across the batch for a single NOTIFY.
        let mut max_inserted_id: i64 = 0;
        for commit in commits {
            let last = self
                .write_commit_in_tx(&mut tx, &events_table, commit)
                .await?;
            max_inserted_id = max_inserted_id.max(last);
        }

        self.notify_inserted(&mut tx, max_inserted_id).await?;

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit transaction", e))?;

        Ok(())
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        let events_table = self.qualify_table("events");
        let query = format!(
            "SELECT {EVENT_COLUMNS}
             FROM {events_table}
             WHERE aggregate_id = $1 AND aggregate_type = $2 AND stream_version >= $3
             ORDER BY stream_version ASC"
        );

        let pool = self.pool.clone();
        let aggregate_id = stream_id.aggregate_id();
        let aggregate_type = stream_id.aggregate_type().as_str().to_string();
        let from_version_i64 = from_version.as_i64();

        Ok(Box::pin(async_stream::try_stream! {
            let mut rows = sqlx::query_as::<_, EventRow>(&query)
                .bind(aggregate_id)
                .bind(&aggregate_type)
                .bind(from_version_i64)
                .fetch(&pool);
            while let Some(row) = rows.try_next().await
                .map_err(|e| Error::backend("Failed to load stream", e))? {
                yield EventEnvelope::from(row);
            }
        }))
    }

    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<event_sauce_core::EventLogEntry>> + Send> {
        let events_table = self.qualify_table("events");
        let query = format!(
            "SELECT {EVENT_COLUMNS}
             FROM {events_table}
             WHERE id > $1
             ORDER BY id ASC"
        );

        let pool = self.pool.clone();
        let from = from_position.as_i64();

        Ok(Box::pin(async_stream::try_stream! {
            let mut rows = sqlx::query_as::<_, EventRow>(&query)
                .bind(from)
                .fetch(&pool);
            while let Some(row) = rows.try_next().await
                .map_err(|e| Error::backend("Failed to stream all", e))? {
                yield row.log_entry();
            }
        }))
    }

    async fn max_position(&self) -> Result<Position> {
        let events_table = self.qualify_table("events");
        let query = format!("SELECT COALESCE(MAX(id), 0) FROM {events_table}");

        let max: i64 = sqlx::query_scalar(&query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to fetch max position", e))?;

        Ok(Position::new(max))
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        let events_table = self.qualify_table("events");
        let query = format!(
            "SELECT MAX(stream_version) FROM {events_table} WHERE aggregate_id = $1 AND aggregate_type = $2"
        );

        let version: Option<i64> = sqlx::query_scalar(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type().as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to get version", e))?;

        let next_version = version.map_or(0, |v| v + 1);
        Ok(AggregateVersion::new(next_version))
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let snapshots_table = self.qualify_table("snapshots");
        let query = format!(
            "INSERT INTO {snapshots_table} AS s (aggregate_id, aggregate_type, snapshot_version, snapshot_data, is_deleted, snapshot_schema_version)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (aggregate_id, aggregate_type)
             DO UPDATE SET snapshot_version = $3, snapshot_data = $4, is_deleted = $5, snapshot_schema_version = $6, created_at = NOW()
             WHERE s.snapshot_version <= EXCLUDED.snapshot_version"
        );

        let snapshot_version_i64 = snapshot.snapshot_version.as_i64();
        let snapshot_schema_version_i64 = i64::from(snapshot.snapshot_schema_version);
        sqlx::query(&query)
            .bind(snapshot.aggregate_id)
            .bind(snapshot.aggregate_type.as_str())
            .bind(snapshot_version_i64)
            .bind(&snapshot.snapshot_data)
            .bind(snapshot.is_deleted)
            .bind(snapshot_schema_version_i64)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to save snapshot", e))?;

        Ok(())
    }

    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>> {
        let snapshots_table = self.qualify_table("snapshots");
        let query = format!(
            "SELECT aggregate_id, aggregate_type, snapshot_version, snapshot_data, is_deleted, snapshot_schema_version
             FROM {snapshots_table}
             WHERE aggregate_id = $1 AND aggregate_type = $2"
        );

        let row: Option<SnapshotRow> = sqlx::query_as::<_, SnapshotRow>(&query)
            .bind(stream_id.aggregate_id())
            .bind(stream_id.aggregate_type().as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to load snapshot", e))?;

        Ok(row.map(Into::into))
    }

    fn snapshot_config(&self) -> &SnapshotConfig {
        &self.snapshot_config
    }

    fn checkpoint_store(&self) -> Option<std::sync::Arc<dyn event_sauce_core::CheckpointStore>> {
        self.checkpoint_store.clone()
    }

    fn crypto_key_store(&self) -> Option<&dyn event_sauce_core::CryptoKeyStore> {
        self.crypto_key_store.as_deref()
    }

    fn crypto_provider(&self) -> Option<&dyn event_sauce_core::CryptoProvider> {
        self.crypto_provider.as_deref()
    }
}

impl PostgresEventStore {
    /// Creates a [`PostgresEventLogQuery`](crate::PostgresEventLogQuery) for this event store.
    ///
    /// The returned query object uses the same connection pool and schema as this store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::EventLogQuery;
    ///
    /// let log_query = store.event_log_query();
    /// let page = log_query.query_events(Default::default()).await?;
    /// ```
    #[must_use]
    pub fn event_log_query(&self) -> crate::PostgresEventLogQuery {
        crate::PostgresEventLogQuery::new(self.pool.clone(), self.schema.clone())
    }

    /// Counts events in a stream using an optimized SQL COUNT(*) query.
    ///
    /// This is a Postgres-specific optimization that's much faster than
    /// counting events by streaming them, especially for streams with many events.
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
            .bind(stream_id.aggregate_type().as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| event_sauce_core::Error::backend("Failed to count events", e))?;

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        Ok(count as usize)
    }
}

/// Every column [`EventRow`] decodes by name, including the global `id` —
/// shared by every query that reads events (per-stream, the global log, and
/// the log-query filters), so a query can never omit one.
pub(crate) const EVENT_COLUMNS: &str = "id, event_id, aggregate_id, aggregate_type, event_type, \
     event_version, event_data, created_by, created_at, correlation_id, causation_id, metadata";

// Database row types
#[derive(sqlx::FromRow)]
pub(crate) struct EventRow {
    /// Global BIGSERIAL position, read solely through [`EventRow::log_entry`].
    id: i64,
    event_id: uuid::Uuid,
    aggregate_id: uuid::Uuid,
    aggregate_type: String,
    event_type: String,
    event_version: i64,
    event_data: serde_json::Value,
    created_by: Option<uuid::Uuid>,
    created_at: chrono::DateTime<chrono::Utc>,
    correlation_id: Option<uuid::Uuid>,
    causation_id: Option<uuid::Uuid>,
    metadata: Option<serde_json::Value>,
}

impl EventRow {
    /// Converts a row that includes the global `id` column into an
    /// [`EventLogEntry`](event_sauce_core::EventLogEntry), pairing the
    /// store-issued [`Position`] with the envelope.
    pub(crate) fn log_entry(self) -> event_sauce_core::EventLogEntry {
        event_sauce_core::EventLogEntry {
            position: Position::new(self.id),
            envelope: EventEnvelope::from(self),
        }
    }
}

impl From<EventRow> for EventEnvelope {
    fn from(row: EventRow) -> Self {
        let metadata =
            if row.correlation_id.is_some() || row.causation_id.is_some() || row.metadata.is_some()
            {
                Some(event_sauce_core::EventMetadata {
                    correlation_id: row.correlation_id,
                    causation_id: row.causation_id,
                    causation_chain: Vec::new(),
                    timestamp: row.created_at,
                    additional: row.metadata,
                })
            } else {
                None
            };

        EventEnvelope {
            id: row.event_id,
            aggregate_id: row.aggregate_id,
            aggregate_type: event_sauce_core::AggregateType::from_owned(row.aggregate_type),
            event_type: row.event_type,
            event_version: EventVersion::new(row.event_version),
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
    is_deleted: bool,
    /// Nullable: rows written before the `snapshot_schema_version` migration
    /// read back as `NULL`, which maps to schema version `0`.
    snapshot_schema_version: Option<i64>,
}

impl From<SnapshotRow> for Snapshot {
    fn from(row: SnapshotRow) -> Self {
        let snapshot_version = AggregateVersion::new(row.snapshot_version);
        // NULL (pre-migration rows) and any negative value defensively map to 0,
        // the default schema version. `u32::try_from` rejects negatives, and
        // `unwrap_or(0)` covers the `None` case.
        let snapshot_schema_version = row
            .snapshot_schema_version
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0);

        Snapshot {
            aggregate_id: row.aggregate_id,
            aggregate_type: event_sauce_core::AggregateType::from_owned(row.aggregate_type),
            snapshot_version,
            snapshot_data: row.snapshot_data,
            is_deleted: row.is_deleted,
            snapshot_schema_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{
        AggregateVersion, EventEnvelope, EventStore, EventVersion, Position, Snapshot, StreamId,
    };
    use futures::StreamExt;
    use serde_json::json;
    use sqlx::PgPool;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    /// Test database helper using testcontainers for isolated `PostgreSQL` testing.
    struct TestDatabase {
        pool: PgPool,
        #[allow(dead_code)]
        container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDatabase {
        /// Creates a new test database with testcontainers, migrated with the
        /// store's real [`PostgresEventStore::migrate`] rather than a
        /// hand-synced copy of its schema, so tests exercise the exact
        /// schema production gets.
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

            PostgresEventStore::builder()
                .pool(pool.clone())
                .schema("public")
                .build()
                .expect("pool was set")
                .migrate()
                .await?;

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
                .expect("pool was set")
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
            EventVersion::new(1),
            json!({"data": "test"}),
        )
    }

    #[tokio::test]
    async fn test_create_store() {
        let db = TestDatabase::new().await.unwrap();
        let _store = db.store();
    }

    /// Regression test for XN-7: several nodes calling `migrate()` at once
    /// against a fresh database (e.g. a deploy with several replicas starting
    /// together) must not crash any of them. Each store shares one pool but
    /// is its own `PostgresEventStore`, matching independent processes hitting
    /// the same database concurrently.
    #[tokio::test]
    async fn test_concurrent_migrate_calls_do_not_crash() {
        let container = crate::test_support::start_postgres().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
        let pool = PgPool::connect(&url).await.unwrap();

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = PostgresEventStore::builder()
                    .pool(pool.clone())
                    .schema("public")
                    .build()
                    .expect("pool was set");
                tokio::spawn(async move { store.migrate().await })
            })
            .collect();

        for handle in handles {
            handle
                .await
                .expect("migrate task must not panic")
                .expect("concurrent migrate() must not fail");
        }
    }

    #[tokio::test]
    async fn test_append_to_new_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let event = create_test_envelope("UserCreated", stream_id.aggregate_id());

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

    #[tokio::test]
    async fn test_load_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(
                stream_id.clone(),
                vec![event.clone()],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let mut stream = store
            .load_stream(stream_id, AggregateVersion::initial())
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
        assert_eq!(version, AggregateVersion::initial());
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
            .append(
                stream_id.clone(),
                vec![event1],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // Try to append with wrong version - should fail
        let result = store
            .append(
                stream_id,
                vec![event2],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_concurrency_conflict());
    }

    /// Regression test for F3 (H1 + M9): a genuine concurrent INSERT that
    /// passes the `SELECT MAX(stream_version)` precheck but loses the race on
    /// the `UNIQUE(aggregate_id, aggregate_type, stream_version)` index must be
    /// surfaced as a [`Error::ConcurrencyConflict`], not a generic
    /// [`Error::Backend`].
    ///
    /// The interleave is forced deterministically:
    /// 1. A seeding transaction inserts a duplicate row at `stream_version = 0`
    ///    and is held open (uncommitted), so the row is invisible to other
    ///    transactions but the unique index already reserves the slot.
    /// 2. The real `store.append(...)` is invoked for the "loser" at
    ///    `expected_version = initial()`. Its precheck still sees an empty
    ///    committed stream (it passes), then its INSERT blocks on the unique
    ///    index.
    /// 3. The seeding transaction commits, releasing the loser's blocked INSERT
    ///    which then fails with SQLSTATE 23505.
    ///
    /// Today this maps to `Error::Backend` so `is_concurrency_conflict()` is
    /// false and the documented retry loop never fires on Postgres.
    #[tokio::test]
    async fn test_unique_violation_race_is_concurrency_conflict() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let loser_event = create_test_envelope("UserCreated", aggregate_id);

        // Step 1: seeding transaction reserves (aggregate_id, "User", 0) but
        // does NOT commit yet. The unique index now blocks any other INSERT
        // targeting the same key, while the row remains invisible to a
        // separate transaction's MAX(stream_version) precheck.
        let mut seeder = db.pool().begin().await.unwrap();
        sqlx::query(
            "INSERT INTO events (
                event_id, aggregate_id, aggregate_type, event_type, event_version,
                event_data, stream_version
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(Uuid::new_v4())
        .bind(aggregate_id)
        .bind("User")
        .bind("UserCreated")
        .bind(1_i64)
        .bind(json!({"data": "seed"}))
        .bind(0_i64)
        .execute(&mut *seeder)
        .await
        .unwrap();

        // Step 2: spawn the loser. Its precheck sees an empty committed stream
        // (the seeder row is still invisible), so it passes the version check
        // and then BLOCKS on the unique index INSERT.
        let loser = tokio::spawn(async move {
            store
                .append(
                    stream_id,
                    vec![loser_event],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
        });

        // Give the loser time to reach (and block on) its INSERT. The held-open
        // seeder guarantees correctness regardless; this sleep only affects
        // timing.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Step 3: commit the seeder, releasing the loser's blocked INSERT which
        // then fails with SQLSTATE 23505 (duplicate key on the unique index).
        seeder.commit().await.unwrap();

        let result = loser.await.unwrap();
        let err = result.expect_err("loser append must fail on the unique violation race");
        assert!(
            err.is_concurrency_conflict(),
            "expected ConcurrencyConflict for a lost unique-violation race, got: {err:?}"
        );
    }

    /// Negative companion to the race test: a non-unique INSERT failure must
    /// still map to [`Error::Backend`]. This guards against the planned
    /// `is_unique_violation()` narrowing accidentally swallowing unrelated
    /// database errors.
    ///
    /// Here the store is pointed at a schema with no `events` table, so the
    /// `append` INSERT fails with "relation does not exist" (SQLSTATE 42P01),
    /// which is a backend error, not a concurrency conflict.
    #[tokio::test]
    async fn test_non_unique_insert_failure_is_backend() {
        let db = TestDatabase::new().await.unwrap();
        // A schema that exists but does not contain the events table.
        sqlx::query("CREATE SCHEMA IF NOT EXISTS empty_schema")
            .execute(db.pool())
            .await
            .unwrap();

        let store = PostgresEventStore::builder()
            .pool(db.pool().clone())
            .schema("empty_schema")
            .build()
            .expect("pool was set");

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

        let err = result.expect_err("append must fail against a schema without an events table");
        assert!(
            err.is_backend(),
            "expected Backend error for a non-unique failure, got: {err:?}"
        );
        assert!(
            !err.is_concurrency_conflict(),
            "a missing-table error must not be reported as a concurrency conflict: {err:?}"
        );
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
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
        store
            .append(
                stream2,
                vec![create_test_envelope_with_type("OrderPlaced", "Order", id2)],
                AggregateVersion::initial(),
                vec![],
                false,
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

        let snapshot = Snapshot::new_with_schema_version(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(10),
            json!({"name": "Alice"}),
            4,
        );

        store.save_snapshot(snapshot.clone()).await.unwrap();
        let loaded = store.load_snapshot(stream_id).await.unwrap();

        let loaded = loaded.expect("snapshot should load");
        assert_eq!(loaded.snapshot_version, AggregateVersion::new(10));
        assert_eq!(
            loaded.snapshot_schema_version, 4,
            "the schema version must round-trip through save/load_snapshot"
        );
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
            .append(
                stream_id.clone(),
                events,
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(3));
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
                .append(
                    stream_id.clone(),
                    vec![event],
                    AggregateVersion::new(i),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        // Load from version 2
        let stream = store
            .load_stream(stream_id, AggregateVersion::new(2))
            .await
            .unwrap();
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
                .append(
                    stream_id,
                    vec![event],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        // Stream events whose id is strictly greater than 2. The five appends
        // get dense ids 1..=5 in a fresh database, so this leaves 3, 4, 5 and
        // each entry carries its real BIGSERIAL id as the position.
        let stream = store.stream_all(Position::new(2)).await.unwrap();
        let entries: Vec<_> = stream.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq!(entries.len(), 3);
        let positions: Vec<i64> = entries.iter().map(|e| e.position.as_i64()).collect();
        assert_eq!(positions, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn test_stream_all_surfaces_real_bigserial_id_as_position() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);

        store
            .append(
                stream_id,
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // The first (and only) entry's position must equal the row's real
        // BIGSERIAL id, not a reconstructed ordinal.
        let real_id: i64 = sqlx::query_scalar("SELECT id FROM public.events")
            .fetch_one(store.pool())
            .await
            .unwrap();

        let stream = store.stream_all(Position::start()).await.unwrap();
        let entries: Vec<_> = stream.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].position, Position::new(real_id));
    }

    #[tokio::test]
    async fn test_max_position_empty_and_after_gap() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();

        // Empty store: max position is the start sentinel.
        assert_eq!(store.max_position().await.unwrap(), Position::start());

        // Append two events (ids 1, 2), then burn id 3 by inserting and
        // deleting a row so the sequence advances past a hole.
        for _ in 0..2 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("User", aggregate_id);
            let event = create_test_envelope("UserCreated", aggregate_id);
            store
                .append(
                    stream_id,
                    vec![event],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        let burned: i64 = sqlx::query_scalar(
            "INSERT INTO public.events (
                event_id, aggregate_id, aggregate_type, event_type, event_version,
                event_data, stream_version
            ) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
        )
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .bind("Burner")
        .bind("UserCreated")
        .bind(1_i64)
        .bind(json!({"data": "burn"}))
        .bind(0_i64)
        .fetch_one(store.pool())
        .await
        .unwrap();
        sqlx::query("DELETE FROM public.events WHERE id = $1")
            .bind(burned)
            .execute(store.pool())
            .await
            .unwrap();

        // A final committed event lands above the gap.
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);
        let event = create_test_envelope("UserCreated", aggregate_id);
        store
            .append(
                stream_id,
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // max_position is the real MAX(id) over committed rows, above the gap.
        let real_max: i64 = sqlx::query_scalar("SELECT MAX(id) FROM public.events")
            .fetch_one(store.pool())
            .await
            .unwrap();
        let store_max = store.max_position().await.unwrap();
        assert_eq!(store_max, Position::new(real_max));
        assert!(store_max.as_i64() > burned, "max must sit above the gap");

        // The postgres override and the generic stream-and-find-last default
        // must agree on the same scenario.
        let stream = store.stream_all(Position::start()).await.unwrap();
        let generic_max = stream
            .map(Result::unwrap)
            .fold(Position::start(), |_, e| async move { e.position })
            .await;
        assert_eq!(store_max, generic_max);
    }

    #[tokio::test]
    async fn test_load_nonexistent_stream() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        let stream = store
            .load_stream(stream_id, AggregateVersion::initial())
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
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // Read via clone - should see the same data (same database)
        let version = store_clone.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));
    }

    #[tokio::test]
    async fn test_version_increments_correctly() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        // Initial version
        let v0 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v0, AggregateVersion::initial());

        // After first append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event1", aggregate_id)],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
        let v1 = store.get_version(stream_id.clone()).await.unwrap();
        assert_eq!(v1, AggregateVersion::new(1));

        // After second append
        store
            .append(
                stream_id.clone(),
                vec![create_test_envelope("Event2", aggregate_id)],
                AggregateVersion::new(1),
                vec![],
                false,
            )
            .await
            .unwrap();
        let v2 = store.get_version(stream_id).await.unwrap();
        assert_eq!(v2, AggregateVersion::new(2));
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
            AggregateVersion::new(5),
            json!({"version": 1}),
        );
        store.save_snapshot(snapshot1).await.unwrap();

        // Save second snapshot (should overwrite via UPSERT)
        let snapshot2 = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(10),
            json!({"version": 2}),
        );
        store.save_snapshot(snapshot2).await.unwrap();

        // Should get the latest snapshot
        let loaded = store.load_snapshot(stream_id).await.unwrap().unwrap();
        assert_eq!(loaded.snapshot_version, AggregateVersion::new(10));
    }

    /// Regression test for XN-15: a lagging node saving an older snapshot
    /// after a newer one has already been saved must not regress it. Snapshots
    /// are only a replay-avoidance cache, but overwriting a newer one wastes
    /// exactly the events it was meant to skip.
    #[tokio::test]
    async fn test_older_snapshot_does_not_regress_newer_one() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("User", aggregate_id);

        let newer = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(200),
            json!({"version": 200}),
        );
        store.save_snapshot(newer).await.unwrap();

        let lagging = Snapshot::new(
            aggregate_id,
            "User".to_string(),
            AggregateVersion::new(100),
            json!({"version": 100}),
        );
        store.save_snapshot(lagging).await.unwrap();

        let loaded = store.load_snapshot(stream_id).await.unwrap().unwrap();
        assert_eq!(
            loaded.snapshot_version,
            AggregateVersion::new(200),
            "a lagging snapshot save must not regress a newer committed snapshot"
        );
    }

    #[tokio::test]
    async fn test_append_empty_events() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        let stream_id = StreamId::new("User", Uuid::new_v4());

        // Appending empty events should succeed (no-op)
        let result = store
            .append(
                stream_id,
                vec![],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await;
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
            AggregateVersion::initial(),
            vec![],
            false,
        );
        let result2 = store.append(
            stream2.clone(),
            vec![create_test_envelope("Event2", id2)],
            AggregateVersion::initial(),
            vec![],
            false,
        );

        let (r1, r2) = tokio::join!(result1, result2);
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    #[tokio::test]
    async fn test_migrate_creates_tables() {
        // Create a fresh database without running migrations
        let container = crate::test_support::start_postgres().await.unwrap();

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
        let container = crate::test_support::start_postgres().await.unwrap();

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let connection_string = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

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
        let container = crate::test_support::start_postgres().await.unwrap();

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let connection_string = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

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
        let container = crate::test_support::start_postgres().await.unwrap();

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let connection_string = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

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
                    AggregateVersion::new(i),
                    vec![],
                    false,
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
                    AggregateVersion::new(i),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        // Compare optimized count against manual stream count
        let fast_count = store.count_events_fast(stream_id.clone()).await.unwrap();

        let event_stream = store
            .load_stream(stream_id, AggregateVersion::initial())
            .await
            .unwrap();
        futures::pin_mut!(event_stream);
        let mut manual_count = 0;
        while let Some(result) = futures::StreamExt::next(&mut event_stream).await {
            result.unwrap();
            manual_count += 1;
        }

        assert_eq!(
            fast_count, manual_count,
            "Optimized count should match manual stream count"
        );
    }

    #[tokio::test]
    async fn test_notify_channel_includes_schema() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        // The default test store uses the "public" schema
        assert_eq!(store.notify_channel(), "event_sauce_events_public");
    }

    #[tokio::test]
    async fn test_listen_receives_notification_on_append() {
        use std::time::Duration;
        use tokio::time::timeout;

        let db = TestDatabase::new().await.unwrap();
        let store = db.store();

        let stream = store.listen_for_events().await.unwrap();
        futures::pin_mut!(stream);

        // Append from a separate task so the listener can wake up.
        let store_for_writer = store.clone();
        tokio::spawn(async move {
            // Tiny sleep so the listener is definitely subscribed before commit.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let stream_id = StreamId::new("User", Uuid::new_v4());
            let event = create_test_envelope("UserCreated", stream_id.aggregate_id());
            store_for_writer
                .append(
                    stream_id,
                    vec![event],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        });

        let notification = timeout(
            Duration::from_secs(5),
            futures::StreamExt::next(&mut stream),
        )
        .await
        .expect("listener should receive a notification within 5s")
        .expect("stream should yield an item")
        .expect("notification should be Ok");

        // The first commit goes to the first BIGSERIAL id. With a fresh DB
        // that's 1; we just check it's a positive position.
        assert!(notification.as_i64() > 0);
    }

    #[tokio::test]
    async fn test_listen_emits_one_notification_per_commit() {
        use std::time::Duration;
        use tokio::time::timeout;

        let db = TestDatabase::new().await.unwrap();
        let store = db.store();

        let stream = store.listen_for_events().await.unwrap();
        futures::pin_mut!(stream);

        let store_for_writer = store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            for i in 0..3 {
                let aggregate_id = Uuid::new_v4();
                let stream_id = StreamId::new("User", aggregate_id);
                let event = create_test_envelope(&format!("Event{i}"), aggregate_id);
                store_for_writer
                    .append(
                        stream_id,
                        vec![event],
                        AggregateVersion::initial(),
                        vec![],
                        false,
                    )
                    .await
                    .unwrap();
            }
        });

        // Three commits → three notifications. Positions are monotonic.
        let mut last_pos: i64 = 0;
        for _ in 0..3 {
            let notification = timeout(
                Duration::from_secs(5),
                futures::StreamExt::next(&mut stream),
            )
            .await
            .expect("listener should receive a notification within 5s")
            .expect("stream should yield an item")
            .expect("notification should be Ok");
            assert!(notification.as_i64() > last_pos);
            last_pos = notification.as_i64();
        }
    }

    #[tokio::test]
    async fn test_listen_no_notification_for_empty_append() {
        use std::time::Duration;
        use tokio::time::timeout;

        let db = TestDatabase::new().await.unwrap();
        let store = db.store();

        let stream = store.listen_for_events().await.unwrap();
        futures::pin_mut!(stream);

        // Empty append should not emit NOTIFY.
        let stream_id = StreamId::new("User", Uuid::new_v4());
        store
            .append(
                stream_id,
                vec![],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // Wait briefly; nothing should arrive.
        let result = timeout(
            Duration::from_millis(300),
            futures::StreamExt::next(&mut stream),
        )
        .await;
        assert!(
            result.is_err(),
            "no notification should be emitted for an empty append"
        );
    }

    /// F2 / C4: [`append_lock_key`] must be a stable, deterministic hash so that
    /// different processes compute the SAME advisory-lock key and actually
    /// serialize against each other; and distinct qualified table names (e.g.
    /// different schemas) must map to DISTINCT keys so unrelated logs do not
    /// serialize. Guards against accidentally swapping in a per-process-seeded
    /// hasher such as `std::hash::DefaultHasher`.
    #[test]
    fn test_append_lock_key_is_stable_and_schema_distinct() {
        // Deterministic across calls (and, because FNV-1a has no per-process
        // seed, across processes — the precondition for cross-process locking).
        assert_eq!(
            append_lock_key("public.events"),
            append_lock_key("public.events"),
            "append_lock_key must be stable for a given table name"
        );

        // Known-answer check pins the exact algorithm + input so the wire
        // contract cannot drift silently: FNV-1a over the bytes of
        // "public.events", reinterpreted as i64.
        let mut expected: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in "public.events".as_bytes() {
            expected ^= u64::from(*byte);
            expected = expected.wrapping_mul(0x0000_0100_0000_01b3);
        }
        #[allow(clippy::cast_possible_wrap)]
        let expected = expected as i64;
        assert_eq!(
            append_lock_key("public.events"),
            expected,
            "append_lock_key must be 64-bit FNV-1a of the qualified table name"
        );

        // Different schemas / tables take different keys → independent logs do
        // not serialize against each other.
        assert_ne!(
            append_lock_key("public.events"),
            append_lock_key("other.events"),
            "different qualified table names must take distinct advisory keys"
        );
    }

    /// F2 / C4: `append()` serializes id-assignment-to-commit on a
    /// transaction-scoped advisory lock keyed by the qualified events table,
    /// so global-id order equals commit order and a checkpoint reader can never
    /// skip a still-uncommitted lower id.
    ///
    /// We hold the SESSION-level advisory lock on a separate, long-lived
    /// connection using the SAME key `append()` uses. Because `append()` takes
    /// the lock, it blocks behind our held lock and never completes while we
    /// hold it; once released it completes — proving the advisory lock was the
    /// only thing serializing it.
    #[tokio::test]
    async fn test_append_serializes_on_advisory_lock() {
        use std::time::Duration;
        use tokio::time::timeout;

        let db = TestDatabase::new().await.unwrap();
        let store = db.store();

        // The store uses the `public` schema, so the qualified events table is
        // `public.events` — the exact string `append_lock_key` hashes.
        let lock_key = append_lock_key("public.events");

        // Hold the session-level advisory lock on a SEPARATE connection so the
        // store's append transaction must wait for it.
        let mut lock_conn = db
            .pool()
            .acquire()
            .await
            .expect("acquire dedicated lock connection");
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_key)
            .execute(&mut *lock_conn)
            .await
            .expect("acquire session advisory lock");

        let stream_id = StreamId::new("User", Uuid::new_v4());
        let event = create_test_envelope("UserCreated", stream_id.aggregate_id());

        // With the lock held, an append that allocates an id must NOT be able
        // to complete (post-fix it blocks on the advisory lock). We give it a
        // generous window; if it returns within that window while the lock is
        // held, the serialization mechanism is not in place.
        let blocked = timeout(
            Duration::from_millis(800),
            store.append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            ),
        )
        .await;

        assert!(
            blocked.is_err(),
            "append must block on the held advisory lock while id-allocating, \
             but it completed (result: {blocked:?}) — append() is not serializing \
             commit order on the advisory lock"
        );

        // Release the side lock; the same append should now succeed, proving the
        // advisory lock was the only thing blocking it.
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *lock_conn)
            .await
            .expect("release session advisory lock");
        drop(lock_conn);

        let stream_id2 = StreamId::new("User", Uuid::new_v4());
        let event2 = create_test_envelope("UserCreated", stream_id2.aggregate_id());
        let after_release = timeout(
            Duration::from_secs(5),
            store.append(
                stream_id2,
                vec![event2],
                AggregateVersion::initial(),
                vec![],
                false,
            ),
        )
        .await;
        assert!(
            matches!(after_release, Ok(Ok(()))),
            "append must succeed once the advisory lock is released, got {after_release:?}"
        );
    }

    /// F2 / C4: a claims-only append (no events) allocates no global ids, so it
    /// must NOT serialize on the log's advisory lock. With the log lock held on
    /// a separate connection, a claims-only append still completes promptly —
    /// proving the lock is taken only on the id-allocating path and that claim
    /// registration is never blocked by concurrent event appends.
    #[tokio::test]
    async fn test_claims_only_append_skips_lock() {
        use event_sauce_core::AggregateClaim;
        use serde_json::json;
        use std::time::Duration;
        use tokio::time::timeout;

        let db = TestDatabase::new().await.unwrap();
        let store = db.store();
        // The aggregate_claims table is created by the store's own migrations.
        store.migrate().await.unwrap();

        // Hold the log advisory lock on a separate connection.
        let lock_key = append_lock_key("public.events");
        let mut lock_conn = db
            .pool()
            .acquire()
            .await
            .expect("acquire dedicated lock connection");
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_key)
            .execute(&mut *lock_conn)
            .await
            .expect("acquire session advisory lock");

        // A claims-only append (events empty) must not wait on the held log lock.
        let stream_id = StreamId::new("User", Uuid::new_v4());
        let claim = AggregateClaim::new("email", json!("user@example.com"));
        let result = timeout(
            Duration::from_secs(5),
            store.append(
                stream_id,
                vec![],
                AggregateVersion::initial(),
                vec![claim],
                false,
            ),
        )
        .await;
        assert!(
            matches!(result, Ok(Ok(()))),
            "claims-only append must not serialize on the log advisory lock, got {result:?}"
        );

        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(lock_key)
            .execute(&mut *lock_conn)
            .await
            .expect("release session advisory lock");
    }

    // ========================================================================
    // M3-L1: Multi-aggregate atomic flush / append_batch
    //
    // A policy reaction that touches two aggregates in one handler (the
    // canonical move-funds-between-two-accounts) must be all-or-nothing on
    // Postgres. Today `PolicyContext::flush` loops `append()` once per buffered
    // commit, each its own transaction, so a partial failure leaves the first
    // aggregate committed and the second not — and at-least-once redelivery of
    // the source event can then double-apply the surviving half.
    // ========================================================================

    use event_sauce_core::{
        Aggregate, AggregateError, AggregateRoot, ApplyEvent, CheckpointStore, DefaultEntity,
        DomainEvent, Entity, EntityId, EventApplicator, EventFilter, Policy, PolicyContext,
        PolicyRunner,
    };

    /// Minimal `DefaultEntity` aggregate used by the multi-aggregate flush test.
    /// One balance, one event kind ("credit"), with its trait impls written
    /// by hand.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Acct {
        id: EntityId,
        balance: i64,
    }

    impl Entity for Acct {
        fn new(id: EntityId) -> Self {
            Self { id, balance: 0 }
        }
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    impl DefaultEntity for Acct {}

    #[derive(Debug, thiserror::Error)]
    #[error("acct error")]
    struct AcctError;
    impl AggregateError for AcctError {}

    impl Aggregate for Acct {
        type Event = AcctEvent;
        type Error = AcctError;
        type DeletedState = Self;
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum AcctEvent {
        Credited { amount: i64 },
    }

    impl DomainEvent for AcctEvent {
        type Aggregate = Acct;
        fn event_type(&self) -> &'static str {
            "Acct.Credited"
        }
        fn event_version(&self) -> EventVersion {
            EventVersion::new(1)
        }
        fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::Utc::now()
        }
    }

    impl ApplyEvent<Acct> for AcctEvent {
        fn apply(&self, entity: &mut Acct) {
            match self {
                AcctEvent::Credited { amount } => entity.balance += amount,
            }
        }
    }

    impl EventApplicator<Acct> for AcctEvent {
        fn dispatch(&self, entity: &mut Acct) -> std::result::Result<(), AcctError> {
            self.apply(entity);
            Ok(())
        }
        fn dispatch_unchecked(&self, entity: &mut Acct) {
            self.apply(entity);
        }
    }

    /// Policy whose handler commits to two DIFFERENT `Acct` aggregates (a
    /// debit/credit pair). Both are buffered in `PolicyContext`; the second
    /// aggregate's stream is pre-seeded so its append conflicts at flush time.
    struct TransferPolicy {
        first_id: EntityId,
        second_id: EntityId,
    }

    #[async_trait]
    impl<S: EventStore + 'static> Policy<S> for TransferPolicy {
        fn name(&self) -> &'static str {
            "TransferPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("Trigger.Fired")
        }
        async fn handle(
            &self,
            _event: &EventEnvelope,
            ctx: &PolicyContext<S>,
        ) -> event_sauce_core::Result<()> {
            // First aggregate commits cleanly.
            let mut first = AggregateRoot::<Acct>::new(self.first_id);
            first.apply(AcctEvent::Credited { amount: 100 }).unwrap();
            ctx.commit(&mut first).await?;

            // Second aggregate: a fresh root (expected_version = initial), but
            // its stream was pre-seeded to version 1, so its append will raise
            // ConcurrencyConflict at flush time.
            let mut second = AggregateRoot::<Acct>::new(self.second_id);
            second.apply(AcctEvent::Credited { amount: 100 }).unwrap();
            ctx.commit(&mut second).await?;

            Ok(())
        }
    }

    /// RED (assertion-level): the canonical multi-aggregate reaction must be
    /// atomic on Postgres. The handler commits aggregate A (first) and then
    /// aggregate B (second), where B's commit conflicts. After the failed
    /// reaction, NEITHER aggregate's event may be persisted.
    ///
    /// TODAY `flush` appends A in its own transaction (which commits) before B
    /// is even attempted, so A's `Acct.Credited` survives the failure — this
    /// assertion fails (A is present). AFTER the M3 fix, both stream commits go
    /// through a single `append_batch` transaction, so A rolls back with B and
    /// this passes.
    #[tokio::test]
    async fn test_flush_multi_aggregate_is_atomic() {
        let db = TestDatabase::new().await.unwrap();

        // Checkpoint store on the public schema (TestDatabase migrates events
        // there); create the checkpoints table.
        let cp = std::sync::Arc::new(
            crate::PostgresCheckpointStore::builder()
                .pool(db.pool().clone())
                .schema("public")
                .build()
                .expect("pool was set"),
        );
        cp.migrate().await.unwrap();

        let store = std::sync::Arc::new(
            PostgresEventStore::builder()
                .pool(db.pool().clone())
                .schema("public")
                .checkpoint_store(cp.clone())
                .build()
                .expect("pool was set"),
        );

        let first_id = EntityId::new();
        let second_id = EntityId::new();

        // Pre-seed the SECOND aggregate's stream so a fresh-root append (which
        // expects initial version) loses on the version precheck → conflict.
        store
            .append(
                StreamId::new("Acct", second_id.as_uuid()),
                vec![create_test_envelope_with_type(
                    "Acct.Credited",
                    "Acct",
                    second_id.as_uuid(),
                )],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        // Seed the policy checkpoint to start so it processes the trigger event.
        cp.save_checkpoint("TransferPolicy", Position::start())
            .await
            .unwrap();

        // Append the trigger event the policy reacts to.
        store
            .append(
                StreamId::new("Trigger", Uuid::new_v4()),
                vec![create_test_envelope_with_type(
                    "Trigger.Fired",
                    "Trigger",
                    Uuid::new_v4(),
                )],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();

        let runner = PolicyRunner::new(std::sync::Arc::clone(&store), cp.clone()).register(
            std::sync::Arc::new(TransferPolicy {
                first_id,
                second_id,
            }),
        );

        // The reaction must fail because the second commit conflicts.
        let result = runner.process_pending().await;
        let err = result.expect_err("multi-aggregate reaction should fail on the conflict");
        assert!(
            err.is_concurrency_conflict(),
            "expected the second commit to surface a ConcurrencyConflict, got: {err:?}"
        );

        // ATOMICITY: the FIRST aggregate must NOT have been persisted — the
        // whole reaction rolls back together. (Today it IS persisted, so this
        // fails: RED.)
        let first_version = store
            .get_version(StreamId::new("Acct", first_id.as_uuid()))
            .await
            .unwrap();
        assert_eq!(
            first_version,
            AggregateVersion::initial(),
            "first aggregate's event must roll back with the failed second commit; \
             it is currently persisted in its own transaction (the multi-aggregate \
             atomicity bug)"
        );

        let first_count = store
            .count_events_fast(StreamId::new("Acct", first_id.as_uuid()))
            .await
            .unwrap();
        assert_eq!(
            first_count, 0,
            "no events for the first aggregate may survive a failed multi-aggregate reaction"
        );
    }

    /// RED (compile-gap + atomicity proof): the new `append_batch` primitive
    /// must commit all streams in ONE transaction. Build two `StreamCommit`s
    /// for two different aggregates; make the second's `expected_version` wrong
    /// so its insert raises `ConcurrencyConflict`. `append_batch` must return
    /// that error AND leave NEITHER stream with any persisted events.
    ///
    /// TODAY `StreamCommit` and `EventStore::append_batch` do not exist, so this
    /// is a compile-gap RED until the M3 production code lands.
    #[tokio::test]
    async fn test_append_batch_is_atomic() {
        let db = TestDatabase::new().await.unwrap();
        let store = db.store();

        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();
        let stream_a = StreamId::new("Acct", id_a);
        let stream_b = StreamId::new("Acct", id_b);

        // Second commit carries a deliberately-wrong expected_version: an empty
        // stream is at initial(), but we claim version 5, so its insert path
        // raises a ConcurrencyConflict.
        let commits = vec![
            event_sauce_core::StreamCommit {
                stream_id: stream_a.clone(),
                events: vec![create_test_envelope_with_type(
                    "Acct.Credited",
                    "Acct",
                    id_a,
                )],
                expected_version: AggregateVersion::initial(),
                claims: vec![],
                clear_claims: false,
            },
            event_sauce_core::StreamCommit {
                stream_id: stream_b.clone(),
                events: vec![create_test_envelope_with_type(
                    "Acct.Credited",
                    "Acct",
                    id_b,
                )],
                expected_version: AggregateVersion::new(5),
                claims: vec![],
                clear_claims: false,
            },
        ];

        let result = store.append_batch(commits).await;
        let err = result.expect_err("append_batch must fail when any commit conflicts");
        assert!(
            err.is_concurrency_conflict(),
            "expected ConcurrencyConflict from the second commit, got: {err:?}"
        );

        // Whole batch rolled back: neither stream has any events.
        assert_eq!(
            store.get_version(stream_a.clone()).await.unwrap(),
            AggregateVersion::initial(),
            "first stream must roll back with the failed batch"
        );
        assert_eq!(
            store.get_version(stream_b.clone()).await.unwrap(),
            AggregateVersion::initial(),
            "second stream must not be partially written"
        );
        assert_eq!(
            store.count_events_fast(stream_a).await.unwrap(),
            0,
            "no events may survive an atomic batch that failed"
        );
    }
}
