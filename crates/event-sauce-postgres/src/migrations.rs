//! Internal helpers for managing migrations across the postgres stores.
//!
//! Each store (event store, checkpoint store, crypto key store) tracks its own
//! migrations in a `_*_migrations` table. The bookkeeping pattern is identical:
//! check whether a version has been applied, run the migration body, record the
//! version. These helpers factor out that boilerplate.

use event_sauce_core::{Error, Result};
use sqlx::{Connection, PgConnection, PgPool};
use std::future::Future;

/// Joins a schema and table name into a fully-qualified identifier.
pub(crate) fn qualify(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

/// Computes a stable 64-bit advisory-lock key from `seed`, for keying
/// `pg_advisory_lock` / `pg_advisory_xact_lock` calls.
///
/// The hash is a 64-bit [FNV-1a] over the UTF-8 bytes of `seed`, reinterpreted
/// as the signed `bigint` these functions expect. FNV-1a is used deliberately
/// rather than [`std::hash::DefaultHasher`]: the latter seeds `SipHash`
/// randomly per process, so two processes would compute *different* keys and
/// any cross-process serialization guarantee keyed on it would silently not
/// hold. The algorithm is therefore part of callers' wire contract and must
/// remain stable.
///
/// [FNV-1a]: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
pub(crate) fn advisory_lock_key(seed: &str) -> i64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash.cast_signed()
}

/// Runs `body` behind a transaction-scoped `pg_advisory_xact_lock` keyed on
/// `schema`, so `migrate()` calls racing from several processes or several
/// stores (a deploy starting several replicas at once, or an event store and
/// a state store both migrating one schema) serialize instead of racing on
/// `CREATE SCHEMA IF NOT EXISTS`, their tracking tables, and any table two
/// stores share (such as `aggregate_claims`). Every store sharing a schema
/// must therefore call this with that same `schema`, not a store-specific
/// key such as its own migrations table name.
///
/// Also creates the schema and `migrations_table` (a bare table name, which
/// this function qualifies with `schema`) before running `body`, and passes
/// `body` that same qualified name — every store's `migrate()` needs both
/// done under this same lock, ahead of its own migration steps.
///
/// The lock is taken on a connection opened directly from `pool`'s connect
/// options, never one checked out of `pool` itself: `body` runs its own
/// queries against `pool`, and a lock connection borrowed from the same pool
/// would starve `body` on a small pool (one with a single connection
/// deadlocks). The lock is transaction-scoped rather than session-scoped so
/// this also works behind a connection pooler running in transaction mode,
/// such as `PgBouncer` configured with `POOL_MODE=transaction` — the setup
/// this project's production docs recommend: a pooler in that mode is free
/// to run any two statements on this connection against different Postgres
/// backends, so a session-level `pg_advisory_lock` paired with a later
/// `pg_advisory_unlock` can have its unlock land on a backend that never
/// held the lock. `PostgreSQL` then only warns, the caller sees `Ok`, and
/// the original backend — kept alive by the pool up to `server_lifetime`,
/// commonly an hour — holds the lock until that connection is recycled or
/// times out, wedging every other node's `migrate()` behind it. A
/// transaction-scoped lock has no such gap: the pooler pins one server
/// backend to the connection for the whole transaction, and the lock
/// releases automatically on commit, rollback, or the connection closing —
/// cancelling this future or a panic in `body` cannot leak it either.
/// `idle_in_transaction_session_timeout` bounds how long a stuck holder can
/// block every other node's `migrate()`.
pub(crate) async fn with_migration_lock<'a, F, Fut>(
    pool: &'a PgPool,
    schema: &'a str,
    migrations_table: &str,
    body: F,
) -> Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<()>> + 'a,
{
    let key = advisory_lock_key(&qualify(schema, "_event_sauce_migrate_lock"));
    let mut conn = PgConnection::connect_with(&*pool.connect_options())
        .await
        .map_err(|e| Error::backend("Failed to open migration-lock connection", e))?;

    let mut lock_tx = conn
        .begin()
        .await
        .map_err(|e| Error::backend("Failed to open migration-lock transaction", e))?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *lock_tx)
        .await
        .map_err(|e| Error::backend("Failed to acquire migration lock", e))?;

    let result = async {
        ensure_schema(pool, schema).await?;
        let qualified_table = qualify(schema, migrations_table);
        ensure_migrations_table(pool, &qualified_table).await?;
        body(qualified_table).await
    }
    .await;

    lock_tx
        .rollback()
        .await
        .map_err(|e| Error::backend("Failed to release migration lock", e))?;

    conn.close()
        .await
        .map_err(|e| Error::backend("Failed to close migration-lock connection", e))?;

    result
}

/// Ensures the named schema exists, skipping the call for the `public` schema
/// (which is created automatically by `PostgreSQL`).
async fn ensure_schema(pool: &PgPool, schema: &str) -> Result<()> {
    if schema != "public" {
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
        sqlx::query(&create_schema)
            .execute(pool)
            .await
            .map_err(|e| Error::backend("Failed to create schema", e))?;
    }
    Ok(())
}

/// Ensures the migration tracking table exists at `qualified_table`.
async fn ensure_migrations_table(pool: &PgPool, qualified_table: &str) -> Result<()> {
    let create = format!(
        "CREATE TABLE IF NOT EXISTS {qualified_table} (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        )"
    );
    sqlx::query(&create)
        .execute(pool)
        .await
        .map_err(|e| Error::backend("Failed to create migrations table", e))?;
    Ok(())
}

/// Returns true if migration `version` is recorded in the tracking table.
pub(crate) async fn migration_applied(
    pool: &PgPool,
    migrations_table: &str,
    version: i64,
) -> Result<bool> {
    let check_query = format!("SELECT COUNT(*) FROM {migrations_table} WHERE version = $1");
    let count: i64 = sqlx::query_scalar(&check_query)
        .bind(version)
        .fetch_one(pool)
        .await
        .map_err(|e| Error::backend("Failed to check migration status", e))?;
    Ok(count > 0)
}

/// Records `version` with `description` in the tracking table.
pub(crate) async fn record_migration(
    pool: &PgPool,
    migrations_table: &str,
    version: i64,
    description: &str,
) -> Result<()> {
    let record_query = format!(
        "INSERT INTO {migrations_table} (version, description) VALUES ($1, $2)
         ON CONFLICT (version) DO NOTHING"
    );
    sqlx::query(&record_query)
        .bind(version)
        .bind(description)
        .execute(pool)
        .await
        .map_err(|e| Error::backend("Failed to record migration", e))?;
    Ok(())
}

/// Runs `body` if migration `version` has not been applied yet, then records it.
///
/// Provides the canonical "check version, run migration, record version" flow.
/// `body` performs the actual schema changes — typically `CREATE TABLE`,
/// `CREATE INDEX`, etc. on the same pool.
pub(crate) async fn apply_once<'a, F, Fut>(
    pool: &'a PgPool,
    migrations_table: &'a str,
    version: i64,
    description: &'a str,
    body: F,
) -> Result<()>
where
    F: FnOnce(&'a PgPool) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    if migration_applied(pool, migrations_table, version).await? {
        return Ok(());
    }
    body(pool).await?;
    record_migration(pool, migrations_table, version, description).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        PostgresCheckpointStore, PostgresCryptoKeyStore, PostgresEventStore, PostgresStateStore,
    };
    use sqlx::postgres::PgPoolOptions;

    /// Pins [`advisory_lock_key`]'s wire contract: it must be deterministic
    /// across calls (and, having no per-process seed, across processes — the
    /// precondition for cross-process locking), it must match a known-answer
    /// FNV-1a hash of `"public.events"` so the algorithm cannot drift
    /// silently, and distinct seeds must take distinct keys so independent
    /// locks never serialize against each other.
    #[test]
    fn advisory_lock_key_is_stable_and_input_distinct() {
        assert_eq!(
            advisory_lock_key("public.events"),
            advisory_lock_key("public.events"),
            "advisory_lock_key must be stable for a given seed"
        );

        assert_eq!(
            advisory_lock_key("public.events"),
            -146_897_220_888_487_505,
            "advisory_lock_key must be 64-bit FNV-1a of the seed"
        );

        assert_ne!(
            advisory_lock_key("public.events"),
            advisory_lock_key("other.events"),
            "different seeds must take distinct advisory keys"
        );
    }

    /// Regression test for XN-7: several replicas each connecting with their
    /// own pool and setting up a fresh, non-public schema (what
    /// `PostgresBackend::setup` does internally: a checkpoint store then an
    /// event store, both migrated in turn) must not crash any of them. Before
    /// the fix, the lock was keyed on each store's own migrations table, so
    /// the checkpoint store's `CREATE SCHEMA IF NOT EXISTS` and its tracking
    /// table raced across replicas.
    #[tokio::test]
    async fn test_concurrent_replica_setup_does_not_race() {
        let (_container, url) = crate::test_support::start_postgres_url().await;

        let handles: Vec<_> = (0..6)
            .map(|_| {
                let url = url.clone();
                tokio::spawn(async move {
                    let pool = PgPool::connect(&url).await.expect("connect");
                    let checkpoint_store = PostgresCheckpointStore::builder()
                        .pool(pool.clone())
                        .schema("replica_setup")
                        .build()
                        .expect("pool was set");
                    checkpoint_store.migrate().await?;

                    let event_store = PostgresEventStore::builder()
                        .pool(pool)
                        .schema("replica_setup")
                        .build()
                        .expect("pool was set");
                    event_store.migrate().await
                })
            })
            .collect();

        for handle in handles {
            handle
                .await
                .expect("migrate task must not panic")
                .expect("concurrent replica setup must not fail");
        }
    }

    /// Regression test for XN-7: stores of DIFFERENT kinds that share one
    /// schema must serialize their `migrate()` calls on the same key. Before
    /// the fix, the event store, state store, checkpoint store and crypto
    /// key store each keyed the lock on their own migrations table, so
    /// running them concurrently on one schema raced on `CREATE SCHEMA IF NOT
    /// EXISTS` and the shared `aggregate_claims` table.
    #[tokio::test]
    async fn test_concurrent_migrate_across_store_kinds_on_shared_schema() {
        let (_container, url) = crate::test_support::start_postgres_url().await;
        let pool = PgPool::connect(&url).await.expect("connect");

        let event_store = PostgresEventStore::builder()
            .pool(pool.clone())
            .schema("shared_schema")
            .build()
            .expect("pool was set");
        let state_store = PostgresStateStore::builder()
            .pool(pool.clone())
            .schema("shared_schema")
            .build()
            .expect("pool was set");
        let checkpoint_store = PostgresCheckpointStore::builder()
            .pool(pool.clone())
            .schema("shared_schema")
            .build()
            .expect("pool was set");
        let crypto_key_store = PostgresCryptoKeyStore::builder()
            .pool(pool)
            .schema("shared_schema")
            .build()
            .expect("pool was set");

        let (event_result, state_result, checkpoint_result, crypto_result) = tokio::join!(
            event_store.migrate(),
            state_store.migrate(),
            checkpoint_store.migrate(),
            crypto_key_store.migrate(),
        );

        event_result.expect("event store migrate must not fail");
        state_result.expect("state store migrate must not fail");
        checkpoint_result.expect("checkpoint store migrate must not fail");
        crypto_result.expect("crypto key store migrate must not fail");
    }

    /// Regression test: `migrate()` must succeed even on a pool that can
    /// only ever check out ONE connection. Before the fix, the lock was
    /// taken on a connection borrowed from the same pool `body` runs its
    /// queries against, so a single-connection pool deadlocked: the lock
    /// held the only connection, and every migration step then timed out
    /// waiting to acquire one.
    #[tokio::test]
    async fn test_migrate_succeeds_on_single_connection_pool() {
        let (_container, url) = crate::test_support::start_postgres_url().await;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&url)
            .await
            .expect("connect");

        let event_store = PostgresEventStore::builder()
            .pool(pool)
            .schema("single_conn")
            .build()
            .expect("pool was set");

        event_store
            .migrate()
            .await
            .expect("migrate must not deadlock on a single-connection pool");
    }

    /// Regression test for the `PgBouncer` transaction-pooling regression the
    /// XN-7 fix introduced: a session-level `pg_advisory_lock` paired with a
    /// later `pg_advisory_unlock` on the same, never-closed client
    /// connection can have `PgBouncer` route each statement to a different
    /// pooled `PostgreSQL` backend when it runs in `POOL_MODE=transaction` —
    /// the setup this project's production docs recommend. The unlock then
    /// silently no-ops (a `WARNING`, not an error `sqlx` would surface) on a
    /// backend that never held the lock, leaving the lock dangling on
    /// whichever backend did, for as long as `PgBouncer`'s `server_lifetime`
    /// keeps it alive. `pg_advisory_xact_lock` closes that gap: `PgBouncer`
    /// pins one backend to a connection for as long as it is inside a
    /// transaction, so the lock always releases with the transaction that
    /// took it, whatever `PgBouncer` does with the surrounding statements.
    ///
    /// Reproducing the leak needs `PgBouncer` to actually hand the lock and
    /// unlock statements to different backends. `server_round_robin = 1`
    /// with the pool capped to 2 connections, plus concurrent noise queries
    /// while the lock is held, forces that deterministically (verified by
    /// hand against a real `PgBouncer` before this test was written). This
    /// test fails against a session-level `pg_advisory_lock` and passes
    /// against `pg_advisory_xact_lock`.
    ///
    /// The leak check queries directly against `PostgreSQL`, bypassing
    /// `PgBouncer` entirely: a connection routed through the pooler could be
    /// handed whichever backend still holds the lock, masking the leak.
    ///
    /// The `PostgreSQL` container gets a fresh name on every retry attempt, so
    /// a failed attempt never collides with a name Docker still considers
    /// taken. That name doubles as `PgBouncer`'s `DB_HOST`, and as a Docker DNS
    /// hostname it is capped at a 63-byte label: past that, resolution fails
    /// silently rather than erroring, which is why the id folded into it is 16
    /// hex characters rather than a full UUID, leaving room for the
    /// `_{attempt}` suffix.
    #[tokio::test]
    async fn test_with_migration_lock_releases_the_lock_behind_pgbouncer_transaction_pooling() {
        use testcontainers::{
            core::{IntoContainerPort, WaitFor},
            GenericImage, ImageExt,
        };

        let network = format!(
            "event_sauce_pgbouncer_test_{}",
            uuid::Uuid::new_v4().simple()
        );
        let pg_name = std::cell::RefCell::new(String::new());

        let pg_container = crate::test_support::start_with_retry(|attempt| {
            let short_id = &uuid::Uuid::new_v4().simple().to_string()[..16];
            let name = format!("event_sauce_pgbouncer_test_pg_{short_id}_{attempt}");
            *pg_name.borrow_mut() = name.clone();
            testcontainers_modules::postgres::Postgres::default()
                .with_host_auth()
                .with_tag("16-alpine")
                .with_container_name(name)
                .with_network(&network)
        })
        .await
        .expect("start postgres");
        let pg_name = pg_name.into_inner();
        let pg_host = pg_container.get_host().await.expect("get postgres host");
        let pg_port = pg_container
            .get_host_port_ipv4(5432)
            .await
            .expect("get postgres port");
        let direct_url = format!("postgresql://postgres@{pg_host}:{pg_port}/postgres");

        let pgbouncer_container = crate::test_support::start_with_retry(|_attempt| {
            GenericImage::new("edoburu/pgbouncer", "latest")
                .with_exposed_port(5432.tcp())
                .with_wait_for(WaitFor::message_on_either_std("process up"))
                .with_network(&network)
                .with_env_var("DB_HOST", &pg_name)
                .with_env_var("DB_PORT", "5432")
                .with_env_var("DB_NAME", "postgres")
                .with_env_var("DB_USER", "postgres")
                .with_env_var("AUTH_TYPE", "trust")
                .with_env_var("POOL_MODE", "transaction")
                .with_env_var("SERVER_ROUND_ROBIN", "1")
                .with_env_var("DEFAULT_POOL_SIZE", "2")
                .with_env_var("MAX_DB_CONNECTIONS", "2")
        })
        .await
        .expect("start pgbouncer");
        let pooler_host = pgbouncer_container
            .get_host()
            .await
            .expect("get pgbouncer host");
        let pooler_port = pgbouncer_container
            .get_host_port_ipv4(5432)
            .await
            .expect("get pgbouncer port");
        let pgbouncer_url = format!("postgresql://postgres@{pooler_host}:{pooler_port}/postgres");

        let pool = PgPool::connect(&pgbouncer_url)
            .await
            .expect("connect through pgbouncer");

        let lock_task = tokio::spawn({
            let pool = pool.clone();
            async move {
                with_migration_lock(&pool, "public", "_test_migration_lock", |_| async {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    Ok(())
                })
                .await
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let noise_handles: Vec<_> = (0..8)
            .map(|_| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    sqlx::query("SELECT pg_backend_pid()")
                        .execute(&pool)
                        .await
                        .expect("noise query must succeed");
                })
            })
            .collect();
        for handle in noise_handles {
            handle.await.expect("noise task must not panic");
        }

        lock_task
            .await
            .expect("lock task must not panic")
            .expect("with_migration_lock must succeed");

        let direct_pool = PgPool::connect(&direct_url)
            .await
            .expect("connect directly to postgres");
        let remaining_advisory_locks: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory'")
                .fetch_one(&direct_pool)
                .await
                .expect("query pg_locks");

        assert_eq!(
            remaining_advisory_locks, 0,
            "the migration lock must not still be held once with_migration_lock returns"
        );
    }
}
