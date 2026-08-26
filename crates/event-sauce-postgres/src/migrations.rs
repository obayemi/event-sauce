//! Internal helpers for managing migrations across the postgres stores.
//!
//! Each store (event store, checkpoint store, crypto key store) tracks its own
//! migrations in a `_*_migrations` table. The bookkeeping pattern is identical:
//! check whether a version has been applied, run the migration body, record the
//! version. These helpers factor out that boilerplate.

use event_sauce_core::{Error, Result};
use sqlx::PgPool;
use std::future::Future;

/// Joins a schema and table name into a fully-qualified identifier.
pub(crate) fn qualify(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

/// Ensures the named schema exists, skipping the call for the `public` schema
/// (which is created automatically by `PostgreSQL`).
pub(crate) async fn ensure_schema(pool: &PgPool, schema: &str) -> Result<()> {
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
pub(crate) async fn ensure_migrations_table(pool: &PgPool, qualified_table: &str) -> Result<()> {
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
    let record_query =
        format!("INSERT INTO {migrations_table} (version, description) VALUES ($1, $2)");
    sqlx::query(&record_query)
        .bind(version)
        .bind(description)
        .execute(pool)
        .await
        .map_err(|e| Error::backend("Failed to record migration", e))?;
    Ok(())
}

/// Creates the shared `aggregate_claims` table and its index at `claims_table`.
///
/// The claims table is persistence-style-agnostic — it keys uniqueness claims
/// by `(claim_type, claim_hash)` per aggregate, with no coupling to event
/// streams — so both the event store and the state store create the same
/// table (idempotently) and enforce claims against it with the same code.
pub(crate) async fn create_aggregate_claims_table(pool: &PgPool, claims_table: &str) -> Result<()> {
    let create_claims = format!(
        "CREATE TABLE IF NOT EXISTS {claims_table} (
            aggregate_id UUID NOT NULL,
            claim_type VARCHAR(255) NOT NULL,
            claim_hash BYTEA NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            UNIQUE (aggregate_id, claim_type),
            UNIQUE (claim_type, claim_hash)
        )"
    );
    sqlx::query(&create_claims)
        .execute(pool)
        .await
        .map_err(|e| Error::backend("Failed to create aggregate_claims table", e))?;

    let idx_query =
        format!("CREATE INDEX IF NOT EXISTS idx_claims_aggregate ON {claims_table} (aggregate_id)");
    sqlx::query(&idx_query)
        .execute(pool)
        .await
        .map_err(|e| Error::backend("Failed to create claims index", e))?;
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
