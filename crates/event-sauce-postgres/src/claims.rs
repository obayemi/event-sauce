//! Cross-aggregate uniqueness claims, shared by the event store and the
//! state store.
//!
//! The `aggregate_claims` table is persistence-style-agnostic — it keys
//! uniqueness claims by `(claim_type, claim_hash)` per aggregate, with no
//! coupling to event streams — so both stores create the same table
//! (idempotently) and enforce claims against it with the same code in this
//! module.

use event_sauce_core::{AggregateClaim, Error, Result, StreamId};
use sqlx::PgPool;

/// Creates the shared `aggregate_claims` table and its index at `claims_table`.
pub(crate) async fn create_table(pool: &PgPool, claims_table: &str) -> Result<()> {
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

/// Enforces `claims` transactionally against `claims_table` for `stream_id`,
/// then clears its claims entirely if `clear_claims` is set.
///
/// A present claim already held by a different aggregate fails the whole
/// call with [`Error::ClaimConflict`](event_sauce_core::Error); claims no
/// longer in `claims` are dropped. Called by both
/// [`PostgresEventStore`](crate::PostgresEventStore) and
/// [`PostgresStateStore`](crate::PostgresStateStore) inside their own
/// write transaction, so a rejected claim rolls back the whole commit.
pub(crate) async fn enforce(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    claims_table: &str,
    stream_id: &StreamId,
    claims: Vec<AggregateClaim>,
    clear_claims: bool,
) -> Result<()> {
    if !claims.is_empty() {
        use sha2::{Digest, Sha256};

        // Hash each claim once — we need both the hash for SQL binding and
        // the original claim metadata for error reporting.
        let mut hashed: Vec<(&AggregateClaim, Vec<u8>)> = Vec::with_capacity(claims.len());
        for claim in &claims {
            let key_json = serde_json::to_string(&claim.claim_key)
                .map_err(|e| Error::backend("Failed to serialize claim key", e))?;
            let claim_hash = Sha256::digest(key_json.as_bytes()).to_vec();
            hashed.push((claim, claim_hash));
        }

        let current_claim_types: Vec<String> = hashed
            .iter()
            .map(|(c, _)| c.claim_type.to_string())
            .collect();

        let aggregate_id = stream_id.aggregate_id();
        upsert_or_conflict(tx, claims_table, aggregate_id, &hashed).await?;

        if !current_claim_types.is_empty() {
            let placeholders: Vec<String> = current_claim_types
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", i + 2))
                .collect();
            let cleanup_query = format!(
                "DELETE FROM {claims_table} WHERE aggregate_id = $1 AND claim_type NOT IN ({})",
                placeholders.join(", ")
            );
            let mut query = sqlx::query(&cleanup_query).bind(aggregate_id);
            for ct in &current_claim_types {
                query = query.bind(ct);
            }
            query
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::backend("Failed to cleanup old claims", e))?;
        }
    }

    if clear_claims {
        let delete_query = format!("DELETE FROM {claims_table} WHERE aggregate_id = $1");
        sqlx::query(&delete_query)
            .bind(stream_id.aggregate_id())
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to clear claims", e))?;
    }

    Ok(())
}

/// Detects a conflicting committed claim and, if none, upserts the caller's
/// claims — all in one round-trip via a CTE that returns any conflicting row
/// (an empty result means success).
///
/// Runs behind a savepoint: two transactions racing this for different
/// aggregates can both see no conflict, then collide on the non-arbiter
/// `UNIQUE(claim_type, claim_hash)` index. That aborts whichever transaction
/// loses the race, and [`recover_concurrent_conflict`] needs to issue a
/// further `SELECT` afterwards. Rolling back to the savepoint (rather than
/// the whole transaction) clears the abort while keeping the version
/// precheck and any already-inserted events.
async fn upsert_or_conflict(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    claims_table: &str,
    aggregate_id: uuid::Uuid,
    hashed: &[(&AggregateClaim, Vec<u8>)],
) -> Result<()> {
    use sqlx::Acquire;

    let row_placeholders: Vec<String> = (0..hashed.len())
        .map(|i| format!("(${}::text, ${}::bytea)", 2 + 2 * i, 3 + 2 * i))
        .collect();
    let combined_query = format!(
        "WITH input(claim_type, claim_hash) AS (VALUES {rows}),
              conflicts AS (
                  SELECT i.claim_type, i.claim_hash, c.aggregate_id AS holder
                  FROM input i
                  JOIN {claims_table} c USING (claim_type, claim_hash)
                  WHERE c.aggregate_id <> $1
              ),
              upsert AS (
                  INSERT INTO {claims_table} (aggregate_id, claim_type, claim_hash)
                  SELECT $1, claim_type, claim_hash FROM input
                  WHERE NOT EXISTS (SELECT 1 FROM conflicts)
                  ON CONFLICT (aggregate_id, claim_type)
                  DO UPDATE SET claim_hash = EXCLUDED.claim_hash
                  RETURNING aggregate_id
              )
          SELECT claim_type, claim_hash, holder FROM conflicts LIMIT 1",
        rows = row_placeholders.join(", "),
    );

    let mut query =
        sqlx::query_as::<_, (String, Vec<u8>, uuid::Uuid)>(&combined_query).bind(aggregate_id);
    for (claim, hash) in hashed {
        query = query.bind(claim.claim_type).bind(hash);
    }

    let mut savepoint = tx
        .begin()
        .await
        .map_err(|e| Error::backend("Failed to open claims savepoint", e))?;
    match query.fetch_optional(&mut *savepoint).await {
        Ok(None) => savepoint
            .commit()
            .await
            .map_err(|e| Error::backend("Failed to commit claims savepoint", e)),
        Ok(Some((claim_type, claim_hash, holder))) => Err(build_conflict_error(
            hashed,
            &claim_type,
            &claim_hash,
            holder,
        )),
        Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
            savepoint
                .rollback()
                .await
                .map_err(|e| Error::backend("Failed to roll back claims savepoint", e))?;
            recover_concurrent_conflict(tx, claims_table, aggregate_id, hashed).await
        }
        Err(e) => Err(Error::backend("Failed to upsert claims", e)),
    }
}

fn build_conflict_error(
    hashed: &[(&AggregateClaim, Vec<u8>)],
    claim_type: &str,
    claim_hash: &[u8],
    holder: uuid::Uuid,
) -> Error {
    let original_key = hashed
        .iter()
        .find(|(c, h)| c.claim_type == claim_type && h.as_slice() == claim_hash)
        .map_or(serde_json::Value::Null, |(c, _)| c.claim_key.clone());
    Error::claim_conflict(claim_type, original_key, Some(holder))
}

/// Fall back when the CTE upsert raced a concurrent transaction: a single
/// query finds any of our (`claim_type`, `claim_hash`) pairs already held by
/// a different aggregate. Avoids N round-trips on contended writes.
async fn recover_concurrent_conflict(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    claims_table: &str,
    aggregate_id: uuid::Uuid,
    hashed: &[(&AggregateClaim, Vec<u8>)],
) -> Result<()> {
    let row_placeholders: Vec<String> = (0..hashed.len())
        .map(|i| format!("(${}::text, ${}::bytea)", 2 + 2 * i, 3 + 2 * i))
        .collect();
    let holder_query = format!(
        "SELECT claim_type, claim_hash, aggregate_id FROM {claims_table}
         WHERE (claim_type, claim_hash) IN ({rows}) AND aggregate_id <> $1
         LIMIT 1",
        rows = row_placeholders.join(", "),
    );
    let mut query =
        sqlx::query_as::<_, (String, Vec<u8>, uuid::Uuid)>(&holder_query).bind(aggregate_id);
    for (claim, hash) in hashed {
        query = query.bind(claim.claim_type).bind(hash);
    }

    match query.fetch_optional(&mut **tx).await {
        Ok(Some((claim_type, claim_hash, holder))) => Err(build_conflict_error(
            hashed,
            &claim_type,
            &claim_hash,
            holder,
        )),
        Ok(None) => Err(Error::custom(
            "Claim upsert failed with unique violation but no conflicting holder found",
        )),
        Err(e) => Err(Error::backend("Failed to look up claim holder", e)),
    }
}
