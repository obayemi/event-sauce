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

/// Hashes each claim's key for SQL binding, pairing it with the claim it
/// came from for error reporting, and returns the pairs sorted by
/// `(claim_type, claim_hash)`.
///
/// A writer that skips the append lock — a state-store save or a
/// claims-only append — races other such writers directly on
/// `aggregate_claims`'s indexes. Two overlapping claim sets touch two or
/// more of the same rows there; without a fixed order, opposite input
/// orders can lock those rows in opposite order and deadlock instead of one
/// caller simply waiting for the other. Sorting first gives every caller
/// the same lock order, so a race waits and then resolves to a typed
/// `ClaimConflict` for the loser. A batch that does take the append lock
/// already serializes claim enforcement across the whole batch, so this
/// ordering is not what protects it.
fn hash_claims(claims: &[AggregateClaim]) -> Result<Vec<(&AggregateClaim, Vec<u8>)>> {
    use sha2::{Digest, Sha256};

    let mut hashed: Vec<(&AggregateClaim, Vec<u8>)> = Vec::with_capacity(claims.len());
    for claim in claims {
        let key_json = serde_json::to_string(&claim.claim_key)
            .map_err(|e| Error::backend("Failed to serialize claim key", e))?;
        let claim_hash = Sha256::digest(key_json.as_bytes()).to_vec();
        hashed.push((claim, claim_hash));
    }
    hashed.sort_by(|(a, a_hash), (b, b_hash)| {
        a.claim_type
            .cmp(b.claim_type)
            .then_with(|| a_hash.cmp(b_hash))
    });
    Ok(hashed)
}

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
        let hashed = hash_claims(&claims)?;
        let types: Vec<&str> = hashed.iter().map(|(c, _)| c.claim_type).collect();
        let claim_hashes: Vec<&[u8]> = hashed.iter().map(|(_, h)| h.as_slice()).collect();

        let aggregate_id = stream_id.aggregate_id();
        upsert_or_conflict(
            tx,
            claims_table,
            aggregate_id,
            &hashed,
            &types,
            &claim_hashes,
        )
        .await?;

        let cleanup_query = format!(
            "DELETE FROM {claims_table} WHERE aggregate_id = $1 AND claim_type <> ALL($2::text[])"
        );
        sqlx::query(&cleanup_query)
            .bind(aggregate_id)
            .bind(&types)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to cleanup old claims", e))?;
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
    types: &[&str],
    claim_hashes: &[&[u8]],
) -> Result<()> {
    use sqlx::Acquire;

    let combined_query = format!(
        "WITH input(claim_type, claim_hash) AS (
                  SELECT claim_type, claim_hash
                  FROM UNNEST($2::text[], $3::bytea[]) WITH ORDINALITY AS t(claim_type, claim_hash, ordinality)
                  ORDER BY ordinality
              ),
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
              )
          SELECT claim_type, claim_hash, holder FROM conflicts LIMIT 1"
    );

    let query = sqlx::query_as::<_, (String, Vec<u8>, uuid::Uuid)>(&combined_query)
        .bind(aggregate_id)
        .bind(types)
        .bind(claim_hashes);

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
            recover_concurrent_conflict(tx, claims_table, aggregate_id, hashed, types, claim_hashes)
                .await
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
    types: &[&str],
    claim_hashes: &[&[u8]],
) -> Result<()> {
    let holder_query = format!(
        "SELECT claim_type, claim_hash, aggregate_id FROM {claims_table}
         WHERE (claim_type, claim_hash) IN (SELECT * FROM UNNEST($2::text[], $3::bytea[]))
         AND aggregate_id <> $1
         LIMIT 1"
    );
    let query = sqlx::query_as::<_, (String, Vec<u8>, uuid::Uuid)>(&holder_query)
        .bind(aggregate_id)
        .bind(types)
        .bind(claim_hashes);

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Two callers enforcing the same overlapping claim set in opposite
    /// input order must still take `upsert_or_conflict`'s row locks in the
    /// same order, or they can deadlock instead of one simply waiting for
    /// the other.
    #[test]
    fn test_hash_claims_orders_independently_of_input_order() {
        let claims = vec![
            AggregateClaim::new("Acct.email", json!("a@example.com")),
            AggregateClaim::new("Acct.username", json!("alice")),
        ];
        let mut reversed = claims.clone();
        reversed.reverse();

        let forward = hash_claims(&claims).unwrap();
        let backward = hash_claims(&reversed).unwrap();

        let forward_order: Vec<&str> = forward.iter().map(|(c, _)| c.claim_type).collect();
        let backward_order: Vec<&str> = backward.iter().map(|(c, _)| c.claim_type).collect();

        assert_eq!(
            forward_order, backward_order,
            "two permutations of the same claim set must hash to the same lock order"
        );
    }
}
