//! Postgres outbox for policy effect dispatch.
//!
//! Side-effecting policies (sending email, calling a third-party API,
//! enqueueing a webhook) want different semantics than projections:
//!
//! - **Per-event retry / DLQ**, not per-checkpoint stalls. If event 47 fails
//!   for a policy, events 48..N should keep flowing.
//! - **Parallelism**, with no requirement of cross-event ordering.
//! - **Operational visibility**: failed rows must stay queryable.
//!
//! Checkpoints don't give you any of those. A queue does. This module
//! implements that queue using `FOR UPDATE SKIP LOCKED`.
//!
//! # How it fits with the rest of event-sauce
//!
//! The event log remains the source of truth. The outbox is **derived** from
//! it — a *dispatcher* (see [`PostgresBackend::dispatch_policies_to_outbox`])
//! reads the log via a checkpoint, applies each policy's [`EventFilter`],
//! and inserts one row per (policy, matching event) into `policy_outbox`.
//! Workers then drain the outbox via [`claim_batch`], process the work, and
//! mark each row [`mark_done`] or [`mark_failed`].
//!
//! Replay always works against the log, so the outbox is disposable.
//!
//! # Delivery is at-least-once — handlers MUST be idempotent
//!
//! Draining is three separate steps: [`claim_batch`] → run the handler →
//! [`mark_done`]. They are **not** one transaction. If a worker runs the side
//! effect and then crashes (or its lease expires) before [`mark_done`]
//! commits, the row stays `pending` and another worker re-delivers it — the
//! side effect runs **again**. The same is true on every lease-expiry
//! reclaim. So the outbox guarantees *at-least-once* delivery, never
//! exactly-once.
//!
//! This makes the outbox the right home for side effects that are **safe to
//! repeat**: idempotent API calls (keyed by `event_id`), upserts, "send at
//! most once" effects deduplicated by the caller. It is **not** a way to make
//! a non-idempotent effect safe — a naive "charge this card" handler can
//! double-charge on redelivery. Make your handler idempotent (dedupe on
//! `claim.event_id`), or, for an effect that is itself a write to *this same
//! Postgres database*, fold that write into the same transaction as the
//! projection/read-model update so it commits or rolls back atomically with
//! the event that triggered it.
//!
//! Two counters track a row's lifecycle, and they mean different things:
//!
//! - **`attempts`** — how many times the row has been *claimed* for delivery,
//!   including lease-expiry reclaims of a crashed worker. A delivery /
//!   observability counter; it does **not** drive the DLQ.
//! - **`failures`** — how many times a handler has reported a real error via
//!   [`mark_failed`]. This is the counter the DLQ keys off: a row is parked in
//!   `failed` only once `failures` reaches `max_attempts`. Crash-reclaims bump
//!   `attempts` but never `failures`, so they can never DLQ a row whose side
//!   effect never actually failed.
//!
//! A built-in transactional drain that runs the handler in the *same*
//! transaction as [`mark_done`] (giving effectively-once for same-DB effects)
//! is future work. Until then, callers needing exactly-once for a database
//! side effect should write to their read model in the same transaction as
//! the projection runner, which already commits atomically with the event.
//!
//! [`EventFilter`]: event_sauce_core::EventFilter
//! [`PostgresBackend::dispatch_policies_to_outbox`]: crate::PostgresBackend::dispatch_policies_to_outbox
//! [`claim_batch`]: PostgresPolicyOutbox::claim_batch
//! [`mark_done`]: PostgresPolicyOutbox::mark_done
//! [`mark_failed`]: PostgresPolicyOutbox::mark_failed

use event_sauce_core::{Error, Result};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

/// Postgres-backed policy outbox.
///
/// Owns its own table (`policy_outbox`, schema-qualified). Multiple workers
/// can drain in parallel safely: claims use `FOR UPDATE SKIP LOCKED` so each
/// row is delivered to exactly one worker at a time.
#[derive(Clone)]
pub struct PostgresPolicyOutbox {
    pool: PgPool,
    schema: String,
}

/// One claimed outbox row, returned by [`PostgresPolicyOutbox::claim_batch`].
#[derive(Debug, Clone)]
pub struct OutboxClaim {
    /// Outbox row id (used to mark done/failed).
    pub id: i64,
    /// Source event id — load the event from the event log to process it.
    pub event_id: Uuid,
    /// Global position of the event in the log.
    pub event_position: i64,
    /// How many times this row has been **claimed** for delivery (incremented
    /// on every claim, *including* lease-expiry reclaims of a crashed worker).
    ///
    /// This is a delivery/observability counter, **not** the DLQ gate — a
    /// worker that claims, crashes mid-side-effect, and never reports back
    /// still burns an attempt. Use [`OutboxClaim::failures`] for retry logic.
    pub attempts: i32,
    /// How many times a handler has reported a real failure for this row via
    /// [`PostgresPolicyOutbox::mark_failed`]. This is the counter the DLQ
    /// decision keys off: a row is parked in `failed` only once `failures`
    /// reaches `max_attempts`.
    pub failures: i32,
}

impl PostgresPolicyOutbox {
    /// Creates an outbox bound to `pool` and `schema`.
    #[must_use]
    pub fn new(pool: PgPool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
        }
    }

    /// Returns the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns the schema name for this outbox.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    fn outbox_table(&self) -> String {
        crate::migrations::qualify(&self.schema, "policy_outbox")
    }

    /// Creates the `policy_outbox` table and supporting indexes if they
    /// don't yet exist. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be created or the migration
    /// statements fail.
    pub async fn migrate(&self) -> Result<()> {
        crate::migrations::ensure_schema(&self.pool, &self.schema).await?;

        let migrations_table =
            crate::migrations::qualify(&self.schema, "_policy_outbox_migrations");
        crate::migrations::ensure_migrations_table(&self.pool, &migrations_table).await?;

        let outbox_table = self.outbox_table();
        crate::migrations::apply_once(
            &self.pool,
            &migrations_table,
            20_260_428_000_001_i64,
            "create_policy_outbox_table",
            |pool| async move {
                let create = format!(
                    "CREATE TABLE IF NOT EXISTS {outbox_table} (
                        id BIGSERIAL PRIMARY KEY,
                        policy_name VARCHAR(255) NOT NULL,
                        event_id UUID NOT NULL,
                        event_position BIGINT NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'pending',
                        attempts INT NOT NULL DEFAULT 0,
                        last_error TEXT,
                        locked_by VARCHAR(255),
                        locked_until TIMESTAMP WITH TIME ZONE,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                        UNIQUE(policy_name, event_id)
                    )"
                );
                sqlx::query(&create)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create policy_outbox table", e))?;

                let claim_index = format!(
                    "CREATE INDEX IF NOT EXISTS idx_policy_outbox_claim
                     ON {outbox_table} (policy_name, event_position)
                     WHERE status = 'pending'"
                );
                sqlx::query(&claim_index)
                    .execute(pool)
                    .await
                    .map_err(|e| Error::backend("Failed to create policy_outbox index", e))?;

                let status_index = format!(
                    "CREATE INDEX IF NOT EXISTS idx_policy_outbox_status
                     ON {outbox_table} (policy_name, status)"
                );
                sqlx::query(&status_index)
                    .execute(pool)
                    .await
                    .map_err(|e| {
                        Error::backend("Failed to create policy_outbox status index", e)
                    })?;
                Ok(())
            },
        )
        .await?;

        // `attempts` counts claims (including lease-expiry reclaims of crashed
        // workers), so it must NOT drive the DLQ decision. `failures` counts
        // only handler errors reported via `mark_failed`; that's what gates the
        // DLQ. Added as a separate migration so existing tables get the column.
        let outbox_table = self.outbox_table();
        crate::migrations::apply_once(
            &self.pool,
            &migrations_table,
            20_260_605_000_001_i64,
            "add_policy_outbox_failures_column",
            |pool| async move {
                let add_column = format!(
                    "ALTER TABLE {outbox_table}
                     ADD COLUMN IF NOT EXISTS failures INT NOT NULL DEFAULT 0"
                );
                sqlx::query(&add_column).execute(pool).await.map_err(|e| {
                    Error::backend("Failed to add policy_outbox failures column", e)
                })?;
                Ok(())
            },
        )
        .await
    }

    /// Enqueues an outbox row for `policy_name` to process `event_id`.
    ///
    /// Idempotent — calling twice with the same `(policy_name, event_id)`
    /// pair does nothing on the second call. Run inside the same transaction
    /// as the dispatcher's checkpoint advance so the row and the checkpoint
    /// commit (or roll back) together.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert fails.
    pub async fn enqueue_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        policy_name: &str,
        event_id: Uuid,
        event_position: i64,
    ) -> Result<()> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "INSERT INTO {outbox_table} (policy_name, event_id, event_position)
             VALUES ($1, $2, $3)
             ON CONFLICT (policy_name, event_id) DO NOTHING"
        );
        sqlx::query(&query)
            .bind(policy_name)
            .bind(event_id)
            .bind(event_position)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to enqueue policy outbox row", e))?;
        Ok(())
    }

    /// Atomically claims up to `batch_size` pending rows for `policy_name`
    /// using `FOR UPDATE SKIP LOCKED`. Each claimed row is locked for
    /// `lock_duration`; if the worker doesn't mark it done/failed before
    /// the lock expires, another worker may claim it.
    ///
    /// Workers running in parallel never receive the same row in the same
    /// claim — that's the point of `SKIP LOCKED`.
    ///
    /// Claiming bumps the row's `attempts` (a delivery/observability counter),
    /// **not** its `failures`. A worker that claims, crashes mid-side-effect,
    /// and never reports back has burned a claim but not failed — so the DLQ
    /// (which keys off `failures`, see [`mark_failed`]) is unaffected.
    ///
    /// [`mark_failed`]: PostgresPolicyOutbox::mark_failed
    ///
    /// # Errors
    ///
    /// Returns an error if the claim query fails.
    pub async fn claim_batch(
        &self,
        policy_name: &str,
        worker_id: &str,
        batch_size: u32,
        lock_duration: Duration,
    ) -> Result<Vec<OutboxClaim>> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "WITH claimed AS (
                SELECT id FROM {outbox_table}
                WHERE policy_name = $1
                  AND status = 'pending'
                  AND (locked_until IS NULL OR locked_until < NOW())
                ORDER BY event_position ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {outbox_table} o
            SET locked_by = $3,
                locked_until = NOW() + ($4 * INTERVAL '1 second'),
                attempts = attempts + 1,
                updated_at = NOW()
            FROM claimed
            WHERE o.id = claimed.id
            RETURNING o.id, o.event_id, o.event_position, o.attempts, o.failures"
        );

        #[allow(clippy::cast_possible_wrap)]
        let secs = lock_duration.as_secs() as i64;
        let rows: Vec<(i64, Uuid, i64, i32, i32)> = sqlx::query_as(&query)
            .bind(policy_name)
            .bind(i64::from(batch_size))
            .bind(worker_id)
            .bind(secs)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to claim outbox batch", e))?;

        Ok(rows
            .into_iter()
            .map(
                |(id, event_id, event_position, attempts, failures)| OutboxClaim {
                    id,
                    event_id,
                    event_position,
                    attempts,
                    failures,
                },
            )
            .collect())
    }

    /// Marks an outbox row as successfully processed. Releases the lock.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn mark_done(&self, id: i64) -> Result<()> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "UPDATE {outbox_table}
             SET status = 'done',
                 locked_by = NULL,
                 locked_until = NULL,
                 last_error = NULL,
                 updated_at = NOW()
             WHERE id = $1"
        );
        sqlx::query(&query)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to mark outbox row done", e))?;
        Ok(())
    }

    /// Records a real handler failure for an outbox row. The row stays in the
    /// table for ops inspection (DLQ); reset to `pending` manually if you want
    /// to retry after an external fix.
    ///
    /// Increments the row's `failures` counter, then decides where it lands:
    /// if `max_attempts` is `None`, or the row's `failures` is still below
    /// `max_attempts`, the row returns to `pending` so the next claim sweep
    /// will retry it. Once `failures` reaches `max_attempts` it's parked in
    /// `failed` permanently.
    ///
    /// The DLQ decision keys off `failures` (handler errors), **not**
    /// `attempts` (claims). A worker that crashes mid-side-effect bumps
    /// `attempts` via reclaim but never `failures`, so crash-reclaims can
    /// never push a row to the DLQ without a side effect actually failing.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn mark_failed(
        &self,
        id: i64,
        error_message: &str,
        max_attempts: Option<i32>,
    ) -> Result<()> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "UPDATE {outbox_table}
             SET failures = failures + 1,
                 status = CASE
                    WHEN $2 IS NOT NULL AND failures + 1 >= $2 THEN 'failed'
                    ELSE 'pending'
                 END,
                 locked_by = NULL,
                 locked_until = NULL,
                 last_error = $3,
                 updated_at = NOW()
             WHERE id = $1"
        );
        sqlx::query(&query)
            .bind(id)
            .bind(max_attempts)
            .bind(error_message)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to mark outbox row failed", e))?;
        Ok(())
    }

    /// Returns the number of pending rows for `policy_name` (rows in
    /// `pending` status with no active lock). Useful for monitoring lag.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn pending_count(&self, policy_name: &str) -> Result<i64> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "SELECT COUNT(*) FROM {outbox_table}
             WHERE policy_name = $1
               AND status = 'pending'
               AND (locked_until IS NULL OR locked_until < NOW())"
        );
        let count: i64 = sqlx::query_scalar(&query)
            .bind(policy_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to count pending outbox rows", e))?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use testcontainers_modules::postgres::Postgres;

    struct TestDb {
        pool: PgPool,
        #[allow(dead_code)]
        container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDb {
        async fn new() -> Self {
            let container = crate::test_support::start_postgres()
                .await
                .expect("start postgres");
            let host = container.get_host().await.expect("get host");
            let port = container.get_host_port_ipv4(5432).await.expect("get port");
            let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
            let pool = PgPool::connect(&url).await.expect("connect");
            Self { pool, container }
        }
    }

    async fn enqueue_one(outbox: &PostgresPolicyOutbox, policy: &str, position: i64) -> Uuid {
        let event_id = Uuid::new_v4();
        let mut tx = outbox.pool().begin().await.unwrap();
        outbox
            .enqueue_tx(&mut tx, policy, event_id, position)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        event_id
    }

    #[tokio::test]
    async fn test_migrate_creates_table() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT FROM information_schema.tables
             WHERE table_schema = 'event_sauce' AND table_name = 'policy_outbox')",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_enqueue_is_idempotent() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let event_id = Uuid::new_v4();
        for _ in 0..3 {
            let mut tx = outbox.pool().begin().await.unwrap();
            outbox
                .enqueue_tx(&mut tx, "policy-a", event_id, 1)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }

        let count = outbox.pending_count("policy-a").await.unwrap();
        assert_eq!(count, 1, "duplicate enqueues should be deduped");
    }

    #[tokio::test]
    async fn test_claim_batch_returns_pending_rows() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        for i in 0..5 {
            enqueue_one(&outbox, "policy-a", i).await;
        }

        let claims = outbox
            .claim_batch("policy-a", "worker-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(claims.len(), 5);
        assert!(claims.iter().all(|c| c.attempts == 1));
    }

    #[tokio::test]
    async fn test_claim_batch_skip_locked_partitions_work() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        for i in 0..10 {
            enqueue_one(&outbox, "policy-a", i).await;
        }

        // Two concurrent claimers — they should split the work, never overlap.
        let outbox_a = outbox.clone();
        let outbox_b = outbox.clone();
        let (claims_a, claims_b) = tokio::join!(
            tokio::spawn(async move {
                outbox_a
                    .claim_batch("policy-a", "worker-1", 10, Duration::from_secs(60))
                    .await
            }),
            tokio::spawn(async move {
                outbox_b
                    .claim_batch("policy-a", "worker-2", 10, Duration::from_secs(60))
                    .await
            })
        );
        let claims_a = claims_a.unwrap().unwrap();
        let claims_b = claims_b.unwrap().unwrap();

        let total = claims_a.len() + claims_b.len();
        assert_eq!(
            total, 10,
            "every row was claimed exactly once across workers"
        );

        let mut all_ids: HashSet<i64> = HashSet::new();
        for c in &claims_a {
            assert!(all_ids.insert(c.id), "no row claimed twice");
        }
        for c in &claims_b {
            assert!(all_ids.insert(c.id), "no row claimed twice");
        }
    }

    #[tokio::test]
    async fn test_mark_done_clears_pending() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let claims = outbox
            .claim_batch("policy-a", "w-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        outbox.mark_done(claims[0].id).await.unwrap();

        let pending = outbox.pending_count("policy-a").await.unwrap();
        assert_eq!(pending, 0);
    }

    #[tokio::test]
    async fn test_mark_failed_under_max_attempts_returns_to_pending() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let claims = outbox
            .claim_batch("policy-a", "w-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        outbox
            .mark_failed(claims[0].id, "transient", Some(3))
            .await
            .unwrap();

        // Row is back to pending (since attempts=1 < max=3); next claim sees it.
        let next = outbox
            .claim_batch("policy-a", "w-2", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].attempts, 2);
    }

    #[tokio::test]
    async fn test_mark_failed_at_max_attempts_marks_failed() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let claims = outbox
            .claim_batch("policy-a", "w-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        // attempts is now 1; with max_attempts=1, this counts as failed.
        outbox
            .mark_failed(claims[0].id, "permanent", Some(1))
            .await
            .unwrap();

        // No more pending rows — it's in failed status.
        let pending = outbox.pending_count("policy-a").await.unwrap();
        assert_eq!(pending, 0);

        let failed: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM event_sauce.policy_outbox WHERE status = 'failed'",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(failed, 1);
    }

    #[tokio::test]
    async fn test_expired_lock_is_reclaimable() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        // Worker A claims with a very short lock and "crashes" (never marks done).
        let _claims = outbox
            .claim_batch("policy-a", "worker-a", 1, Duration::from_millis(50))
            .await
            .unwrap();

        // Wait past the lock expiry.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Worker B should now be able to claim it.
        let claims_b = outbox
            .claim_batch("policy-a", "worker-b", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(claims_b.len(), 1);
        assert_eq!(claims_b[0].attempts, 2);
    }

    /// Forces a claimed row's lock to look expired so the next claim sweep
    /// reclaims it — simulating a worker that crashed mid-side-effect and
    /// never called `mark_done`/`mark_failed`.
    async fn expire_lock(outbox: &PostgresPolicyOutbox, id: i64) {
        sqlx::query(
            "UPDATE event_sauce.policy_outbox SET locked_until = NOW() - INTERVAL '1 hour' WHERE id = $1",
        )
        .bind(id)
        .execute(outbox.pool())
        .await
        .unwrap();
    }

    async fn status_of(outbox: &PostgresPolicyOutbox, id: i64) -> String {
        sqlx::query_scalar("SELECT status FROM event_sauce.policy_outbox WHERE id = $1")
            .bind(id)
            .fetch_one(outbox.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_outbox_dlq_keys_off_failures_not_claims() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        // Claim the row several times via expired-lock reclaim, simulating a
        // worker that crashes mid-side-effect each time and never calls
        // mark_failed. This drives `attempts` (the claim counter) up to 3
        // WITHOUT any handler ever having returned an error.
        let mut id = 0_i64;
        for _ in 0..3 {
            let claims = outbox
                .claim_batch("policy-a", "crasher", 1, Duration::from_secs(60))
                .await
                .unwrap();
            assert_eq!(claims.len(), 1, "row should be reclaimable each sweep");
            id = claims[0].id;
            expire_lock(&outbox, id).await;
        }

        // Three claims happened but zero handler failures. The row must NOT be
        // in the DLQ — crash-reclaims are not delivery failures.
        let status = status_of(&outbox, id).await;
        assert_eq!(
            status, "pending",
            "claim/reclaim count must not push a row to the DLQ (failures==0)"
        );

        // Now the handler actually fails. With max_attempts=2, the DLQ decision
        // must key off real failures: first failure -> still pending (1 < 2),
        // second failure -> failed (2 >= 2). It must NOT flip to failed on the
        // first failure just because `attempts` (claims) already reached 3.
        outbox.mark_failed(id, "transient", Some(2)).await.unwrap();
        let status = status_of(&outbox, id).await;
        assert_eq!(
            status, "pending",
            "first real failure (failures==1 < max=2) must return to pending, not DLQ"
        );

        // Reclaim it (handler retried), then it fails a second time.
        let claims = outbox
            .claim_batch("policy-a", "retrier", 1, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            claims.len(),
            1,
            "row must still be claimable after 1st failure"
        );
        outbox
            .mark_failed(claims[0].id, "permanent", Some(2))
            .await
            .unwrap();
        let status = status_of(&outbox, id).await;
        assert_eq!(
            status, "failed",
            "second real failure (failures==2 >= max=2) flips to DLQ"
        );
    }

    #[tokio::test]
    async fn test_claim_respects_event_position_order() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        // Enqueue out of order — claim should still come back in event_position order.
        enqueue_one(&outbox, "policy-a", 3).await;
        enqueue_one(&outbox, "policy-a", 1).await;
        enqueue_one(&outbox, "policy-a", 2).await;

        let claims = outbox
            .claim_batch("policy-a", "w-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        let positions: Vec<i64> = claims.iter().map(|c| c.event_position).collect();
        assert_eq!(positions, vec![1, 2, 3]);
    }
}
