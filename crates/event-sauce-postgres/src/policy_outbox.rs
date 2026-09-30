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

/// Backoff applied to an outbox row returned to `pending` after a handler
/// failure, so N polling workers don't all retry the same failed row on the
/// very next sweep.
///
/// The delay before a failed row becomes reclaimable again grows
/// exponentially with the row's `failures` count (`base * 2^failures`,
/// exponent clamped so it never overflows regardless of how large
/// `failures` gets — in particular under `max_attempts: None`, which
/// retries forever), capped at `cap`. `jitter` is then added on top of that
/// *capped* delay, so the actual delay can reach `cap * (1 + jitter)`; it
/// exists so many rows backing off together don't all become reclaimable in
/// the same instant.
///
/// Fields are private so [`new`](Self::new) is the only way to build one —
/// `jitter` is clamped to `0.0..=1.0` there, an invariant a public field
/// could not enforce:
///
/// ```compile_fail
/// # use event_sauce_postgres::BackoffPolicy;
/// # use std::time::Duration;
/// let policy = BackoffPolicy {
///     base: Duration::ZERO,
///     cap: Duration::ZERO,
///     jitter: 5.0,
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackoffPolicy {
    base: Duration,
    cap: Duration,
    jitter: f64,
}

impl BackoffPolicy {
    /// Creates a policy from its base delay, cap, and jitter fraction.
    ///
    /// `jitter` is clamped to `0.0..=1.0`.
    #[must_use]
    pub fn new(base: Duration, cap: Duration, jitter: f64) -> Self {
        Self {
            base,
            cap,
            jitter: if jitter.is_nan() {
                0.0
            } else {
                jitter.clamp(0.0, 1.0)
            },
        }
    }

    /// Delay applied after the first failure (`failures` goes from 0 to 1).
    #[must_use]
    pub fn base(&self) -> Duration {
        self.base
    }

    /// Upper bound on the jitter-free delay, however many failures
    /// accumulate. The actual delay can exceed this by up to `jitter` times
    /// this value.
    #[must_use]
    pub fn cap(&self) -> Duration {
        self.cap
    }

    /// Extra random delay added on top of the capped delay, as a fraction
    /// of it (e.g. `0.5` adds up to 50% more). Always in `0.0..=1.0`.
    #[must_use]
    pub fn jitter(&self) -> f64 {
        self.jitter
    }
}

impl Default for BackoffPolicy {
    /// 1 second base, capped at 5 minutes, up to +50% jitter.
    fn default() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(300), 0.5)
    }
}

#[cfg(test)]
impl BackoffPolicy {
    /// A policy whose `mark_failed` always applies zero delay, for tests
    /// that assert on state transitions without waiting out a backoff.
    pub(crate) fn none() -> Self {
        Self::new(Duration::ZERO, Duration::ZERO, 0.0)
    }
}

/// Exponent clamp for [`BackoffPolicy`]'s `base * 2^failures` growth, so the
/// delay never overflows however large `failures` gets — in particular under
/// `max_attempts: None`, which retries forever.
const MAX_BACKOFF_EXPONENT: i32 = 30;

/// Outcome of a fenced ack ([`PostgresPolicyOutbox::mark_done`],
/// [`PostgresPolicyOutbox::mark_failed`] and their state-outbox
/// counterparts) against one outbox row.
///
/// The fence is `WHERE id = $1 AND locked_by = $2`: [`Fenced`](Self::Fenced)
/// means that predicate matched no row, because another worker already
/// reclaimed the lease (or, for `mark_done`, the row was already acked), so
/// the write was rejected rather than clobbering the new claimant's
/// in-flight work or resurrecting a finished row.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// The row matched the fence and was updated (or, for `mark_done` on the
    /// state outbox, deleted) as requested.
    Acked,
    /// The fence rejected the write: this worker no longer owns the row.
    Fenced,
}

impl AckOutcome {
    pub(crate) fn from_rows_affected(affected: u64) -> Self {
        if affected == 1 {
            Self::Acked
        } else {
            Self::Fenced
        }
    }
}

/// Applies a fenced `mark_failed` update to one outbox row: increments
/// `failures`, moves the row to `failed` once `max_failures` is reached
/// (otherwise back to `pending` behind [`BackoffPolicy`]'s delay), and clears
/// the lock. Shared by [`PostgresPolicyOutbox::mark_failed`] and
/// [`crate::state_outbox::PostgresStateOutbox::mark_failed`], whose tables
/// have identical failure/backoff/DLQ columns.
///
/// Fenced on `WHERE id = $1 AND locked_by = $2`: a worker that no longer
/// owns the claim gets [`AckOutcome::Fenced`] rather than clearing another
/// claimant's lock. Returns the raw `sqlx::Error` so each caller can label
/// it with its own context.
pub(crate) async fn mark_row_failed(
    pool: &PgPool,
    table: &str,
    id: i64,
    worker_id: &str,
    error_message: &str,
    max_failures: Option<i32>,
    backoff: BackoffPolicy,
) -> sqlx::Result<AckOutcome> {
    let query = format!(
        "UPDATE {table}
         SET failures = failures + 1,
             status = CASE
                WHEN $3 IS NOT NULL AND failures + 1 >= $3 THEN 'failed'
                ELSE 'pending'
             END,
             locked_by = NULL,
             locked_until = NOW() + make_interval(secs =>
                LEAST($5::float8, $4::float8 * POWER(2, LEAST(failures, {MAX_BACKOFF_EXPONENT})))
                * (1 + random() * $6::float8)),
             last_error = $7,
             updated_at = NOW()
         WHERE id = $1 AND locked_by = $2"
    );
    let affected = sqlx::query(&query)
        .bind(id)
        .bind(worker_id)
        .bind(max_failures)
        .bind(backoff.base().as_secs_f64())
        .bind(backoff.cap().as_secs_f64())
        .bind(backoff.jitter())
        .bind(error_message)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(AckOutcome::from_rows_affected(affected))
}

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
#[derive(Debug, Clone, sqlx::FromRow)]
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
        crate::sql::qualify(&self.schema, "policy_outbox")
    }

    /// Creates the `policy_outbox` table and supporting indexes if they
    /// don't yet exist. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be created or the migration
    /// statements fail.
    pub async fn migrate(&self) -> Result<()> {
        crate::migrations::with_migration_lock(
            &self.pool,
            &self.schema,
            "_policy_outbox_migrations",
            |migrations_table| async move { self.apply_migrations(&migrations_table).await },
        )
        .await
    }

    /// Applies every policy-outbox migration step, run by [`Self::migrate`]
    /// while it holds the cross-store migration lock.
    async fn apply_migrations(&self, migrations_table: &str) -> Result<()> {
        let outbox_table = self.outbox_table();
        crate::migrations::apply_once(
            &self.pool,
            migrations_table,
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
            migrations_table,
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
                locked_until = NOW() + make_interval(secs => $4::float8),
                attempts = attempts + 1,
                updated_at = NOW()
            FROM claimed
            WHERE o.id = claimed.id
            RETURNING o.id, o.event_id, o.event_position, o.attempts, o.failures"
        );

        sqlx::query_as(&query)
            .bind(policy_name)
            .bind(i64::from(batch_size))
            .bind(worker_id)
            .bind(lock_duration.as_secs_f64())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to claim outbox batch", e))
    }

    /// Marks an outbox row as successfully processed by `worker_id`. Releases
    /// the lock.
    ///
    /// Fenced on `locked_by = worker_id`: a worker whose claim already expired
    /// and was reclaimed by someone else gets [`AckOutcome::Fenced`] instead
    /// of overwriting the new claimant's in-flight work. Once a row is done,
    /// `locked_by` is cleared, so a stale, delayed ack for the same worker
    /// can never resurrect it either.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn mark_done(&self, id: i64, worker_id: &str) -> Result<AckOutcome> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "UPDATE {outbox_table}
             SET status = 'done',
                 locked_by = NULL,
                 locked_until = NULL,
                 last_error = NULL,
                 updated_at = NOW()
             WHERE id = $1 AND locked_by = $2"
        );
        let affected = sqlx::query(&query)
            .bind(id)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to mark outbox row done", e))?
            .rows_affected();
        Ok(AckOutcome::from_rows_affected(affected))
    }

    /// Records a real handler failure for an outbox row claimed by
    /// `worker_id`. The row stays in the table for ops inspection (DLQ);
    /// reset to `pending` manually if you want to retry after an external
    /// fix.
    ///
    /// Fenced on `locked_by = worker_id`, for the same reason as
    /// [`mark_done`](Self::mark_done): a worker that no longer owns the claim
    /// (reclaimed after its lock expired, or already acked) gets
    /// [`AckOutcome::Fenced`] rather than resurrecting a done row or
    /// clearing the new claimant's lock.
    ///
    /// Increments the row's `failures` counter, then decides where it lands:
    /// if `max_attempts` is `None`, or the row's `failures` is still below
    /// `max_attempts`, the row returns to `pending` — but not immediately
    /// reclaimable: `backoff` sets its `locked_until` into the future so N
    /// polling workers don't all retry it on the very next sweep. Once
    /// `failures` reaches `max_attempts` it's parked in `failed` permanently.
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
        worker_id: &str,
        error_message: &str,
        max_attempts: Option<i32>,
        backoff: BackoffPolicy,
    ) -> Result<AckOutcome> {
        let outbox_table = self.outbox_table();
        mark_row_failed(
            &self.pool,
            &outbox_table,
            id,
            worker_id,
            error_message,
            max_attempts,
            backoff,
        )
        .await
        .map_err(|e| Error::backend("Failed to mark outbox row failed", e))
    }

    /// Returns the number of pending rows for `policy_name` (rows in
    /// `pending` status with no active lock). Useful for monitoring lag.
    ///
    /// A row currently backing off after a failure (see
    /// [`mark_failed`](Self::mark_failed)) has a future `locked_until` and so
    /// is **not** counted here, the same as a row still held by an in-flight
    /// claim.
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

    /// Deletes up to `batch_size` `done` rows last updated more than
    /// `older_than` ago, and returns how many were deleted.
    ///
    /// Unlike the state outbox (which deletes on success, since it has no log
    /// to replay from), this outbox keeps `done` rows — see the module docs —
    /// so nothing prunes it automatically and it grows without bound. Call
    /// this periodically (e.g. from a scheduled task) with an `older_than`
    /// retention longer than any deliberate checkpoint rewind you rely on for
    /// a policy, so the rewind can still re-enqueue idempotently against rows
    /// the `UNIQUE(policy_name, event_id)` constraint would otherwise still
    /// hold. A policy's own checkpoint only ever moves forward or rewinds
    /// deliberately — it is never rewound by another policy joining or
    /// leaving a
    /// [`dispatch_policies_to_outbox`](crate::PostgresBackend::dispatch_policies_to_outbox)
    /// call — so pruning one policy's `done` rows has no effect on any other
    /// policy's delivery.
    ///
    /// Deletes in bounded batches rather than one unbounded statement, so a
    /// large backlog doesn't hold a long-lived lock or a huge transaction. A
    /// single call may leave more prunable rows behind if there are more than
    /// `batch_size`; call it again (or loop) to fully drain a backlog.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails.
    pub async fn prune_done(&self, older_than: Duration, batch_size: u32) -> Result<u64> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "DELETE FROM {outbox_table}
             WHERE id IN (
                 SELECT id FROM {outbox_table}
                 WHERE status = 'done'
                   AND updated_at < NOW() - make_interval(secs => $1::float8)
                 LIMIT $2
             )"
        );
        let deleted = sqlx::query(&query)
            .bind(older_than.as_secs_f64())
            .bind(i64::from(batch_size))
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to prune done outbox rows", e))?
            .rows_affected();
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use testcontainers_modules::postgres::Postgres;

    #[test]
    fn backoff_policy_default_is_one_second_capped_at_five_minutes() {
        let policy = BackoffPolicy::default();
        assert_eq!(policy.base(), Duration::from_secs(1));
        assert_eq!(policy.cap(), Duration::from_secs(300));
        assert!((policy.jitter() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn backoff_policy_clamps_jitter_to_unit_range() {
        let over = BackoffPolicy::new(Duration::ZERO, Duration::ZERO, 2.0);
        assert!((over.jitter() - 1.0).abs() < f64::EPSILON);
        let under = BackoffPolicy::new(Duration::ZERO, Duration::ZERO, -1.0);
        assert!((under.jitter() - 0.0).abs() < f64::EPSILON);
        let nan = BackoffPolicy::new(Duration::ZERO, Duration::ZERO, f64::NAN);
        assert!((nan.jitter() - 0.0).abs() < f64::EPSILON);
    }

    struct TestDb {
        pool: PgPool,
        #[allow(dead_code)]
        container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    }

    impl TestDb {
        async fn new() -> Self {
            let (container, url) = crate::test_support::start_postgres_url().await;
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
        assert_eq!(
            outbox.mark_done(claims[0].id, "w-1").await.unwrap(),
            AckOutcome::Acked
        );

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
        assert_eq!(
            outbox
                .mark_failed(
                    claims[0].id,
                    "w-1",
                    "transient",
                    Some(3),
                    BackoffPolicy::none()
                )
                .await
                .unwrap(),
            AckOutcome::Acked
        );

        // Row is back to pending (since attempts=1 < max=3); next claim sees it.
        let next = outbox
            .claim_batch("policy-a", "w-2", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].attempts, 2);
    }

    /// Immediately after the failure, the row must not be reclaimable — it
    /// is backing off, not free for the next poll to hot-loop on. Uses a
    /// long backoff and backdates `locked_until` by SQL instead of
    /// sleeping past a short one, so the assertion never races the clock.
    #[tokio::test]
    async fn test_mark_failed_backoff_delays_reclaim() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let claims = outbox
            .claim_batch("policy-a", "w-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        let id = claims[0].id;
        let backoff = BackoffPolicy::new(Duration::from_secs(60), Duration::from_secs(300), 0.0);
        assert_eq!(
            outbox
                .mark_failed(id, "w-1", "transient", None, backoff)
                .await
                .unwrap(),
            AckOutcome::Acked
        );

        let offset = locked_until_offset_secs(&outbox, id).await;
        assert!(
            (59.0..=61.0).contains(&offset),
            "expected ~60s backoff after the failure, got {offset}"
        );

        let immediate = outbox
            .claim_batch("policy-a", "w-2", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            immediate.is_empty(),
            "a just-failed row must not be immediately reclaimable"
        );
        assert_eq!(
            outbox.pending_count("policy-a").await.unwrap(),
            0,
            "pending_count must exclude rows still backing off"
        );

        sqlx::query(
            "UPDATE event_sauce.policy_outbox
             SET locked_until = NOW() - INTERVAL '1 second' WHERE id = $1",
        )
        .bind(id)
        .execute(outbox.pool())
        .await
        .unwrap();
        let after_backoff = outbox
            .claim_batch("policy-a", "w-2", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            after_backoff.len(),
            1,
            "the row must become reclaimable once its backoff elapses"
        );
    }

    /// 1st failure: failures 0 -> 1, delay = base * 2^0 = 10s.
    /// 2nd failure: failures 1 -> 2, delay = base * 2^1 = 20s, capped at 15s.
    #[tokio::test]
    async fn test_mark_failed_backoff_grows_and_caps() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let backoff = BackoffPolicy::new(Duration::from_secs(10), Duration::from_secs(15), 0.0);

        let claims = outbox
            .claim_batch("policy-a", "w-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        let id = claims[0].id;
        let _ = outbox
            .mark_failed(id, "w-1", "e1", None, backoff)
            .await
            .unwrap();
        let after_first = locked_until_offset_secs(&outbox, id).await;
        assert!(
            (9.0..=10.5).contains(&after_first),
            "expected ~10s backoff after 1st failure, got {after_first}"
        );

        sqlx::query("UPDATE event_sauce.policy_outbox SET locked_until = NOW() WHERE id = $1")
            .bind(id)
            .execute(outbox.pool())
            .await
            .unwrap();
        let claims = outbox
            .claim_batch("policy-a", "w-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        let _ = outbox
            .mark_failed(claims[0].id, "w-1", "e2", None, backoff)
            .await
            .unwrap();
        let after_second = locked_until_offset_secs(&outbox, id).await;
        assert!(
            (14.0..=15.5).contains(&after_second),
            "expected the cap (~15s) once the doubled delay exceeds it, got {after_second}"
        );
    }

    /// Retry-forever mode (`max_attempts=None`) can drive `failures` far past
    /// the point where `base * 2^failures` overflows float8, well before any
    /// sane cap would ever be reached.
    #[tokio::test]
    async fn test_mark_failed_does_not_overflow_with_many_failures() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let claims = outbox
            .claim_batch("policy-a", "w-1", 10, Duration::from_secs(60))
            .await
            .unwrap();
        let id = claims[0].id;

        sqlx::query("UPDATE event_sauce.policy_outbox SET failures = 5000 WHERE id = $1")
            .bind(id)
            .execute(outbox.pool())
            .await
            .unwrap();

        let backoff = BackoffPolicy::new(Duration::from_secs(1), Duration::from_secs(300), 0.0);
        let landed = outbox
            .mark_failed(id, "w-1", "still failing", None, backoff)
            .await
            .unwrap();
        assert_eq!(
            landed,
            AckOutcome::Acked,
            "the exponent must be clamped, never overflow"
        );

        let locked_until_offset = locked_until_offset_secs(&outbox, id).await;
        assert!(
            (299.0..=300.5).contains(&locked_until_offset),
            "expected the cap (~300s) once failures is this large, got {locked_until_offset}"
        );
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
        assert_eq!(
            outbox
                .mark_failed(
                    claims[0].id,
                    "w-1",
                    "permanent",
                    Some(1),
                    BackoffPolicy::none()
                )
                .await
                .unwrap(),
            AckOutcome::Acked
        );

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

    /// Seconds between `NOW()` and the row's `locked_until` (negative if past).
    async fn locked_until_offset_secs(outbox: &PostgresPolicyOutbox, id: i64) -> f64 {
        sqlx::query_scalar(
            "SELECT EXTRACT(EPOCH FROM (locked_until - NOW()))::float8
             FROM event_sauce.policy_outbox WHERE id = $1",
        )
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
        let _ = outbox
            .mark_failed(id, "crasher", "transient", Some(2), BackoffPolicy::none())
            .await
            .unwrap();
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
        let _ = outbox
            .mark_failed(
                claims[0].id,
                "retrier",
                "permanent",
                Some(2),
                BackoffPolicy::none(),
            )
            .await
            .unwrap();
        let status = status_of(&outbox, id).await;
        assert_eq!(
            status, "failed",
            "second real failure (failures==2 >= max=2) flips to DLQ"
        );
    }

    /// Worker A claims with a short lock and "crashes"; worker B reclaims
    /// after the lock expires. Worker A's ack, unaware it lost the claim,
    /// must be rejected by the fence, and worker B's must land.
    #[tokio::test]
    async fn test_mark_done_is_fenced_on_claiming_worker() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let claims = outbox
            .claim_batch("policy-a", "worker-a", 10, Duration::from_millis(50))
            .await
            .unwrap();
        let id = claims[0].id;
        expire_lock(&outbox, id).await;

        let reclaimed = outbox
            .claim_batch("policy-a", "worker-b", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 1);

        let landed = outbox.mark_done(id, "worker-a").await.unwrap();
        assert_eq!(
            landed,
            AckOutcome::Fenced,
            "a worker that no longer owns the claim must not be able to mark it done"
        );
        assert_eq!(status_of(&outbox, id).await, "pending");

        let landed = outbox.mark_done(id, "worker-b").await.unwrap();
        assert_eq!(
            landed,
            AckOutcome::Acked,
            "the current owner must be able to mark it done"
        );
        assert_eq!(status_of(&outbox, id).await, "done");
    }

    /// Same fencing as `test_mark_done_is_fenced_on_claiming_worker`, for
    /// `mark_failed` instead of `mark_done`.
    #[tokio::test]
    async fn test_mark_failed_is_fenced_on_claiming_worker() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let claims = outbox
            .claim_batch("policy-a", "worker-a", 10, Duration::from_millis(50))
            .await
            .unwrap();
        let id = claims[0].id;
        expire_lock(&outbox, id).await;

        outbox
            .claim_batch("policy-a", "worker-b", 10, Duration::from_secs(60))
            .await
            .unwrap();

        let landed = outbox
            .mark_failed(id, "worker-a", "stale", Some(3), BackoffPolicy::none())
            .await
            .unwrap();
        assert_eq!(
            landed,
            AckOutcome::Fenced,
            "a worker that no longer owns the claim must not be able to mark it failed"
        );
        assert_eq!(status_of(&outbox, id).await, "pending");
    }

    /// A stale, delayed failure report for the same worker must not flip a
    /// delivered row back to pending/failed: `mark_done` already cleared the
    /// lock, so the fence rejects it.
    #[tokio::test]
    async fn test_mark_failed_cannot_resurrect_a_done_row() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        let claims = outbox
            .claim_batch("policy-a", "worker-a", 10, Duration::from_secs(60))
            .await
            .unwrap();
        let id = claims[0].id;
        assert_eq!(
            outbox.mark_done(id, "worker-a").await.unwrap(),
            AckOutcome::Acked
        );
        assert_eq!(status_of(&outbox, id).await, "done");

        let landed = outbox
            .mark_failed(
                id,
                "worker-a",
                "late failure report",
                Some(3),
                BackoffPolicy::none(),
            )
            .await
            .unwrap();
        assert_eq!(
            landed,
            AckOutcome::Fenced,
            "a done row must never be resurrected"
        );
        assert_eq!(status_of(&outbox, id).await, "done");
    }

    /// A 300ms lock, truncated to whole seconds, would become 0s and let
    /// another worker reclaim immediately. It must still be held 150ms in.
    #[tokio::test]
    async fn test_claim_lock_duration_has_millisecond_precision() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();
        enqueue_one(&outbox, "policy-a", 1).await;

        outbox
            .claim_batch("policy-a", "worker-a", 10, Duration::from_millis(300))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(150)).await;
        let still_locked = outbox
            .claim_batch("policy-a", "worker-b", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            still_locked.is_empty(),
            "a sub-second lock must not truncate to zero"
        );

        tokio::time::sleep(Duration::from_millis(250)).await;
        let now_reclaimable = outbox
            .claim_batch("policy-a", "worker-b", 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(now_reclaimable.len(), 1, "lock must expire after 300ms");
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

    /// Enqueues, claims and marks a row done, then backdates its
    /// `updated_at` by `age` so it looks like it finished `age` ago.
    async fn done_row_aged(outbox: &PostgresPolicyOutbox, policy: &str, age: Duration) -> i64 {
        let event_id = enqueue_one(outbox, policy, 1).await;
        let claims = outbox
            .claim_batch(policy, "pruner-setup", 10, Duration::from_secs(60))
            .await
            .unwrap();
        let id = claims.iter().find(|c| c.event_id == event_id).unwrap().id;
        assert_eq!(
            outbox.mark_done(id, "pruner-setup").await.unwrap(),
            AckOutcome::Acked
        );
        sqlx::query(
            "UPDATE event_sauce.policy_outbox
             SET updated_at = NOW() - make_interval(secs => $2::float8)
             WHERE id = $1",
        )
        .bind(id)
        .bind(age.as_secs_f64())
        .execute(outbox.pool())
        .await
        .unwrap();
        id
    }

    #[tokio::test]
    async fn test_prune_done_removes_only_old_done_rows() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let old_done = done_row_aged(&outbox, "policy-a", Duration::from_secs(3600)).await;
        let recent_done = done_row_aged(&outbox, "policy-a", Duration::from_secs(1)).await;
        enqueue_one(&outbox, "policy-a", 2).await;

        let deleted = outbox
            .prune_done(Duration::from_secs(60), 100)
            .await
            .unwrap();
        assert_eq!(deleted, 1, "only the old done row is prunable");

        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_sauce.policy_outbox")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(
            remaining, 2,
            "the recent done row and the pending row must survive"
        );
        assert_eq!(status_of(&outbox, recent_done).await, "done");
        let old_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM event_sauce.policy_outbox WHERE id = $1)",
        )
        .bind(old_done)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(!old_exists, "the old done row must have been pruned");
    }

    #[tokio::test]
    async fn test_prune_done_respects_batch_size() {
        let db = TestDb::new().await;
        let outbox = PostgresPolicyOutbox::new(db.pool.clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        for _ in 0..5 {
            done_row_aged(&outbox, "policy-a", Duration::from_secs(3600)).await;
        }

        let deleted = outbox.prune_done(Duration::from_secs(60), 2).await.unwrap();
        assert_eq!(
            deleted, 2,
            "a call must not delete more than batch_size rows"
        );

        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM event_sauce.policy_outbox WHERE status = 'done'",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(remaining, 3, "the rest must survive this one call");
    }
}
