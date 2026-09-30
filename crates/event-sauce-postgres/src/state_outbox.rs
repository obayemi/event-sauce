//! Transactional outbox for the Postgres state store.
//!
//! On the state-stored path there is no event log, so the outbox is the
//! **only** mechanism for effects that must outlive the save transaction
//! (policies, notifications, external I/O). When enabled on
//! [`PostgresStateStore`](crate::PostgresStateStore), every event of a
//! [`StateCommit`](event_sauce_core::StateCommit) is inserted into the
//! `state_outbox` table **inside the same transaction** as the state write —
//! a failed save enqueues nothing, a committed save enqueues exactly its
//! events.
//!
//! A background [`StateOutboxDispatcher`] then drains the table with
//! `FOR UPDATE SKIP LOCKED` claims, invokes a [`StateOutboxHandler`] per
//! event envelope, and **deletes** rows on success (the outbox is pruned, not
//! kept — unlike the event-sourced path there is no log to replay from, so a
//! delivered row has no further value). Failed rows follow the same
//! retry/DLQ semantics as the policy outbox: each handler error bumps the
//! row's `failures` counter and returns it to `pending`, until `failures`
//! reaches the configured maximum and the row is parked in `failed` status
//! for operational inspection.
//!
//! # Delivery is at-least-once — handlers MUST be idempotent
//!
//! Claiming, handling, and pruning are separate steps, not one transaction.
//! A worker that runs the side effect and crashes before the prune leaves the
//! row `pending`, and another worker re-delivers it after the lock expires.
//! Deduplicate on [`EventEnvelope::id`] if the effect must not repeat.
//!
//! [`EventEnvelope::id`]: event_sauce_core::EventEnvelope

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use event_sauce_core::{Error, EventEnvelope, Result};
use sqlx::PgPool;

/// Handler invoked by the [`StateOutboxDispatcher`] for each outbox event.
///
/// Implementations must be idempotent: delivery is at-least-once, keyed by
/// [`EventEnvelope::id`](event_sauce_core::EventEnvelope) for deduplication.
#[async_trait]
pub trait StateOutboxHandler: Send + Sync {
    /// Processes one event envelope drained from the outbox.
    ///
    /// # Errors
    ///
    /// Returns an error to record a delivery failure: the row's `failures`
    /// counter is incremented and the row is retried (or parked in the DLQ
    /// once the dispatcher's maximum failure count is reached).
    async fn handle(&self, envelope: &EventEnvelope) -> Result<()>;
}

/// One claimed state-outbox row, returned by
/// [`PostgresStateOutbox::claim_batch`].
#[derive(Debug, Clone)]
pub struct StateOutboxClaim {
    /// Outbox row id (used to mark the row done or failed).
    pub id: i64,
    /// The full event envelope enqueued by the state save.
    pub envelope: EventEnvelope,
    /// How many times this row has been claimed for delivery (including
    /// lease-expiry reclaims of a crashed worker). Observability only — the
    /// DLQ decision keys off [`failures`](Self::failures).
    pub attempts: i32,
    /// How many times a handler has reported a real error for this row.
    pub failures: i32,
}

/// Postgres-backed state outbox queue.
///
/// Owns the `state_outbox` table (schema-qualified, created by
/// [`PostgresStateStore::migrate`](crate::PostgresStateStore::migrate)).
/// Multiple workers can drain in parallel safely: claims use
/// `FOR UPDATE SKIP LOCKED` so each row is delivered to exactly one worker at
/// a time. Most callers drive it through a [`StateOutboxDispatcher`] rather
/// than calling the queue primitives directly.
#[derive(Clone)]
pub struct PostgresStateOutbox {
    pool: PgPool,
    schema: String,
}

impl PostgresStateOutbox {
    /// Creates an outbox handle bound to `pool` and `schema`.
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
        crate::migrations::qualify(&self.schema, "state_outbox")
    }

    pub(crate) async fn enqueue_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        envelope: &EventEnvelope,
    ) -> Result<()> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "INSERT INTO {outbox_table} (event_id, envelope)
             VALUES ($1, $2)
             ON CONFLICT (event_id) DO NOTHING"
        );
        sqlx::query(&query)
            .bind(envelope.id)
            .bind(serde_json::to_value(envelope)?)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::backend("Failed to enqueue state outbox row", e))?;
        Ok(())
    }

    /// Atomically claims up to `batch_size` pending rows using
    /// `FOR UPDATE SKIP LOCKED`, in enqueue order. Each claimed row is locked
    /// for `lock_duration`; if the worker doesn't mark it done/failed before
    /// the lock expires, another worker may reclaim it.
    ///
    /// Claiming bumps `attempts` (a delivery/observability counter), never
    /// `failures` — a crashed worker can not push a row toward the DLQ.
    ///
    /// # Errors
    ///
    /// Returns an error if the claim query fails or a stored envelope cannot
    /// be deserialized.
    pub async fn claim_batch(
        &self,
        worker_id: &str,
        batch_size: u32,
        lock_duration: Duration,
    ) -> Result<Vec<StateOutboxClaim>> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "WITH claimed AS (
                SELECT id FROM {outbox_table}
                WHERE status = 'pending'
                  AND (locked_until IS NULL OR locked_until < NOW())
                ORDER BY id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {outbox_table} o
            SET locked_by = $2,
                locked_until = NOW() + make_interval(secs => $3::float8),
                attempts = attempts + 1,
                updated_at = NOW()
            FROM claimed
            WHERE o.id = claimed.id
            RETURNING o.id, o.envelope, o.attempts, o.failures"
        );

        let rows: Vec<(i64, serde_json::Value, i32, i32)> = sqlx::query_as(&query)
            .bind(i64::from(batch_size))
            .bind(worker_id)
            .bind(lock_duration.as_secs_f64())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to claim state outbox batch", e))?;

        let mut claims = Vec::with_capacity(rows.len());
        for (id, envelope, attempts, failures) in rows {
            claims.push(StateOutboxClaim {
                id,
                envelope: serde_json::from_value(envelope)?,
                attempts,
                failures,
            });
        }
        claims.sort_by_key(|claim| claim.id);
        Ok(claims)
    }

    /// Marks an outbox row as successfully dispatched by `worker_id` by
    /// **deleting** it.
    ///
    /// Delivered rows are pruned, not kept: the state-stored path has no event
    /// log, so a dispatched row has no replay value.
    ///
    /// Fenced on `locked_by = worker_id`: a worker whose lock already expired
    /// and was reclaimed by another worker has its delete silently rejected
    /// (`Ok(false)`) instead of deleting a row the new claimant is still
    /// processing.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails.
    pub async fn mark_done(&self, id: i64, worker_id: &str) -> Result<bool> {
        let outbox_table = self.outbox_table();
        let query = format!("DELETE FROM {outbox_table} WHERE id = $1 AND locked_by = $2");
        let affected = sqlx::query(&query)
            .bind(id)
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to prune state outbox row", e))?
            .rows_affected();
        Ok(affected == 1)
    }

    /// Records a handler failure for an outbox row claimed by `worker_id`.
    ///
    /// Fenced on `locked_by = worker_id`, for the same reason as
    /// [`mark_done`](Self::mark_done): a worker that no longer owns the claim
    /// has its report rejected (`Ok(false)`) rather than clearing a new
    /// claimant's lock or resurrecting an already-pruned row.
    ///
    /// Increments the row's `failures` counter and releases its lock. While
    /// `failures` stays below `max_failures` the row returns to `pending` —
    /// but not immediately reclaimable: `backoff` sets its `locked_until`
    /// into the future so N polling workers don't all retry it on the very
    /// next sweep. Once it reaches `max_failures` it is parked in `failed`
    /// status permanently (DLQ) for operational inspection — reset it to
    /// `pending` manually to retry after an external fix. With
    /// `max_failures` of `None` the row retries forever — `backoff`'s
    /// exponent is clamped so its delay never overflows however large
    /// `failures` gets in that mode.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn mark_failed(
        &self,
        id: i64,
        worker_id: &str,
        error_message: &str,
        max_failures: Option<i32>,
        backoff: crate::BackoffPolicy,
    ) -> Result<bool> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "UPDATE {outbox_table}
             SET failures = failures + 1,
                 status = CASE
                    WHEN $3 IS NOT NULL AND failures + 1 >= $3 THEN 'failed'
                    ELSE 'pending'
                 END,
                 locked_by = NULL,
                 locked_until = NOW() + make_interval(secs =>
                    LEAST($5::float8, $4::float8 * POWER(2, LEAST(failures, 30)))
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
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to mark state outbox row failed", e))?
            .rows_affected();
        Ok(affected == 1)
    }

    /// Returns the number of pending rows (rows in `pending` status with no
    /// active lock). Useful for monitoring dispatch lag.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn pending_count(&self) -> Result<i64> {
        let outbox_table = self.outbox_table();
        let query = format!(
            "SELECT COUNT(*) FROM {outbox_table}
             WHERE status = 'pending'
               AND (locked_until IS NULL OR locked_until < NOW())"
        );
        let count: i64 = sqlx::query_scalar(&query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::backend("Failed to count pending state outbox rows", e))?;
        Ok(count)
    }

    /// Creates a [`StateOutboxDispatcher`] draining this outbox into `handler`.
    #[must_use]
    pub fn dispatcher(&self, handler: Arc<dyn StateOutboxHandler>) -> StateOutboxDispatcher {
        StateOutboxDispatcher {
            outbox: self.clone(),
            handler,
            worker_id: format!("state-outbox-{}", uuid::Uuid::new_v4()),
            batch_size: DEFAULT_BATCH_SIZE,
            lock_duration: DEFAULT_LOCK_DURATION,
            max_failures: Some(DEFAULT_MAX_FAILURES),
            backoff: crate::BackoffPolicy::default(),
        }
    }
}

const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_LOCK_DURATION: Duration = Duration::from_secs(60);
const DEFAULT_MAX_FAILURES: i32 = 3;

/// Background worker draining the state outbox into a
/// [`StateOutboxHandler`].
///
/// Each [`run_once`](Self::run_once) claims a batch, invokes the handler per
/// event envelope, prunes rows on success, and records failures with the
/// configured DLQ threshold. Multiple dispatchers over the same outbox split
/// the work safely thanks to `SKIP LOCKED` claims. Loop it from a task at
/// whatever cadence fits, e.g. on a `tokio::time::interval`.
pub struct StateOutboxDispatcher {
    outbox: PostgresStateOutbox,
    handler: Arc<dyn StateOutboxHandler>,
    worker_id: String,
    batch_size: u32,
    lock_duration: Duration,
    max_failures: Option<i32>,
    backoff: crate::BackoffPolicy,
}

impl StateOutboxDispatcher {
    /// Sets the worker identity recorded on claimed rows (defaults to a
    /// random per-dispatcher id).
    #[must_use]
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = worker_id.into();
        self
    }

    /// Sets how many rows one [`run_once`](Self::run_once) claims (default 100).
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets how long claimed rows stay locked before another worker may
    /// reclaim them (default 60 seconds).
    #[must_use]
    pub fn with_lock_duration(mut self, lock_duration: Duration) -> Self {
        self.lock_duration = lock_duration;
        self
    }

    /// Sets how many handler failures park a row in the `failed` DLQ status
    /// (default 3); `None` retries forever.
    #[must_use]
    pub fn with_max_failures(mut self, max_failures: Option<i32>) -> Self {
        self.max_failures = max_failures;
        self
    }

    /// Sets the backoff applied to a row returned to `pending` after a
    /// handler failure (default: [`BackoffPolicy::default`](crate::BackoffPolicy::default)).
    #[must_use]
    pub fn with_backoff(mut self, backoff: crate::BackoffPolicy) -> Self {
        self.backoff = backoff;
        self
    }

    /// Claims one batch and dispatches it: the handler runs once per claimed
    /// envelope, successful rows are pruned, failing rows are recorded via
    /// [`PostgresStateOutbox::mark_failed`] with this dispatcher's failure
    /// threshold. Returns the number of successfully dispatched events.
    ///
    /// The whole batch shares one lock, started at the moment it was claimed.
    /// If handling runs long enough that the lock would already have expired,
    /// the remaining claims in the batch are left untouched — not handled,
    /// not acked — rather than risk acking a row another worker has since
    /// reclaimed and is concurrently processing (they are picked up, fenced,
    /// on the next sweep, by this worker or another). Every ack is itself
    /// fenced on this worker still owning the row's claim.
    ///
    /// # Errors
    ///
    /// Returns an error if claiming or the done/failed bookkeeping fails.
    /// Handler errors do **not** abort the run — they are recorded per row and
    /// the remaining claims are still processed.
    pub async fn run_once(&self) -> Result<usize> {
        let claimed_at = std::time::Instant::now();
        let claims = self
            .outbox
            .claim_batch(&self.worker_id, self.batch_size, self.lock_duration)
            .await?;

        let mut dispatched = 0;
        for claim in claims {
            if claimed_at.elapsed() >= self.lock_duration {
                break;
            }

            match self.handler.handle(&claim.envelope).await {
                Ok(()) => {
                    if self.outbox.mark_done(claim.id, &self.worker_id).await? {
                        dispatched += 1;
                    }
                }
                Err(e) => {
                    self.outbox
                        .mark_failed(
                            claim.id,
                            &self.worker_id,
                            &e.to_string(),
                            self.max_failures,
                            self.backoff,
                        )
                        .await?;
                }
            }
        }
        Ok(dispatched)
    }
}
