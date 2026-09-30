//! Retry backoff and the shared fenced `mark_failed` update, used by both
//! [`PostgresPolicyOutbox`](crate::PostgresPolicyOutbox) and
//! [`crate::state_outbox::PostgresStateOutbox`], whose outbox tables share
//! the same failure/backoff/DLQ columns.

use sqlx::PgPool;
use std::time::Duration;

use crate::policy_outbox::AckOutcome;

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

/// Applies a fenced `mark_failed` update to one outbox row: increments
/// `failures`, moves the row to `failed` once `max_failures` is reached
/// (otherwise back to `pending` behind [`BackoffPolicy`]'s delay), and clears
/// the lock. Shared by [`PostgresPolicyOutbox::mark_failed`](crate::PostgresPolicyOutbox::mark_failed) and
/// [`crate::state_outbox::PostgresStateOutbox::mark_failed`], whose tables
/// have identical failure/backoff/DLQ columns.
///
/// Fenced on `WHERE id = $1 AND locked_by = $2`: a worker that no longer
/// owns the claim gets [`AckOutcome::Fenced`](crate::policy_outbox::AckOutcome::Fenced) rather than clearing another
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
