//! Error-handling strategy for a policy reaction: fail, skip, or retry with
//! backoff, and what to do once retries are exhausted.

use std::time::Duration;

/// What to do when a policy handler returns an error.
///
/// The variants differ in how they affect the per-policy checkpoint, and hence
/// what a subsequent
/// [`PolicyRunner::process_pending`](crate::PolicyRunner::process_pending)
/// call re-attempts:
///
/// - [`OnError::Fail`] **stops and resumes at the failing event**. Checkpoints
///   are persisted up to the last event each policy fully handled, but never
///   past the failing event — so a re-run resumes at it and never replays the
///   already-flushed effects of earlier events in the batch.
/// - [`OnError::Skip`] **permanently skips the failing event**: the checkpoint
///   advances past it and the source event is never retried. Use this only when
///   dropping the event is acceptable.
/// - [`OnError::Retry`] retries the same event with backoff; on exhaustion it
///   either fails (resume at the event) or skips it, per [`OnRetryExhausted`].
#[derive(Debug, Clone)]
pub enum OnError {
    /// Stop immediately and resume at the failing event on the next run.
    ///
    /// Checkpoints are persisted up to the last successfully-handled event but
    /// NOT advanced past the failing event.
    Fail,
    /// Log a warning and permanently skip the event: advance the checkpoint past
    /// it and continue. The source event is not retried on subsequent runs.
    ///
    /// Applies equally to a `handle()` error and to a `ConcurrencyConflict`
    /// from the subsequent `ctx.flush()` — a reaction whose flush conflicted
    /// is skipped exactly like one whose handler failed.
    Skip,
    /// Retry with exponential backoff before giving up.
    ///
    /// Retries the whole handle-then-flush unit: a `ConcurrencyConflict` from
    /// `ctx.flush()` is retried the same way a `handle()` error is, since each
    /// attempt reloads its aggregates and buffers a fresh commit.
    Retry(RetryConfig),
}

/// Configuration for exponential backoff retry.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Base delay between retries — doubles each attempt (default: 100ms).
    pub base_delay: Duration,
    /// Maximum delay cap (default: 30s).
    pub max_delay: Duration,
    /// When to stop retrying.
    pub limit: RetryLimit,
    /// What to do when all retries are exhausted.
    pub on_exhausted: OnRetryExhausted,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            limit: RetryLimit::MaxRetries(3),
            on_exhausted: OnRetryExhausted::Fail,
        }
    }
}

/// When to stop retrying.
#[derive(Debug, Clone)]
pub enum RetryLimit {
    /// Stop after N retry attempts.
    MaxRetries(usize),
    /// Stop after total elapsed time exceeds this duration.
    MaxDuration(Duration),
    /// Retry indefinitely until success (blocks processing on this event).
    Indefinite,
}

/// What to do when retry limit is reached.
#[derive(Debug, Clone)]
pub enum OnRetryExhausted {
    /// Return error, stop processing. Checkpoint NOT advanced.
    Fail,
    /// Log warning, skip the event, advance checkpoint, continue.
    Skip,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();

        assert_eq!(config.base_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert!(matches!(config.limit, RetryLimit::MaxRetries(3)));
        assert!(matches!(config.on_exhausted, OnRetryExhausted::Fail));
    }

    #[test]
    fn test_retry_limit_variants() {
        let max_retries = RetryLimit::MaxRetries(5);
        assert!(matches!(max_retries, RetryLimit::MaxRetries(5)));

        let max_duration = RetryLimit::MaxDuration(Duration::from_secs(60));
        assert!(matches!(max_duration, RetryLimit::MaxDuration(_)));

        let indefinite = RetryLimit::Indefinite;
        assert!(matches!(indefinite, RetryLimit::Indefinite));
    }
}
