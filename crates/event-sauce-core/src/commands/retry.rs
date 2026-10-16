//! [`RetryPolicy`] — how many attempts a command gets, and how long to wait
//! between them.

use std::num::NonZeroU32;
use std::time::Duration;

const DEFAULT_MAX_ATTEMPTS: NonZeroU32 = NonZeroU32::new(3).unwrap();

/// Exponential-backoff retry configuration for running a [`Command`](super::Command)
/// against a transient failure, such as a concurrency conflict.
///
/// This type computes delays only; it does not sleep or loop itself — that
/// belongs to the [`Repository`](crate::Repository) that drives the retry.
/// `max_attempts` is a [`NonZeroU32`] so "zero attempts" is not a state this
/// type can express.
///
/// # Examples
///
/// ```
/// use event_sauce_core::RetryPolicy;
/// use std::num::NonZeroU32;
/// use std::time::Duration;
///
/// let policy = RetryPolicy::default()
///     .with_max_attempts(NonZeroU32::new(5).unwrap())
///     .with_initial_backoff(Duration::from_millis(50));
///
/// assert_eq!(policy.max_attempts().get(), 5);
/// assert_eq!(policy.backoff(0), Duration::ZERO);
/// assert_eq!(policy.backoff(1), Duration::from_millis(50));
///
/// let disabled = RetryPolicy::none();
/// assert_eq!(disabled.max_attempts().get(), 1);
/// assert_eq!(disabled.backoff(1), Duration::ZERO);
/// ```
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    max_attempts: NonZeroU32,
    initial_backoff: Duration,
    multiplier: f64,
    max_backoff: Duration,
    jitter: Option<fn(Duration) -> Duration>,
}

impl Default for RetryPolicy {
    /// Three attempts, a 10ms initial backoff doubling up to a 1s cap, no
    /// jitter.
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            initial_backoff: Duration::from_millis(10),
            multiplier: 2.0,
            max_backoff: Duration::from_secs(1),
            jitter: None,
        }
    }
}

impl RetryPolicy {
    /// A policy with a single attempt and zero backoff — retrying is
    /// disabled.
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_attempts: NonZeroU32::MIN,
            initial_backoff: Duration::ZERO,
            multiplier: 1.0,
            max_backoff: Duration::ZERO,
            jitter: None,
        }
    }

    /// Sets the total number of attempts, including the first.
    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: NonZeroU32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the delay before the first retry.
    #[must_use]
    pub fn with_initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    /// Sets the factor each retry's delay is scaled by.
    #[must_use]
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Sets the cap no computed delay can exceed.
    #[must_use]
    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Sets a pure function applied to each computed delay, for spreading
    /// out retries from multiple callers (e.g. randomizing within a range).
    #[must_use]
    pub fn with_jitter(mut self, jitter: fn(Duration) -> Duration) -> Self {
        self.jitter = Some(jitter);
        self
    }

    /// The total number of attempts, including the first.
    #[must_use]
    pub fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }

    /// The delay before the first retry.
    #[must_use]
    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    /// The factor each retry's delay is scaled by.
    #[must_use]
    pub fn multiplier(&self) -> f64 {
        self.multiplier
    }

    /// The cap no computed delay can exceed.
    #[must_use]
    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// Computes the delay before a given attempt.
    ///
    /// `attempt` is 1-indexed: `backoff(1)` is the delay before the second
    /// attempt (the first retry), `backoff(2)` before the third, and so on.
    /// `backoff(0)` is always [`Duration::ZERO`] — there is nothing to wait
    /// for before the very first attempt.
    ///
    /// The series is `initial_backoff * multiplier.powi(attempt - 1)`,
    /// capped at `max_backoff` and then passed through the jitter function,
    /// if one is set.
    #[must_use]
    pub fn backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        let mut delay = self.initial_backoff;
        for _ in 0..(attempt - 1).min(64) {
            if delay >= self.max_backoff {
                break;
            }
            delay = scale(delay, self.multiplier);
        }
        let delay = delay.min(self.max_backoff);

        self.jitter.map_or(delay, |jitter| jitter(delay))
    }
}

fn scale(duration: Duration, factor: f64) -> Duration {
    if !factor.is_finite() || factor <= 0.0 {
        return Duration::ZERO;
    }
    Duration::try_from_secs_f64(duration.as_secs_f64() * factor).unwrap_or(Duration::MAX)
}

#[cfg(test)]
mod tests;
