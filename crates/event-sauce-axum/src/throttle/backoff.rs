use std::time::Duration;

#[cfg(test)]
#[path = "tests/backoff_tests.rs"]
mod tests;

/// How the failures of a key turn into delays.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use event_sauce_axum::Backoff;
///
/// let backoff = Backoff {
///     free: 2,
///     base: Duration::from_secs(1),
///     cap: Duration::from_secs(8),
///     forget_after: Duration::from_secs(3600),
/// };
///
/// assert_eq!(backoff.delay(2), Duration::ZERO);
/// assert_eq!(backoff.delay(3), Duration::from_secs(1));
/// assert_eq!(backoff.delay(6), Duration::from_secs(8));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// Failures counted without any delay.
    pub free: u32,
    /// The delay after the first failure past the free ones; each further
    /// failure doubles it.
    pub base: Duration,
    /// The longest delay.
    pub cap: Duration,
    /// How long a key is remembered after its last failure.
    pub forget_after: Duration,
}

impl Backoff {
    /// The delay a key owes after its `failures`-th failure.
    #[must_use]
    pub fn delay(&self, failures: u32) -> Duration {
        if failures <= self.free {
            return Duration::ZERO;
        }
        let past_free = failures - self.free - 1;
        let factor = 2u32.checked_pow(past_free).unwrap_or(u32::MAX);
        self.base.saturating_mul(factor).min(self.cap)
    }
}
