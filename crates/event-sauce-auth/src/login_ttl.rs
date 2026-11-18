//! [`LoginTtl`], the time to live of an entry in a login store or a user
//! cache.

use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

/// A time to live that a login store or a user cache can honour.
///
/// A `LoginTtl` is never zero and never longer than [`LoginTtl::MAX`], a leap
/// year, so every store accepts it. It counts whole milliseconds: a duration
/// is rounded up, so a short but non-zero one never becomes zero.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use event_sauce_auth::{LoginTtl, LoginTtlError};
///
/// assert_eq!(LoginTtl::FIVE_MINUTES.as_duration(), Duration::from_secs(300));
/// assert_eq!(
///     LoginTtl::new(Duration::from_micros(1))?.as_duration(),
///     Duration::from_millis(1),
/// );
/// assert_eq!(LoginTtl::new(Duration::ZERO), Err(LoginTtlError::Zero));
/// # Ok::<(), LoginTtlError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LoginTtl(NonZeroU64);

/// Why a [`Duration`] is not a [`LoginTtl`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LoginTtlError {
    /// The duration is zero.
    #[error("time to live is zero")]
    Zero,
    /// The duration is longer than [`LoginTtl::MAX`].
    #[error("time to live {0:?} is longer than a leap year")]
    TooLong(Duration),
}

const MAX_MILLIS: u64 = 366 * 24 * 3_600_000;

const fn millis(count: u64) -> LoginTtl {
    match NonZeroU64::new(count) {
        Some(count) => LoginTtl(count),
        None => unreachable!(),
    }
}

impl LoginTtl {
    /// The longest time to live, one leap year.
    pub const MAX: Self = millis(MAX_MILLIS);

    /// One minute.
    pub const ONE_MINUTE: Self = millis(60_000);

    /// Five minutes, how long a pending login lives by default.
    pub const FIVE_MINUTES: Self = millis(300_000);

    /// One hour.
    pub const ONE_HOUR: Self = millis(3_600_000);

    /// One day, how long a user cache entry lives by default.
    pub const ONE_DAY: Self = millis(86_400_000);

    /// A time to live of `duration`, rounded up to whole milliseconds.
    ///
    /// # Errors
    ///
    /// Returns [`LoginTtlError::Zero`] for a zero duration and
    /// [`LoginTtlError::TooLong`] for one longer than [`LoginTtl::MAX`].
    pub fn new(duration: Duration) -> Result<Self, LoginTtlError> {
        if duration.is_zero() {
            return Err(LoginTtlError::Zero);
        }
        let rounded = duration.as_nanos().div_ceil(1_000_000);
        u64::try_from(rounded)
            .ok()
            .filter(|&count| count <= MAX_MILLIS)
            .map(millis)
            .ok_or(LoginTtlError::TooLong(duration))
    }

    /// A time to live of `count` milliseconds, which is always valid.
    #[must_use]
    pub const fn from_millis(count: NonZeroU32) -> Self {
        millis(count.get() as u64)
    }

    /// The time to live as a duration.
    #[must_use]
    pub const fn as_duration(self) -> Duration {
        Duration::from_millis(self.0.get())
    }

    /// The milliseconds a SQL interval counts.
    #[cfg(feature = "postgres")]
    pub(crate) fn sql_millis(self) -> i64 {
        i64::try_from(self.0.get()).expect("a time to live fits in i64 milliseconds")
    }

    /// The `PX` expiry of the Redis `SET` command.
    #[cfg(feature = "redis")]
    pub(crate) fn redis_expiry(self) -> redis::SetExpiry {
        redis::SetExpiry::PX(self.0.get())
    }
}

impl TryFrom<Duration> for LoginTtl {
    type Error = LoginTtlError;

    fn try_from(duration: Duration) -> Result<Self, Self::Error> {
        Self::new(duration)
    }
}

impl From<LoginTtl> for Duration {
    fn from(ttl: LoginTtl) -> Self {
        ttl.as_duration()
    }
}

#[cfg(test)]
mod tests;
