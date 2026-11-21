//! The [`Clock`] that logins and TOTP read the time from.

use std::fmt::Debug;

use chrono::{DateTime, Utc};

/// A source of the current time, injected so that time-dependent checks are
/// deterministic under test.
pub trait Clock: Debug + Send + Sync {
    /// The current instant.
    fn now(&self) -> DateTime<Utc>;
}

/// The wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock stopped at one instant, for tests.
///
/// # Examples
///
/// ```
/// use chrono::DateTime;
/// use event_sauce_auth::{Clock, FixedClock};
///
/// let instant = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
/// assert_eq!(FixedClock(instant).now(), instant);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct FixedClock(pub DateTime<Utc>);

impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

#[cfg(test)]
mod tests;
