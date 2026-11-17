//! Validation of the time to live shared by the login stores and the Redis
//! user cache.

use std::time::Duration;

/// The longest time to live accepted, one leap year.
pub(crate) const MAX_TTL: Duration = Duration::from_hours(366 * 24);

/// Converts `ttl` to whole milliseconds, rounding up so a short but non-zero
/// ttl never becomes zero.
///
/// Returns `None` when `ttl` is zero or above [`MAX_TTL`].
pub(crate) fn millis(ttl: Duration) -> Option<i64> {
    if ttl.is_zero() || ttl > MAX_TTL {
        return None;
    }
    let rounded = ttl.as_nanos().div_ceil(1_000_000);
    Some(i64::try_from(rounded).expect("a ttl within MAX_TTL fits in i64 milliseconds"))
}

/// The `PX` expiry of the Redis `SET` command for `millis`, as returned by
/// [`millis`].
#[cfg(feature = "redis")]
pub(crate) fn redis_expiry(millis: i64) -> redis::SetExpiry {
    redis::SetExpiry::PX(u64::try_from(millis).expect("a validated ttl is a positive count"))
}

#[cfg(test)]
mod tests;
