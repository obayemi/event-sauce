//! Validation of the time to live every [`LoginStore`](crate::LoginStore)
//! backend shares.

use std::time::Duration;

use crate::LoginStoreError;

/// The longest time to live a store accepts, one leap year.
pub(crate) const MAX_TTL: Duration = Duration::from_hours(366 * 24);

/// Converts `ttl` to whole milliseconds, rounding up so a short but non-zero
/// ttl never becomes zero.
///
/// # Errors
///
/// Fails with [`LoginStoreError::InvalidTtl`] when `ttl` is zero or above
/// [`MAX_TTL`].
pub(crate) fn millis(ttl: Duration) -> Result<i64, LoginStoreError> {
    if ttl.is_zero() || ttl > MAX_TTL {
        return Err(LoginStoreError::InvalidTtl(ttl));
    }
    let whole = ttl.as_millis();
    let rounded = whole + u128::from(!ttl.subsec_nanos().is_multiple_of(1_000_000));
    Ok(i64::try_from(rounded).expect("a ttl within MAX_TTL fits in i64 milliseconds"))
}

#[cfg(test)]
mod tests;
