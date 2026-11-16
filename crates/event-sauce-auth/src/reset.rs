//! Single-use tokens for password resets, stored as hashes only.
//!
//! [`ResetToken::issue`] draws 32 random bytes and returns the token, to show
//! to the user once, together with the [`ResetTokenHash`], to store in an
//! event. The token itself is never stored, so a leaked event stream cannot
//! be replayed into a reset. [`ResetTokenHash::verify`] takes the clock as a
//! parameter, which keeps the check pure.

use std::fmt;

use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64;
use base64::Engine;
use chrono::{DateTime, TimeDelta, Utc};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

const TOKEN_BYTES: usize = 32;

/// Why a string is not a reset token or a reset token hash: it is not the
/// canonical base64url encoding of 32 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("not a base64url encoded 32 byte value")]
pub struct ResetTokenError;

/// Why a reset token was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ResetError {
    /// The token is not the one the hash was made from.
    #[error("the reset token does not match")]
    Mismatch,
    /// The token matches but was issued longer ago than its time to live.
    #[error("the reset token has expired")]
    Expired,
}

/// The secret a user presents to reset a password: 32 random bytes encoded as
/// base64url without padding.
///
/// It is meant to be shown once, for example in an emailed link, and never
/// stored. `Debug` never prints it.
///
/// # Examples
///
/// ```
/// use chrono::{DateTime, TimeDelta};
/// use event_sauce_auth::{ResetError, ResetToken};
///
/// let (token, hash) = ResetToken::issue();
/// let issued_at = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
/// let ttl = TimeDelta::hours(1);
///
/// let presented = ResetToken::parse(token.as_str())?;
/// assert_eq!(hash.verify(&presented, issued_at, issued_at + TimeDelta::minutes(5), ttl), Ok(()));
/// assert_eq!(
///     hash.verify(&presented, issued_at, issued_at + TimeDelta::hours(2), ttl),
///     Err(ResetError::Expired)
/// );
/// # Ok::<(), event_sauce_auth::ResetTokenError>(())
/// ```
pub struct ResetToken(SecretString);

impl ResetToken {
    /// Draws a fresh token and the hash to store for it.
    #[must_use]
    pub fn issue() -> (Self, ResetTokenHash) {
        let mut bytes = [0u8; TOKEN_BYTES];
        rand::fill(&mut bytes);
        let token = Self(BASE64.encode(bytes).into());
        let hash = ResetTokenHash::of(&token);
        (token, hash)
    }

    /// Reads back a token a user presented.
    ///
    /// # Errors
    ///
    /// Returns [`ResetTokenError`] when `raw` is not the canonical base64url
    /// encoding of 32 bytes.
    pub fn parse(raw: &str) -> Result<Self, ResetTokenError> {
        decode(raw)?;
        Ok(Self(raw.into()))
    }

    /// The text to show the user. Whoever holds it can reset the password.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for ResetToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResetToken(<redacted>)")
    }
}

/// The SHA-256 of a [`ResetToken`]'s text, safe to store in an event.
///
/// It serializes as a base64url string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ResetTokenHash([u8; 32]);

impl ResetTokenHash {
    /// The hash of `token`.
    #[must_use]
    pub fn of(token: &ResetToken) -> Self {
        Self(Sha256::digest(token.as_str()).into())
    }

    /// Checks a presented token against this hash and its age.
    ///
    /// The token is valid through `issued_at + ttl` inclusive. The hash is
    /// compared first, so a wrong token learns nothing about expiry. A
    /// negative age, from clock skew between instances, counts as fresh.
    ///
    /// # Errors
    ///
    /// Returns [`ResetError::Mismatch`] when `token` is not the one this hash
    /// was made from, and [`ResetError::Expired`] when it is but `now` is
    /// past `issued_at + ttl`.
    pub fn verify(
        &self,
        token: &ResetToken,
        issued_at: DateTime<Utc>,
        now: DateTime<Utc>,
        ttl: TimeDelta,
    ) -> Result<(), ResetError> {
        if !bool::from(self.0.ct_eq(&Self::of(token).0)) {
            return Err(ResetError::Mismatch);
        }
        if now - issued_at > ttl {
            return Err(ResetError::Expired);
        }
        Ok(())
    }
}

impl TryFrom<String> for ResetTokenHash {
    type Error = ResetTokenError;

    fn try_from(encoded: String) -> Result<Self, Self::Error> {
        decode(&encoded).map(Self)
    }
}

impl From<ResetTokenHash> for String {
    fn from(hash: ResetTokenHash) -> Self {
        BASE64.encode(hash.0)
    }
}

fn decode(encoded: &str) -> Result<[u8; TOKEN_BYTES], ResetTokenError> {
    BASE64
        .decode(encoded)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ResetTokenError)
}

#[cfg(test)]
mod tests;
