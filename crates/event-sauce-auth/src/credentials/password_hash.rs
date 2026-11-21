//! The [`PasswordHash`] newtype.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Why a string is not a PHC password hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("not a valid PHC password hash")]
pub struct PasswordHashError;

/// A password hash in the PHC string format, such as an Argon2 hash.
///
/// The constructor only accepts well-formed PHC strings, and `Debug` never
/// prints the hash.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::PasswordHash;
///
/// let phc = "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";
/// assert_eq!(PasswordHash::new(phc)?.as_str(), phc);
/// assert!(PasswordHash::new("hunter2").is_err());
/// # Ok::<(), event_sauce_auth::PasswordHashError>(())
/// ```
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PasswordHash(String);

impl PasswordHash {
    /// Wraps a PHC string.
    ///
    /// # Errors
    ///
    /// Returns [`PasswordHashError`] when `phc` is not a well-formed PHC
    /// string.
    pub fn new(phc: impl Into<String>) -> Result<Self, PasswordHashError> {
        let phc = phc.into();
        phc::PasswordHash::new(&phc).map_err(|_| PasswordHashError)?;
        Ok(Self(phc))
    }

    /// The PHC string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PasswordHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PasswordHash(<redacted>)")
    }
}

impl TryFrom<String> for PasswordHash {
    type Error = PasswordHashError;

    fn try_from(phc: String) -> Result<Self, Self::Error> {
        Self::new(phc)
    }
}

impl From<PasswordHash> for String {
    fn from(hash: PasswordHash) -> Self {
        hash.0
    }
}

#[cfg(test)]
#[path = "tests/password_hash_tests.rs"]
mod tests;
