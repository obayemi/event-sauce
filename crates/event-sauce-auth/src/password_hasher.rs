//! The [`PasswordHasher`] trait.

use async_trait::async_trait;
use secrecy::SecretString;

use crate::PasswordHash;

#[cfg(feature = "argon2")]
mod argon2;

#[cfg(feature = "argon2")]
pub use self::argon2::Argon2Hasher;

/// Why a password could not be hashed or checked.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("password hashing failed: {0}")]
pub struct HashError(pub String);

/// Hashes passwords and checks them against stored hashes.
///
/// Both operations are async because hashing is CPU bound by design: an
/// implementation must not stall the async executor while it works, and
/// should move the work to a blocking thread.
#[async_trait]
pub trait PasswordHasher: Send + Sync {
    /// Hashes `password` under a fresh random salt.
    ///
    /// # Errors
    ///
    /// Returns [`HashError`] when the password cannot be hashed.
    async fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError>;

    /// Whether `password` matches `hash`.
    ///
    /// A mismatch is `Ok(false)`, never an error.
    ///
    /// # Errors
    ///
    /// Returns [`HashError`] when `hash` cannot be checked at all, such as
    /// a hash of an algorithm this hasher does not support.
    async fn verify(&self, password: &SecretString, hash: &PasswordHash)
        -> Result<bool, HashError>;
}
