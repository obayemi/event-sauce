//! The [`PasswordHasher`] trait.

use secrecy::SecretString;

use crate::PasswordHash;

/// Why a password could not be hashed or checked.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("password hashing failed: {0}")]
pub struct HashError(pub String);

/// Hashes passwords and checks them against stored hashes.
///
/// Implementations are CPU bound by design: run them off the async
/// executor's threads when the cost is high.
pub trait PasswordHasher: Send + Sync {
    /// Hashes `password` under a fresh random salt.
    ///
    /// # Errors
    ///
    /// Returns [`HashError`] when the password cannot be hashed.
    fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError>;

    /// Whether `password` matches `hash`.
    ///
    /// A mismatch is `Ok(false)`, never an error.
    ///
    /// # Errors
    ///
    /// Returns [`HashError`] when `hash` cannot be checked at all, such as
    /// a hash of an algorithm this hasher does not support.
    fn verify(&self, password: &SecretString, hash: &PasswordHash) -> Result<bool, HashError>;
}
