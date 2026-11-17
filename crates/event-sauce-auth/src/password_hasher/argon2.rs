//! The [`Argon2Hasher`] password hasher.

use std::fmt::Display;

use argon2::{
    password_hash::{phc, Error, PasswordHasher as _, PasswordVerifier as _},
    Argon2, Params,
};
use secrecy::{ExposeSecret, SecretString};

use crate::{HashError, PasswordHash, PasswordHasher};

/// A [`PasswordHasher`] using Argon2id.
///
/// The default cost follows the recommended Argon2 parameters. A hash made
/// at another cost still verifies, because the cost is read off the hash.
///
/// # Examples
///
/// ```
/// use argon2::Params;
/// use event_sauce_auth::{Argon2Hasher, PasswordHasher};
/// use secrecy::SecretString;
///
/// let hasher = Argon2Hasher::new(Params::new(8, 1, 1, None)?);
/// let password = SecretString::from("correct horse");
/// let hash = hasher.hash(&password)?;
/// assert!(hasher.verify(&password, &hash)?);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Default)]
pub struct Argon2Hasher {
    params: Params,
}

impl Argon2Hasher {
    /// A hasher with the given cost parameters.
    #[must_use]
    pub fn new(params: Params) -> Self {
        Self { params }
    }
}

impl PasswordHasher for Argon2Hasher {
    fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError> {
        let hash = Argon2::from(&self.params)
            .hash_password(password.expose_secret().as_bytes())
            .map_err(failure)?;
        PasswordHash::new(hash.to_string()).map_err(failure)
    }

    fn verify(&self, password: &SecretString, hash: &PasswordHash) -> Result<bool, HashError> {
        let parsed = phc::PasswordHash::new(hash.as_str()).map_err(failure)?;
        match Argon2::default().verify_password(password.expose_secret().as_bytes(), &parsed) {
            Ok(()) => Ok(true),
            Err(Error::PasswordInvalid) => Ok(false),
            Err(error) => Err(failure(error)),
        }
    }
}

fn failure(error: impl Display) -> HashError {
    HashError(error.to_string())
}

#[cfg(test)]
mod tests;
