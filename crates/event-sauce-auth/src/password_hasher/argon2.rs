//! The [`Argon2Hasher`] password hasher.

use std::fmt::Display;

use argon2::{
    password_hash::{phc, Error, PasswordHasher as _, PasswordVerifier as _},
    Argon2, Params,
};
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use tokio::task::{spawn_blocking, JoinError};

use crate::{HashError, PasswordHash, PasswordHasher};

/// A [`PasswordHasher`] using Argon2id.
///
/// The default cost follows the recommended Argon2 parameters. A hash made
/// at another cost still verifies, because the cost is read off the hash.
///
/// The Argon2 work runs on tokio's blocking thread pool, so it never stalls
/// the async executor. A panic in that work is reported as a [`HashError`].
///
/// # Examples
///
/// ```
/// use argon2::Params;
/// use event_sauce_auth::{Argon2Hasher, PasswordHasher};
/// use secrecy::SecretString;
///
/// # tokio_test::block_on(async {
/// let hasher = Argon2Hasher::new(Params::new(8, 1, 1, None)?);
/// let password = SecretString::from("correct horse");
/// let hash = hasher.hash(&password).await?;
/// assert!(hasher.verify(&password, &hash).await?);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # }).unwrap();
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

#[async_trait]
impl PasswordHasher for Argon2Hasher {
    async fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError> {
        let params = self.params.clone();
        let password = password.clone();
        flatten(spawn_blocking(move || hash_blocking(&params, &password)).await)
    }

    async fn verify(
        &self,
        password: &SecretString,
        hash: &PasswordHash,
    ) -> Result<bool, HashError> {
        let password = password.clone();
        let hash = hash.clone();
        flatten(spawn_blocking(move || verify_blocking(&password, &hash)).await)
    }
}

fn hash_blocking(params: &Params, password: &SecretString) -> Result<PasswordHash, HashError> {
    let hash = Argon2::from(params)
        .hash_password(password.expose_secret().as_bytes())
        .map_err(failure)?;
    PasswordHash::new(hash.to_string()).map_err(failure)
}

fn verify_blocking(password: &SecretString, hash: &PasswordHash) -> Result<bool, HashError> {
    let parsed = phc::PasswordHash::new(hash.as_str()).map_err(failure)?;
    match Argon2::default().verify_password(password.expose_secret().as_bytes(), &parsed) {
        Ok(()) => Ok(true),
        Err(Error::PasswordInvalid) => Ok(false),
        Err(error) => Err(failure(error)),
    }
}

fn flatten<T>(joined: Result<Result<T, HashError>, JoinError>) -> Result<T, HashError> {
    joined.map_err(failure)?
}

fn failure(error: impl Display) -> HashError {
    HashError(error.to_string())
}

#[cfg(test)]
#[path = "tests/argon2_tests.rs"]
mod tests;
