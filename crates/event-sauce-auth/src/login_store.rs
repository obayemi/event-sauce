//! The [`LoginStore`] of logins in progress.

use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;

use async_trait::async_trait;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "redis")]
mod redis;

#[cfg(feature = "postgres")]
pub use postgres::{InvalidSchemaName, PostgresLoginStore};
#[cfg(feature = "redis")]
pub use redis::RedisLoginStore;

/// A namespaced key into a [`LoginStore`], such as `pending:{id}` or
/// `totp:{user}:{step}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoginKey(String);

impl LoginKey {
    /// Builds the key `{namespace}:{id}`.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_auth::LoginKey;
    ///
    /// assert_eq!(LoginKey::new("pending", "7b0e").as_str(), "pending:7b0e");
    /// ```
    #[must_use]
    pub fn new(namespace: &str, id: impl fmt::Display) -> Self {
        Self(format!("{namespace}:{id}"))
    }

    /// The key as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LoginKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a [`LoginStore`] operation failed.
#[derive(Debug, thiserror::Error)]
pub enum LoginStoreError {
    /// The time to live is zero or longer than the stores accept.
    #[error("invalid time to live: {0:?}")]
    InvalidTtl(Duration),
    /// The backend failed.
    #[error("login store backend failed: {context}")]
    Backend {
        /// What the store was doing when the backend failed.
        context: &'static str,
        /// The backend's own error.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl LoginStoreError {
    /// Wraps a backend failure with what the store was doing.
    pub fn backend(context: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Backend {
            context,
            source: Box::new(source),
        }
    }
}

#[cfg(any(feature = "postgres", feature = "redis"))]
fn backend<E: StdError + Send + Sync + 'static>(
    context: &'static str,
) -> impl FnOnce(E) -> LoginStoreError {
    move |error| LoginStoreError::backend(context, error)
}

/// Short-lived state of logins in progress, kept apart from the event store.
///
/// Pending second factors, consumed one-time-password steps and device-link
/// challenges live here rather than in a session: a device link spans two
/// devices, and replay protection is per user. Every entry expires, and a
/// store is shared by every instance of the application, so a login can
/// continue on another instance than the one that started it.
///
/// The ready-made implementations are `PostgresLoginStore` (feature
/// `postgres`) and `RedisLoginStore` (feature `redis`). There is deliberately
/// no in-memory implementation: state that does not survive across instances
/// would break a login in progress.
///
/// Every operation takes a time to live that must be greater than zero and at
/// most a year, and fails with [`LoginStoreError::InvalidTtl`] otherwise.
#[async_trait]
pub trait LoginStore: Send + Sync {
    /// Stores `value` under `key` until `ttl` elapses, replacing any entry.
    ///
    /// # Errors
    ///
    /// Fails if `ttl` is invalid or the backend fails.
    async fn put(&self, key: &LoginKey, value: &[u8], ttl: Duration)
        -> Result<(), LoginStoreError>;

    /// Atomically reads and deletes `key`, so every entry is single use.
    ///
    /// Returns `None` when the key is unknown or expired.
    ///
    /// # Errors
    ///
    /// Fails if the backend fails.
    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError>;

    /// Records `key` until `ttl` elapses if it is not already recorded.
    ///
    /// Returns `false` when the key was already recorded. Exactly one of
    /// several concurrent callers gets `true`.
    ///
    /// # Errors
    ///
    /// Fails if `ttl` is invalid or the backend fails.
    async fn mark_once(&self, key: &LoginKey, ttl: Duration) -> Result<bool, LoginStoreError>;
}

#[cfg(test)]
mod tests;
