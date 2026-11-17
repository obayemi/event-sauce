//! The Redis [`LoginStore`].

use std::time::Duration;

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use crate::{ttl, LoginKey, LoginStore, LoginStoreError};

/// The prefix a [`RedisLoginStore`] puts before every key unless told
/// otherwise.
const DEFAULT_PREFIX: &str = "event_sauce:login:";

fn storage_key(prefix: &str, key: &LoginKey) -> String {
    format!("{prefix}{key}")
}

fn backend(context: &'static str) -> impl FnOnce(redis::RedisError) -> LoginStoreError {
    move |error| LoginStoreError::backend(context, error)
}

/// A [`LoginStore`] in Redis, using its native expiry.
///
/// `take` is a `GETDEL` and `mark_once` a `SET NX PX`, so both are atomic on
/// the server. `GETDEL` needs Redis 6.2 or newer.
///
/// # Examples
///
/// ```no_run
/// use event_sauce_auth::RedisLoginStore;
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let store = RedisLoginStore::connect("redis://localhost").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct RedisLoginStore {
    connection: ConnectionManager,
    prefix: String,
}

impl RedisLoginStore {
    /// Connects to the server at `url`, reconnecting automatically later.
    ///
    /// # Errors
    ///
    /// Fails if `url` is invalid or the server cannot be reached.
    pub async fn connect(url: &str) -> Result<Self, LoginStoreError> {
        let client = redis::Client::open(url).map_err(backend("open client"))?;
        let connection = ConnectionManager::new(client)
            .await
            .map_err(backend("connect"))?;
        Ok(Self::new(connection))
    }

    /// A store over a connection the application already manages.
    #[must_use]
    pub fn new(connection: ConnectionManager) -> Self {
        Self {
            connection,
            prefix: DEFAULT_PREFIX.to_owned(),
        }
    }

    /// Puts `prefix` before every key instead of the default one, so several
    /// applications can share a server.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
}

#[async_trait]
impl LoginStore for RedisLoginStore {
    async fn put(
        &self,
        key: &LoginKey,
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), LoginStoreError> {
        let millis = ttl::millis(ttl).ok_or(LoginStoreError::InvalidTtl(ttl))?;
        redis::cmd("SET")
            .arg(storage_key(&self.prefix, key))
            .arg(value)
            .arg("PX")
            .arg(millis)
            .query_async::<()>(&mut self.connection.clone())
            .await
            .map_err(backend("put entry"))
    }

    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        redis::cmd("GETDEL")
            .arg(storage_key(&self.prefix, key))
            .query_async(&mut self.connection.clone())
            .await
            .map_err(backend("take entry"))
    }

    async fn mark_once(&self, key: &LoginKey, ttl: Duration) -> Result<bool, LoginStoreError> {
        let millis = ttl::millis(ttl).ok_or(LoginStoreError::InvalidTtl(ttl))?;
        let recorded: Option<String> = redis::cmd("SET")
            .arg(storage_key(&self.prefix, key))
            .arg("")
            .arg("NX")
            .arg("PX")
            .arg(millis)
            .query_async(&mut self.connection.clone())
            .await
            .map_err(backend("mark entry"))?;
        Ok(recorded.is_some())
    }
}

#[cfg(test)]
mod tests;
