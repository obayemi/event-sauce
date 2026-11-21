//! The Redis [`LoginStore`].

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, ExistenceCheck, SetOptions};

use super::backend;
use crate::{LoginKey, LoginStore, LoginStoreError, LoginTtl};

/// The prefix a [`RedisLoginStore`] puts before every key unless told
/// otherwise.
const DEFAULT_PREFIX: &str = "event_sauce:login:";

fn storage_key(prefix: &str, key: &LoginKey) -> String {
    format!("{prefix}{key}")
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
        ttl: LoginTtl,
    ) -> Result<(), LoginStoreError> {
        let options = SetOptions::default().with_expiration(ttl.redis_expiry());
        self.connection
            .clone()
            .set_options::<_, _, ()>(storage_key(&self.prefix, key), value, options)
            .await
            .map_err(backend("put entry"))
    }

    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        self.connection
            .clone()
            .get_del(storage_key(&self.prefix, key))
            .await
            .map_err(backend("take entry"))
    }

    async fn mark_once(&self, key: &LoginKey, ttl: LoginTtl) -> Result<bool, LoginStoreError> {
        let options = SetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .with_expiration(ttl.redis_expiry());
        let recorded: Option<String> = self
            .connection
            .clone()
            .set_options(storage_key(&self.prefix, key), "", options)
            .await
            .map_err(backend("mark entry"))?;
        Ok(recorded.is_some())
    }
}

#[cfg(test)]
#[path = "tests/redis_tests.rs"]
mod tests;
