//! The Redis [`UserCache`], shared by every instance of an application.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use event_sauce_core::{AggregateRoot, AggregateVersion, EntityId, EventStore};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, SetOptions};
use serde::{de::DeserializeOwned, Serialize};

use self::sealed::Sealer;
use super::is_stored;
use crate::{ttl, CacheError, UserAggregate, UserCache};

mod sealed;

/// The prefix a [`RedisUserCache`] puts before every key unless told
/// otherwise.
const DEFAULT_PREFIX: &str = "event_sauce:user:";

/// How long an entry lives unless told otherwise.
const DEFAULT_TTL: Duration = Duration::from_hours(24);

fn backend(context: &'static str) -> impl FnOnce(redis::RedisError) -> CacheError {
    move |error| CacheError::backend(context, error)
}

/// A [`UserCache`] in Redis, so that every instance shares its entries.
///
/// A hit costs one version query against the event store and one Redis read.
/// Entries expire after a day unless [`with_ttl`](Self::with_ttl) says
/// otherwise.
///
/// A user that is encrypted, fully or by fields, is never held in plaintext.
/// Its state is encrypted with the key the user's events and snapshots use,
/// so crypto-shredding the user also makes its cache entries unreadable. A
/// cache that holds such users must be told where to find that key, with
/// [`with_crypto_from`](Self::with_crypto_from). A user whose key is missing
/// is not cached, and a cached user whose key was deleted is a miss.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
///
/// use event_sauce_auth::{RedisUserCache, UserAggregate};
/// use event_sauce_core::EventStore;
/// use redis::aio::ConnectionManager;
///
/// fn cache_for<U, S>(connection: ConnectionManager, store: Arc<S>) -> RedisUserCache<U>
/// where
///     U: UserAggregate,
///     S: EventStore + 'static,
/// {
///     RedisUserCache::new(connection).with_crypto_from(store)
/// }
/// ```
pub struct RedisUserCache<U> {
    connection: ConnectionManager,
    prefix: String,
    ttl: Duration,
    sealer: Sealer,
    user: PhantomData<fn() -> U>,
}

impl<U> RedisUserCache<U> {
    /// A cache over a connection the application already manages.
    #[must_use]
    pub fn new(connection: ConnectionManager) -> Self {
        Self {
            connection,
            prefix: DEFAULT_PREFIX.to_owned(),
            ttl: DEFAULT_TTL,
            sealer: Sealer::default(),
            user: PhantomData,
        }
    }

    /// Puts `prefix` before every key instead of the default one, so several
    /// applications can share a server.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Lets entries live for `ttl` instead of a day.
    ///
    /// A ttl of zero, or of more than a leap year, makes every `put` fail.
    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Encrypts users with the crypto provider and the key store of `store`,
    /// the event store the users live in.
    #[must_use]
    pub fn with_crypto_from<S: EventStore + 'static>(mut self, store: Arc<S>) -> Self {
        self.sealer = Sealer::over(store);
        self
    }

    fn key(&self, id: EntityId) -> String {
        format!("{}{id}", self.prefix)
    }

    fn millis(&self) -> Result<i64, CacheError> {
        ttl::millis(self.ttl).ok_or(CacheError::InvalidTtl(self.ttl))
    }
}

impl<U> std::fmt::Debug for RedisUserCache<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisUserCache")
            .field("prefix", &self.prefix)
            .field("ttl", &self.ttl)
            .field("encrypting", &self.sealer.has_crypto())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<U> UserCache<U> for RedisUserCache<U>
where
    U: UserAggregate + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get(
        &self,
        id: EntityId,
        version: AggregateVersion,
    ) -> Result<Option<Arc<AggregateRoot<U>>>, CacheError> {
        let stored: Option<Vec<u8>> = self
            .connection
            .clone()
            .get(self.key(id))
            .await
            .map_err(backend("read entry"))?;
        match stored {
            Some(bytes) => Ok(self.sealer.open(id, version, &bytes).await?.map(Arc::new)),
            None => Ok(None),
        }
    }

    async fn put(&self, user: Arc<AggregateRoot<U>>) -> Result<(), CacheError> {
        if !is_stored(&user) {
            return Ok(());
        }
        let Some(bytes) = self.sealer.seal(&user).await? else {
            return Ok(());
        };
        let options = SetOptions::default().with_expiration(ttl::redis_expiry(self.millis()?));
        self.connection
            .clone()
            .set_options::<_, _, ()>(self.key(user.entity_id()), bytes, options)
            .await
            .map_err(backend("write entry"))
    }
}
