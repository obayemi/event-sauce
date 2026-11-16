//! The Redis [`UserCache`], shared by every instance of an application.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use event_sauce_core::{
    AggregateRoot, AggregateVersion, CryptoKeyStore, CryptoProvider, DomainEvent, EntityId,
    EventStore, StoredVersion,
};
use redis::aio::ConnectionManager;
use serde::{de::DeserializeOwned, Serialize};
use zeroize::Zeroizing;

use crate::{login_ttl, user_cache::is_stored, CacheError, UserAggregate, UserCache};

/// The prefix a [`RedisUserCache`] puts before every key unless told
/// otherwise.
const DEFAULT_PREFIX: &str = "event_sauce:user:";

/// How long an entry lives unless told otherwise.
const DEFAULT_TTL: Duration = Duration::from_hours(24);

const VERSION_LEN: usize = 8;

const DOMAIN: &[u8] = b"user-cache";

/// What a cached user is stored as: its version in the clear, then its state.
///
/// The version is not secret, and having it up front lets a read that does
/// not match skip decryption altogether.
struct Entry {
    version: AggregateVersion,
    body: Vec<u8>,
}

impl Entry {
    fn new(version: AggregateVersion, body: Vec<u8>) -> Self {
        Self { version, body }
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VERSION_LEN + self.body.len());
        bytes.extend_from_slice(&self.version.as_i64().to_be_bytes());
        bytes.extend_from_slice(&self.body);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, CacheError> {
        let (version, body) = bytes
            .split_first_chunk::<VERSION_LEN>()
            .ok_or(CacheError::Malformed)?;
        Ok(Self::new(
            AggregateVersion::new(i64::from_be_bytes(*version)),
            body.to_vec(),
        ))
    }
}

/// Binds a ciphertext to the user and the version it was cached at, and
/// separates it from every other ciphertext made with the user's key.
fn associated_data(id: EntityId, version: AggregateVersion) -> Vec<u8> {
    let mut data = Vec::with_capacity(16 + VERSION_LEN + DOMAIN.len());
    data.extend_from_slice(id.as_uuid().as_bytes());
    data.extend_from_slice(&version.as_i64().to_be_bytes());
    data.extend_from_slice(DOMAIN);
    data
}

fn backend(context: &'static str) -> impl FnOnce(redis::RedisError) -> CacheError {
    move |error| CacheError::backend(context, error)
}

/// What the event store the users live in offers for encryption.
trait CryptoSource: Send + Sync {
    fn provider(&self) -> Option<&dyn CryptoProvider>;
    fn key_store(&self) -> Option<&dyn CryptoKeyStore>;
}

impl<S: EventStore> CryptoSource for S {
    fn provider(&self) -> Option<&dyn CryptoProvider> {
        self.crypto_provider()
    }

    fn key_store(&self) -> Option<&dyn CryptoKeyStore> {
        self.crypto_key_store()
    }
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
    crypto: Option<Arc<dyn CryptoSource>>,
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
            crypto: None,
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
        self.crypto = Some(store);
        self
    }

    fn key(&self, id: EntityId) -> String {
        format!("{}{id}", self.prefix)
    }

    fn millis(&self) -> Result<i64, CacheError> {
        login_ttl::millis(self.ttl).map_err(|_| CacheError::InvalidTtl(self.ttl))
    }
}

impl<U> std::fmt::Debug for RedisUserCache<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisUserCache")
            .field("prefix", &self.prefix)
            .field("ttl", &self.ttl)
            .field("encrypting", &self.crypto.is_some())
            .finish_non_exhaustive()
    }
}

impl<U: UserAggregate> RedisUserCache<U> {
    fn is_encrypted() -> bool {
        U::is_encrypted() || U::Event::has_any_encrypted_fields()
    }

    fn crypto(&self) -> Result<(&dyn CryptoProvider, &dyn CryptoKeyStore), CacheError> {
        let source = self.crypto.as_deref();
        source
            .and_then(|source| source.provider().zip(source.key_store()))
            .ok_or_else(|| {
                event_sauce_core::Error::invalid_state(
                    "an encrypted user needs a crypto provider and key store, see with_crypto_from",
                )
                .into()
            })
    }

    async fn key_of(
        key_store: &dyn CryptoKeyStore,
        id: EntityId,
    ) -> Result<Option<Zeroizing<Vec<u8>>>, CacheError> {
        Ok(key_store.get_key(id.as_uuid()).await?.map(Zeroizing::new))
    }
}

impl<U> RedisUserCache<U>
where
    U: UserAggregate + Serialize,
{
    async fn seal(&self, user: &AggregateRoot<U>) -> Result<Option<Vec<u8>>, CacheError> {
        let state = serde_json::to_vec(&**user)?;
        let body = if Self::is_encrypted() {
            let (provider, key_store) = self.crypto()?;
            let Some(key) = Self::key_of(key_store, user.entity_id()).await? else {
                return Ok(None);
            };
            let data = associated_data(user.entity_id(), user.version());
            provider.encrypt(&key, &state, &data)?
        } else {
            state
        };
        Ok(Some(Entry::new(user.version(), body).encode()))
    }
}

impl<U> RedisUserCache<U>
where
    U: UserAggregate + DeserializeOwned,
{
    async fn open(
        &self,
        id: EntityId,
        version: AggregateVersion,
        bytes: &[u8],
    ) -> Result<Option<AggregateRoot<U>>, CacheError> {
        let entry = Entry::decode(bytes)?;
        if entry.version != version {
            return Ok(None);
        }
        let state = if Self::is_encrypted() {
            let (provider, key_store) = self.crypto()?;
            let Some(key) = Self::key_of(key_store, id).await? else {
                return Ok(None);
            };
            provider.decrypt(&key, &entry.body, &associated_data(id, version))?
        } else {
            entry.body
        };
        let version = StoredVersion::try_from(version).map_err(event_sauce_core::Error::from)?;
        let Ok(user) = AggregateRoot::restore(version, serde_json::from_slice::<U>(&state)?);
        Ok(Some(user))
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
        let stored: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key(id))
            .query_async(&mut self.connection.clone())
            .await
            .map_err(backend("read entry"))?;
        match stored {
            Some(bytes) => Ok(self.open(id, version, &bytes).await?.map(Arc::new)),
            None => Ok(None),
        }
    }

    async fn put(&self, user: Arc<AggregateRoot<U>>) -> Result<(), CacheError> {
        if !is_stored(&user) {
            return Ok(());
        }
        let Some(bytes) = self.seal(&user).await? else {
            return Ok(());
        };
        redis::cmd("SET")
            .arg(self.key(user.entity_id()))
            .arg(bytes)
            .arg("PX")
            .arg(self.millis()?)
            .query_async::<()>(&mut self.connection.clone())
            .await
            .map_err(backend("write entry"))
    }
}

#[cfg(test)]
mod tests;
