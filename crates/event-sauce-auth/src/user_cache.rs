//! The [`UserCache`] of loaded users.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use event_sauce_core::{AggregateRoot, EntityId, StoredVersion};

use crate::UserAggregate;

mod loader;
#[cfg(feature = "moka")]
mod moka;
#[cfg(feature = "redis")]
mod redis;
#[cfg(all(test, any(feature = "moka", feature = "redis")))]
mod test_support;

pub use loader::CachedUserLoader;
#[cfg(feature = "moka")]
pub use moka::MokaUserCache;
#[cfg(feature = "redis")]
pub use redis::RedisUserCache;

/// Why a [`UserCache`] operation failed.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// The cache backend failed.
    #[error("user cache backend failed: {context}")]
    Backend {
        /// What the cache was doing when the backend failed.
        context: &'static str,
        /// The backend's own error.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
    /// A user could not be turned into bytes or back.
    #[error("cached user could not be encoded or decoded")]
    Encoding(#[from] serde_json::Error),
    /// A user could not be encrypted or decrypted.
    #[error("cached user could not be encrypted or decrypted")]
    Crypto(#[from] event_sauce_core::Error),
    /// An entry does not have the shape a cache wrote.
    #[error("cache entry is malformed")]
    Malformed,
}

impl CacheError {
    /// Wraps a backend failure with what the cache was doing.
    pub fn backend(context: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Backend {
            context,
            source: Box::new(source),
        }
    }
}

/// A version-checked cache of loaded users.
///
/// `get` only answers with a user that is exactly at the version the caller
/// read from the event store, so a cache never needs invalidating: editing
/// or deactivating a user bumps its version and the next read misses. Every
/// instance of an application can keep its own cache, or share one, without
/// coordination.
///
/// A cache only holds users as stored. Users with uncommitted changes are
/// left out by [`put`](Self::put).
///
/// The ready-made implementations are `MokaUserCache` (feature `moka`) and
/// `RedisUserCache` (feature `redis`).
#[async_trait]
pub trait UserCache<U: UserAggregate>: Debug + Send + Sync {
    /// Returns the cached user only if it is exactly at `version`.
    ///
    /// # Errors
    ///
    /// Fails if the backend fails or an entry cannot be read.
    async fn get(
        &self,
        id: EntityId,
        version: StoredVersion,
    ) -> Result<Option<Arc<AggregateRoot<U>>>, CacheError>;

    /// Caches `user` at the version it was loaded at.
    ///
    /// A user that was changed since it was loaded is not cached.
    ///
    /// # Errors
    ///
    /// Fails if the backend fails or the user cannot be written.
    async fn put(&self, user: Arc<AggregateRoot<U>>) -> Result<(), CacheError>;
}

/// The version `user` has in the event store, or `None` when `user` is not
/// exactly what the store holds: it has uncommitted changes, or was never saved.
#[cfg(any(feature = "moka", feature = "redis"))]
fn stored_version<U: UserAggregate>(user: &AggregateRoot<U>) -> Option<StoredVersion> {
    if !user.pending_events().is_empty() || user.is_poisoned() {
        return None;
    }
    StoredVersion::try_from(user.version()).ok()
}

#[cfg(all(test, any(feature = "moka", feature = "redis")))]
#[path = "tests/user_cache_tests.rs"]
mod tests;
