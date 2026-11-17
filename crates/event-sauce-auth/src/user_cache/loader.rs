//! [`CachedUserLoader`]: loading the current user through a [`UserCache`].

use std::sync::Arc;

use event_sauce_core::{
    AggregateRoot, AggregateVersion, EntityId, EventSourcedRepository, EventStore, Repository,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{CacheError, UserAggregate, UserCache};

/// Loads active users, going through a [`UserCache`] when one is configured.
///
/// This is how a session resolves its user on every request:
///
/// 1. the stored version is read, which is one cheap query;
/// 2. a cached user at exactly that version is served as is, so a stale or
///    deactivated user is never served;
/// 3. otherwise the user is loaded in full, which decrypts it and replays it
///    from its latest snapshot, and the cache is filled.
///
/// A cache is an optimisation, never a source of failure: when it errors, the
/// error is logged and the user is loaded in full. Without a cache, every
/// call is a full load.
#[derive(Debug)]
pub struct CachedUserLoader<U: UserAggregate, S> {
    repository: EventSourcedRepository<S, U>,
    cache: Option<Arc<dyn UserCache<U>>>,
}

impl<U, S> CachedUserLoader<U, S>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
{
    /// A loader for the users of `store`, with no cache.
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self {
            repository: EventSourcedRepository::new(store),
            cache: None,
        }
    }

    /// Serves users through `cache`.
    #[must_use]
    pub fn with_cache(self, cache: impl UserCache<U> + 'static) -> Self {
        self.with_shared_cache(Arc::new(cache))
    }

    /// Serves users through a cache that other loaders share.
    #[must_use]
    pub fn with_shared_cache(mut self, cache: Arc<dyn UserCache<U>>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Loads the user `id` names.
    ///
    /// Returns `None` when there is no such user, when it was deleted, and
    /// when it is not active, since an inactive user cannot be the current
    /// one.
    ///
    /// # Errors
    ///
    /// Returns the event store's error when the user cannot be loaded.
    pub async fn load(
        &self,
        id: EntityId,
    ) -> Result<Option<Arc<AggregateRoot<U>>>, event_sauce_core::Error> {
        let version = self.repository.version_by_id(id).await?;
        if version == AggregateVersion::initial() {
            return Ok(None);
        }
        let user = match self.cached(id, version).await {
            Some(user) => Some(user),
            None => self.load_in_full(id).await?,
        };
        Ok(user.filter(|user| user.is_active()))
    }

    async fn cached(
        &self,
        id: EntityId,
        version: AggregateVersion,
    ) -> Option<Arc<AggregateRoot<U>>> {
        let cache = self.cache.as_ref()?;
        match cache.get(id, version).await {
            Ok(user) => user,
            Err(error) => {
                warn_cache_failed("read", &error);
                None
            }
        }
    }

    async fn load_in_full(
        &self,
        id: EntityId,
    ) -> Result<Option<Arc<AggregateRoot<U>>>, event_sauce_core::Error> {
        let user = match self.repository.load_by_id(id).await {
            Ok(user) => Arc::new(user),
            Err(error) if error.is_not_found() || error.is_aggregate_deleted() => return Ok(None),
            Err(error) => return Err(error),
        };
        if let Some(cache) = &self.cache {
            if let Err(error) = cache.put(user.clone()).await {
                warn_cache_failed("write", &error);
            }
        }
        Ok(Some(user))
    }
}

fn warn_cache_failed(action: &str, error: &CacheError) {
    tracing::warn!(%error, action, "user cache failed, loading the user in full");
}
