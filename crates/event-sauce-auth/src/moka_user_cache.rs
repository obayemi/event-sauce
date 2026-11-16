//! The in-process [`UserCache`].

use std::sync::Arc;

use async_trait::async_trait;
use event_sauce_core::{AggregateRoot, AggregateVersion, EntityId};
use moka::{ops::compute::Op, sync::Cache};

use crate::{user_cache::is_stored, CacheError, UserAggregate, UserCache};

/// A [`UserCache`] that keeps users in this process, bounded by a number of
/// entries.
///
/// A hit costs one version query against the event store and shares the
/// cached [`Arc`]. Instances do not share entries, which is safe because every
/// read is checked against the stored version.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{MokaUserCache, UserAggregate};
///
/// fn cache_for<U: UserAggregate + Send + Sync + 'static>() -> MokaUserCache<U> {
///     MokaUserCache::new(10_000)
/// }
/// ```
pub struct MokaUserCache<U: UserAggregate> {
    entries: Cache<EntityId, Arc<AggregateRoot<U>>>,
}

impl<U: UserAggregate + Send + Sync + 'static> MokaUserCache<U> {
    /// A cache that evicts the least useful entries beyond `capacity` users.
    #[must_use]
    pub fn new(capacity: u64) -> Self {
        Self {
            entries: Cache::new(capacity),
        }
    }
}

impl<U: UserAggregate> std::fmt::Debug for MokaUserCache<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaUserCache")
            .field("entries", &self.entries.entry_count())
            .finish()
    }
}

#[async_trait]
impl<U> UserCache<U> for MokaUserCache<U>
where
    U: UserAggregate + Send + Sync + 'static,
{
    async fn get(
        &self,
        id: EntityId,
        version: AggregateVersion,
    ) -> Result<Option<Arc<AggregateRoot<U>>>, CacheError> {
        Ok(self
            .entries
            .get(&id)
            .filter(|user| user.version() == version))
    }

    async fn put(&self, user: Arc<AggregateRoot<U>>) -> Result<(), CacheError> {
        if is_stored(&user) {
            self.entries
                .entry(user.entity_id())
                .and_compute_with(|current| match current {
                    Some(cached) if cached.value().version() >= user.version() => Op::Nop,
                    _ => Op::Put(user),
                });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
