//! The event store behind a login, erased so that [`Login`](crate::Login)
//! does not name it.

use std::sync::Arc;

use async_trait::async_trait;
use event_sauce_core::{
    AggregateRoot, AggregateVersion, EntityId, Error, EventSourcedRepository, EventStore,
    Repository,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{CachedUserLoader, ClaimLocator, SignUpError, UserAggregate, UserCache};

/// What a lookup of a user found.
pub(crate) enum Found<U: UserAggregate> {
    Active(Arc<AggregateRoot<U>>),
    Inactive,
    Missing,
}

#[async_trait]
pub(crate) trait Users<U: UserAggregate>: Send + Sync {
    fn locator(&self) -> &dyn ClaimLocator;

    fn with_cache(&self, cache: Arc<dyn UserCache<U>>) -> Arc<dyn Users<U>>;

    async fn find(&self, id: EntityId) -> Result<Found<U>, Error>;

    async fn current(&self, held: &Arc<AggregateRoot<U>>) -> Result<Found<U>, Error>;

    async fn create(&self, user: AggregateRoot<U>) -> Result<Arc<AggregateRoot<U>>, SignUpError>;
}

pub(crate) struct StoreUsers<U: UserAggregate, S> {
    store: Arc<S>,
    repository: EventSourcedRepository<S, U>,
    loader: CachedUserLoader<U, S>,
}

impl<U, S> StoreUsers<U, S>
where
    S: EventStore + 'static,
    U: UserAggregate,
    U::DeletedState: Serialize + DeserializeOwned,
{
    pub(crate) fn new(store: Arc<S>) -> Self {
        Self {
            repository: EventSourcedRepository::new(store.clone()),
            loader: CachedUserLoader::new(store.clone()),
            store,
        }
    }
}

/// Whether `user` was built in memory: all of its events are still pending.
fn is_new<U: UserAggregate>(user: &AggregateRoot<U>) -> bool {
    let pending = user.pending_events().len();
    pending > 0
        && i64::try_from(pending).is_ok_and(|count| {
            user.version().as_i64() == AggregateVersion::initial().as_i64() + count
        })
}

#[async_trait]
impl<U, S> Users<U> for StoreUsers<U, S>
where
    S: EventStore + 'static,
    U: UserAggregate,
    U::DeletedState: Serialize + DeserializeOwned,
{
    fn locator(&self) -> &dyn ClaimLocator {
        &*self.store
    }

    fn with_cache(&self, cache: Arc<dyn UserCache<U>>) -> Arc<dyn Users<U>> {
        Arc::new(Self {
            store: self.store.clone(),
            repository: self.repository.clone(),
            loader: CachedUserLoader::new(self.store.clone()).with_shared_cache(cache),
        })
    }

    async fn find(&self, id: EntityId) -> Result<Found<U>, Error> {
        if let Some(user) = self.loader.load(id).await? {
            return Ok(Found::Active(user));
        }
        match self.repository.load_by_id(id).await {
            Ok(_) => Ok(Found::Inactive),
            Err(error) if error.is_not_found() || error.is_aggregate_deleted() => {
                Ok(Found::Missing)
            }
            Err(error) => Err(error),
        }
    }

    async fn current(&self, held: &Arc<AggregateRoot<U>>) -> Result<Found<U>, Error> {
        let id = held.entity_id();
        if held.is_active() && self.repository.version_by_id(id).await? == held.version() {
            return Ok(Found::Active(held.clone()));
        }
        self.find(id).await
    }

    async fn create(
        &self,
        mut user: AggregateRoot<U>,
    ) -> Result<Arc<AggregateRoot<U>>, SignUpError> {
        if !is_new(&user) {
            return Err(SignUpError::NotNew);
        }
        match self.repository.save(&mut user).await {
            Ok(()) => Ok(Arc::new(user)),
            Err(error) if error.is_concurrency_conflict() => Err(SignUpError::NotNew),
            Err(error) => Err(error.into()),
        }
    }
}
