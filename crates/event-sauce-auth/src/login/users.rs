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

/// Why a user found again no longer matches the login that holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stale {
    Inactive,
    CredentialsChanged,
}

impl<U: UserAggregate> Found<U> {
    /// The user, if it is still active with the session fingerprint a login
    /// pinned.
    pub(crate) fn confirm(self, fingerprint: [u8; 32]) -> Result<Arc<AggregateRoot<U>>, Stale> {
        match self {
            Self::Active(user) if user.identity().session_fingerprint() == fingerprint => Ok(user),
            Self::Active(_) => Err(Stale::CredentialsChanged),
            Self::Inactive | Self::Missing => Err(Stale::Inactive),
        }
    }
}

impl<U: UserAggregate> From<Option<Arc<AggregateRoot<U>>>> for Found<U> {
    fn from(user: Option<Arc<AggregateRoot<U>>>) -> Self {
        match user {
            Some(user) if user.is_active() => Self::Active(user),
            Some(_) => Self::Inactive,
            None => Self::Missing,
        }
    }
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

/// Whether `user` was built in memory: nothing was stored before its pending
/// events.
fn is_new<U: UserAggregate>(user: &AggregateRoot<U>) -> bool {
    let Ok(pending) = i64::try_from(user.pending_events().len()) else {
        return false;
    };
    pending > 0 && user.version().as_i64() - pending == AggregateVersion::initial().as_i64()
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
        Ok(self.loader.find(id).await?.into())
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

#[cfg(test)]
#[path = "tests/users_tests.rs"]
mod tests;
