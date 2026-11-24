//! [`Backend`]: the `axum-login` backend that turns a finished login into a
//! session, and that no application can name.

use std::convert::Infallible;
use std::future::{ready, Future};
use std::sync::Arc;

use async_trait::async_trait;
use axum_login::AuthnBackend;
use event_sauce_auth::{CachedUserLoader, UserAggregate};
use event_sauce_core::{AggregateRoot, EntityId, EventStore};
use serde::{de::DeserializeOwned, Serialize};

use crate::login::SessionUser;

/// Loads the active users of one store, which hides the store's type from
/// [`Backend`].
#[async_trait]
pub(crate) trait LoadUser<U: UserAggregate>: Send + Sync {
    async fn load(
        &self,
        id: EntityId,
    ) -> Result<Option<Arc<AggregateRoot<U>>>, event_sauce_core::Error>;
}

#[async_trait]
impl<U, S> LoadUser<U> for CachedUserLoader<U, S>
where
    S: EventStore + 'static,
    U: UserAggregate,
    U::DeletedState: Serialize + DeserializeOwned,
{
    async fn load(
        &self,
        id: EntityId,
    ) -> Result<Option<Arc<AggregateRoot<U>>>, event_sauce_core::Error> {
        CachedUserLoader::load(self, id).await
    }
}

/// The `axum-login` backend of an event-sourced user aggregate.
///
/// It is `pub(crate)` on purpose, so that no application can name it: its
/// `get_user` answers a [`SessionUser`] for any stored user, and
/// `axum_login::AuthSession::login` takes that same type, so whoever holds
/// the backend, or `axum_login::AuthSession<Backend<U>>`, could open a
/// session for a user who never logged in. The crate reaches it only
/// through [`AuthLayer`](crate::AuthLayer) and
/// [`AuthSession`](crate::AuthSession), which open a session from an
/// [`Authenticated`](event_sauce_auth::Authenticated) user alone.
///
/// The backend authenticates nothing: a session opens only through
/// [`AuthSession::login`](crate::AuthSession::login). `get_user` resolves the
/// user of every request through [`CachedUserLoader::load`]. It answers
/// `None` for a missing, deleted or inactive user, which ends the session.
/// The session auth hash of [`SessionUser`] is the identity's session
/// fingerprint, so a credential change ends every session opened before it.
pub(crate) struct Backend<U: UserAggregate> {
    users: Arc<dyn LoadUser<U>>,
}

impl<U: UserAggregate> Backend<U> {
    pub(crate) fn new<S>(loader: CachedUserLoader<U, S>) -> Self
    where
        S: EventStore + 'static,
        U::DeletedState: Serialize + DeserializeOwned,
    {
        Self {
            users: Arc::new(loader),
        }
    }
}

impl<U: UserAggregate> Clone for Backend<U> {
    fn clone(&self) -> Self {
        Self {
            users: Arc::clone(&self.users),
        }
    }
}

impl<U: UserAggregate> AuthnBackend for Backend<U> {
    type User = SessionUser<U>;
    type Credentials = Infallible;
    type Error = event_sauce_core::Error;

    fn authenticate(
        &self,
        creds: Infallible,
    ) -> impl Future<Output = Result<Option<SessionUser<U>>, event_sauce_core::Error>> {
        ready(Err(creds).map_err(|never| match never {}))
    }

    async fn get_user(
        &self,
        user_id: &EntityId,
    ) -> Result<Option<SessionUser<U>>, event_sauce_core::Error> {
        Ok(self
            .users
            .load(*user_id)
            .await?
            .map(SessionUser::from_loaded))
    }
}

#[cfg(test)]
#[path = "tests/backend_tests.rs"]
mod tests;
