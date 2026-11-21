//! [`Backend`]: the `axum-login` backend that turns a finished login into a
//! session, and that no application can name.

use std::fmt;
use std::future::{ready, Future};
use std::pin::Pin;
use std::sync::Arc;

use axum_login::AuthnBackend;
use event_sauce_auth::{Authenticated, CachedUserLoader, UserAggregate};
use event_sauce_core::{AggregateRoot, EntityId, EventStore};
use serde::{de::DeserializeOwned, Serialize};

use crate::login::SessionUser;

type Loaded<'a, U> = Pin<
    Box<
        dyn Future<Output = Result<Option<Arc<AggregateRoot<U>>>, event_sauce_core::Error>>
            + Send
            + 'a,
    >,
>;

/// Loads the active users of one store, which hides the store's type from
/// [`Backend`].
pub(crate) trait LoadUser<U: UserAggregate>: Send + Sync {
    fn load(&self, id: EntityId) -> Loaded<'_, U>;
}

impl<U, S> LoadUser<U> for CachedUserLoader<U, S>
where
    S: EventStore + 'static,
    U: UserAggregate,
    U::DeletedState: Serialize + DeserializeOwned,
{
    fn load(&self, id: EntityId) -> Loaded<'_, U> {
        Box::pin(CachedUserLoader::load(self, id))
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
/// [`Authenticated`] user alone.
///
/// `authenticate` accepts only what the end of a login yields, an
/// [`Authenticated`] user. `get_user` resolves the user of every request
/// through the [`CachedUserLoader`]: a version check, then a cached user when
/// the cache holds exactly that version, otherwise a full load. It answers
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

    pub(crate) async fn load(
        &self,
        id: EntityId,
    ) -> Result<Option<SessionUser<U>>, event_sauce_core::Error> {
        Ok(self.users.load(id).await?.map(SessionUser::from_loaded))
    }
}

impl<U: UserAggregate> Clone for Backend<U> {
    fn clone(&self) -> Self {
        Self {
            users: Arc::clone(&self.users),
        }
    }
}

impl<U: UserAggregate> fmt::Debug for Backend<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend").finish_non_exhaustive()
    }
}

impl<U: UserAggregate> AuthnBackend for Backend<U> {
    type User = SessionUser<U>;
    type Credentials = Authenticated<U>;
    type Error = event_sauce_core::Error;

    fn authenticate(
        &self,
        creds: Authenticated<U>,
    ) -> impl Future<Output = Result<Option<SessionUser<U>>, event_sauce_core::Error>> {
        ready(Ok(Some(SessionUser::from(creds))))
    }

    async fn get_user(
        &self,
        user_id: &EntityId,
    ) -> Result<Option<SessionUser<U>>, event_sauce_core::Error> {
        self.load(*user_id).await
    }
}

#[cfg(test)]
mod tests;
