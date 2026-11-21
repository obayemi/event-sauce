//! [`EventSauceBackend`]: the `axum-login` backend that turns a finished
//! login into a session.

use std::fmt;
use std::future::{ready, Future};
use std::sync::Arc;

use axum_login::AuthnBackend;
use event_sauce_auth::{Authenticated, CachedUserLoader, UserAggregate, UserCache};
use event_sauce_core::{EntityId, EventStore};
use serde::{de::DeserializeOwned, Serialize};

use crate::login::SessionUser;

/// Why the backend could not answer `axum-login`.
///
/// A user that is missing or inactive is not an error: the backend answers
/// `Ok(None)` for those, as `axum-login` expects.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// The user store failed.
    #[error(transparent)]
    Store(#[from] event_sauce_core::Error),
}

/// The `axum-login` backend of an event-sourced user aggregate.
///
/// It authenticates nothing itself: a login runs through
/// [`Logins`](event_sauce_auth::Logins), and `authenticate` accepts only what
/// the end of that process yields, an [`Authenticated`] user.
///
/// `get_user` and `renew` also return a [`SessionUser`], so that `axum-login`
/// can rehydrate an existing session, and a handler can reach them through
/// `AuthSession::backend`. Pass `AuthSession::login` only
/// `SessionUser::from(login.finish()?)` or the `renew` of the request's own
/// session user: the type chain holds up to [`Authenticated`], and the step
/// from there to the session is a convention.
///
/// It resolves the user of every request through a [`CachedUserLoader`]: a
/// version check, then a cached user when the cache holds exactly that
/// version, otherwise a full load. Without [`with_cache`](Self::with_cache)
/// every request is a full load.
///
/// `get_user` answers `None` for a missing, deleted or inactive user, which
/// ends the session. The session auth hash of [`SessionUser`] is the
/// identity's session fingerprint, so a credential change ends every session
/// opened before it. A request that changes its own user's credentials asks
/// for the session user that matches them with [`renew`](Self::renew).
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
///
/// use axum::Router;
/// use axum_login::{tower_sessions::{MemoryStore, SessionManagerLayer}, AuthManagerLayerBuilder};
/// use event_sauce_auth::{MokaUserCache, UserAggregate};
/// use event_sauce_axum::EventSauceBackend;
/// use event_sauce_core::EventStore;
/// use serde::{de::DeserializeOwned, Serialize};
///
/// fn router<U, S>(store: Arc<S>) -> Router
/// where
///     S: EventStore + 'static,
///     U: UserAggregate,
///     U::DeletedState: Serialize + DeserializeOwned,
/// {
///     let backend = EventSauceBackend::<U, S>::new(store)
///         .with_cache(MokaUserCache::new(10_000));
///     let sessions = SessionManagerLayer::new(MemoryStore::default());
///     Router::new().layer(AuthManagerLayerBuilder::new(backend, sessions).build())
/// }
/// ```
pub struct EventSauceBackend<U: UserAggregate, S> {
    store: Arc<S>,
    loader: Arc<CachedUserLoader<U, S>>,
}

impl<U, S> EventSauceBackend<U, S>
where
    S: EventStore + 'static,
    U: UserAggregate,
    U::DeletedState: Serialize + DeserializeOwned,
{
    /// A backend over the users of `store`, with no cache.
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        let loader = Arc::new(CachedUserLoader::new(store.clone()));
        Self { store, loader }
    }

    /// Resolves the user of every request through `cache`.
    #[must_use]
    pub fn with_cache(self, cache: impl UserCache<U> + 'static) -> Self {
        self.with_shared_cache(Arc::new(cache))
    }

    /// Resolves the user of every request through a cache that other
    /// backends and [`Logins`](event_sauce_auth::Logins) share.
    #[must_use]
    pub fn with_shared_cache(mut self, cache: Arc<dyn UserCache<U>>) -> Self {
        self.loader = Arc::new(CachedUserLoader::new(self.store.clone()).with_shared_cache(cache));
        self
    }

    /// Reloads the user of `session`, after the request changed its own
    /// credentials.
    ///
    /// The session that changed a password, or claimed an anonymous account,
    /// logs in again with the result, which carries the new fingerprint. It
    /// never changes the user id, and answers `None` for a user that is gone
    /// or inactive.
    ///
    /// # Errors
    ///
    /// Returns [`BackendError::Store`] when the user store fails.
    pub async fn renew(
        &self,
        session: &SessionUser<U>,
    ) -> Result<Option<SessionUser<U>>, BackendError> {
        self.get_user_by_id(session.entity_id()).await
    }

    async fn get_user_by_id(&self, id: EntityId) -> Result<Option<SessionUser<U>>, BackendError> {
        Ok(self.loader.load(id).await?.map(SessionUser::from_loaded))
    }
}

impl<U: UserAggregate, S> Clone for EventSauceBackend<U, S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            loader: Arc::clone(&self.loader),
        }
    }
}

impl<U: UserAggregate, S> fmt::Debug for EventSauceBackend<U, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSauceBackend").finish_non_exhaustive()
    }
}

/// An [`AuthnBackend`] whose users are [`SessionUser`]s of one aggregate.
///
/// The user extractors are generic over it, which is how they know the
/// aggregate they yield.
pub trait UserBackend:
    AuthnBackend<User = SessionUser<<Self as UserBackend>::Aggregate>> + 'static
{
    /// The user aggregate.
    type Aggregate: UserAggregate;
}

impl<U, S> AuthnBackend for EventSauceBackend<U, S>
where
    S: EventStore + 'static,
    U: UserAggregate,
    U::DeletedState: Serialize + DeserializeOwned,
{
    type User = SessionUser<U>;
    type Credentials = Authenticated<U>;
    type Error = BackendError;

    fn authenticate(
        &self,
        creds: Authenticated<U>,
    ) -> impl Future<Output = Result<Option<SessionUser<U>>, BackendError>> {
        ready(Ok(Some(SessionUser::from(creds))))
    }

    async fn get_user(&self, user_id: &EntityId) -> Result<Option<SessionUser<U>>, BackendError> {
        self.get_user_by_id(*user_id).await
    }
}

impl<U, S> UserBackend for EventSauceBackend<U, S>
where
    S: EventStore + 'static,
    U: UserAggregate,
    U::DeletedState: Serialize + DeserializeOwned,
{
    type Aggregate = U;
}

#[cfg(test)]
mod tests;
