//! [`EventSauceBackend`]: the `axum-login` backend over an
//! [`Authenticator`].

use std::fmt;
use std::sync::Arc;

use axum_login::AuthnBackend;
use event_sauce_auth::{
    AuthError, Authenticator, CachedUserLoader, ClaimAuthenticator, CompleteError, HasSecondFactor,
    HashError, LoginOutcome, PendingLoginError, PendingLoginId, SecondFactor, TwoFactor,
    UserAggregate, UserCache,
};
use event_sauce_core::{EntityId, EventStore};
use serde::{de::DeserializeOwned, Serialize};

use crate::login::SessionUser;

/// Why the backend could not answer `axum-login`.
///
/// A wrong password or an unknown login is not an error: `authenticate`
/// answers `Ok(None)` for those, as `axum-login` expects.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// The first factor passed and a second one is required.
    ///
    /// No session is opened. The handler answers with
    /// [`PendingLogin::set_cookie`](crate::PendingLogin::set_cookie) for this
    /// id.
    #[error("a second factor is required")]
    SecondFactorRequired(PendingLoginId),
    /// The user store failed.
    #[error(transparent)]
    Store(#[from] event_sauce_core::Error),
    /// Hashing failed.
    #[error(transparent)]
    Hash(#[from] HashError),
    /// The login store failed.
    #[error(transparent)]
    Pending(#[from] PendingLoginError),
}

fn into_session_user<U: UserAggregate, A>(
    result: Result<LoginOutcome<U>, AuthError<A>>,
) -> Result<Option<SessionUser<U>>, BackendError> {
    match result {
        Ok(LoginOutcome::Authenticated(user)) => Ok(Some(SessionUser::from(user))),
        Ok(LoginOutcome::SecondFactorRequired(pending)) => {
            Err(BackendError::SecondFactorRequired(pending))
        }
        Ok(_)
        | Err(AuthError::InvalidCredentials | AuthError::Inactive | AuthError::Unknown { .. }) => {
            Ok(None)
        }
        Err(AuthError::Store(error)) => Err(error.into()),
        Err(AuthError::Hash(error)) => Err(error.into()),
        Err(AuthError::Pending(error)) => Err(error.into()),
    }
}

/// The `axum-login` backend of an event-sourced user aggregate.
///
/// It authenticates through an [`Authenticator`], and resolves the user of
/// every request through a [`CachedUserLoader`]: a version check, then a
/// cached user when the cache holds exactly that version, otherwise a full
/// load. Without [`with_cache`](Self::with_cache) every request is a full
/// load.
///
/// `get_user` answers `None` for a missing, deleted or inactive user, which
/// ends the session. The session auth hash of [`SessionUser`] is the
/// identity's session fingerprint, so a credential change ends every
/// session opened before it.
///
/// A login that needs a second factor is not an authentication yet:
/// `authenticate` reports it as [`BackendError::SecondFactorRequired`] and
/// opens nothing. Handlers that serve such logins call [`start`](Self::start)
/// and [`complete`](Self::complete) themselves, and call
/// `AuthSession::login` only with what `complete` returned.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
///
/// use axum::Router;
/// use axum_login::{tower_sessions::{MemoryStore, SessionManagerLayer}, AuthManagerLayerBuilder};
/// use event_sauce_auth::{
///     Argon2Hasher, ClaimAuthenticator, Email, MokaUserCache, Password, PasswordLogin,
///     UserAggregate,
/// };
/// use event_sauce_axum::EventSauceBackend;
/// use event_sauce_core::EventStore;
/// use serde::{de::DeserializeOwned, Serialize};
///
/// fn router<U, S>(store: Arc<S>) -> Router
/// where
///     S: EventStore + 'static,
///     U: UserAggregate<Credentials = Password<Email>> + Serialize + DeserializeOwned + 'static,
///     U::DeletedState: Serialize + DeserializeOwned,
///     U::Event: Serialize + DeserializeOwned,
/// {
///     let authenticator =
///         ClaimAuthenticator::<U, _, _>::new(store.clone(), PasswordLogin::new(Argon2Hasher::default()));
///     let backend = EventSauceBackend::new(authenticator, store)
///         .with_cache(MokaUserCache::new(10_000));
///     let sessions = SessionManagerLayer::new(MemoryStore::default());
///     Router::new().layer(AuthManagerLayerBuilder::new(backend, sessions).build())
/// }
/// ```
pub struct EventSauceBackend<U: UserAggregate, A, S> {
    authenticator: Arc<A>,
    store: Arc<S>,
    loader: Arc<CachedUserLoader<U, S>>,
}

impl<U, A, S> EventSauceBackend<U, A, S>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
{
    /// A backend that authenticates with `authenticator` and loads the users
    /// of `store`, with no cache.
    #[must_use]
    pub fn new(authenticator: A, store: Arc<S>) -> Self {
        let loader = Arc::new(CachedUserLoader::new(store.clone()));
        Self {
            authenticator: Arc::new(authenticator),
            store,
            loader,
        }
    }

    /// Resolves the user of every request through `cache`.
    #[must_use]
    pub fn with_cache(self, cache: impl UserCache<U> + 'static) -> Self {
        self.with_shared_cache(Arc::new(cache))
    }

    /// Resolves the user of every request through a cache that other
    /// backends share.
    #[must_use]
    pub fn with_shared_cache(mut self, cache: Arc<dyn UserCache<U>>) -> Self {
        self.loader = Arc::new(CachedUserLoader::new(self.store.clone()).with_shared_cache(cache));
        self
    }
}

impl<U: UserAggregate, A, S> EventSauceBackend<U, A, S> {
    /// The authenticator the backend delegates to.
    #[must_use]
    pub fn authenticator(&self) -> &A {
        &self.authenticator
    }
}

impl<U: UserAggregate, A: Authenticator<U>, S> EventSauceBackend<U, A, S> {
    /// Starts a login.
    ///
    /// Unlike `authenticate`, this keeps every outcome apart: the handler
    /// sees [`LoginOutcome::SecondFactorRequired`], and
    /// [`AuthError::Unknown`] hands the attempt back for apps that link or
    /// create users.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the attempt does not authenticate a user.
    pub async fn start(
        &self,
        attempt: A::Attempt,
    ) -> Result<LoginOutcome<U>, AuthError<A::Attempt>> {
        self.authenticator.authenticate(attempt).await
    }
}

impl<U, M, S, F> EventSauceBackend<U, ClaimAuthenticator<U, M, S, TwoFactor<F>>, S>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned,
    U::Credentials: HasSecondFactor,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
    F: SecondFactor<U::Credentials>,
{
    /// Completes the login held under `pending` with the client's
    /// `response` to the second factor.
    ///
    /// # Errors
    ///
    /// Returns [`CompleteError`] when the login is not in progress, the user
    /// is inactive, or the response does not prove the second factor.
    pub async fn complete(
        &self,
        pending: PendingLoginId,
        response: &F::Response,
    ) -> Result<SessionUser<U>, CompleteError> {
        self.authenticator
            .complete(pending, response)
            .await
            .map(SessionUser::from)
    }
}

impl<U: UserAggregate, A, S> Clone for EventSauceBackend<U, A, S> {
    fn clone(&self) -> Self {
        Self {
            authenticator: Arc::clone(&self.authenticator),
            store: Arc::clone(&self.store),
            loader: Arc::clone(&self.loader),
        }
    }
}

impl<U: UserAggregate, A, S> fmt::Debug for EventSauceBackend<U, A, S> {
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

impl<U, A, S> AuthnBackend for EventSauceBackend<U, A, S>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
    A: Authenticator<U> + 'static,
{
    type User = SessionUser<U>;
    type Credentials = A::Attempt;
    type Error = BackendError;

    async fn authenticate(
        &self,
        creds: A::Attempt,
    ) -> Result<Option<SessionUser<U>>, BackendError> {
        into_session_user(self.start(creds).await)
    }

    async fn get_user(&self, user_id: &EntityId) -> Result<Option<SessionUser<U>>, BackendError> {
        Ok(self.loader.load(*user_id).await?.map(SessionUser::from))
    }
}

impl<U, A, S> UserBackend for EventSauceBackend<U, A, S>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned + 'static,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
    A: Authenticator<U> + 'static,
{
    type Aggregate = U;
}

#[cfg(test)]
mod tests;
