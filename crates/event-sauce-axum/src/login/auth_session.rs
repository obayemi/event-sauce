//! [`AuthSession`]: the session a handler opens, renews and ends.

use std::fmt;
use std::future::{ready, Future};
use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum_login::{tower_sessions::session, AuthUser, AuthnBackend};
use event_sauce_auth::{Authenticated, UserAggregate};
use event_sauce_core::AggregateRoot;
use http::request::Parts;

use crate::login::backend::Backend;
use crate::login::rejection::AuthRejection;
use crate::login::SessionUser;

/// Why a session could not be opened, renewed or ended.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    /// The session store failed.
    #[error(transparent)]
    Session(#[from] session::Error),
    /// The user store failed.
    #[error(transparent)]
    Store(#[from] event_sauce_core::Error),
}

fn session_error<U: UserAggregate>(error: axum_login::Error<Backend<U>>) -> SessionError {
    match error {
        axum_login::Error::Session(error) => SessionError::Session(error),
        axum_login::Error::Backend(error) => SessionError::Store(error),
    }
}

/// The login session of the request, for the users of the aggregate `U`.
///
/// A session opens only from an [`Authenticated`] user, which only the end
/// of a login yields, and [`login`](Self::login) consumes it. Nothing hands
/// out the `axum-login` session or backend underneath, whose `login` would
/// open a session for any stored user, and the backend type cannot be named,
/// so there is no other way to open one through this crate.
///
/// Extraction is rejected with [`AuthRejection::MissingLayer`] when
/// [`AuthLayer`](crate::AuthLayer) is not installed.
///
/// ```no_run
/// use axum::http::StatusCode;
/// use event_sauce_auth::{Authenticated, UserAggregate};
/// use event_sauce_axum::AuthSession;
///
/// async fn open<U: UserAggregate>(
///     mut session: AuthSession<U>,
///     finished: Authenticated<U>,
/// ) -> StatusCode {
///     match session.login(finished).await {
///         Ok(()) => StatusCode::NO_CONTENT,
///         Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
///     }
/// }
/// ```
pub struct AuthSession<U: UserAggregate> {
    inner: axum_login::AuthSession<Backend<U>>,
}

impl<U: UserAggregate> AuthSession<U> {
    /// The logged-in user of the session, if there is one.
    #[must_use]
    pub fn user(&self) -> Option<&Arc<AggregateRoot<U>>> {
        user_of(&self.inner)
    }

    /// Opens a session for the user whose login just finished.
    ///
    /// It consumes `user`, so one finished login opens one session. A
    /// session that held no user gets a new id, against session fixation.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::Session`] when the session store fails.
    pub async fn login(&mut self, user: Authenticated<U>) -> Result<(), SessionError> {
        self.inner
            .login(&SessionUser::from(user))
            .await
            .map_err(session_error)
    }

    /// Reloads the user of this session, after the request changed its own
    /// credentials.
    ///
    /// The session that changed a password, or claimed an anonymous account,
    /// keeps going with the new session fingerprint, and every other session
    /// of that user ends. It never changes the user, and answers `None`
    /// when nobody is logged in. When the user is now gone or inactive, it
    /// ends the session and answers `None` too.
    ///
    /// Call it only after the request's own credential change: it adopts
    /// whatever fingerprint the user has by then.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError`] when the user store or the session store
    /// fails.
    pub async fn renew(&mut self) -> Result<Option<Arc<AggregateRoot<U>>>, SessionError> {
        let Some(id) = self.inner.user.as_ref().map(AuthUser::id) else {
            return Ok(None);
        };
        let Some(renewed) = self.inner.backend.get_user(&id).await? else {
            self.inner.logout().await.map_err(session_error)?;
            return Ok(None);
        };
        self.inner.login(&renewed).await.map_err(session_error)?;
        Ok(Some(Arc::clone(renewed.user())))
    }

    /// Ends the session and answers the user it held.
    ///
    /// # Errors
    ///
    /// Returns [`SessionError::Session`] when the session store fails.
    pub async fn logout(&mut self) -> Result<Option<Arc<AggregateRoot<U>>>, SessionError> {
        let user = self.inner.logout().await.map_err(session_error)?;
        Ok(user.map(|user| Arc::clone(user.user())))
    }
}

impl<U: UserAggregate> fmt::Debug for AuthSession<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthSession")
            .field("user", &self.inner.user)
            .finish_non_exhaustive()
    }
}

fn installed<U: UserAggregate>(
    parts: &Parts,
) -> Result<&axum_login::AuthSession<Backend<U>>, AuthRejection> {
    parts.extensions.get().ok_or(AuthRejection::MissingLayer)
}

fn user_of<U: UserAggregate>(
    inner: &axum_login::AuthSession<Backend<U>>,
) -> Option<&Arc<AggregateRoot<U>>> {
    inner.user.as_ref().map(SessionUser::user)
}

pub(crate) fn session_user<U: UserAggregate>(
    parts: &Parts,
) -> Result<Option<Arc<AggregateRoot<U>>>, AuthRejection> {
    Ok(user_of(installed::<U>(parts)?).cloned())
}

impl<St: Send + Sync, U: UserAggregate> FromRequestParts<St> for AuthSession<U> {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(installed::<U>(parts).map(|inner| Self {
            inner: inner.clone(),
        }))
    }
}

#[cfg(test)]
mod tests;
