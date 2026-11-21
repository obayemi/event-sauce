//! The extractors of the current user: [`CurrentUser`], [`MaybeUser`] and
//! [`Registered`].

use std::future::{ready, Future};
use std::ops::Deref;
use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::response::{IntoResponse, Response};
use event_sauce_auth::UserAggregate;
use event_sauce_core::AggregateRoot;
use http::{request::Parts, StatusCode};

use crate::login::auth_session::installed;

/// Why a user extractor refused the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthRejection {
    /// Nobody is logged in: answered with `401 Unauthorized`.
    #[error("not logged in")]
    Unauthenticated,
    /// The user is logged in but anonymous, so not registered: answered with
    /// `403 Forbidden`.
    #[error("registration required")]
    Forbidden,
    /// The [`AuthLayer`](crate::AuthLayer) is not installed: answered with
    /// `500 Internal Server Error`.
    #[error("the authentication layer is not installed")]
    MissingLayer,
}

impl AuthRejection {
    /// The status the rejection is answered with.
    #[must_use]
    pub fn status(self) -> StatusCode {
        match self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::MissingLayer => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        (self.status(), self.to_string()).into_response()
    }
}

fn resolved<U: UserAggregate>(
    parts: &Parts,
) -> Result<Option<Arc<AggregateRoot<U>>>, AuthRejection> {
    Ok(installed::<U>(parts)?
        .user
        .as_ref()
        .map(|user| Arc::clone(user.user())))
}

fn logged_in<U: UserAggregate>(parts: &Parts) -> Result<Arc<AggregateRoot<U>>, AuthRejection> {
    resolved::<U>(parts)?.ok_or(AuthRejection::Unauthenticated)
}

fn registered<U: UserAggregate>(parts: &Parts) -> Result<Arc<AggregateRoot<U>>, AuthRejection> {
    let user = logged_in::<U>(parts)?;
    if user.identity().credentials().is_none() {
        return Err(AuthRejection::Forbidden);
    }
    Ok(user)
}

/// The logged-in user of the request, as the aggregate the commands take.
///
/// `U` is the user aggregate, which is how the extractor finds the session
/// [`AuthLayer`](crate::AuthLayer) installed. The user was loaded once for the request, or not at all on a version
/// checked cache hit, and derefs to its [`AggregateRoot`], so it can be
/// passed straight to an `@actor` command.
///
/// A request with no logged-in user is rejected with `401`.
pub struct CurrentUser<U: UserAggregate>(pub Arc<AggregateRoot<U>>);

impl<U: UserAggregate> Deref for CurrentUser<U> {
    type Target = AggregateRoot<U>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<St: Send + Sync, U: UserAggregate> FromRequestParts<St> for CurrentUser<U> {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(logged_in::<U>(parts).map(Self))
    }
}

/// The logged-in user of the request, if there is one.
///
/// Rejected only with [`AuthRejection::MissingLayer`], which is a wiring
/// mistake, never because nobody is logged in.
pub struct MaybeUser<U: UserAggregate>(pub Option<Arc<AggregateRoot<U>>>);

impl<U: UserAggregate> Deref for MaybeUser<U> {
    type Target = Option<Arc<AggregateRoot<U>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<St: Send + Sync, U: UserAggregate> FromRequestParts<St> for MaybeUser<U> {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(resolved::<U>(parts).map(Self))
    }
}

/// The logged-in user of the request, who must be registered.
///
/// Rejected with `401` when nobody is logged in and with `403` when the
/// logged-in user is anonymous, which is to say its identity holds no
/// credentials.
pub struct Registered<U: UserAggregate>(pub Arc<AggregateRoot<U>>);

impl<U: UserAggregate> Deref for Registered<U> {
    type Target = AggregateRoot<U>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<St: Send + Sync, U: UserAggregate> FromRequestParts<St> for Registered<U> {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(registered::<U>(parts).map(Self))
    }
}

#[cfg(test)]
mod tests;
