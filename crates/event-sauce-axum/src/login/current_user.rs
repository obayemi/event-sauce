//! The extractors of the current user: [`CurrentUser`], [`MaybeUser`] and
//! [`Registered`].

use std::future::{ready, Future};
use std::ops::Deref;
use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::response::{IntoResponse, Response};
use axum_login::AuthSession;
use event_sauce_auth::UserAggregate;
use event_sauce_core::AggregateRoot;
use http::{request::Parts, StatusCode};

use crate::login::UserBackend;

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
    /// The `axum-login` layer is not installed: answered with
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

type Resolved<B> = Arc<AggregateRoot<<B as UserBackend>::Aggregate>>;

fn resolved<B: UserBackend>(parts: &Parts) -> Result<Option<Resolved<B>>, AuthRejection> {
    let session = parts
        .extensions
        .get::<AuthSession<B>>()
        .ok_or(AuthRejection::MissingLayer)?;
    Ok(session.user.as_ref().map(|user| Arc::clone(user.user())))
}

fn logged_in<B: UserBackend>(parts: &Parts) -> Result<Resolved<B>, AuthRejection> {
    resolved::<B>(parts)?.ok_or(AuthRejection::Unauthenticated)
}

fn registered<B: UserBackend>(parts: &Parts) -> Result<Resolved<B>, AuthRejection> {
    let user = logged_in::<B>(parts)?;
    if user.identity().credentials().is_none() {
        return Err(AuthRejection::Forbidden);
    }
    Ok(user)
}

/// The logged-in user of the request, as the aggregate the commands take.
///
/// `B` is the application's [`EventSauceBackend`](crate::EventSauceBackend)
/// type, which is how the extractor finds the session `axum-login` resolved.
/// The user was loaded once for the request, or not at all on a version
/// checked cache hit, and derefs to its [`AggregateRoot`], so it can be
/// passed straight to an `@actor` command.
///
/// A request with no logged-in user is rejected with `401`.
pub struct CurrentUser<B: UserBackend>(pub Resolved<B>);

impl<B: UserBackend> Deref for CurrentUser<B> {
    type Target = AggregateRoot<B::Aggregate>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<St: Send + Sync, B: UserBackend> FromRequestParts<St> for CurrentUser<B> {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(logged_in::<B>(parts).map(Self))
    }
}

/// The logged-in user of the request, if there is one.
///
/// Rejected only with [`AuthRejection::MissingLayer`], which is a wiring
/// mistake, never because nobody is logged in.
pub struct MaybeUser<B: UserBackend>(pub Option<Resolved<B>>);

impl<B: UserBackend> Deref for MaybeUser<B> {
    type Target = Option<Resolved<B>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<St: Send + Sync, B: UserBackend> FromRequestParts<St> for MaybeUser<B> {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(resolved::<B>(parts).map(Self))
    }
}

/// The logged-in user of the request, who must be registered.
///
/// Rejected with `401` when nobody is logged in and with `403` when the
/// logged-in user is anonymous, which is to say its identity holds no
/// credentials.
pub struct Registered<B: UserBackend>(pub Resolved<B>);

impl<B: UserBackend> Deref for Registered<B> {
    type Target = AggregateRoot<B::Aggregate>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<St: Send + Sync, B: UserBackend> FromRequestParts<St> for Registered<B> {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(registered::<B>(parts).map(Self))
    }
}

#[cfg(test)]
mod tests;
