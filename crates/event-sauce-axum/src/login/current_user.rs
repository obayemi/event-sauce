//! The extractors of the current user: [`CurrentUser`], [`MaybeUser`] and
//! [`Registered`].

use std::future::{ready, Future};
use std::ops::Deref;
use std::sync::Arc;

use axum::extract::FromRequestParts;
use event_sauce_auth::UserAggregate;
use event_sauce_core::AggregateRoot;
use http::request::Parts;

use crate::login::auth_session::session_user;
use crate::login::redirect::refuse;
use crate::login::rejection::AuthRejection;

#[cfg(test)]
#[path = "tests/current_user_tests.rs"]
mod tests;

pub(super) fn admit<U: UserAggregate>(
    parts: &Parts,
    user: Option<Arc<AggregateRoot<U>>>,
    registered: bool,
) -> Result<Arc<AggregateRoot<U>>, AuthRejection> {
    let user = user.ok_or_else(|| refuse(parts, AuthRejection::Unauthenticated))?;
    if registered && user.identity().credentials().is_none() {
        return Err(refuse(parts, AuthRejection::Forbidden));
    }
    Ok(user)
}

pub(super) fn logged_in<U: UserAggregate>(
    parts: &Parts,
) -> Result<Arc<AggregateRoot<U>>, AuthRejection> {
    admit(parts, session_user::<U>(parts)?, false)
}

fn registered<U: UserAggregate>(parts: &Parts) -> Result<Arc<AggregateRoot<U>>, AuthRejection> {
    admit(parts, session_user::<U>(parts)?, true)
}

/// The logged-in user of the request, as the aggregate the commands take.
///
/// `U` is the user aggregate, which is how the extractor finds the session
/// [`AuthLayer`](crate::AuthLayer) installed. The user was loaded once for
/// the request, or not at all on a version checked cache hit, and derefs to
/// its [`AggregateRoot`], so it can be passed straight to an `@actor`
/// command.
///
/// A request with no logged-in user is rejected with `401`, or with a `303`
/// to the login page under a [`LoginRedirect`](crate::LoginRedirect).
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
        ready(session_user::<U>(parts).map(Self))
    }
}

/// The logged-in user of the request, who must be registered.
///
/// Rejected with `401` when nobody is logged in and with `403` when the
/// logged-in user is anonymous, which is to say its identity holds no
/// credentials. A [`LoginRedirect`](crate::LoginRedirect) turns the first
/// into a `303` to the login page and, when it has a registration page, the
/// second into a `303` to it.
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
