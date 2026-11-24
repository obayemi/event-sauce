//! [`FromUser`] and [`Acting`]: the command context of the current user.

use std::future::{ready, Future};
use std::ops::Deref;
use std::sync::Arc;

use axum::extract::FromRequestParts;
use event_sauce_auth::UserAggregate;
use event_sauce_core::AggregateRoot;
use http::request::Parts;

use crate::login::current_user::logged_in;
use crate::login::rejection::AuthRejection;

/// A command context that is built from the user who acts.
///
/// A context acts for one user aggregate, so the user is an associated type:
/// [`Acting<Ctx>`] finds the session to read from `Ctx` alone. Building a
/// context cannot fail, because the session already refuses a missing or
/// deactivated user before the extractor runs.
///
/// A context that needs request data beyond the user is built in the handler
/// from [`Acting`] and [`Found`](crate::Found) or
/// [`Authorized`](crate::Authorized).
///
/// ```
/// use std::sync::Arc;
///
/// use event_sauce_auth::UserAggregate;
/// use event_sauce_axum::FromUser;
/// use event_sauce_core::AggregateRoot;
///
/// struct Actor<U: UserAggregate>(Arc<AggregateRoot<U>>);
///
/// impl<U: UserAggregate> FromUser for Actor<U> {
///     type User = U;
///
///     fn from_user(user: Arc<AggregateRoot<U>>) -> Self {
///         Self(user)
///     }
/// }
/// ```
pub trait FromUser: Sized + Send + 'static {
    /// The user aggregate the context acts for.
    type User: UserAggregate;

    /// Builds the context from the logged-in user, shared with the session
    /// and with every other extractor of the request.
    fn from_user(user: Arc<AggregateRoot<Self::User>>) -> Self;
}

/// The command context of the logged-in user, for
/// [`Repository::execute`](event_sauce_core::Repository::execute) and for
/// units of work.
///
/// `Ctx` is the application's [`FromUser`] type. The extractor is built
/// from the same user as [`CurrentUser`](crate::CurrentUser), so a request
/// that takes both loads the user once. It derefs to `Ctx`, so a handler can
/// pass `&ctx`.
///
/// A request with no logged-in user is rejected with `401`, or with a `303`
/// to the login page under a [`LoginRedirect`](crate::LoginRedirect).
pub struct Acting<Ctx>(pub Ctx);

impl<Ctx> Deref for Acting<Ctx> {
    type Target = Ctx;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<St: Send + Sync, Ctx: FromUser> FromRequestParts<St> for Acting<Ctx> {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(logged_in::<Ctx::User>(parts).map(|user| Self(Ctx::from_user(user))))
    }
}
