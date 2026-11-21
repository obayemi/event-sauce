//! [`PendingLogin`]: the login waiting for its next step.

use std::future::{ready, Future};

use axum::extract::FromRequestParts;
use axum_login::tower_sessions::cookie::{time, Cookie, SameSite};
use event_sauce_auth::{Login, LoginState, PendingLoginId, PendingLogins, UserAggregate};
use http::{header::COOKIE, request::Parts, HeaderMap};

use crate::login::rejection::AuthRejection;
use crate::login::{LoginError, LoginStep};

/// The login a client started and has yet to finish.
///
/// A handler whose login needs another request returns
/// [`PendingLogin::hold`], which saves it in the
/// [`PendingLogins`](event_sauce_auth::PendingLogins) and answers with the
/// cookie for the saved id. The next request, which carries the proof of the
/// next step, extracts the pending login from that cookie and resumes the
/// login with its id. No session exists in between, and the cookie is cleared
/// once the login ends, by [`AuthSession::finish`](crate::AuthSession::finish)
/// or with [`PendingLogin::clear_cookie`] when the login is abandoned.
///
/// The cookie holds the id only. The state the login is in and the user it
/// belongs to stay on the server, so a client cannot skip a step by editing
/// it.
///
/// Extraction is rejected with [`AuthRejection::Unauthenticated`] when the
/// cookie is missing or malformed.
///
/// ```
/// use axum::http::{header::SET_COOKIE, StatusCode};
/// use event_sauce_auth::{Login, LoginState, PendingLogins, UserAggregate};
/// use event_sauce_axum::{LoginError, LoginStep, PendingLogin};
///
/// async fn ask_for_the_next_step<U: UserAggregate, S: LoginState>(
///     pending: &PendingLogins<U>,
///     login: Login<U, S>,
/// ) -> Result<LoginStep, LoginError> {
///     PendingLogin::hold(pending, login).await
/// }
///
/// async fn abandon() -> (StatusCode, [(http::HeaderName, String); 1]) {
///     (StatusCode::NO_CONTENT, [(SET_COOKIE, PendingLogin::clear_cookie())])
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingLogin(PendingLoginId);

impl PendingLogin {
    /// The name of the cookie that carries the pending login id.
    pub const COOKIE: &'static str = "event_sauce_pending_login";

    /// The id of the waiting login.
    #[must_use]
    pub fn id(self) -> PendingLoginId {
        self.0
    }

    /// The pending login the `Cookie` headers carry, if any.
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        headers
            .get_all(COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(Cookie::split_parse)
            .filter_map(Result::ok)
            .find(|cookie| cookie.name() == Self::COOKIE)
            .and_then(|cookie| cookie.value().parse().ok())
            .map(Self)
    }

    /// Saves `login` and answers with the cookie that carries its id.
    ///
    /// The state `S` comes from the login, and the cookie lives as long as
    /// the saved login does, which is `S::TTL`. The cookie is `HttpOnly`,
    /// `Secure` and `SameSite=Lax`.
    ///
    /// # Errors
    ///
    /// Returns [`LoginError`] when the state cannot be encoded or the login
    /// store fails, answered as an outage.
    pub async fn hold<U: UserAggregate, S: LoginState>(
        pending: &PendingLogins<U>,
        login: Login<U, S>,
    ) -> Result<LoginStep, LoginError> {
        let id = pending.save(login).await?;
        Ok(LoginStep::waiting::<S>(id))
    }

    pub(super) fn set_cookie<S: LoginState>(id: PendingLoginId) -> String {
        let max_age = time::Duration::try_from(S::TTL.as_duration()).unwrap_or(time::Duration::MAX);
        Self::cookie(id.to_string(), max_age)
    }

    /// The `Set-Cookie` value that removes the cookie once the login is
    /// complete or abandoned.
    #[must_use]
    pub fn clear_cookie() -> String {
        Self::cookie(String::new(), time::Duration::ZERO)
    }

    fn cookie(value: String, max_age: time::Duration) -> String {
        Cookie::build((Self::COOKIE, value))
            .path("/")
            .http_only(true)
            .secure(true)
            .same_site(SameSite::Lax)
            .max_age(max_age)
            .build()
            .to_string()
    }
}

impl<St: Send + Sync> FromRequestParts<St> for PendingLogin {
    type Rejection = AuthRejection;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &St,
    ) -> impl Future<Output = Result<Self, AuthRejection>> {
        ready(Self::from_headers(&parts.headers).ok_or(AuthRejection::Unauthenticated))
    }
}

#[cfg(test)]
mod tests;
