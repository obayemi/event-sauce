//! [`PendingLogin`]: the login waiting for its second factor.

use std::future::{ready, Future};
use std::time::Duration;

use axum::extract::FromRequestParts;
use axum_login::tower_sessions::cookie::{time, Cookie, SameSite};
use event_sauce_auth::PendingLoginId;
use http::{header::COOKIE, request::Parts, HeaderMap};

use crate::login::AuthRejection;

/// The login a client started and has yet to finish with a second factor.
///
/// A handler that gets
/// [`LoginOutcome::SecondFactorRequired`](event_sauce_auth::LoginOutcome::SecondFactorRequired)
/// answers with [`PendingLogin::set_cookie`]. The next request, which carries
/// the second factor, extracts the pending login from that cookie and passes
/// its id to the backend's `complete`. No session exists in between, and
/// the cookie dies with the pending login.
///
/// Extraction is rejected with [`AuthRejection::Unauthenticated`] when the
/// cookie is missing or malformed.
///
/// ```
/// use axum::http::header::SET_COOKIE;
/// use axum::response::IntoResponse;
/// use event_sauce_auth::PendingLoginId;
/// use event_sauce_axum::PendingLogin;
///
/// fn ask_for_the_second_factor(id: PendingLoginId) -> impl IntoResponse {
///     ([(SET_COOKIE, PendingLogin::set_cookie(id, PendingLogin::DEFAULT_TTL))], "second factor required")
/// }
/// # let _ = ask_for_the_second_factor;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingLogin(PendingLoginId);

impl PendingLogin {
    /// The name of the cookie that carries the pending login id.
    pub const COOKIE: &'static str = "event_sauce_pending_login";

    /// How long a pending login lives unless the stage says otherwise:
    /// five minutes, the default of
    /// [`TwoFactor`](event_sauce_auth::TwoFactor).
    pub const DEFAULT_TTL: Duration = event_sauce_auth::DEFAULT_PENDING_TTL;

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

    /// The `Set-Cookie` value that hands `id` to the client for `ttl`.
    ///
    /// The cookie is `HttpOnly`, `Secure` and `SameSite=Lax`.
    #[must_use]
    pub fn set_cookie(id: PendingLoginId, ttl: Duration) -> String {
        let max_age = time::Duration::try_from(ttl).unwrap_or(time::Duration::MAX);
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
