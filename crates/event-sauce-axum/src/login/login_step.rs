//! [`LoginStep`]: where a login request left the login.

use axum::response::{IntoResponse, IntoResponseParts, Response, ResponseParts};
use event_sauce_auth::{LoginState, PendingLoginId};
use http::{header::SET_COOKIE, HeaderValue, StatusCode};

use crate::login::PendingLogin;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Opened,
    Pending {
        id: PendingLoginId,
        cookie: HeaderValue,
    },
}

/// Where a login request left the login: opened, or waiting for its next
/// step.
///
/// A handler gets one from [`AuthSession::finish`](crate::AuthSession::finish)
/// when the login ended and the session opened, or from
/// [`PendingLogin::hold`] when the login waits for another request, and
/// returns it. A refusal is not a step: it is the `Err` of the handler's
/// `Result<LoginStep, LoginError>`, which is already a response.
///
/// | Step | Answer |
/// |---|---|
/// | opened | `204 No Content`, pending cookie cleared |
/// | pending | `202 Accepted`, `Set-Cookie` with the id, alive for the state's `TTL` |
///
/// The fields are private and there is no public constructor, so an opened
/// step exists only once [`AuthSession::finish`](crate::AuthSession::finish)
/// opened a session. As response parts, a step adds its `Set-Cookie` alone,
/// for a handler that answers something else, a redirect for instance.
///
/// ```
/// use event_sauce_auth::{Done, Login, UserAggregate};
/// use event_sauce_axum::{AuthSession, LoginError, LoginStep};
///
/// async fn open<U: UserAggregate>(
///     mut auth: AuthSession<U>,
///     login: Login<U, Done>,
/// ) -> Result<LoginStep, LoginError> {
///     auth.finish(login).await
/// }
/// ```
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginStep(Outcome);

impl LoginStep {
    pub(crate) fn opened() -> Self {
        Self(Outcome::Opened)
    }

    pub(crate) fn waiting<S: LoginState>(id: PendingLoginId) -> Self {
        let cookie = HeaderValue::try_from(PendingLogin::set_cookie::<S>(id))
            .expect("a cookie of a uuid is a valid header value");
        Self(Outcome::Pending { id, cookie })
    }

    /// Whether the login ended and its session opened.
    #[must_use]
    pub fn is_opened(&self) -> bool {
        matches!(self.0, Outcome::Opened)
    }

    /// The id of the login waiting for its next step, if there is one.
    #[must_use]
    pub fn pending(&self) -> Option<PendingLoginId> {
        match self.0 {
            Outcome::Opened => None,
            Outcome::Pending { id, .. } => Some(id),
        }
    }

    fn cookie(self) -> HeaderValue {
        match self.0 {
            Outcome::Opened => HeaderValue::try_from(PendingLogin::clear_cookie())
                .expect("the clearing cookie is a valid header value"),
            Outcome::Pending { cookie, .. } => cookie,
        }
    }
}

impl IntoResponseParts for LoginStep {
    type Error = std::convert::Infallible;

    fn into_response_parts(self, mut parts: ResponseParts) -> Result<ResponseParts, Self::Error> {
        parts.headers_mut().append(SET_COOKIE, self.cookie());
        Ok(parts)
    }
}

impl IntoResponse for LoginStep {
    fn into_response(self) -> Response {
        let status = match self.0 {
            Outcome::Opened => StatusCode::NO_CONTENT,
            Outcome::Pending { .. } => StatusCode::ACCEPTED,
        };
        (self, status).into_response()
    }
}

#[cfg(test)]
mod tests;
