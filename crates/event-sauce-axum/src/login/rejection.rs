//! [`AuthRejection`]: why a login extractor refused the request.

use axum::response::{IntoResponse, Response};
use http::header::LOCATION;
use http::{HeaderValue, StatusCode};

/// Why an extractor of the login session refused the request.
///
/// Without a [`LoginRedirect`](crate::LoginRedirect) on the route, nobody
/// logged in is [`Unauthenticated`](Self::Unauthenticated) and an anonymous
/// user is [`Forbidden`](Self::Forbidden). Under one, a browser is answered
/// with [`Redirect`](Self::Redirect) instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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
    /// The browser is sent to the login or the registration page: answered
    /// with `303 See Other`.
    #[error("redirected to the login or registration page")]
    Redirect {
        /// The `Location` the browser is sent to, with the page it came from
        /// in the query.
        location: HeaderValue,
    },
}

impl AuthRejection {
    /// The status the rejection is answered with.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::MissingLayer => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Redirect { .. } => StatusCode::SEE_OTHER,
        }
    }
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        match self {
            Self::Redirect { location } => {
                (StatusCode::SEE_OTHER, [(LOCATION, location)]).into_response()
            }
            Self::MissingLayer => crate::diagnostics::internal_error(&self),
            other => (other.status(), other.to_string()).into_response(),
        }
    }
}

#[cfg(test)]
#[path = "tests/rejection_tests.rs"]
mod tests;
