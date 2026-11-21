//! [`AuthRejection`]: why a login extractor refused the request.

use axum::response::{IntoResponse, Response};
use http::StatusCode;

/// Why an extractor of the login session refused the request.
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

#[cfg(test)]
mod tests;
