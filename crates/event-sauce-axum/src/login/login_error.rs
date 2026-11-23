//! [`LoginError`]: why a login did not go on, answered without telling why.

use std::error::Error as StdError;

use axum::response::{IntoResponse, Response};
use event_sauce_auth::{
    AuthError, EmailError, FinishError, PendingLoginError, Rejected, ResumeError,
};
use http::{header::SET_COOKIE, StatusCode};

use crate::diagnostics;
use crate::login::{PendingLogin, SessionError};

type Source = Box<dyn StdError + Send + Sync>;

#[derive(Debug, thiserror::Error)]
enum LoginFailure {
    #[error("login refused")]
    Refused,
    #[error("login unavailable")]
    Unavailable(#[source] Source),
}

/// Why a login did not go on: refused, or unavailable.
///
/// Every refusal answers `401 Unauthorized` with the same body and clears
/// the pending login cookie, so a client cannot tell an unknown email from a
/// wrong password, an inactive account or a login that already ended. Every
/// outage, a store, hasher or session failure, answers `500` with no detail
/// and leaves the cookie alone: when the store failed before the pending
/// login was taken it is still there to retry, and when it failed after, the
/// next attempt is refused and clears it.
///
/// | Source | Answer |
/// |---|---|
/// | `AuthError::{InvalidCredentials, Inactive, Unknown}`, `EmailError`, `Rejected::Mismatch`, `FinishError::{Inactive, CredentialsChanged}`, `ResumeError::{NotInProgress, Inactive, CredentialsChanged}`, [`LoginError::refused`] | `401` |
/// | every other `AuthError`, `Rejected`, `FinishError` and `ResumeError`, `PendingLoginError`, [`SessionError`] | `500` |
///
/// An application that handles `AuthError::Unknown` itself, to sign the user
/// up, matches it before the `?`.
///
/// ```
/// use axum::http::StatusCode;
/// use axum::response::IntoResponse;
/// use event_sauce_auth::AuthError;
/// use event_sauce_axum::LoginError;
///
/// let wrong_password = LoginError::from(AuthError::<()>::InvalidCredentials);
/// let unknown_email = LoginError::from(AuthError::Unknown { attempt: () });
///
/// assert!(wrong_password.is_refused());
/// assert_eq!(wrong_password.into_response().status(), StatusCode::UNAUTHORIZED);
/// assert_eq!(unknown_email.into_response().status(), StatusCode::UNAUTHORIZED);
/// ```
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct LoginError(LoginFailure);

impl LoginError {
    /// A refusal the application decides itself, such as a malformed login
    /// id, answered exactly like every other refusal.
    #[must_use]
    pub fn refused() -> Self {
        Self(LoginFailure::Refused)
    }

    /// Whether the login was refused, as opposed to unavailable.
    #[must_use]
    pub fn is_refused(&self) -> bool {
        matches!(self.0, LoginFailure::Refused)
    }

    fn unavailable(source: impl Into<Source>) -> Self {
        Self(LoginFailure::Unavailable(source.into()))
    }
}

impl<A> From<AuthError<A>> for LoginError {
    fn from(error: AuthError<A>) -> Self {
        match error {
            AuthError::InvalidCredentials | AuthError::Inactive | AuthError::Unknown { .. } => {
                Self::refused()
            }
            AuthError::Store(error) => Self::unavailable(error),
            AuthError::Hash(error) => Self::unavailable(error),
            AuthError::LoginStore(error) => Self::unavailable(error),
            AuthError::Unavailable(error) => Self::unavailable(error),
        }
    }
}

impl From<Rejected> for LoginError {
    fn from(error: Rejected) -> Self {
        match error {
            Rejected::Mismatch => Self::refused(),
            Rejected::Hash(error) => Self::unavailable(error),
            Rejected::LoginStore(error) => Self::unavailable(error),
            Rejected::Unavailable(error) => Self::unavailable(error),
        }
    }
}

impl From<FinishError> for LoginError {
    fn from(error: FinishError) -> Self {
        match error {
            FinishError::Inactive | FinishError::CredentialsChanged => Self::refused(),
            FinishError::Store(error) => Self::unavailable(error),
        }
    }
}

impl From<ResumeError> for LoginError {
    fn from(error: ResumeError) -> Self {
        match error {
            ResumeError::NotInProgress
            | ResumeError::Inactive
            | ResumeError::CredentialsChanged => Self::refused(),
            ResumeError::Store(error) => Self::unavailable(error),
            ResumeError::Pending(error) => Self::unavailable(error),
        }
    }
}

impl From<EmailError> for LoginError {
    fn from(_: EmailError) -> Self {
        Self::refused()
    }
}

impl From<PendingLoginError> for LoginError {
    fn from(error: PendingLoginError) -> Self {
        Self::unavailable(error)
    }
}

impl From<SessionError> for LoginError {
    fn from(error: SessionError) -> Self {
        Self::unavailable(error)
    }
}

impl IntoResponse for LoginError {
    fn into_response(self) -> Response {
        match self.0 {
            LoginFailure::Refused => (
                StatusCode::UNAUTHORIZED,
                [(SET_COOKIE, PendingLogin::clear_cookie())],
                "login refused",
            )
                .into_response(),
            LoginFailure::Unavailable(source) => diagnostics::internal_error(source.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests;
