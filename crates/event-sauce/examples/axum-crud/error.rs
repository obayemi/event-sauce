//! The errors of the HTTP surface and the status each one answers with.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use event_sauce::auth::EmailError;
use event_sauce::prelude::{Error, ModifyError};

use crate::member::{HashingError, MemberError};
use crate::note::NoteError;

/// Everything a handler can fail with.
///
/// A note of somebody else answers `404`, exactly like a note that does not
/// exist, so a caller cannot probe which ids are taken.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("wrong email or password")]
    Unauthorized,
    #[error(transparent)]
    Member(#[from] MemberError),
    #[error(transparent)]
    Note(#[from] NoteError),
    #[error(transparent)]
    Email(#[from] EmailError),
    #[error("the password hasher failed")]
    Hashing(#[source] HashingError),
    #[error(transparent)]
    Store(#[from] Error),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("the session failed: {0}")]
    Session(String),
}

impl ApiError {
    /// Wraps a failure of the session or of the authentication layer.
    pub fn session(error: impl std::fmt::Display) -> Self {
        Self::Session(error.to_string())
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Member(_) | Self::Email(_) | Self::Note(_) if self.is_not_found() => {
                StatusCode::NOT_FOUND
            }
            Self::Member(_) | Self::Email(_) | Self::Note(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Store(error) if error.as_claim_conflict().is_some() => StatusCode::CONFLICT,
            Self::Store(error) if error.is_concurrency_conflict() => StatusCode::CONFLICT,
            Self::Store(_) if self.is_not_found() => StatusCode::NOT_FOUND,
            Self::Hashing(_) | Self::Store(_) | Self::Database(_) | Self::Session(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    fn is_not_found(&self) -> bool {
        match self {
            Self::Note(NoteError::NotOwner) => true,
            Self::Store(error) => error.is_not_found() || error.is_aggregate_deleted(),
            _ => false,
        }
    }
}

impl From<HashingError> for ApiError {
    fn from(error: HashingError) -> Self {
        match error {
            HashingError::Refused(refused) => Self::Member(refused),
            failed @ HashingError::Failed(_) => Self::Hashing(failed),
        }
    }
}

impl<E> From<ModifyError<E>> for ApiError
where
    Self: From<E>,
{
    fn from(error: ModifyError<E>) -> Self {
        match error {
            ModifyError::Domain(domain) => domain.into(),
            ModifyError::Load(error) | ModifyError::Save(error) => Self::Store(error),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let message = match status {
            StatusCode::NOT_FOUND => "not found".to_owned(),
            StatusCode::INTERNAL_SERVER_ERROR => "internal error".to_owned(),
            _ => self.to_string(),
        };
        (status, message).into_response()
    }
}
