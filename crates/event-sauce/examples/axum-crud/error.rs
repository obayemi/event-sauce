//! The errors of the HTTP surface and the status each one answers with.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use event_sauce::auth::{EmailError, HashError};
use event_sauce::axum::{Split, SplitDomain, StoreError};
use event_sauce::prelude::{Error, ModifyError};

use crate::member::MemberError;
use crate::note::NoteError;

/// Everything a handler can fail with.
///
/// A note of somebody else answers `404`, exactly like a note that does not
/// exist, so a caller cannot probe which ids are taken. The statuses of the
/// store failures come from [`StoreError`].
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    Member(#[from] MemberError),
    #[error(transparent)]
    Note(#[from] NoteError),
    #[error(transparent)]
    Email(#[from] EmailError),
    #[error("the password hasher failed")]
    Hashing(#[from] HashError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self::Store(error.into())
    }
}

impl<E> From<ModifyError<E>> for ApiError
where
    Self: From<E>,
{
    fn from(error: ModifyError<E>) -> Self {
        match error.split() {
            Split::Domain(domain) => domain.into(),
            Split::Store(store) => store.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::Note(NoteError::NotOwner) => (StatusCode::NOT_FOUND, "not found").into_response(),
            Self::Member(_) | Self::Email(_) | Self::Note(_) => {
                (StatusCode::UNPROCESSABLE_ENTITY, self.to_string()).into_response()
            }
            Self::Store(store) => store.into_response(),
            Self::Hashing(_) | Self::Database(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}
