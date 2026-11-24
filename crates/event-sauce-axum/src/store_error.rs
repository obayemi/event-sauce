//! [`StoreError`]: a store failure answered with the status of its kind.

use std::error::Error as StdError;

use axum::response::{IntoResponse, Response};
use event_sauce_core::{Error, ExecuteError, Lifecycle, ModifyError, UnitOfWorkError};
use http::StatusCode;

use crate::diagnostics;

pub(crate) const NOT_FOUND: (StatusCode, &str) = (StatusCode::NOT_FOUND, "not found");

type Source = Box<dyn StdError + Send + Sync>;

/// What kind of failure a [`StoreError`] is, which decides its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreFailure {
    /// The aggregate does not exist.
    NotFound,
    /// The aggregate was deleted.
    Deleted,
    /// Another write got there first, or the aggregate already exists.
    Conflict,
    /// Anything else: the store failed, or held something unreadable.
    Internal,
}

/// How a deleted aggregate is answered: hidden like a missing one, or gone.
///
/// The same deletion can be hidden on one route and reported on another, so
/// the choice belongs to the response, see [`StoreError::deleted_as`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeletedStatus {
    /// `404 Not Found`, so a caller cannot tell a deletion from an id that
    /// never existed.
    #[default]
    NotFound,
    /// `410 Gone`.
    Gone,
}

#[derive(Debug, thiserror::Error)]
#[error("expected aggregate to be {expected}, found {found}")]
struct LifecycleMismatch {
    expected: Lifecycle,
    found: Lifecycle,
}

/// A store failure, answered with the status of its kind and no detail.
///
/// The orphan rule forbids answering [`event_sauce_core::Error`] directly,
/// so a handler returns this wrapper, usually through `?`:
///
/// | Failure | Status |
/// |---|---|
/// | not found | `404` |
/// | deleted | `404`, or `410` after [`deleted_as`](Self::deleted_as) |
/// | concurrency or claim conflict, an aggregate that already exists | `409` |
/// | anything else | `500` |
///
/// The body is a fixed text per status, never the error's own message,
/// which names aggregate types, ids and claim keys. With the `tracing`
/// feature a `500` is logged with its cause at `error` level and a `409` at
/// `debug`.
///
/// ```
/// use axum::http::StatusCode;
/// use axum::response::IntoResponse;
/// use event_sauce_axum::{DeletedStatus, StoreError};
/// use event_sauce_core::Error;
///
/// async fn handler() -> Result<String, StoreError> {
///     Err(Error::not_found("Note", "n-1"))?
/// }
///
/// let missing = StoreError::from(Error::not_found("Note", "n-1"));
/// assert_eq!(missing.into_response().status(), StatusCode::NOT_FOUND);
///
/// let deleted = Error::AggregateDeleted {
///     aggregate_type: "Note".into(),
///     aggregate_id: "n-1".into(),
/// };
/// let gone = StoreError::from(deleted).deleted_as(DeletedStatus::Gone);
/// assert_eq!(gone.status(), StatusCode::GONE);
/// # let _ = handler;
/// ```
#[derive(Debug, thiserror::Error)]
#[error("store request failed ({failure:?})")]
pub struct StoreError {
    failure: StoreFailure,
    deleted: DeletedStatus,
    #[source]
    source: Source,
}

impl StoreError {
    fn new(failure: StoreFailure, source: impl Into<Source>) -> Self {
        Self {
            failure,
            deleted: DeletedStatus::default(),
            source: source.into(),
        }
    }

    fn of_lifecycle(expected: Lifecycle, found: Lifecycle) -> Self {
        let failure = match found {
            Lifecycle::Active => StoreFailure::Conflict,
            Lifecycle::Missing => StoreFailure::NotFound,
            Lifecycle::Deleted => StoreFailure::Deleted,
        };
        Self::new(failure, LifecycleMismatch { expected, found })
    }

    /// The kind of failure.
    #[must_use]
    pub fn failure(&self) -> StoreFailure {
        self.failure
    }

    /// The status the response carries.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.answer().0
    }

    /// Answers a deleted aggregate with `status` instead of the default
    /// `404`. Every other failure is unaffected.
    #[must_use]
    pub fn deleted_as(mut self, status: DeletedStatus) -> Self {
        self.deleted = status;
        self
    }

    fn answer(&self) -> (StatusCode, &'static str) {
        match (self.failure, self.deleted) {
            (StoreFailure::NotFound, _) | (StoreFailure::Deleted, DeletedStatus::NotFound) => {
                NOT_FOUND
            }
            (StoreFailure::Deleted, DeletedStatus::Gone) => (StatusCode::GONE, "gone"),
            (StoreFailure::Conflict, _) => (StatusCode::CONFLICT, "conflict"),
            (StoreFailure::Internal, _) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error"),
        }
    }
}

impl From<Error> for StoreError {
    fn from(error: Error) -> Self {
        let failure = match &error {
            Error::NotFound { .. } => StoreFailure::NotFound,
            Error::AggregateDeleted { .. } => StoreFailure::Deleted,
            Error::ConcurrencyConflict { .. } | Error::ClaimConflict { .. } => {
                StoreFailure::Conflict
            }
            _ => StoreFailure::Internal,
        };
        Self::new(failure, error)
    }
}

impl IntoResponse for StoreError {
    fn into_response(self) -> Response {
        match self.failure {
            StoreFailure::Internal => diagnostics::internal(self.source.as_ref()),
            StoreFailure::Conflict => diagnostics::conflict(self.source.as_ref()),
            StoreFailure::NotFound | StoreFailure::Deleted => {}
        }
        let (status, text) = self.answer();
        (status, text).into_response()
    }
}

/// A failed operation split in two: the domain error, which only the
/// application knows how to answer, and the store failure, which answers on
/// its own.
#[derive(Debug)]
pub enum Split<D> {
    /// The aggregate's own error, handed back unchanged.
    Domain(D),
    /// The store failed or refused.
    Store(StoreError),
}

/// An error that holds a domain error next to store failures.
///
/// An application maps the domain error to its own status (a `NotOwner` is
/// a `404`, an `EmptyTitle` a `422`) and lets the store failure answer:
///
/// ```
/// use axum::http::StatusCode;
/// use axum::response::{IntoResponse, Response};
/// use event_sauce_axum::{Split, SplitDomain};
/// use event_sauce_core::{Error, ModifyError};
///
/// #[derive(Debug)]
/// struct EmptyTitle;
///
/// fn answer(error: ModifyError<EmptyTitle>) -> Response {
///     match error.split() {
///         Split::Domain(EmptyTitle) => StatusCode::UNPROCESSABLE_ENTITY.into_response(),
///         Split::Store(store) => store.into_response(),
///     }
/// }
///
/// let refused = answer(ModifyError::Domain(EmptyTitle));
/// let missing = answer(ModifyError::Load(Error::not_found("Note", "n-1")));
/// assert_eq!(refused.status(), StatusCode::UNPROCESSABLE_ENTITY);
/// assert_eq!(missing.status(), StatusCode::NOT_FOUND);
/// ```
pub trait SplitDomain {
    /// The domain error the operation can fail with.
    type Domain;

    /// Separates the domain error from the store failures.
    fn split(self) -> Split<Self::Domain>;
}

impl<E> SplitDomain for ModifyError<E> {
    type Domain = E;

    fn split(self) -> Split<E> {
        match self {
            Self::Domain(domain) => Split::Domain(domain),
            Self::Load(error) | Self::Save(error) => Split::Store(error.into()),
        }
    }
}

impl<E> SplitDomain for ExecuteError<E> {
    type Domain = E;

    fn split(self) -> Split<E> {
        match self {
            Self::Domain(domain) => Split::Domain(domain),
            Self::Lifecycle { expected, found } => {
                Split::Store(StoreError::of_lifecycle(expected, found))
            }
            Self::Load(error) | Self::Save(error) => Split::Store(error.into()),
        }
    }
}

impl SplitDomain for UnitOfWorkError {
    type Domain = Source;

    fn split(self) -> Split<Source> {
        match self {
            Self::Domain(domain) => Split::Domain(domain),
            Self::Lifecycle { expected, found } => {
                Split::Store(StoreError::of_lifecycle(expected, found))
            }
            Self::Store(error) => Split::Store(error.into()),
            other => Split::Store(StoreError::new(StoreFailure::Internal, other)),
        }
    }
}

#[cfg(test)]
#[path = "tests/store_error_tests.rs"]
mod tests;
