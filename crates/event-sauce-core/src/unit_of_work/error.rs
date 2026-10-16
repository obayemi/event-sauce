//! [`UnitOfWorkError`] — everything a unit of work can fail with.

use crate::{AggregateError, AggregateType, CommandError, EntityId, Error, Lifecycle};

/// The error a [`UnitOfWorkCommand`](super::UnitOfWorkCommand) handler, a
/// [`UnitOfWork`](super::UnitOfWork) operation or
/// [`UnitOfWorkStore::execute`](super::UnitOfWorkStore::execute) returns.
///
/// A unit of work holds aggregates of several types, so the aggregate's own
/// error is type-erased into [`Domain`](Self::Domain). Any
/// [`AggregateError`] — and any [`CommandError`] — converts with `?`:
///
/// ```
/// # use event_sauce_core::{AggregateError, UnitOfWorkError};
/// # use thiserror::Error;
/// # #[derive(Debug, Error)]
/// # #[error("rejected")]
/// # struct Rejected;
/// # impl AggregateError for Rejected {}
/// fn fails() -> Result<(), Rejected> {
///     Err(Rejected)
/// }
/// fn handler() -> Result<(), UnitOfWorkError> {
///     fails()?;
///     Ok(())
/// }
/// assert!(matches!(handler(), Err(UnitOfWorkError::Domain(_))));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum UnitOfWorkError {
    /// An aggregate rejected an event: its own
    /// [`Aggregate::Error`](crate::Aggregate::Error), type-erased.
    #[error("domain error: {0}")]
    Domain(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// An aggregate was not in the lifecycle the operation requires: loading
    /// a missing or deleted aggregate, or creating one that already exists.
    #[error("expected aggregate to be {expected}, found {found}")]
    Lifecycle {
        /// The lifecycle the operation required.
        expected: Lifecycle,
        /// The lifecycle the aggregate was actually in.
        found: Lifecycle,
    },
    /// The store failed, or refused the commit (a concurrency conflict, which
    /// [`UnitOfWorkStore::execute`](super::UnitOfWorkStore::execute) retries).
    #[error("store error: {0}")]
    Store(#[source] Error),
    /// The aggregate type was never registered with the
    /// [`UnitOfWorkStore`](super::UnitOfWorkStore).
    #[error("aggregate type {aggregate_type} is not registered with this unit-of-work store")]
    Unregistered {
        /// The unregistered aggregate type.
        aggregate_type: AggregateType,
    },
    /// The aggregate is already checked out of this unit of work by a live
    /// [`Tracked`](super::Tracked) handle.
    #[error("{aggregate_type} {id} is already checked out of this unit of work")]
    AlreadyCheckedOut {
        /// The checked-out aggregate's type.
        aggregate_type: AggregateType,
        /// The checked-out aggregate's id.
        id: EntityId,
    },
    /// This many [`Tracked`](super::Tracked) handles were leaked (with
    /// [`std::mem::forget`], say) instead of dropped, so their changes never
    /// came back to the unit of work. Nothing is committed.
    #[error("{0} aggregate handle(s) were leaked instead of dropped; nothing was committed")]
    LeakedHandle(usize),
    /// A [`Tracked::delete_with`](super::Tracked::delete_with) closure failed
    /// after taking the aggregate, so the unit of work no longer holds a
    /// root to commit for it. Nothing is committed.
    #[error("delete_with failed for {aggregate_type} {id}; nothing was committed")]
    DeleteFailed {
        /// The aggregate's type.
        aggregate_type: AggregateType,
        /// The aggregate's id.
        id: EntityId,
    },
}

impl UnitOfWorkError {
    /// Returns `true` for a store error that is a concurrency conflict — the
    /// one failure [`UnitOfWorkStore::execute`](super::UnitOfWorkStore::execute)
    /// retries.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_core::{AggregateVersion, Error, UnitOfWorkError};
    ///
    /// let conflict = Error::concurrency_conflict(AggregateVersion::new(1), AggregateVersion::new(2));
    /// assert!(UnitOfWorkError::from(conflict).is_concurrency_conflict());
    /// assert!(!UnitOfWorkError::LeakedHandle(1).is_concurrency_conflict());
    /// ```
    #[must_use]
    pub fn is_concurrency_conflict(&self) -> bool {
        matches!(self, Self::Store(e) if e.is_concurrency_conflict())
    }
}

impl From<Error> for UnitOfWorkError {
    fn from(error: Error) -> Self {
        Self::Store(error)
    }
}

impl<E: AggregateError> From<E> for UnitOfWorkError {
    fn from(error: E) -> Self {
        Self::Domain(Box::new(error))
    }
}

impl<E: AggregateError> From<CommandError<E>> for UnitOfWorkError {
    fn from(error: CommandError<E>) -> Self {
        match error {
            CommandError::Domain(e) => e.into(),
            CommandError::Lifecycle { expected, found } => Self::Lifecycle { expected, found },
        }
    }
}

#[cfg(test)]
mod tests;
