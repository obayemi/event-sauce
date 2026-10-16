//! [`ExecuteError`] and [`Executed`] — the outcome of
//! [`Repository::execute`](crate::Repository::execute).

use std::num::NonZeroU32;

use super::{Lifecycle, Root};
use crate::Aggregate;

/// The error [`Repository::execute`](crate::Repository::execute) returns.
///
/// Widens [`CommandError`](super::CommandError) with the two ways the
/// surrounding load/save cycle can fail, so a caller can match on exactly
/// four outcomes: the command's own domain error, a lifecycle mismatch, a
/// load failure, or a save failure (which — unlike the other three — may
/// already have been retried according to the
/// [`RetryPolicy`](super::RetryPolicy) in effect).
///
/// # Examples
///
/// ```
/// # use event_sauce_core::{AggregateError, Error, ExecuteError, AggregateVersion};
/// # use thiserror::Error as ThisError;
/// # #[derive(Debug, ThisError)]
/// # #[error("rejected")]
/// # struct Rejected;
/// # impl AggregateError for Rejected {}
/// let err: ExecuteError<Rejected> = ExecuteError::Save(Error::concurrency_conflict(
///     AggregateVersion::new(1),
///     AggregateVersion::new(2),
/// ));
/// assert!(err.is_concurrency_conflict());
/// ```
#[derive(Debug, thiserror::Error)]
pub enum ExecuteError<E> {
    /// The command rejected the event: the aggregate's own
    /// [`Aggregate::Error`](crate::Aggregate::Error).
    #[error("domain error: {0}")]
    Domain(#[source] E),
    /// The command narrowed the loaded [`Root`] to a lifecycle it was not
    /// actually in.
    #[error("expected aggregate to be {expected}, found {found}")]
    Lifecycle {
        /// The lifecycle the command required.
        expected: Lifecycle,
        /// The lifecycle the aggregate was actually loaded in.
        found: Lifecycle,
    },
    /// Loading the aggregate failed. Never retried.
    #[error("load failed: {0}")]
    Load(#[source] crate::Error),
    /// Saving the aggregate failed, after the [`RetryPolicy`](super::RetryPolicy)
    /// gave up or declined to retry (a non-concurrency failure is never retried).
    #[error("save failed: {0}")]
    Save(#[source] crate::Error),
}

impl<E> ExecuteError<E> {
    /// Returns `true` for a [`Save`](Self::Save) error that is a concurrency
    /// conflict — the one failure [`Repository::execute`](crate::Repository::execute)
    /// retries, up to the given [`RetryPolicy`](super::RetryPolicy)'s attempt budget.
    ///
    /// # Examples
    ///
    /// ```
    /// # use event_sauce_core::{AggregateError, Error, ExecuteError};
    /// # use thiserror::Error as ThisError;
    /// # #[derive(Debug, ThisError)]
    /// # #[error("rejected")]
    /// # struct Rejected;
    /// # impl AggregateError for Rejected {}
    /// let err: ExecuteError<Rejected> = ExecuteError::Domain(Rejected);
    /// assert!(!err.is_concurrency_conflict());
    ///
    /// let err: ExecuteError<Rejected> = ExecuteError::Save(Error::custom("disk full"));
    /// assert!(!err.is_concurrency_conflict());
    /// ```
    #[must_use]
    pub fn is_concurrency_conflict(&self) -> bool {
        matches!(self, Self::Save(e) if e.is_concurrency_conflict())
    }
}

/// The outcome of a successful [`Repository::execute`](crate::Repository::execute):
/// the aggregate's lifecycle state after the command ran and was saved, and
/// how many attempts it took.
///
/// # Examples
///
/// ```
/// # use event_sauce_core::{
/// #     Aggregate, AggregateError, AggregateRoot, DefaultEntity, DomainEvent, Entity, EntityId,
/// #     EventApplicator, EventVersion, Executed, Lifecycle, Root,
/// # };
/// # use serde::{Deserialize, Serialize};
/// # use thiserror::Error;
/// # #[derive(Debug, Serialize, Deserialize)]
/// # struct Counter { id: EntityId, value: i32 }
/// # impl Entity for Counter {
/// #     fn new(id: EntityId) -> Self { Self { id, value: 0 } }
/// #     fn entity_id(&self) -> EntityId { self.id }
/// # }
/// # impl DefaultEntity for Counter {}
/// # #[derive(Debug, Clone, Serialize, Deserialize)]
/// # enum CounterEvent { Incremented { amount: i32 } }
/// # impl DomainEvent for CounterEvent {
/// #     type Aggregate = Counter;
/// #     fn event_type(&self) -> &'static str { "Incremented" }
/// #     fn event_version(&self) -> EventVersion { EventVersion::new(1) }
/// #     fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> { chrono::Utc::now() }
/// # }
/// # impl EventApplicator<Counter> for CounterEvent {
/// #     fn dispatch(&self, c: &mut Counter) -> Result<(), CounterError> {
/// #         match self { CounterEvent::Incremented { amount } => c.value += amount }
/// #         Ok(())
/// #     }
/// #     fn dispatch_unchecked(&self, c: &mut Counter) {
/// #         match self { CounterEvent::Incremented { amount } => c.value += amount }
/// #     }
/// # }
/// # #[derive(Debug, Error)]
/// # #[error("counter error")]
/// # struct CounterError;
/// # impl AggregateError for CounterError {}
/// # impl Aggregate for Counter {
/// #     type Event = CounterEvent;
/// #     type Error = CounterError;
/// #     type DeletedState = Self;
/// # }
/// let executed = Executed {
///     root: Root::Active(AggregateRoot::<Counter>::new(EntityId::new())),
///     attempts: std::num::NonZeroU32::new(1).unwrap(),
/// };
/// assert_eq!(executed.attempts.get(), 1);
/// assert_eq!(executed.root.lifecycle(), Lifecycle::Active);
/// ```
#[derive(Debug)]
pub struct Executed<A: Aggregate> {
    /// The aggregate's lifecycle state once the command's result was saved.
    pub root: Root<A>,
    /// How many attempts the command took, including the first. Always `1`
    /// unless a save was retried after a concurrency conflict.
    pub attempts: NonZeroU32,
}

#[cfg(test)]
mod tests;
