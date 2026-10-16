//! [`CommandError`] — a command's domain error widened with lifecycle
//! mismatches.

use super::Lifecycle;

/// The error a [`Command`](super::Command) returns: either the aggregate's
/// own domain error, or a lifecycle mismatch raised by narrowing a [`Root`](super::Root)
/// (`root.active()`, `root.missing()`, `root.deleted()`) to a state it is not
/// actually in.
///
/// # `?` from a domain error
///
/// A blanket `From<E> for CommandError<E>` is provided, so a handler
/// returning `Result<_, E>` from the aggregate (`root.apply(event)?`, say)
/// converts with the same `?` used everywhere else — no `.map_err` needed:
///
/// ```
/// # use event_sauce_core::{AggregateError, CommandError};
/// # use thiserror::Error;
/// # #[derive(Debug, Error)]
/// # #[error("rejected")]
/// # struct Rejected;
/// # impl AggregateError for Rejected {}
/// fn fails() -> Result<(), Rejected> {
///     Err(Rejected)
/// }
/// fn handler() -> Result<(), CommandError<Rejected>> {
///     fails()?;
///     Ok(())
/// }
/// assert!(matches!(handler(), Err(CommandError::Domain(Rejected))));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum CommandError<E> {
    /// The aggregate rejected the event: a validation or dispatch failure
    /// from the aggregate's own [`Aggregate::Error`](crate::Aggregate::Error).
    #[error("domain error: {0}")]
    Domain(#[source] E),
    /// A [`Root`](super::Root) was narrowed to a lifecycle it is not in.
    #[error("expected aggregate to be {expected}, found {found}")]
    Lifecycle {
        /// The lifecycle the narrowing call required.
        expected: Lifecycle,
        /// The lifecycle the root was actually in.
        found: Lifecycle,
    },
}

impl<E> From<E> for CommandError<E> {
    fn from(error: E) -> Self {
        CommandError::Domain(error)
    }
}

#[cfg(test)]
mod tests;
