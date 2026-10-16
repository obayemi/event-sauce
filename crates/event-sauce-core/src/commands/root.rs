//! [`Root`] — an aggregate wherever it currently stands in its lifecycle.

use super::{CommandError, Lifecycle};
use crate::{
    Aggregate, AggregateRoot, DeletedAggregateRoot, EntityId, Loaded, UninitAggregateRoot,
};

/// An aggregate at one of its three lifecycle states.
///
/// What a [`Repository`](crate::Repository) load or a [`Command`](super::Command)
/// hands back when the caller does not already know, from context, which
/// state the aggregate is in: not yet created
/// ([`UninitAggregateRoot`]), created and live ([`AggregateRoot`]), or
/// deleted ([`DeletedAggregateRoot`]).
///
/// A command that only makes sense in one state narrows with
/// [`active()`](Self::active), [`missing()`](Self::missing) or
/// [`deleted()`](Self::deleted), which take `self` and return the matching
/// root or a [`CommandError::Lifecycle`] naming both the expected and the
/// actual state.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{
///     Aggregate, AggregateError, AggregateRoot, CommandError, DefaultEntity, DomainEvent,
///     Entity, EntityId, EventApplicator, EventVersion, Lifecycle, Root,
/// };
/// use serde::{Deserialize, Serialize};
/// use thiserror::Error;
///
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
/// let root: Root<Counter> = AggregateRoot::<Counter>::new(EntityId::new()).into();
/// assert_eq!(root.lifecycle(), Lifecycle::Active);
///
/// let mut active = root.active().expect("root is active");
/// active.apply(CounterEvent::Incremented { amount: 3 }).unwrap();
/// assert_eq!(active.value, 3);
///
/// let root: Root<Counter> = active.into();
/// let err = root.missing().unwrap_err();
/// assert!(matches!(
///     err,
///     CommandError::Lifecycle { expected: Lifecycle::Missing, found: Lifecycle::Active }
/// ));
/// ```
#[derive(Debug)]
pub enum Root<A: Aggregate> {
    /// No init event has been applied yet.
    Missing(UninitAggregateRoot<A>),
    /// The aggregate has been created and is not deleted.
    Active(AggregateRoot<A>),
    /// A delete event has been applied; the aggregate is in its terminal
    /// state.
    Deleted(DeletedAggregateRoot<A>),
}

impl<A: Aggregate> Root<A> {
    /// Returns where this root currently stands in its lifecycle.
    #[must_use]
    pub fn lifecycle(&self) -> Lifecycle {
        match self {
            Root::Missing(_) => Lifecycle::Missing,
            Root::Active(_) => Lifecycle::Active,
            Root::Deleted(_) => Lifecycle::Deleted,
        }
    }

    /// Returns the entity's unique identifier, whichever lifecycle state it
    /// is in.
    #[must_use]
    pub fn entity_id(&self) -> EntityId {
        match self {
            Root::Missing(root) => root.entity_id(),
            Root::Active(root) => root.entity_id(),
            Root::Deleted(root) => root.entity_id(),
        }
    }

    /// Narrows to the active root, or a [`CommandError::Lifecycle`] naming
    /// the state actually found.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Lifecycle`] when this root is
    /// [`Missing`](Self::Missing) or [`Deleted`](Self::Deleted).
    pub fn active(self) -> Result<AggregateRoot<A>, CommandError<A::Error>> {
        match self {
            Root::Active(root) => Ok(root),
            other => Err(CommandError::Lifecycle {
                expected: Lifecycle::Active,
                found: other.lifecycle(),
            }),
        }
    }

    /// Narrows to the uninitialized root, or a [`CommandError::Lifecycle`]
    /// naming the state actually found.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Lifecycle`] when this root is
    /// [`Active`](Self::Active) or [`Deleted`](Self::Deleted).
    pub fn missing(self) -> Result<UninitAggregateRoot<A>, CommandError<A::Error>> {
        match self {
            Root::Missing(root) => Ok(root),
            other => Err(CommandError::Lifecycle {
                expected: Lifecycle::Missing,
                found: other.lifecycle(),
            }),
        }
    }

    /// Narrows to the deleted root, or a [`CommandError::Lifecycle`] naming
    /// the state actually found.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Lifecycle`] when this root is
    /// [`Missing`](Self::Missing) or [`Active`](Self::Active).
    pub fn deleted(self) -> Result<DeletedAggregateRoot<A>, CommandError<A::Error>> {
        match self {
            Root::Deleted(root) => Ok(root),
            other => Err(CommandError::Lifecycle {
                expected: Lifecycle::Deleted,
                found: other.lifecycle(),
            }),
        }
    }
}

impl<A: Aggregate> From<AggregateRoot<A>> for Root<A> {
    fn from(root: AggregateRoot<A>) -> Self {
        Root::Active(root)
    }
}

impl<A: Aggregate> From<UninitAggregateRoot<A>> for Root<A> {
    fn from(root: UninitAggregateRoot<A>) -> Self {
        Root::Missing(root)
    }
}

impl<A: Aggregate> From<DeletedAggregateRoot<A>> for Root<A> {
    fn from(root: DeletedAggregateRoot<A>) -> Self {
        Root::Deleted(root)
    }
}

impl<A: Aggregate> From<Loaded<A>> for Root<A> {
    fn from(loaded: Loaded<A>) -> Self {
        match loaded {
            Loaded::Active(root) => Root::Active(root),
            Loaded::Deleted(root) => Root::Deleted(root),
        }
    }
}

#[cfg(test)]
mod tests;
