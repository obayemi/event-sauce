//! Typed commands over an aggregate's lifecycle.
//!
//! An aggregate command handler written against [`AggregateRoot`](crate::AggregateRoot)
//! alone cannot express "this command only makes sense before the aggregate
//! exists" or "only after it has been deleted" — every handler has to accept
//! whatever shape the repository handed it and re-derive the lifecycle it
//! expects by hand. [`Root`] makes the three lifecycle states —
//! [missing](crate::UninitAggregateRoot), [active](crate::AggregateRoot) and
//! [deleted](crate::DeletedAggregateRoot) — a single type a [`Command`] can
//! match on or narrow with `?`, turning a lifecycle mismatch into a typed
//! [`CommandError::Lifecycle`] instead of a panic or a silent no-op.
//!
//! [`RetryPolicy`] is the data half of retrying a command on a transient
//! failure (a concurrency conflict, say): how many attempts, and how long to
//! wait between them. The retry loop that applies it is shared by
//! [`Repository::execute`](crate::Repository::execute) and the unit-of-work
//! engine.

mod error;
mod execute;
mod retry;
mod root;

pub use error::CommandError;
pub use execute::{ExecuteError, Executed};
pub(crate) use retry::run_with_retry;
pub use retry::RetryPolicy;
pub use root::Root;

/// Where an aggregate stands in its lifecycle: not yet created, created and
/// live, or deleted.
///
/// Returned by [`Root::lifecycle`] and carried by
/// [`CommandError::Lifecycle`] to say which state a command expected and
/// which one it actually found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    /// No init event has been applied yet.
    Missing,
    /// The aggregate has been created and is not deleted.
    Active,
    /// A delete event has been applied; the aggregate is in its terminal
    /// state.
    Deleted,
}

impl std::fmt::Display for Lifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Lifecycle::Missing => "missing",
            Lifecycle::Active => "active",
            Lifecycle::Deleted => "deleted",
        })
    }
}

/// A command handler for aggregate `A`, run with a context `Ctx`.
///
/// Takes the aggregate wherever it currently stands in its lifecycle
/// ([`Root`]) and returns it wherever the command leaves it — the same
/// [`Root`] variant when nothing changed lifecycle, [`Root::Active`] after an
/// init or regular event, [`Root::Deleted`] after a delete event.
///
/// `Ctx` defaults to `()` for handlers that need no external dependency; give
/// it a concrete type (a clock, a config, a read-only lookup) when the
/// handler needs one injected rather than closed over.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{
///     Aggregate, AggregateError, AggregateRoot, Command, CommandError, DefaultEntity,
///     DomainEvent, Entity, EntityId, EventApplicator, EventVersion, Root,
/// };
/// use serde::{Deserialize, Serialize};
/// use thiserror::Error;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct Counter {
///     id: EntityId,
///     value: i32,
/// }
/// impl Entity for Counter {
///     fn new(id: EntityId) -> Self {
///         Self { id, value: 0 }
///     }
///     fn entity_id(&self) -> EntityId {
///         self.id
///     }
/// }
/// impl DefaultEntity for Counter {}
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// enum CounterEvent {
///     Incremented { amount: i32 },
/// }
/// impl DomainEvent for CounterEvent {
///     type Aggregate = Counter;
///     fn event_type(&self) -> &'static str {
///         "Incremented"
///     }
///     fn event_version(&self) -> EventVersion {
///         EventVersion::new(1)
///     }
///     fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
///         chrono::Utc::now()
///     }
/// }
/// impl EventApplicator<Counter> for CounterEvent {
///     fn dispatch(&self, c: &mut Counter) -> Result<(), CounterError> {
///         match self {
///             CounterEvent::Incremented { amount } => c.value += amount,
///         }
///         Ok(())
///     }
///     fn dispatch_unchecked(&self, c: &mut Counter) {
///         match self {
///             CounterEvent::Incremented { amount } => c.value += amount,
///         }
///     }
/// }
/// #[derive(Debug, Error)]
/// #[error("counter error")]
/// struct CounterError;
/// impl AggregateError for CounterError {}
/// impl Aggregate for Counter {
///     type Event = CounterEvent;
///     type Error = CounterError;
///     type DeletedState = Self;
/// }
///
/// struct Increment(i32);
///
/// impl Command<Counter> for Increment {
///     fn handle(
///         &self,
///         root: Root<Counter>,
///         _ctx: &(),
///     ) -> Result<Root<Counter>, CommandError<CounterError>> {
///         let mut root = root.active()?;
///         root.apply(CounterEvent::Incremented { amount: self.0 })?;
///         Ok(root.into())
///     }
///
///     fn name(&self) -> Option<&str> {
///         Some("Increment")
///     }
/// }
///
/// assert_eq!(Increment(5).name(), Some("Increment"));
/// let root = Root::Active(AggregateRoot::<Counter>::new(EntityId::new()));
/// let root = Increment(5).handle(root, &()).unwrap();
/// assert_eq!(root.active().unwrap().value, 5);
/// ```
pub trait Command<A: crate::Aggregate, Ctx = ()>: Send + Sync {
    /// Runs the command against `root`, returning the lifecycle `root` ends
    /// up in, or a [`CommandError`].
    ///
    /// # Errors
    ///
    /// Returns [`CommandError::Lifecycle`] when `root` is not in the
    /// lifecycle this command requires, or [`CommandError::Domain`] when the
    /// aggregate itself rejects the event.
    fn handle(&self, root: Root<A>, ctx: &Ctx) -> Result<Root<A>, CommandError<A::Error>>;

    /// The name recorded on the events the command produces, when it has
    /// one. Commands are anonymous by default.
    ///
    /// See the trait documentation for an example.
    fn name(&self) -> Option<&str> {
        None
    }
}

impl<A, Ctx, F> Command<A, Ctx> for F
where
    A: crate::Aggregate,
    F: Fn(Root<A>, &Ctx) -> Result<Root<A>, CommandError<A::Error>> + Send + Sync,
{
    fn handle(&self, root: Root<A>, ctx: &Ctx) -> Result<Root<A>, CommandError<A::Error>> {
        self(root, ctx)
    }
}

#[cfg(test)]
mod tests;
