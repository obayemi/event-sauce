//! Units of work: commands that change several aggregates atomically.
//!
//! A unit of work loads aggregates of several types — some event-sourced,
//! some state-stored — lets a command modify them, then commits every
//! pending write in one atomic commit that also asserts every aggregate it
//! only read is still at the version it loaded. This module defines the
//! primitives that make that possible without the unit of work ever
//! branching on persistence kind:
//!
//! - [`PreparedWrite`] and [`Expectation`] — what
//!   [`UnitOfWorkRepository::prepare`]/[`prepare_deleted`](UnitOfWorkRepository::prepare_deleted)
//!   and [`expect`](UnitOfWorkRepository::expect) hand back: opaque values a
//!   unit of work collects without inspecting.
//! - [`AtomicCommit`] — the collected writes and expectations, sorted by
//!   persistence kind, ready for a backend to apply.
//! - [`TransactionalStore`] — the pair of stores (an [`EventStore`] and a
//!   [`StateStore`]) a backend commits an [`AtomicCommit`] against,
//!   atomically.
//!
//! On top of that contract sits the unit of work itself:
//!
//! - [`UnitOfWorkStore`] — a [`TransactionalStore`] plus the registry of
//!   aggregate types it holds, each registered as event-sourced or
//!   state-stored; it runs [`UnitOfWorkCommand`]s, retrying on conflict.
//! - [`UnitOfWork`] — one attempt of a command: loads, creates and peeks at
//!   aggregates, then commits every change atomically.
//! - [`Tracked`] — an aggregate checked out of a unit of work.
//! - [`UnitOfWorkError`] — everything a unit of work can fail with.

mod atomic_commit;
mod command;
mod erased;
mod error;
mod identity_map;
mod repository_ext;
mod store;
#[cfg(test)]
mod test_support;
mod tracked;
mod uow;

pub use atomic_commit::{AtomicCommit, Expectation, PreparedWrite, StreamWrite};
pub use command::UnitOfWorkCommand;
pub use error::UnitOfWorkError;
pub use repository_ext::UnitOfWorkRepository;
pub use store::{UnitOfWorkExecuted, UnitOfWorkStore};
pub use tracked::Tracked;
pub use uow::UnitOfWork;

use async_trait::async_trait;

use crate::{EventStore, Result, StateStore};

/// A pair of stores a unit of work commits [`AtomicCommit`]s against,
/// atomically.
///
/// Pairs an [`EventStore`] with a [`StateStore`] so a single unit of work can
/// hold aggregates of both persistence styles and commit them together.
/// `commit_atomic` has no default implementation: looping over each write
/// the way [`EventStore::append_batch`]'s default does cannot be atomic
/// across two different stores, so every implementor must provide its own
/// real transaction (or fail outright if it cannot offer one).
///
/// # Examples
///
/// ```
/// use async_trait::async_trait;
/// use event_sauce_core::{AtomicCommit, EventStore, Result, StateStore, TransactionalStore};
///
/// struct Stores<E, S> {
///     events: E,
///     states: S,
/// }
///
/// #[async_trait]
/// impl<E: EventStore, S: StateStore> TransactionalStore for Stores<E, S> {
///     type Events = E;
///     type States = S;
///
///     fn event_store(&self) -> &Self::Events {
///         &self.events
///     }
///
///     fn state_store(&self) -> &Self::States {
///         &self.states
///     }
///
///     async fn commit_atomic(&self, commit: AtomicCommit) -> Result<()> {
///         // A real backend applies `commit.stream_writes`, `commit.state_writes`
///         // and both expectation lists inside one transaction.
///         let _ = commit;
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait TransactionalStore: Send + Sync {
    /// The event store half of this pair.
    type Events: EventStore;
    /// The state store half of this pair.
    type States: StateStore;

    /// Returns the event store.
    fn event_store(&self) -> &Self::Events;

    /// Returns the state store.
    fn state_store(&self) -> &Self::States;

    /// Commits every write and checks every expectation in `commit`
    /// atomically: either all of it is durable, or none of it is.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if any write's or expectation's
    /// version does not match what is currently stored. Returns
    /// `Error::ClaimConflict` if a claim in a write is already held by
    /// another aggregate.
    async fn commit_atomic(&self, commit: AtomicCommit) -> Result<()>;
}
