//! [`UnitOfWorkCommand`] — a command spanning several aggregates.

use async_trait::async_trait;

use super::{TransactionalStore, UnitOfWork, UnitOfWorkError};

/// A command handler that loads, creates, modifies and deletes any number of
/// aggregates through a [`UnitOfWork`], run with a context `Ctx`.
///
/// The handler never saves anything itself: once it returns `Ok`,
/// [`UnitOfWorkStore::execute`](super::UnitOfWorkStore::execute) commits
/// every change it made in one atomic commit, asserting every aggregate it
/// only read is unchanged. Returning `Err` discards every change.
///
/// The handler may run more than once: on a concurrency conflict,
/// `execute` retries it against a fresh unit of work, so it must not have
/// side effects outside the unit of work.
///
/// # Examples
///
/// See [`UnitOfWorkStore`](super::UnitOfWorkStore).
#[async_trait]
pub trait UnitOfWorkCommand<T: TransactionalStore, Ctx = ()>: Send + Sync {
    /// Runs the command against `uow`.
    ///
    /// # Errors
    ///
    /// Returns whatever [`UnitOfWorkError`] the handler raises; any
    /// [`AggregateError`](crate::AggregateError) or
    /// [`CommandError`](crate::CommandError) converts with `?`.
    async fn handle(&self, uow: &UnitOfWork<'_, T>, ctx: &Ctx) -> Result<(), UnitOfWorkError>;

    /// The name recorded on the events the command produces, when it has
    /// one. Commands are anonymous by default.
    fn name(&self) -> Option<&str> {
        None
    }
}
