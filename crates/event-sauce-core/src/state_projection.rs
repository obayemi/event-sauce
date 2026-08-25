//! In-transaction projection handlers for state stores.
//!
//! A [`StateProjection`] maintains a read model **inside the same transaction**
//! as the state write that produced the events: exactly-once, read-your-writes,
//! and a failing projection rolls the whole command back. Handlers are
//! registered on the concrete state store (whose transaction type fills the
//! `Ctx` parameter) and must stay DB-local — anything that outlives the
//! transaction (external calls, notifications, policies) belongs in the
//! backend's transactional outbox instead. By design there is no after-commit
//! callback: without an event log, an at-most-once callback that misses a
//! delivery leaves the read model silently diverged with no way to repair it.

use async_trait::async_trait;

use crate::{EventEnvelope, EventFilter, Result};

/// An in-transaction projection over the events of a state commit.
///
/// `Ctx` is the backend's transaction handle (e.g. a `PostgreSQL` transaction
/// for the postgres backend, an in-memory context for the memory backend).
/// The store invokes [`project`](Self::project) inside the save transaction
/// with the commit's events, already filtered by [`filter`](Self::filter).
///
/// # Examples
///
/// ```ignore
/// struct OrderTotals;
///
/// #[async_trait]
/// impl StateProjection<PgTransaction> for OrderTotals {
///     fn name(&self) -> &str {
///         "order_totals"
///     }
///
///     fn filter(&self) -> EventFilter {
///         EventFilter::by_event::<ItemAddedEvent>()
///     }
///
///     async fn project(&self, tx: &mut PgTransaction, events: &[EventEnvelope]) -> Result<()> {
///         for event in events {
///             // UPDATE order_totals ... within the same transaction
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait StateProjection<Ctx: Send>: Send + Sync {
    /// A stable, unique name identifying this projection (used for
    /// diagnostics and backend bookkeeping).
    fn name(&self) -> &str;

    /// The filter selecting which events this projection receives.
    ///
    /// Defaults to [`EventFilter::All`].
    fn filter(&self) -> EventFilter {
        EventFilter::All
    }

    /// Applies a batch of events to the read model within the transaction.
    ///
    /// Invoked once per state commit with the commit's matching events, in
    /// application order. Must only touch state reachable through `ctx` — a
    /// returned error aborts the whole save.
    ///
    /// # Errors
    ///
    /// Returns an error to abort the transaction and fail the originating
    /// command.
    async fn project(&self, ctx: &mut Ctx, events: &[EventEnvelope]) -> Result<()>;
}
