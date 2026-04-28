//! Transactional projections for the `PostgreSQL` backend.
//!
//! [`PostgresProjection`] is the contract every postgres-backed read model
//! implements. Unlike an in-memory projection, the materialization lives
//! in the same database as the events; the projection's [`handle`] method
//! receives a `&mut sqlx::Transaction` so its writes commit atomically with
//! the checkpoint advance — see
//! [`PostgresBackend::run_postgres_projection`](crate::PostgresBackend::run_postgres_projection).
//!
//! [`handle`]: PostgresProjection::handle
//!
//! # Example
//!
//! ```ignore
//! use event_sauce_postgres::{PostgresBackend, PostgresProjection};
//! use event_sauce_core::{EventEnvelope, EventFilter, Result};
//!
//! struct OrderTotalsProjection;
//!
//! #[async_trait::async_trait]
//! impl PostgresProjection for OrderTotalsProjection {
//!     const NAME: &'static str = "OrderTotalsProjection";
//!
//!     fn handled_event_types() -> Option<Vec<&'static str>> {
//!         Some(vec!["Order.ItemAdded"])
//!     }
//!
//!     async fn handle(
//!         &mut self,
//!         envelope: &EventEnvelope,
//!         tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
//!     ) -> Result<()> {
//!         sqlx::query("UPDATE order_totals SET total = total + $1 WHERE order_id = $2")
//!             .bind(0_i64)
//!             .bind(envelope.aggregate_id)
//!             .execute(&mut **tx)
//!             .await
//!             .map_err(|e| event_sauce_core::Error::custom(e.to_string()))?;
//!         Ok(())
//!     }
//! }
//! ```

use async_trait::async_trait;
use event_sauce_core::{EventEnvelope, EventFilter, Result};

/// Postgres-backed projection contract.
///
/// Implementors apply events to a materialization stored in the same database
/// as the event log. The runner provides a transaction inside which the write
/// must happen, so that the projection's mutations and the subscription
/// checkpoint commit (or roll back) together.
#[async_trait]
pub trait PostgresProjection: Send {
    /// Projection name. Used as the subscription name for checkpointing.
    const NAME: &'static str;

    /// Event types this projection consumes.
    ///
    /// Returning `None` means "every event"; returning `Some(types)` filters
    /// the stream to those event types — the same convention as
    /// [`EventFilter::any_of_event_types`].
    #[must_use]
    fn handled_event_types() -> Option<Vec<&'static str>> {
        None
    }

    /// Filter derived from [`handled_event_types`](Self::handled_event_types).
    #[must_use]
    fn event_filter() -> EventFilter {
        match Self::handled_event_types() {
            None => EventFilter::All,
            Some(types) => EventFilter::any_of_event_types(types),
        }
    }

    /// Apply a single event inside the supplied transaction.
    ///
    /// All writes that materialize the projection must go through `tx`. The
    /// runner appends a checkpoint update to the same transaction and commits
    /// it; an `Err` return causes the transaction to be rolled back, leaving
    /// both the projection state and the checkpoint untouched so the event
    /// will be retried on the next run.
    ///
    /// # Errors
    ///
    /// Any error returned aborts the transaction and propagates out of the
    /// runner.
    async fn handle(
        &mut self,
        envelope: &EventEnvelope,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AllEvents;

    #[async_trait]
    impl PostgresProjection for AllEvents {
        const NAME: &'static str = "AllEvents";

        async fn handle(
            &mut self,
            _envelope: &EventEnvelope,
            _tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> Result<()> {
            Ok(())
        }
    }

    struct SomeEvents;

    #[async_trait]
    impl PostgresProjection for SomeEvents {
        const NAME: &'static str = "SomeEvents";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["A", "B"])
        }

        async fn handle(
            &mut self,
            _envelope: &EventEnvelope,
            _tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn default_filter_is_all() {
        assert_eq!(AllEvents::event_filter(), EventFilter::All);
    }

    #[test]
    fn filter_derived_from_handled_event_types() {
        assert_eq!(
            SomeEvents::event_filter(),
            EventFilter::any_of_event_types(vec!["A", "B"])
        );
    }
}
