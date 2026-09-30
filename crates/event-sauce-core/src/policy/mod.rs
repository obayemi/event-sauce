//! Policy system for cross-aggregate event reactions.
//!
//! Provides the [`Policy`] trait for defining event handlers that react to events
//! by issuing commands on other aggregates, and [`PolicyContext`] for safe event
//! store access with automatic causation tracking.
//!
//! # Overview
//!
//! In an event-sourced system, a single command may produce events that should
//! trigger effects on other aggregates. For example, kicking a user may need to
//! update a group aggregate. Policies provide this cross-aggregate orchestration.
//!
//! # Design Principles
//!
//! - **Async/eventually consistent**: Policies process events outside the original
//!   transaction.
//! - **Forced causation tracking**: All interactions go through [`PolicyContext`],
//!   which guarantees causation metadata is always set.
//! - **Cascade depth limits**: Reactions can trigger further reactions; depth is
//!   bounded to prevent infinite loops.
//! - **Checkpoint-based resumption**: Policies track their position via
//!   [`CheckpointStore`](crate::CheckpointStore), preventing duplicate processing
//!   on restart.
//! - **Configurable error handling**: [`OnError`] controls whether failures abort,
//!   skip, or retry with exponential backoff.
//!
//! # Examples
//!
//! ```ignore
//! use event_sauce_core::policy::{Policy, PolicyContext, PolicyRunner};
//!
//! struct MyPolicy;
//!
//! #[async_trait::async_trait]
//! impl<S: EventStore + 'static> Policy<S> for MyPolicy {
//!     fn name(&self) -> &str { "MyPolicy" }
//!     fn event_filter(&self) -> EventFilter { EventFilter::by_event_type("User.Kicked") }
//!     async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<S>) -> Result<()> {
//!         // Load, modify, and commit another aggregate
//!         Ok(())
//!     }
//! }
//! ```

mod context;
mod macros;
mod on_error;
mod runner;
#[cfg(test)]
mod test_support;

pub use context::PolicyContext;
pub use on_error::{OnError, OnRetryExhausted, RetryConfig, RetryLimit};
pub use runner::PolicyRunner;

use crate::{EventEnvelope, EventFilter, EventStore, Result};

/// A policy handles events by issuing commands on other aggregates.
///
/// Policies are the mechanism for cross-aggregate event orchestration
/// (known as "process managers" or "sagas" in some literature).
/// Each policy declares which events it handles via [`event_filter()`](Policy::event_filter)
/// and processes matching events in [`handle()`](Policy::handle).
///
/// The type parameter `S` is the event store type, allowing the policy to
/// work with any `EventStore` implementation while remaining dyn-compatible.
///
/// # Implementing a Policy
///
/// ```ignore
/// struct NotifyOnKick;
///
/// #[async_trait]
/// impl<S: EventStore + 'static> Policy<S> for NotifyOnKick {
///     fn name(&self) -> &str { "NotifyOnKick" }
///
///     fn event_filter(&self) -> EventFilter {
///         EventFilter::by_event_type("User.Kicked")
///     }
///
///     async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<S>) -> Result<()> {
///         // React to the event by loading and modifying other aggregates
///         Ok(())
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Policy<S: EventStore + 'static>: Send + Sync {
    /// Unique name for this policy.
    ///
    /// Used for checkpoint tracking, so each policy resumes
    /// from where it left off after restarts.
    fn name(&self) -> &str;

    /// Which events this policy handles.
    ///
    /// Only events matching this filter will be passed to [`handle()`](Self::handle).
    fn event_filter(&self) -> EventFilter;

    /// Handle a single event.
    ///
    /// Use `ctx` to load aggregates and commit changes. The context
    /// automatically injects causation metadata into all produced events.
    ///
    /// # Errors
    ///
    /// Returns an error if loading, applying, or committing fails.
    async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<S>) -> Result<()>;
}
