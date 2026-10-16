//! The in-memory backend's [`TransactionalStore`] implementation.
//!
//! Commits an [`AtomicCommit`] across an [`InMemoryEventStore`] and an
//! [`InMemoryStateStore`] atomically, by nesting the event store's commit
//! inside the state store's: every state-side check (and every state write
//! and projection) runs first, then the event store applies its own writes
//! and expectations as the state store's `before_swap` hook, so neither
//! store's data becomes visible unless both succeed.

use async_trait::async_trait;
use event_sauce_core::{AtomicCommit, Result, TransactionalStore};
use std::sync::Arc;

use crate::{InMemoryEventStore, InMemoryStateStore};

/// Pairs an [`InMemoryEventStore`] and an [`InMemoryStateStore`] for atomic
/// cross-store commits, via [`TransactionalStore::commit_atomic`].
///
/// # Lock order
///
/// `commit_atomic` always takes the state store's write lock before the
/// event store's — it checks and stages every state write, then, still
/// holding that lock, checks and applies the stream writes. An ordinary
/// [`StateStore::save`](event_sauce_core::StateStore::save) or
/// [`EventStore::append`](event_sauce_core::EventStore::append) takes only
/// its own store's lock, so this fixed order can never deadlock against a
/// concurrent plain save: there is no code path that takes the event
/// store's lock first and then waits on the state store's.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use event_sauce_core::TransactionalStore;
/// use event_sauce_memory::{InMemoryEventStore, InMemoryStateStore, InMemoryTransactionalStore};
///
/// let store = InMemoryTransactionalStore::new(
///     Arc::new(InMemoryEventStore::new()),
///     Arc::new(InMemoryStateStore::new()),
/// );
///
/// let _events = store.event_store();
/// let _states = store.state_store();
/// ```
#[derive(Clone)]
pub struct InMemoryTransactionalStore {
    events: Arc<InMemoryEventStore>,
    states: Arc<InMemoryStateStore>,
}

impl InMemoryTransactionalStore {
    /// Pairs an event store and a state store for atomic cross-store commits.
    #[must_use]
    pub fn new(events: Arc<InMemoryEventStore>, states: Arc<InMemoryStateStore>) -> Self {
        Self { events, states }
    }
}

#[async_trait]
impl TransactionalStore for InMemoryTransactionalStore {
    type Events = InMemoryEventStore;
    type States = InMemoryStateStore;

    fn event_store(&self) -> &Self::Events {
        &self.events
    }

    fn state_store(&self) -> &Self::States {
        &self.states
    }

    /// Commits every stream and state write in `commit` and checks every
    /// expectation, atomically.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConcurrencyConflict` if any write's or expectation's
    /// version does not match what is currently stored, or any error a
    /// state projection returns. On error, neither the stream writes nor the
    /// state writes are visible.
    async fn commit_atomic(&self, commit: AtomicCommit) -> Result<()> {
        let AtomicCommit {
            stream_writes,
            state_writes,
            stream_expectations,
            state_expectations,
        } = commit;
        let events = &self.events;

        self.states
            .commit_all_then(state_writes, &state_expectations, || {
                events.commit_with_expectations(stream_writes, &stream_expectations)
            })
            .await
    }
}

#[cfg(test)]
mod tests;
