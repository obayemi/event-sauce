//! The data a [`TransactionalStore::commit_atomic`](super::TransactionalStore::commit_atomic)
//! call persists: prepared writes and read-only version assertions, sorted
//! by persistence kind so a backend never has to match on it itself.

use crate::{AggregateVersion, Snapshot, StateCommit, StreamCommit, StreamId};

/// An event-sourced aggregate's prepared stream write.
///
/// Carries the events to append and, when the snapshot strategy calls for
/// one at this version, the snapshot to save best-effort once the commit is
/// durable. Bundling them together (rather than two parallel vectors on
/// [`AtomicCommit`]) keeps a write and its snapshot from drifting apart by
/// index.
#[derive(Debug, Clone)]
pub struct StreamWrite {
    /// The events to append, under optimistic concurrency.
    pub commit: StreamCommit,
    /// The snapshot to save best-effort once the commit is durable, if the
    /// snapshot strategy called for one at this version.
    pub snapshot: Option<Snapshot>,
}

/// One aggregate's pending write, prepared but not yet persisted.
///
/// Returned by [`UnitOfWorkRepository::prepare`](super::UnitOfWorkRepository::prepare)
/// and [`prepare_deleted`](super::UnitOfWorkRepository::prepare_deleted). Opaque
/// to the unit of work collecting it — it hands each one to
/// [`AtomicCommit::push_write`] without inspecting which variant it got, so
/// mixing event-sourced and state-stored aggregates in one unit of work never
/// requires branching on persistence kind. Which variant a repository
/// produces follows from its own persistence style, never from a runtime
/// choice.
#[derive(Debug, Clone)]
pub enum PreparedWrite {
    /// An event-sourced aggregate's stream write.
    Stream(StreamWrite),
    /// A state-stored aggregate's row write.
    State(StateCommit),
}

/// A version assertion without a write.
///
/// Returned by [`UnitOfWorkRepository::expect`](super::UnitOfWorkRepository::expect)
/// for an aggregate the unit of work only read: asserts its stream or row is
/// still at the loaded version, without writing anything for it. Like
/// [`PreparedWrite`], it is opaque to the unit of work — it is sorted into
/// the right bucket by [`AtomicCommit::push_expectation`].
#[derive(Debug, Clone)]
pub enum Expectation {
    /// An event-sourced aggregate's stream must still be at this version.
    Stream {
        /// The stream that must still be at `version`.
        stream_id: StreamId,
        /// The version the stream was loaded at.
        version: AggregateVersion,
    },
    /// A state-stored aggregate's row must still be at this version.
    State {
        /// The row that must still be at `version`.
        stream_id: StreamId,
        /// The version the row was loaded at.
        version: AggregateVersion,
    },
}

/// Everything a [`TransactionalStore::commit_atomic`](super::TransactionalStore::commit_atomic)
/// call must apply as one atomic unit: every write the unit of work prepared
/// and every read-only version assertion for the aggregates it only read.
///
/// Built by pushing [`PreparedWrite`]s and [`Expectation`]s one at a time —
/// the unit of work never has to know which bucket each one lands in. A
/// backend consumes the public fields directly: `stream_writes` and
/// `stream_expectations` go to its event store, `state_writes` and
/// `state_expectations` to its state store, all inside one transaction.
///
/// # Examples
///
/// ```
/// use event_sauce_core::{AggregateVersion, Expectation, AtomicCommit, StreamId};
/// use uuid::Uuid;
///
/// let mut commit = AtomicCommit::new();
/// assert!(commit.is_empty());
///
/// commit.push_expectation(Expectation::Stream {
///     stream_id: StreamId::new("Order", Uuid::new_v4()),
///     version: AggregateVersion::new(3),
/// });
/// assert_eq!(commit.stream_expectations.len(), 1);
/// assert!(!commit.is_empty());
/// ```
#[derive(Debug, Clone, Default)]
pub struct AtomicCommit {
    /// Event-sourced aggregates' stream writes.
    pub stream_writes: Vec<StreamWrite>,
    /// State-stored aggregates' row writes.
    pub state_writes: Vec<StateCommit>,
    /// Read-only stream version assertions, for event-sourced aggregates the
    /// unit of work only read.
    pub stream_expectations: Vec<(StreamId, AggregateVersion)>,
    /// Read-only row version assertions, for state-stored aggregates the
    /// unit of work only read.
    pub state_expectations: Vec<(StreamId, AggregateVersion)>,
}

impl AtomicCommit {
    /// Creates an empty atomic commit.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sorts a prepared write into `stream_writes` or `state_writes` by its
    /// variant.
    pub fn push_write(&mut self, write: PreparedWrite) {
        match write {
            PreparedWrite::Stream(write) => self.stream_writes.push(write),
            PreparedWrite::State(commit) => self.state_writes.push(commit),
        }
    }

    /// Sorts a version assertion into `stream_expectations` or
    /// `state_expectations` by its variant.
    pub fn push_expectation(&mut self, expectation: Expectation) {
        match expectation {
            Expectation::Stream { stream_id, version } => {
                self.stream_expectations.push((stream_id, version));
            }
            Expectation::State { stream_id, version } => {
                self.state_expectations.push((stream_id, version));
            }
        }
    }

    /// Whether this commit has nothing to write or assert.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stream_writes.is_empty()
            && self.state_writes.is_empty()
            && self.stream_expectations.is_empty()
            && self.state_expectations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoredState;
    use uuid::Uuid;

    fn stream_write(stream_id: StreamId) -> StreamWrite {
        StreamWrite {
            commit: StreamCommit {
                stream_id,
                events: vec![],
                expected_version: AggregateVersion::initial(),
                claims: vec![],
                clear_claims: false,
            },
            snapshot: None,
        }
    }

    fn state_commit(stream_id: &StreamId) -> StateCommit {
        StateCommit {
            state: StoredState {
                aggregate_id: stream_id.aggregate_id(),
                aggregate_type: stream_id.aggregate_type().clone(),
                state_data: serde_json::json!({}),
                version: AggregateVersion::new(1),
                is_deleted: false,
                schema_version: 1,
            },
            expected_version: AggregateVersion::initial(),
            events: vec![],
            claims: vec![],
            clear_claims: false,
        }
    }

    #[test]
    fn new_commit_is_empty() {
        assert!(AtomicCommit::new().is_empty());
    }

    #[test]
    fn push_write_sorts_a_stream_write_into_stream_writes() {
        let mut commit = AtomicCommit::new();
        let stream_id = StreamId::new("Order", Uuid::new_v4());

        commit.push_write(PreparedWrite::Stream(stream_write(stream_id)));

        assert_eq!(commit.stream_writes.len(), 1);
        assert!(commit.state_writes.is_empty());
        assert!(!commit.is_empty());
    }

    #[test]
    fn push_write_sorts_a_state_write_into_state_writes() {
        let mut commit = AtomicCommit::new();
        let stream_id = StreamId::new("Product", Uuid::new_v4());

        commit.push_write(PreparedWrite::State(state_commit(&stream_id)));

        assert_eq!(commit.state_writes.len(), 1);
        assert!(commit.stream_writes.is_empty());
    }

    #[test]
    fn push_expectation_sorts_a_stream_expectation() {
        let mut commit = AtomicCommit::new();
        let stream_id = StreamId::new("Order", Uuid::new_v4());
        let version = AggregateVersion::new(5);

        commit.push_expectation(Expectation::Stream {
            stream_id: stream_id.clone(),
            version,
        });

        assert_eq!(commit.stream_expectations, vec![(stream_id, version)]);
        assert!(commit.state_expectations.is_empty());
    }

    #[test]
    fn push_expectation_sorts_a_state_expectation() {
        let mut commit = AtomicCommit::new();
        let stream_id = StreamId::new("Product", Uuid::new_v4());
        let version = AggregateVersion::new(2);

        commit.push_expectation(Expectation::State {
            stream_id: stream_id.clone(),
            version,
        });

        assert_eq!(commit.state_expectations, vec![(stream_id, version)]);
        assert!(commit.stream_expectations.is_empty());
    }
}
