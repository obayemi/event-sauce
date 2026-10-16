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
fn push_write_returns_the_stream_snapshot_and_keeps_only_the_commit() {
    let mut commit = AtomicCommit::new();
    let stream_id = StreamId::new("Order", Uuid::new_v4());
    let mut write = stream_write(stream_id.clone());
    write.snapshot = Some(Snapshot::new(
        stream_id.aggregate_id(),
        "Order",
        AggregateVersion::new(1),
        serde_json::json!({}),
    ));

    let snapshot = commit.push_write(PreparedWrite::Stream(write));

    assert_eq!(snapshot.unwrap().snapshot_version, AggregateVersion::new(1));
    assert_eq!(commit.stream_writes[0].stream_id, stream_id);
}

#[test]
fn push_write_returns_no_snapshot_for_a_state_write() {
    let mut commit = AtomicCommit::new();
    let stream_id = StreamId::new("Product", Uuid::new_v4());

    assert!(commit
        .push_write(PreparedWrite::State(state_commit(&stream_id)))
        .is_none());
}

#[test]
fn has_writes_ignores_expectations() {
    let mut commit = AtomicCommit::new();
    commit.push_expectation(Expectation::Stream {
        stream_id: StreamId::new("Order", Uuid::new_v4()),
        version: AggregateVersion::new(1),
    });
    assert!(!commit.has_writes());

    commit.push_write(PreparedWrite::State(state_commit(&StreamId::new(
        "Product",
        Uuid::new_v4(),
    ))));
    assert!(commit.has_writes());
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
