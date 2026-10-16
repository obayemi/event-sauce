use super::InMemoryTransactionalStore;
use async_trait::async_trait;
use event_sauce_core::{
    AggregateVersion, AtomicCommit, Error, EventEnvelope, EventStore, Result, StateCommit,
    StateProjection, StateStore, StoredState, StreamCommit, StreamId, TransactionalStore,
};
use std::sync::Arc;
use uuid::Uuid;

fn stream_write(stream_id: &StreamId, expected_version: AggregateVersion) -> StreamCommit {
    StreamCommit {
        stream_id: stream_id.clone(),
        events: vec![EventEnvelope::new(
            Uuid::new_v4(),
            stream_id.aggregate_id(),
            stream_id.aggregate_type().to_string(),
            "Happened".to_string(),
            event_sauce_core::EventVersion::new(1),
            serde_json::json!({}),
        )],
        expected_version,
        claims: vec![],
        clear_claims: false,
    }
}

fn state_commit(stream_id: &StreamId, version: i64) -> StateCommit {
    StateCommit {
        state: StoredState {
            aggregate_id: stream_id.aggregate_id(),
            aggregate_type: stream_id.aggregate_type().clone(),
            state_data: serde_json::json!({"version": version}),
            version: AggregateVersion::new(version),
            is_deleted: false,
            schema_version: 1,
        },
        expected_version: AggregateVersion::new(version - 1),
        events: vec![],
        claims: vec![],
        clear_claims: false,
    }
}

fn new_store() -> (
    InMemoryTransactionalStore,
    Arc<crate::InMemoryEventStore>,
    Arc<crate::InMemoryStateStore>,
) {
    let events = Arc::new(crate::InMemoryEventStore::new());
    let states = Arc::new(crate::InMemoryStateStore::new());
    let store = InMemoryTransactionalStore::new(Arc::clone(&events), Arc::clone(&states));
    (store, events, states)
}

#[tokio::test]
async fn test_mixed_commit_makes_both_writes_visible() {
    let (store, events, states) = new_store();
    let stream_stream_id = StreamId::new("Order", Uuid::new_v4());
    let state_stream_id = StreamId::new("Product", Uuid::new_v4());

    let mut commit = AtomicCommit::new();
    commit
        .stream_writes
        .push(stream_write(&stream_stream_id, AggregateVersion::initial()));
    commit.state_writes.push(state_commit(&state_stream_id, 1));

    store.commit_atomic(commit).await.unwrap();

    assert_eq!(
        events.get_version(stream_stream_id).await.unwrap(),
        AggregateVersion::new(1)
    );
    assert!(states.load(state_stream_id).await.unwrap().is_some());
}

#[tokio::test]
async fn test_stale_stream_expectation_rolls_back_both_writes() {
    let (store, events, states) = new_store();
    let stream_stream_id = StreamId::new("Order", Uuid::new_v4());
    let state_stream_id = StreamId::new("Product", Uuid::new_v4());

    let mut commit = AtomicCommit::new();
    commit.stream_writes.push(stream_write(
        &StreamId::new("Order", Uuid::new_v4()),
        AggregateVersion::initial(),
    ));
    commit.state_writes.push(state_commit(&state_stream_id, 1));
    commit
        .stream_expectations
        .push((stream_stream_id.clone(), AggregateVersion::new(5)));

    let error = store.commit_atomic(commit).await.unwrap_err();
    assert!(error.is_concurrency_conflict());

    assert_eq!(
        events.get_version(stream_stream_id).await.unwrap(),
        AggregateVersion::initial()
    );
    assert!(states.load(state_stream_id).await.unwrap().is_none());
}

#[tokio::test]
async fn test_stale_state_expectation_rolls_back_everything() {
    let (store, events, states) = new_store();
    let stream_stream_id = StreamId::new("Order", Uuid::new_v4());
    let state_stream_id = StreamId::new("Product", Uuid::new_v4());

    let mut commit = AtomicCommit::new();
    commit
        .stream_writes
        .push(stream_write(&stream_stream_id, AggregateVersion::initial()));
    commit
        .state_expectations
        .push((state_stream_id.clone(), AggregateVersion::new(5)));

    let error = store.commit_atomic(commit).await.unwrap_err();
    assert!(error.is_concurrency_conflict());

    assert_eq!(
        events.get_version(stream_stream_id).await.unwrap(),
        AggregateVersion::initial(),
        "the stream write must not survive a rejected state expectation"
    );
    assert!(states.load(state_stream_id).await.unwrap().is_none());
}

#[tokio::test]
async fn test_stale_stream_write_rolls_back_the_state_write() {
    let (store, events, states) = new_store();
    let stream_stream_id = StreamId::new("Order", Uuid::new_v4());
    let state_stream_id = StreamId::new("Product", Uuid::new_v4());

    let mut commit = AtomicCommit::new();
    commit
        .stream_writes
        .push(stream_write(&stream_stream_id, AggregateVersion::new(5)));
    commit.state_writes.push(state_commit(&state_stream_id, 1));

    let error = store.commit_atomic(commit).await.unwrap_err();
    assert!(error.is_concurrency_conflict());

    assert_eq!(
        events.get_version(stream_stream_id).await.unwrap(),
        AggregateVersion::initial()
    );
    assert!(
        states.load(state_stream_id).await.unwrap().is_none(),
        "a failing stream write must roll back the already-staged state write"
    );
}

struct FailingProjection;

#[async_trait]
impl StateProjection<crate::InMemoryProjectionContext> for FailingProjection {
    fn name(&self) -> &'static str {
        "failing"
    }

    async fn project(
        &self,
        _ctx: &mut crate::InMemoryProjectionContext,
        _events: &[EventEnvelope],
    ) -> Result<()> {
        Err(Error::Custom("projection failed".into()))
    }
}

#[tokio::test]
async fn test_a_failing_state_projection_rolls_back_the_stream_write_too() {
    let events = Arc::new(crate::InMemoryEventStore::new());
    let states = Arc::new(
        crate::InMemoryStateStore::builder()
            .with_projection(Arc::new(FailingProjection))
            .build(),
    );
    let store = InMemoryTransactionalStore::new(Arc::clone(&events), Arc::clone(&states));

    let stream_stream_id = StreamId::new("Order", Uuid::new_v4());
    let state_stream_id = StreamId::new("Product", Uuid::new_v4());
    let mut state_write = state_commit(&state_stream_id, 1);
    state_write.events.push(EventEnvelope::new(
        Uuid::new_v4(),
        state_stream_id.aggregate_id(),
        "Product".to_string(),
        "Happened".to_string(),
        event_sauce_core::EventVersion::new(1),
        serde_json::json!({}),
    ));

    let mut commit = AtomicCommit::new();
    commit
        .stream_writes
        .push(stream_write(&stream_stream_id, AggregateVersion::initial()));
    commit.state_writes.push(state_write);

    let error = store.commit_atomic(commit).await.unwrap_err();
    assert!(matches!(error, Error::Custom(_)));

    assert_eq!(
        events.get_version(stream_stream_id).await.unwrap(),
        AggregateVersion::initial(),
        "a failing projection must roll back the stream write"
    );
}

#[tokio::test]
async fn test_expectations_alone_succeed_when_versions_match() {
    let (store, events, states) = new_store();
    let stream_stream_id = StreamId::new("Order", Uuid::new_v4());
    let state_stream_id = StreamId::new("Product", Uuid::new_v4());
    events
        .append(
            stream_stream_id.clone(),
            vec![EventEnvelope::new(
                Uuid::new_v4(),
                stream_stream_id.aggregate_id(),
                "Order".to_string(),
                "Happened".to_string(),
                event_sauce_core::EventVersion::new(1),
                serde_json::json!({}),
            )],
            AggregateVersion::initial(),
            vec![],
            false,
        )
        .await
        .unwrap();
    states
        .save(state_commit(&state_stream_id, 1))
        .await
        .unwrap();

    let mut commit = AtomicCommit::new();
    commit
        .stream_expectations
        .push((stream_stream_id, AggregateVersion::new(1)));
    commit
        .state_expectations
        .push((state_stream_id, AggregateVersion::new(1)));

    store.commit_atomic(commit).await.unwrap();
}

#[tokio::test]
async fn test_concurrent_commits_racing_on_the_same_stream_exactly_one_succeeds() {
    let (store, events, _states) = new_store();
    let stream_id = StreamId::new("Order", Uuid::new_v4());

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let stream_id = stream_id.clone();
        handles.push(tokio::spawn(async move {
            let mut commit = AtomicCommit::new();
            commit
                .stream_writes
                .push(stream_write(&stream_id, AggregateVersion::initial()));
            store.commit_atomic(commit).await
        }));
    }

    let mut successes = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            successes += 1;
        }
    }

    assert_eq!(successes, 1, "exactly one racing commit must win");
    assert_eq!(
        events.get_version(stream_id).await.unwrap(),
        AggregateVersion::new(1)
    );
}

#[tokio::test]
async fn test_plain_save_still_works_while_no_unit_of_work_runs() {
    let events = Arc::new(crate::InMemoryEventStore::new());
    let states = Arc::new(crate::InMemoryStateStore::new());
    let stream_id = StreamId::new("Order", Uuid::new_v4());

    events
        .append(
            stream_id.clone(),
            vec![EventEnvelope::new(
                Uuid::new_v4(),
                stream_id.aggregate_id(),
                "Order".to_string(),
                "Happened".to_string(),
                event_sauce_core::EventVersion::new(1),
                serde_json::json!({}),
            )],
            AggregateVersion::initial(),
            vec![],
            false,
        )
        .await
        .unwrap();
    states.save(state_commit(&stream_id, 1)).await.unwrap();

    assert_eq!(
        events.get_version(stream_id.clone()).await.unwrap(),
        AggregateVersion::new(1)
    );
    assert!(states.load(stream_id).await.unwrap().is_some());
}
