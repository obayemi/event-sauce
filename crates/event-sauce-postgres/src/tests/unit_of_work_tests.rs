use super::*;
use async_trait::async_trait;
use event_sauce_core::{
    AggregateType, EventEnvelope, EventFilter, EventStore, EventVersion, Expectation,
    PreparedWrite, StateCommit, StateProjection, StateStore, StoredState, StreamCommit,
};
use serde_json::json;
use sqlx::{PgConnection, PgPool};
use std::time::Duration;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;

use crate::event_store::PostgresEventStoreBuilder;
use crate::state_store::PostgresStateStoreBuilder;

struct TestDb {
    pool: PgPool,
    url: String,
    #[allow(dead_code)]
    container: ContainerAsync<Postgres>,
}

impl TestDb {
    async fn new() -> Self {
        let (container, url) = crate::test_support::start_postgres_url().await;
        let pool = PgPool::connect(&url).await.expect("connect");
        Self {
            pool,
            url,
            container,
        }
    }

    async fn stores_with(
        &self,
        state_builder: PostgresStateStoreBuilder,
    ) -> (Arc<PostgresEventStore>, Arc<PostgresStateStore>) {
        let events = PostgresEventStoreBuilder::new()
            .pool(self.pool.clone())
            .schema("event_sauce")
            .build()
            .expect("pool was set");
        events.migrate().await.expect("migrate events");

        let states = state_builder.build().expect("pool was set");
        states.migrate().await.expect("migrate states");

        (Arc::new(events), Arc::new(states))
    }

    async fn stores(&self) -> (Arc<PostgresEventStore>, Arc<PostgresStateStore>) {
        self.stores_with(
            PostgresStateStore::builder()
                .pool(self.pool.clone())
                .schema("event_sauce"),
        )
        .await
    }

    async fn transactional(
        &self,
    ) -> (
        Arc<PostgresEventStore>,
        Arc<PostgresStateStore>,
        PostgresTransactionalStore,
    ) {
        let (events, states) = self.stores().await;
        let store = PostgresTransactionalStore::new(Arc::clone(&events), Arc::clone(&states))
            .expect("same database");
        (events, states, store)
    }
}

fn manual_event(aggregate_id: Uuid, aggregate_type: &str, event_type: &str) -> EventEnvelope {
    EventEnvelope::new(
        Uuid::new_v4(),
        aggregate_id,
        aggregate_type.to_string(),
        event_type.to_string(),
        EventVersion::new(1),
        json!({}),
    )
}

fn manual_stream_commit(
    stream_id: StreamId,
    expected_version: AggregateVersion,
    events: Vec<EventEnvelope>,
) -> StreamCommit {
    StreamCommit {
        stream_id,
        events,
        expected_version,
        claims: vec![],
        clear_claims: false,
    }
}

fn manual_state_commit(
    aggregate_type: &str,
    aggregate_id: Uuid,
    version: i64,
    expected: i64,
    events: Vec<EventEnvelope>,
) -> StateCommit {
    StateCommit {
        state: StoredState {
            aggregate_id,
            aggregate_type: AggregateType::from_owned(aggregate_type.to_string()),
            state_data: json!({ "n": version }),
            version: AggregateVersion::new(version),
            is_deleted: false,
            schema_version: 0,
        },
        expected_version: AggregateVersion::new(expected),
        events,
        claims: vec![],
        clear_claims: false,
    }
}

#[tokio::test]
async fn test_new_refuses_stores_on_different_databases() {
    let db = TestDb::new().await;
    let events = Arc::new(PostgresEventStore::new(db.pool.clone()));

    sqlx::query("CREATE DATABASE other_db")
        .execute(&db.pool)
        .await
        .expect("create other database");
    let cut = db.url.rfind('/').expect("url has a database segment");
    let other_url = format!("{}other_db", &db.url[..=cut]);
    let other_pool = PgPool::connect(&other_url).await.expect("connect");
    let states = Arc::new(PostgresStateStore::new(other_pool));

    let err = PostgresTransactionalStore::new(events, states).unwrap_err();
    assert!(err.is_invalid_state());
}

#[tokio::test]
async fn test_commit_atomic_writes_stream_and_state() {
    let db = TestDb::new().await;
    let (events, states, store) = db.transactional().await;

    let stream_id = StreamId::new("Order", Uuid::new_v4());
    let state_id = Uuid::new_v4();

    let mut commit = AtomicCommit::new();
    commit.stream_writes.push(manual_stream_commit(
        stream_id.clone(),
        AggregateVersion::initial(),
        vec![manual_event(
            stream_id.aggregate_id(),
            "Order",
            "Order.Placed",
        )],
    ));
    commit.push_write(PreparedWrite::State(manual_state_commit(
        "Invoice",
        state_id,
        1,
        0,
        vec![],
    )));

    store.commit_atomic(commit).await.expect("commit_atomic");

    assert_eq!(events.get_version(stream_id).await.unwrap().as_i64(), 1);
    assert!(states
        .exists(StreamId::new("Invoice", state_id))
        .await
        .unwrap());
}

#[tokio::test]
async fn test_stale_stream_expectation_rolls_back_state_write() {
    let db = TestDb::new().await;
    let (events, states, store) = db.transactional().await;

    let stream_id = StreamId::new("Order", Uuid::new_v4());
    events
        .append(
            stream_id.clone(),
            vec![manual_event(
                stream_id.aggregate_id(),
                "Order",
                "Order.Placed",
            )],
            AggregateVersion::initial(),
            vec![],
            false,
        )
        .await
        .expect("seed stream");

    let state_id = Uuid::new_v4();
    let mut commit = AtomicCommit::new();
    commit.push_expectation(Expectation::Stream {
        stream_id: stream_id.clone(),
        version: AggregateVersion::initial(),
    });
    commit.push_write(PreparedWrite::State(manual_state_commit(
        "Invoice",
        state_id,
        1,
        0,
        vec![],
    )));

    let err = store.commit_atomic(commit).await.unwrap_err();
    assert!(err.is_concurrency_conflict());
    assert!(!states
        .exists(StreamId::new("Invoice", state_id))
        .await
        .unwrap());
    assert_eq!(
        events.get_version(stream_id).await.unwrap().as_i64(),
        1,
        "the stream must be unaffected by the failed commit"
    );
}

#[tokio::test]
async fn test_stale_state_expectation_rolls_back_stream_write() {
    let db = TestDb::new().await;
    let (events, states, store) = db.transactional().await;

    let state_id = Uuid::new_v4();
    states
        .save(manual_state_commit("Invoice", state_id, 1, 0, vec![]))
        .await
        .expect("seed state");

    let stream_id = StreamId::new("Order", Uuid::new_v4());
    let mut commit = AtomicCommit::new();
    commit.push_expectation(Expectation::State {
        stream_id: StreamId::new("Invoice", state_id),
        version: AggregateVersion::initial(),
    });
    commit.stream_writes.push(manual_stream_commit(
        stream_id.clone(),
        AggregateVersion::initial(),
        vec![manual_event(
            stream_id.aggregate_id(),
            "Order",
            "Order.Placed",
        )],
    ));

    let err = store.commit_atomic(commit).await.unwrap_err();
    assert!(err.is_concurrency_conflict());
    assert_eq!(events.get_version(stream_id).await.unwrap().as_i64(), 0);
    assert_eq!(
        states
            .get_version(StreamId::new("Invoice", state_id))
            .await
            .unwrap()
            .as_i64(),
        1,
        "the state row must be unaffected by the failed commit"
    );
}

#[tokio::test]
async fn test_stale_stream_write_rolls_back_state_write() {
    let db = TestDb::new().await;
    let (events, states, store) = db.transactional().await;

    let stream_id = StreamId::new("Order", Uuid::new_v4());
    events
        .append(
            stream_id.clone(),
            vec![manual_event(
                stream_id.aggregate_id(),
                "Order",
                "Order.Placed",
            )],
            AggregateVersion::initial(),
            vec![],
            false,
        )
        .await
        .expect("seed stream");

    let state_id = Uuid::new_v4();
    let mut commit = AtomicCommit::new();
    commit.stream_writes.push(manual_stream_commit(
        stream_id.clone(),
        AggregateVersion::initial(),
        vec![manual_event(
            stream_id.aggregate_id(),
            "Order",
            "Order.Stale",
        )],
    ));
    commit.push_write(PreparedWrite::State(manual_state_commit(
        "Invoice",
        state_id,
        1,
        0,
        vec![],
    )));

    let err = store.commit_atomic(commit).await.unwrap_err();
    assert!(err.is_concurrency_conflict());
    assert!(!states
        .exists(StreamId::new("Invoice", state_id))
        .await
        .unwrap());
}

#[tokio::test]
async fn test_stale_state_write_rolls_back_stream_write() {
    let db = TestDb::new().await;
    let (events, states, store) = db.transactional().await;

    let state_id = Uuid::new_v4();
    states
        .save(manual_state_commit("Invoice", state_id, 1, 0, vec![]))
        .await
        .expect("seed state");

    let stream_id = StreamId::new("Order", Uuid::new_v4());
    let mut commit = AtomicCommit::new();
    commit.push_write(PreparedWrite::State(manual_state_commit(
        "Invoice",
        state_id,
        2,
        0,
        vec![],
    )));
    commit.stream_writes.push(manual_stream_commit(
        stream_id.clone(),
        AggregateVersion::initial(),
        vec![manual_event(
            stream_id.aggregate_id(),
            "Order",
            "Order.Placed",
        )],
    ));

    let err = store.commit_atomic(commit).await.unwrap_err();
    assert!(err.is_concurrency_conflict());
    assert_eq!(events.get_version(stream_id).await.unwrap().as_i64(), 0);
    assert_eq!(
        states
            .get_version(StreamId::new("Invoice", state_id))
            .await
            .unwrap()
            .as_i64(),
        1,
        "the stale write must leave the state row unchanged"
    );
}

struct BoomProjection;

#[async_trait]
impl StateProjection<PgConnection> for BoomProjection {
    fn name(&self) -> &'static str {
        "boom"
    }

    fn filter(&self) -> EventFilter {
        EventFilter::by_event_type("Invoice.Boom")
    }

    async fn project(&self, _ctx: &mut PgConnection, _events: &[EventEnvelope]) -> Result<()> {
        Err(Error::custom("projection boom"))
    }
}

#[tokio::test]
async fn test_failing_projection_rolls_back_stream_write_too() {
    let db = TestDb::new().await;
    let (events, states) = db
        .stores_with(
            PostgresStateStore::builder()
                .pool(db.pool.clone())
                .schema("event_sauce")
                .with_projection(Arc::new(BoomProjection)),
        )
        .await;
    let store = PostgresTransactionalStore::new(Arc::clone(&events), Arc::clone(&states))
        .expect("same database");

    let stream_id = StreamId::new("Order", Uuid::new_v4());
    let state_id = Uuid::new_v4();

    let mut commit = AtomicCommit::new();
    commit.stream_writes.push(manual_stream_commit(
        stream_id.clone(),
        AggregateVersion::initial(),
        vec![manual_event(
            stream_id.aggregate_id(),
            "Order",
            "Order.Placed",
        )],
    ));
    commit.push_write(PreparedWrite::State(manual_state_commit(
        "Invoice",
        state_id,
        1,
        0,
        vec![manual_event(state_id, "Invoice", "Invoice.Boom")],
    )));

    let err = store.commit_atomic(commit).await.unwrap_err();
    assert_eq!(err.to_string(), "projection boom");
    assert_eq!(
        events.get_version(stream_id).await.unwrap().as_i64(),
        0,
        "the paired stream write must roll back when the projection fails"
    );
    assert!(!states
        .exists(StreamId::new("Invoice", state_id))
        .await
        .unwrap());
}

#[tokio::test]
async fn test_concurrent_commits_on_same_stream_only_one_wins() {
    let db = TestDb::new().await;
    let (events, _states, store) = db.transactional().await;
    let stream_id = StreamId::new("Order", Uuid::new_v4());

    let mut commit_a = AtomicCommit::new();
    commit_a.stream_writes.push(manual_stream_commit(
        stream_id.clone(),
        AggregateVersion::initial(),
        vec![manual_event(stream_id.aggregate_id(), "Order", "Order.A")],
    ));
    let mut commit_b = AtomicCommit::new();
    commit_b.stream_writes.push(manual_stream_commit(
        stream_id.clone(),
        AggregateVersion::initial(),
        vec![manual_event(stream_id.aggregate_id(), "Order", "Order.B")],
    ));

    let store_a = store.clone();
    let store_b = store.clone();
    let task_a = tokio::spawn(async move { store_a.commit_atomic(commit_a).await });
    let task_b = tokio::spawn(async move { store_b.commit_atomic(commit_b).await });
    let result_a = task_a.await.unwrap();
    let result_b = task_b.await.unwrap();

    let ok_count = usize::from(result_a.is_ok()) + usize::from(result_b.is_ok());
    assert_eq!(ok_count, 1, "exactly one commit must win the race");

    let loser = if result_a.is_err() {
        result_a
    } else {
        result_b
    };
    assert!(
        loser.unwrap_err().is_concurrency_conflict(),
        "the loser must surface a typed concurrency conflict, not a raw backend error"
    );
    assert_eq!(events.get_version(stream_id).await.unwrap().as_i64(), 1);
}

#[tokio::test]
async fn test_state_expectation_sees_a_concurrently_committed_update() {
    let db = TestDb::new().await;
    let (_events, states, store) = db.transactional().await;

    let state_id = Uuid::new_v4();
    states
        .save(manual_state_commit("Invoice", state_id, 1, 0, vec![]))
        .await
        .expect("seed state");

    let mut racer = db.pool.begin().await.expect("begin racer tx");
    sqlx::query(
        "UPDATE event_sauce.aggregate_states SET version = 2
         WHERE aggregate_id = $1 AND aggregate_type = $2",
    )
    .bind(state_id)
    .bind("Invoice")
    .execute(&mut *racer)
    .await
    .expect("racer update");

    let mut commit = AtomicCommit::new();
    commit.push_expectation(Expectation::State {
        stream_id: StreamId::new("Invoice", state_id),
        version: AggregateVersion::new(1),
    });
    let checker_store = store.clone();
    let checker = tokio::spawn(async move { checker_store.commit_atomic(commit).await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    racer.commit().await.expect("racer commit");

    let err = checker
        .await
        .unwrap()
        .expect_err("the expectation must see the concurrently committed update");
    assert!(err.is_concurrency_conflict());
}
