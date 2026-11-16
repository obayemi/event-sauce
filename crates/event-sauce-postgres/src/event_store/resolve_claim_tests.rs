use super::tests::{create_test_envelope, TestDatabase};
use super::PostgresEventStore;
use event_sauce_core::{AggregateClaim, AggregateVersion, EntityId, EventStore, StreamId};
use serde_json::json;
use uuid::Uuid;

fn store_of(db: &TestDatabase) -> PostgresEventStore {
    PostgresEventStore::builder()
        .pool(db.pool().clone())
        .schema("public")
        .build()
        .expect("pool was set")
}

fn email(value: &str) -> AggregateClaim {
    AggregateClaim::new("User.email", json!(value))
}

async fn append_claiming(
    store: &PostgresEventStore,
    aggregate_id: Uuid,
    version: AggregateVersion,
    claims: Vec<AggregateClaim>,
    clear_claims: bool,
) {
    store
        .append(
            StreamId::new("User", aggregate_id),
            vec![create_test_envelope("UserChanged", aggregate_id)],
            version,
            claims,
            clear_claims,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_resolve_claim_returns_the_holder() {
    let db = TestDatabase::new().await.unwrap();
    let store = store_of(&db);
    let id = Uuid::new_v4();
    append_claiming(
        &store,
        id,
        AggregateVersion::initial(),
        vec![email("a@x")],
        false,
    )
    .await;

    let holder = store.resolve_claim(&email("a@x")).await.unwrap();

    assert_eq!(holder, Some(EntityId::from(id)));
}

#[tokio::test]
async fn test_resolve_claim_is_none_for_an_unheld_value_or_type() {
    let db = TestDatabase::new().await.unwrap();
    let store = store_of(&db);
    append_claiming(
        &store,
        Uuid::new_v4(),
        AggregateVersion::initial(),
        vec![email("a@x")],
        false,
    )
    .await;

    let other_value = store.resolve_claim(&email("b@x")).await.unwrap();
    let other_type = store
        .resolve_claim(&AggregateClaim::new("User.name", json!("a@x")))
        .await
        .unwrap();

    assert_eq!(other_value, None);
    assert_eq!(other_type, None);
}

#[tokio::test]
async fn test_resolve_claim_follows_a_changed_claim() {
    let db = TestDatabase::new().await.unwrap();
    let store = store_of(&db);
    let id = Uuid::new_v4();
    append_claiming(
        &store,
        id,
        AggregateVersion::initial(),
        vec![email("old@x")],
        false,
    )
    .await;
    append_claiming(
        &store,
        id,
        AggregateVersion::new(1),
        vec![email("new@x")],
        false,
    )
    .await;

    let old = store.resolve_claim(&email("old@x")).await.unwrap();
    let new = store.resolve_claim(&email("new@x")).await.unwrap();

    assert_eq!(old, None);
    assert_eq!(new, Some(EntityId::from(id)));
}

#[tokio::test]
async fn test_resolve_claim_is_none_after_claims_are_cleared() {
    let db = TestDatabase::new().await.unwrap();
    let store = store_of(&db);
    let id = Uuid::new_v4();
    append_claiming(
        &store,
        id,
        AggregateVersion::initial(),
        vec![email("gone@x")],
        false,
    )
    .await;
    append_claiming(&store, id, AggregateVersion::new(1), vec![], true).await;

    let holder = store.resolve_claim(&email("gone@x")).await.unwrap();

    assert_eq!(holder, None);
}
