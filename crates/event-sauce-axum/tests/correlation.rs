//! `CorrelationLayer` through a real axum router: events committed while a
//! request is served carry that request's correlation id.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::routing::get;
use axum::Router;
use event_sauce_axum::CorrelationLayer;
use event_sauce_core::{
    define_events, Aggregate, AggregateError, AggregateRoot, AggregateVersion, Entity, EntityId,
    EventContext, EventStore, Repository, StreamId, WithEventContextExt,
};
use event_sauce_memory::InMemoryEventStore;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    id: EntityId,
}

impl Entity for Account {
    fn new(id: EntityId) -> Self {
        Self { id }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Account {}

#[derive(Debug, thiserror::Error)]
#[error("account error")]
struct AccountError;
impl AggregateError for AccountError {}

impl Aggregate for Account {
    type Event = AccountEvent;
    type Error = AccountError;
    type DeletedState = Self;
}

define_events! {
    enum AccountEvent for Account {
        Opened {}
        => |_account, _event| {},
    }
}

type Store = Arc<InMemoryEventStore>;

fn open_account() -> AggregateRoot<Account> {
    let mut account = AggregateRoot::<Account>::new(EntityId::new());
    account
        .apply(AccountEvent::Opened {
            timestamp: chrono::Utc::now(),
        })
        .unwrap();
    account
}

async fn open_and_save(store: &Store) -> EntityId {
    let mut account = open_account();
    store
        .repository::<Account>()
        .save(&mut account)
        .await
        .unwrap();
    account.entity_id()
}

async fn open_after_a_yield(State(store): State<Store>) -> String {
    tokio::task::yield_now().await;
    open_and_save(&store).await.to_string()
}

async fn open_in_a_spawned_task(State(store): State<Store>) -> String {
    let task = open_and_save_owned(store).with_event_context(EventContext::current());
    tokio::spawn(task).await.unwrap().to_string()
}

async fn open_and_save_owned(store: Store) -> EntityId {
    open_and_save(&store).await
}

fn router(store: &Store) -> Router {
    Router::new()
        .route("/open", get(open_after_a_yield))
        .route("/spawn", get(open_in_a_spawned_task))
        .layer(CorrelationLayer::new())
        .with_state(store.clone())
}

async fn correlation_of(store: &Store, id: &str) -> Option<String> {
    let stream = store
        .load_stream(
            StreamId::new("Account", id.parse().unwrap()),
            AggregateVersion::initial(),
        )
        .await
        .unwrap();
    let envelopes: Vec<_> = stream.collect().await;
    assert_eq!(envelopes.len(), 1);
    let envelope = envelopes.into_iter().next().unwrap().unwrap();
    envelope
        .metadata
        .and_then(|metadata| metadata.correlation_id)
}

async fn get_body(router: Router, uri: &str, request_id: Option<&str>) -> (String, String) {
    let mut request = Request::builder().uri(uri);
    if let Some(id) = request_id {
        request = request.header("x-request-id", id);
    }
    let response = router
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let echoed = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (String::from_utf8(body.to_vec()).unwrap(), echoed)
}

#[tokio::test]
async fn events_committed_by_a_request_carry_its_incoming_id() {
    let store = Arc::new(InMemoryEventStore::new());
    let (account, _) = get_body(router(&store), "/open", Some("req-123")).await;
    assert_eq!(
        correlation_of(&store, &account).await.as_deref(),
        Some("req-123")
    );
}

#[tokio::test]
async fn a_request_without_an_id_correlates_its_events_with_the_minted_one() {
    let store = Arc::new(InMemoryEventStore::new());
    let (account, echoed) = get_body(router(&store), "/open", None).await;
    assert_eq!(correlation_of(&store, &account).await, Some(echoed));
}

#[tokio::test]
async fn concurrent_requests_never_share_a_correlation_id() {
    let store = Arc::new(InMemoryEventStore::new());
    let app = router(&store);
    let ids: Vec<String> = (0..16).map(|n| format!("req-{n}")).collect();
    let results = futures::future::join_all(
        ids.iter()
            .map(|id| get_body(app.clone(), "/open", Some(id))),
    )
    .await;
    for (id, (account, _)) in ids.iter().zip(results) {
        assert_eq!(
            correlation_of(&store, &account).await.as_deref(),
            Some(id.as_str())
        );
    }
}

#[tokio::test]
async fn a_spawned_task_keeps_the_correlation_when_handed_the_current_context() {
    let store = Arc::new(InMemoryEventStore::new());
    let (account, _) = get_body(router(&store), "/spawn", Some("req-spawn")).await;
    assert_eq!(
        correlation_of(&store, &account).await.as_deref(),
        Some("req-spawn")
    );
}
