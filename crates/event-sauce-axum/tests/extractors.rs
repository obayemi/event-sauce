//! `Store`, `Repo` and `UnitOfWork` through a real axum router, with the
//! in-memory backend and a `FromRef`-derived application state.

use std::sync::Arc;

mod broken_store;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{FromRef, Path};
use axum::http::{Method, Request, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use broken_store::BrokenStore;
use event_sauce_axum::{Repo, Store, StoreError, UnitOfWork};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateError, Entity, EntityId, EventStore,
    Repository, RetryPolicy, UnitOfWork as Work, UnitOfWorkCommand, UnitOfWorkError,
    UnitOfWorkStore,
};
use event_sauce_memory::{InMemoryEventStore, InMemoryStateStore, InMemoryTransactionalStore};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

#[derive(Debug, thiserror::Error)]
#[error("ticket error")]
struct TicketError;
impl AggregateError for TicketError {}

#[derive(Debug, Serialize, Deserialize)]
struct Ticket {
    id: EntityId,
    title: String,
}

impl Entity for Ticket {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Ticket {
    type Event = TicketEvent;
    type Error = TicketError;
    type DeletedState = Self;
}

define_events! {
    enum TicketEvent for Ticket {
        Opened {
            title: String,
        }
        @init
        => |id, event| Ticket { id, title: event.title.clone() },

        Renamed {
            title: String,
        }
        => |ticket, event| {
            ticket.title = event.title.clone();
        },
    }
}

aggregate_command! {
    impl Ticket {
        @clock @init fn open(title: String) -> OpenedEvent { title };
    }
}

struct OpenPair {
    first: EntityId,
    second: EntityId,
}

#[async_trait]
impl UnitOfWorkCommand<InMemoryTransactionalStore> for OpenPair {
    async fn handle(
        &self,
        uow: &Work<'_, InMemoryTransactionalStore>,
        _ctx: &(),
    ) -> Result<(), UnitOfWorkError> {
        uow.create::<Ticket>(self.first, |root| root.open("first".into()))?;
        uow.create::<Ticket>(self.second, |root| root.open("second".into()))?;
        Ok(())
    }
}

#[derive(Clone, FromRef)]
struct AppState {
    store: Store<InMemoryEventStore>,
    uow: Arc<UnitOfWorkStore<InMemoryTransactionalStore>>,
}

impl AppState {
    fn new() -> Self {
        let events = Arc::new(InMemoryEventStore::new());
        let stores = InMemoryTransactionalStore::new(
            Arc::clone(&events),
            Arc::new(InMemoryStateStore::new()),
        );
        Self {
            store: Store::from(events),
            uow: Arc::new(UnitOfWorkStore::new(stores).event_sourced::<Ticket>()),
        }
    }
}

async fn open_ticket(
    Repo(tickets): Repo<Ticket, InMemoryEventStore>,
    Path(title): Path<String>,
) -> String {
    let mut ticket = tickets.create_uninit().open(title).unwrap();
    tickets.save(&mut ticket).await.unwrap();
    ticket.entity_id().to_string()
}

async fn ticket_title<S: EventStore + 'static>(
    Repo(tickets): Repo<Ticket, S>,
    Path(id): Path<EntityId>,
) -> Result<String, StoreError> {
    Ok(tickets.load(id).await?.title.clone())
}

async fn ticket_exists(store: Store<InMemoryEventStore>, Path(id): Path<EntityId>) -> String {
    store
        .arc()
        .repository::<Ticket>()
        .exists(id)
        .await
        .unwrap()
        .to_string()
}

async fn open_pair(UnitOfWork(uow): UnitOfWork<InMemoryTransactionalStore>) -> String {
    let (first, second) = (EntityId::new(), EntityId::new());
    uow.execute(&OpenPair { first, second }, &(), RetryPolicy::none())
        .await
        .unwrap();
    format!("{first} {second}")
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/tickets/{title}", post(open_ticket))
        .route(
            "/tickets/{id}/title",
            get(ticket_title::<InMemoryEventStore>),
        )
        .route("/tickets/{id}/exists", get(ticket_exists))
        .route("/pairs", post(open_pair))
        .with_state(state)
}

async fn call(router: &Router, method: Method, uri: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn a_repo_extractor_saves_in_one_request_what_another_loads() {
    let app = router(AppState::new());
    let (status, id) = call(&app, Method::POST, "/tickets/printer-jam").await;
    assert_eq!(status, StatusCode::OK);

    let (status, title) = call(&app, Method::GET, &format!("/tickets/{id}/title")).await;

    assert_eq!((status, title.as_str()), (StatusCode::OK, "printer-jam"));
}

#[tokio::test]
async fn a_missing_aggregate_answers_not_found_without_naming_it() {
    let app = router(AppState::new());
    let missing = EntityId::new();

    let (status, body) = call(&app, Method::GET, &format!("/tickets/{missing}/title")).await;

    assert_eq!(
        (status, body.as_str()),
        (StatusCode::NOT_FOUND, "not found")
    );
}

#[tokio::test]
async fn a_store_extractor_builds_repositories_for_any_aggregate_type() {
    let app = router(AppState::new());
    let (_, id) = call(&app, Method::POST, "/tickets/vpn-down").await;

    let (_, known) = call(&app, Method::GET, &format!("/tickets/{id}/exists")).await;
    let (_, unknown) = call(
        &app,
        Method::GET,
        &format!("/tickets/{}/exists", EntityId::new()),
    )
    .await;

    assert_eq!((known.as_str(), unknown.as_str()), ("true", "false"));
}

#[tokio::test]
async fn a_unit_of_work_extractor_commits_several_aggregates_atomically() {
    let app = router(AppState::new());

    let (status, ids) = call(&app, Method::POST, "/pairs").await;
    assert_eq!(status, StatusCode::OK);
    let (first, second) = ids.split_once(' ').unwrap();

    let (_, first_title) = call(&app, Method::GET, &format!("/tickets/{first}/title")).await;
    let (_, second_title) = call(&app, Method::GET, &format!("/tickets/{second}/title")).await;
    assert_eq!(
        (first_title.as_str(), second_title.as_str()),
        ("first", "second")
    );
}

#[derive(Clone, FromRef)]
struct BrokenState {
    store: Store<BrokenStore>,
}

#[tokio::test]
async fn a_failing_store_is_a_server_error_not_an_unknown_aggregate() {
    let app = Router::new()
        .route("/tickets/{id}/title", get(ticket_title::<BrokenStore>))
        .with_state(BrokenState {
            store: Store::new(BrokenStore),
        });

    let (status, body) = call(
        &app,
        Method::GET,
        &format!("/tickets/{}/title", EntityId::new()),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body, "internal error");
}
