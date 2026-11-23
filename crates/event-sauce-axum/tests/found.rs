//! `Found` through a real axum router and the in-memory backend: the id of
//! the path, the statuses of what can go wrong, and the parameter name a
//! route chooses.

mod broken_store;

use axum::body::Body;
use axum::extract::FromRef;
use axum::http::{Method, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use broken_store::BrokenStore;
use event_sauce_axum::{Found, Store};
use event_sauce_core::{EntityId, EventStore, Repository};
use event_sauce_memory::InMemoryEventStore;
use tower::ServiceExt;

macro_rules! tickets {
    ($module:ident, $param:literal, $deleted:expr) => {
        mod $module {
            use event_sauce_axum::{DeletedStatus, PathAggregate};
            use event_sauce_core::{
                aggregate_command, define_events, Aggregate, AggregateError, Entity, EntityId,
            };
            use serde::{Deserialize, Serialize};

            #[derive(Debug, thiserror::Error)]
            #[error("ticket error")]
            pub struct TicketError;

            impl AggregateError for TicketError {}

            #[derive(Debug, Serialize, Deserialize)]
            pub struct Ticket {
                id: EntityId,
                pub title: String,
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

            impl PathAggregate for Ticket {
                const PARAM: &'static str = $param;
                const DELETED: DeletedStatus = $deleted;
            }

            define_events! {
                pub enum TicketEvent for Ticket {
                    Opened {
                        title: String,
                    }
                    @init
                    => |id, event| Ticket { id, title: event.title.clone() },

                    Touched {}
                    => |_ticket, _event| {},

                    Closed {}
                    @delete
                    => |ticket, _event| ticket,
                }
            }

            aggregate_command! {
                impl Ticket {
                    @clock @init fn open(title: String) -> OpenedEvent { title };
                    @clock @delete fn close() -> ClosedEvent { };
                }
            }
        }
    };
}

tickets!(hidden, "id", DeletedStatus::NotFound);
tickets!(gone, "id", DeletedStatus::Gone);
tickets!(nested, "ticket_id", DeletedStatus::NotFound);

#[derive(Clone, FromRef)]
struct AppState {
    store: Store<InMemoryEventStore>,
}

#[derive(Clone, FromRef)]
struct BrokenState {
    store: Store<BrokenStore>,
}

async fn hidden_title(ticket: Found<hidden::Ticket, InMemoryEventStore>) -> String {
    ticket.title.clone()
}

async fn gone_title(ticket: Found<gone::Ticket, InMemoryEventStore>) -> String {
    ticket.title.clone()
}

async fn nested_title(ticket: Found<nested::Ticket, InMemoryEventStore>) -> String {
    ticket.title.clone()
}

async fn broken_title(_ticket: Found<hidden::Ticket, BrokenStore>) -> String {
    String::new()
}

fn state() -> AppState {
    AppState {
        store: Store::new(InMemoryEventStore::new()),
    }
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/hidden/{id}", get(hidden_title))
        .route("/gone/{id}", get(gone_title))
        .route("/groups/{group_id}/tickets/{ticket_id}", get(nested_title))
        .route("/orphan", get(hidden_title))
        .with_state(state)
}

macro_rules! seed {
    ($state:expr, $module:ident, $close:expr) => {{
        use $module::TicketDeleteCommands as _;
        let tickets = $state.store.arc().repository::<$module::Ticket>();
        let mut ticket = $module::Ticket::open("printer-jam".to_owned()).unwrap();
        tickets.save(&mut ticket).await.unwrap();
        let id = ticket.entity_id();
        if $close {
            let mut closed = ticket.close().unwrap();
            tickets.save_deleted(&mut closed).await.unwrap();
        }
        id
    }};
}

async fn get_body(router: &Router, uri: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method(Method::GET)
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
async fn the_aggregate_of_the_path_is_loaded() {
    let state = state();
    let id = seed!(state, hidden, false);

    let (status, body) = get_body(&router(state), &format!("/hidden/{id}")).await;

    assert_eq!((status, body.as_str()), (StatusCode::OK, "printer-jam"));
}

#[tokio::test]
async fn an_id_that_is_not_a_uuid_is_a_bad_request() {
    let (status, _) = get_body(&router(state()), "/hidden/not-a-uuid").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_missing_aggregate_is_not_found() {
    let (status, body) = get_body(&router(state()), &format!("/hidden/{}", EntityId::new())).await;

    assert_eq!(
        (status, body.as_str()),
        (StatusCode::NOT_FOUND, "not found")
    );
}

#[tokio::test]
async fn a_deleted_aggregate_is_not_found_by_default() {
    let state = state();
    let id = seed!(state, hidden, true);

    let (status, _) = get_body(&router(state), &format!("/hidden/{id}")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_deleted_aggregate_is_gone_when_its_type_says_so() {
    let state = state();
    let id = seed!(state, gone, true);

    let (status, body) = get_body(&router(state), &format!("/gone/{id}")).await;

    assert_eq!((status, body.as_str()), (StatusCode::GONE, "gone"));
}

#[tokio::test]
async fn a_nested_route_reads_the_parameter_its_aggregate_names() {
    let state = state();
    let id = seed!(state, nested, false);
    let group = EntityId::new();

    let (status, body) = get_body(&router(state), &format!("/groups/{group}/tickets/{id}")).await;

    assert_eq!((status, body.as_str()), (StatusCode::OK, "printer-jam"));
}

#[tokio::test]
async fn a_route_without_the_parameter_is_a_server_error() {
    let (status, body) = get_body(&router(state()), "/orphan").await;

    assert_eq!(
        (status, body.as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
    );
}

#[tokio::test]
async fn a_failing_store_is_a_server_error_not_an_unknown_aggregate() {
    let app = Router::new()
        .route("/hidden/{id}", get(broken_title))
        .with_state(BrokenState {
            store: Store::new(BrokenStore),
        });

    let (status, _) = get_body(&app, &format!("/hidden/{}", EntityId::new())).await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}
