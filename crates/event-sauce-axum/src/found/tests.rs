use std::cell::Cell;
use std::sync::Arc;

use axum::response::IntoResponse;
use event_sauce_core::{EntityId, Error};
use http::{Extensions, StatusCode};

use super::{load_once, path_id, DeletedStatus, FoundRejection, PathAggregate};
use crate::test_ticket::{ticket_of, Ticket};

struct OtherStore;
struct SomeStore;

fn params<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Iterator<Item = (&'a str, &'a str)> {
    pairs.iter().copied()
}

#[test]
fn a_path_aggregate_reads_the_id_parameter_and_hides_deletions_by_default() {
    assert_eq!(Ticket::PARAM, "id");
    assert_eq!(Ticket::DELETED, DeletedStatus::NotFound);
}

#[test]
fn the_id_is_read_from_the_named_parameter() {
    let id = EntityId::new();
    let text = id.to_string();

    let found = path_id(params(&[("group", "ignored"), ("id", &text)]), "id");

    assert_eq!(found.unwrap(), id);
}

#[test]
fn a_missing_parameter_is_a_wiring_mistake() {
    let rejection = path_id(params(&[("other", "x")]), "id").unwrap_err();

    assert!(matches!(rejection, FoundRejection::MissingParam("id")));
    assert_eq!(rejection.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn a_parameter_that_is_not_a_uuid_is_a_bad_request() {
    let rejection = path_id(params(&[("id", "not-a-uuid")]), "id").unwrap_err();

    assert!(matches!(rejection, FoundRejection::BadId));
    assert_eq!(rejection.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn a_store_rejection_answers_as_the_store_error_does() {
    let missing = FoundRejection::Store(Error::not_found("Ticket", "t-1").into());

    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(missing.into_response().status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_rejection_response_never_names_what_failed() {
    let response = FoundRejection::MissingParam("secret_param").into_response();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert_eq!(&bytes[..], b"internal error");
}

async fn counted_load(
    extensions: &mut Extensions,
    id: EntityId,
    loads: &Cell<u32>,
) -> Arc<event_sauce_core::AggregateRoot<Ticket>> {
    load_once::<Ticket, SomeStore, _, _>(extensions, id, || async {
        loads.set(loads.get() + 1);
        Ok(ticket_of(EntityId::new()))
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn the_same_id_loads_once_per_request() {
    let (mut extensions, loads, id) = (Extensions::new(), Cell::new(0), EntityId::new());

    let first = counted_load(&mut extensions, id, &loads).await;
    let second = counted_load(&mut extensions, id, &loads).await;

    assert_eq!(loads.get(), 1);
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn another_id_loads_again() {
    let (mut extensions, loads) = (Extensions::new(), Cell::new(0));

    counted_load(&mut extensions, EntityId::new(), &loads).await;
    counted_load(&mut extensions, EntityId::new(), &loads).await;

    assert_eq!(loads.get(), 2);
}

#[tokio::test]
async fn two_stores_of_one_aggregate_never_share_an_entry() {
    let (mut extensions, id) = (Extensions::new(), EntityId::new());
    let loads = Cell::new(0);
    let load_from = |loads: &Cell<u32>| {
        loads.set(loads.get() + 1);
        async { Ok(ticket_of(EntityId::new())) }
    };

    load_once::<Ticket, SomeStore, _, _>(&mut extensions, id, || load_from(&loads))
        .await
        .unwrap();
    load_once::<Ticket, OtherStore, _, _>(&mut extensions, id, || load_from(&loads))
        .await
        .unwrap();

    assert_eq!(loads.get(), 2);
}

#[tokio::test]
async fn a_failed_load_is_not_cached() {
    let (mut extensions, id) = (Extensions::new(), EntityId::new());

    let failed = load_once::<Ticket, SomeStore, _, _>(&mut extensions, id, || async {
        Err(Error::not_found("Ticket", "t-1"))
    })
    .await;
    let loads = Cell::new(0);
    counted_load(&mut extensions, id, &loads).await;

    assert!(failed.is_err());
    assert_eq!(loads.get(), 1);
}
