use axum::http::{header::LOCATION, Method, StatusCode};
use event_sauce::prelude::*;
use serde_json::json;

use super::harness::{first_event_of, Harness};
use super::{EMAIL, PASSWORD};

#[tokio::test]
async fn a_visitor_is_refused_everywhere_but_the_front_door() {
    let mut visitor = Harness::start().await.client;
    let id = EntityId::new();
    let note = Some(json!({ "title": "t", "body": "b" }));
    let one = format!("/notes/{id}");

    let refused = [
        (Method::GET, "/me".to_owned(), None),
        (
            Method::PUT,
            "/password".to_owned(),
            Some(json!({ "password": PASSWORD })),
        ),
        (Method::GET, "/notes".to_owned(), None),
        (Method::POST, "/notes".to_owned(), note.clone()),
        (Method::GET, one.clone(), None),
        (Method::PUT, one.clone(), note),
        (Method::PATCH, one.clone(), Some(json!({ "title": "t" }))),
        (Method::DELETE, one, None),
    ];

    for (method, uri, body) in refused {
        let reply = visitor.call(method.clone(), &uri, body).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{method} {uri}");
    }
}

#[tokio::test]
async fn a_member_registers_logs_in_and_sees_themselves() {
    let mut harness = Harness::signed_up().await;

    let me = harness.client.get("/me").await;

    assert_eq!(me.status, StatusCode::OK);
    assert_eq!(me.json()["nickname"], "ada");
    assert_eq!(me.json()["email"], EMAIL);
}

#[tokio::test]
async fn a_wrong_password_opens_no_session() {
    let mut harness = Harness::start().await;
    harness.client.register(EMAIL, PASSWORD).await;

    let login = harness.client.log_in(EMAIL, "wrong-horse").await;

    assert_eq!(login.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        harness.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn logging_out_ends_the_session() {
    let mut harness = Harness::signed_up().await;

    let logout = harness.client.call(Method::POST, "/logout", None).await;

    assert_eq!(logout.status, StatusCode::NO_CONTENT);
    assert_eq!(
        harness.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn an_email_registers_only_once() {
    let mut harness = Harness::start().await;
    harness.client.register(EMAIL, PASSWORD).await;

    let again = harness.client.register(EMAIL, PASSWORD).await;

    assert_eq!(again.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn an_invalid_registration_is_unprocessable() {
    let mut harness = Harness::start().await;

    let bad_email = harness.client.register("not-an-email", PASSWORD).await;
    let weak_password = harness.client.register(EMAIL, "short").await;
    let no_nickname = harness
        .client
        .post(
            "/register",
            json!({ "nickname": " ", "email": EMAIL, "password": PASSWORD }),
        )
        .await;

    assert_eq!(bad_email.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(weak_password.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(no_nickname.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn changing_the_password_ends_every_session() {
    let mut harness = Harness::signed_up().await;
    let mut other = harness.client.another_browser();
    other.log_in(EMAIL, PASSWORD).await;
    assert_eq!(other.get("/me").await.status, StatusCode::OK);

    let change = harness
        .client
        .put("/password", json!({ "password": "battery-staple" }))
        .await;

    assert_eq!(change.status, StatusCode::NO_CONTENT);
    assert_eq!(other.get("/me").await.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        harness.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        harness.client.log_in(EMAIL, PASSWORD).await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        harness.client.log_in(EMAIL, "battery-staple").await.status,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn a_weak_new_password_is_refused() {
    let mut harness = Harness::signed_up().await;

    let change = harness
        .client
        .put("/password", json!({ "password": "short" }))
        .await;

    assert_eq!(change.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(harness.client.get("/me").await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_note_goes_through_create_read_update_and_delete() {
    let mut harness = Harness::signed_up().await;

    let created = harness.client.write_note("Groceries", "milk").await;
    assert_eq!(created.status, StatusCode::CREATED);
    let id = created.id();
    let uri = format!("/notes/{id}");
    assert_eq!(created.header(LOCATION.as_str()), Some(uri.as_str()));
    assert_eq!(created.json()["title"], "Groceries");

    let read = harness.client.get(&uri).await;
    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(read.json()["body"], "milk");
    assert_eq!(harness.titles().await, ["Groceries"]);

    let replaced = harness
        .client
        .put(&uri, json!({ "title": "Shopping", "body": "milk, eggs" }))
        .await;
    assert_eq!(replaced.status, StatusCode::OK);
    assert_eq!(replaced.json()["title"], "Shopping");

    let patched = harness
        .client
        .patch(&uri, json!({ "body": "milk, eggs, flour" }))
        .await;
    assert_eq!(patched.status, StatusCode::OK);
    assert_eq!(patched.json()["title"], "Shopping");
    assert_eq!(patched.json()["body"], "milk, eggs, flour");
    assert_eq!(harness.titles().await, ["Shopping"]);
    let listed = harness.client.get("/notes").await.json();
    assert_eq!(listed[0]["body"], "milk, eggs, flour");

    let deleted = harness.client.delete(&uri).await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    assert_eq!(harness.client.get(&uri).await.status, StatusCode::NOT_FOUND);
    assert!(harness.titles().await.is_empty());
}

#[tokio::test]
async fn a_deleted_note_cannot_be_deleted_or_edited_again() {
    let mut harness = Harness::signed_up().await;
    let id = harness.client.write_note("Groceries", "milk").await.id();
    let uri = format!("/notes/{id}");
    harness.client.delete(&uri).await;

    let again = harness.client.delete(&uri).await;
    let edit = harness.client.patch(&uri, json!({ "title": "Back" })).await;

    assert_eq!(again.status, StatusCode::NOT_FOUND);
    assert_eq!(edit.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_unknown_note_is_not_found() {
    let mut harness = Harness::signed_up().await;

    let reply = harness
        .client
        .get(&format!("/notes/{}", EntityId::new()))
        .await;

    assert_eq!(reply.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_invalid_note_is_unprocessable() {
    let mut harness = Harness::signed_up().await;
    let id = harness.client.write_note("Groceries", "milk").await.id();
    let uri = format!("/notes/{id}");

    let blank = harness.client.write_note("  ", "milk").await;
    let too_long = harness.client.write_note(&"x".repeat(201), "milk").await;
    let empty_patch = harness.client.patch(&uri, json!({})).await;
    let incomplete_put = harness
        .client
        .put(&uri, json!({ "title": "Only a title" }))
        .await;

    assert_eq!(blank.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(too_long.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(empty_patch.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(incomplete_put.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn the_list_only_shows_the_notes_of_the_caller() {
    let mut harness = Harness::signed_up().await;
    let mut grace = harness.another_member("grace@example.com").await;
    harness.client.write_note("Ada's", "").await;
    grace.write_note("Grace's", "").await;
    harness.project().await;

    assert_eq!(harness.client.titles().await, ["Ada's"]);
    assert_eq!(grace.titles().await, ["Grace's"]);
}

#[tokio::test]
async fn another_member_cannot_reach_a_note() {
    let mut harness = Harness::signed_up().await;
    let mut grace = harness.another_member("grace@example.com").await;
    let id = harness.client.write_note("Private", "secret").await.id();
    let uri = format!("/notes/{id}");
    let attempt = json!({ "title": "Mine", "body": "stolen" });

    let replies = [
        grace.get(&uri).await,
        grace.put(&uri, attempt).await,
        grace.patch(&uri, json!({ "title": "Mine" })).await,
        grace.delete(&uri).await,
    ];

    for reply in replies {
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
    }
    let intact = harness.client.get(&uri).await;
    assert_eq!(intact.status, StatusCode::OK);
    assert_eq!(intact.json()["title"], "Private");
    assert_eq!(intact.json()["body"], "secret");
}

#[tokio::test]
async fn catching_up_twice_applies_each_event_once() {
    let mut harness = Harness::signed_up().await;
    harness.client.write_note("Groceries", "milk").await;

    harness.project().await;
    harness.project().await;

    assert_eq!(harness.client.titles().await, ["Groceries"]);
}

#[tokio::test]
async fn the_request_id_is_echoed_and_recorded_on_the_events() {
    let mut harness = Harness::signed_up().await;

    let created = harness
        .client
        .send(
            Method::POST,
            "/notes",
            Some(json!({ "title": "Groceries", "body": "milk" })),
            Some("req-42"),
        )
        .await;

    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(created.header("x-request-id"), Some("req-42"));
    let note_id: uuid::Uuid = created.id().parse().unwrap();
    let envelope = first_event_of(&harness.service.projector.event_store(), "Note", note_id).await;
    assert_eq!(
        envelope
            .metadata
            .and_then(|metadata| metadata.correlation_id),
        Some("req-42".to_owned())
    );
    assert_eq!(
        envelope.created_by.map(|id| id.to_string()),
        Some(harness.client.get("/me").await.id())
    );
}

#[tokio::test]
async fn every_response_carries_a_request_id() {
    let mut harness = Harness::start().await;

    let reply = harness.client.get("/me").await;

    assert!(reply
        .header("x-request-id")
        .is_some_and(|id| !id.is_empty()));
}
