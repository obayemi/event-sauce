//! The domain rules of the example, then its whole HTTP surface driven
//! through the router against a real `PostgreSQL` container.

use std::collections::BTreeMap;
use std::time::Duration;

use argon2::Params;
use axum::{
    body::Body,
    http::{
        header::{COOKIE, LOCATION, SET_COOKIE},
        HeaderMap, Method, Request, StatusCode,
    },
    Router,
};
use event_sauce::auth::{Argon2Hasher, Email, LoginId, Password, PasswordHasher};
use event_sauce::postgres::PostgresEventStore;
use event_sauce::prelude::*;
use futures::StreamExt;
use secrecy::SecretString;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use tower::ServiceExt;

use crate::app::{self, Service};
use crate::member::{check_password, Member, MemberError};
use crate::note::{Note, NoteCommands, NoteDeleteCommands, NoteError};

const EMAIL: &str = "ada@example.com";
const PASSWORD: &str = "correct-horse";

fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

fn member(nickname: &str) -> AggregateRoot<Member> {
    let hash = cheap_hasher().hash(&SecretString::from(PASSWORD)).unwrap();
    let email = Email::parse(&format!("{nickname}@example.com")).unwrap();
    Member::join(nickname.to_owned(), Password::new(email, hash)).unwrap()
}

#[test]
fn a_member_needs_a_nickname() {
    let hash = cheap_hasher().hash(&SecretString::from(PASSWORD)).unwrap();
    let credentials = Password::new(Email::parse(EMAIL).unwrap(), hash);

    let result = Member::join("  ".to_owned(), credentials);

    assert!(matches!(result, Err(MemberError::EmptyNickname)));
}

#[test]
fn a_short_password_is_refused() {
    assert!(matches!(
        check_password("short"),
        Err(MemberError::WeakPassword)
    ));
}

#[test]
fn a_long_enough_password_is_accepted() {
    assert!(check_password(PASSWORD).is_ok());
}

#[test]
fn a_note_belongs_to_its_author() {
    let ada = member("ada");

    let note = Note::write(&ada, "Groceries".to_owned(), "milk".to_owned()).unwrap();

    assert!(note.ensure_owned_by(&ada).is_ok());
    assert!(matches!(
        note.ensure_owned_by(&member("grace")),
        Err(NoteError::NotOwner)
    ));
    assert_eq!(note.title, "Groceries");
}

#[test]
fn a_note_needs_a_title() {
    let ada = member("ada");

    let result = Note::write(&ada, " ".to_owned(), "milk".to_owned());

    assert!(matches!(result, Err(NoteError::EmptyTitle)));
}

#[test]
fn a_title_has_a_maximum_length() {
    let ada = member("ada");

    let result = Note::write(&ada, "x".repeat(201), String::new());

    assert!(matches!(result, Err(NoteError::TitleTooLong)));
}

#[test]
fn the_owner_edits_the_note() {
    let ada = member("ada");
    let mut note = Note::write(&ada, "Groceries".to_owned(), "milk".to_owned()).unwrap();

    note.edit(&ada, None, Some("milk, eggs".to_owned()))
        .unwrap();

    assert_eq!(note.title, "Groceries");
    assert_eq!(note.body, "milk, eggs");
}

#[test]
fn an_edit_must_change_something() {
    let ada = member("ada");
    let mut note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.edit(&ada, None, None);

    assert!(matches!(result, Err(NoteError::NothingToEdit)));
}

#[test]
fn another_member_cannot_edit_the_note() {
    let ada = member("ada");
    let grace = member("grace");
    let mut note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.edit(&grace, Some("Mine".to_owned()), None);

    assert!(matches!(result, Err(NoteError::NotOwner)));
    assert_eq!(note.title, "Groceries");
}

#[test]
fn another_member_cannot_delete_the_note() {
    let ada = member("ada");
    let grace = member("grace");
    let note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.delete(&grace);

    assert!(matches!(result, Err(NoteError::NotOwner)));
}

#[test]
fn the_owner_deletes_the_note() {
    let ada = member("ada");
    let note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    assert!(note.delete(&ada).is_ok());
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }

    fn id(&self) -> String {
        self.json()["id"].as_str().unwrap().to_owned()
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|value| value.to_str().unwrap())
    }
}

struct Client {
    router: Router,
    cookies: BTreeMap<String, String>,
}

impl Client {
    fn new(router: Router) -> Self {
        Self {
            router,
            cookies: BTreeMap::new(),
        }
    }

    fn another_browser(&self) -> Self {
        Self::new(self.router.clone())
    }

    async fn send(
        &mut self,
        method: Method,
        uri: &str,
        body: Option<Value>,
        request_id: Option<&str>,
    ) -> Reply {
        let mut builder = Request::builder().method(method).uri(uri);
        if !self.cookies.is_empty() {
            let cookies: Vec<_> = self
                .cookies
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect();
            builder = builder.header(COOKIE, cookies.join("; "));
        }
        if let Some(id) = request_id {
            builder = builder.header("x-request-id", id);
        }
        let request = match body {
            Some(body) => builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string())),
            None => builder.body(Body::empty()),
        }
        .unwrap();
        let response = self.router.clone().oneshot(request).await.unwrap();
        let (parts, body) = response.into_parts();
        for set_cookie in parts.headers.get_all(SET_COOKIE) {
            let (name, value) = set_cookie
                .to_str()
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .split_once('=')
                .unwrap();
            if value.is_empty() {
                self.cookies.remove(name);
            } else {
                self.cookies.insert(name.to_owned(), value.to_owned());
            }
        }
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        Reply {
            status: parts.status,
            headers: parts.headers,
            body: String::from_utf8(bytes.to_vec()).unwrap(),
        }
    }

    async fn call(&mut self, method: Method, uri: &str, body: Option<Value>) -> Reply {
        self.send(method, uri, body, None).await
    }

    async fn get(&mut self, uri: &str) -> Reply {
        self.call(Method::GET, uri, None).await
    }

    async fn register(&mut self, email: &str, password: &str) -> Reply {
        let form = json!({ "nickname": "ada", "email": email, "password": password });
        self.call(Method::POST, "/register", Some(form)).await
    }

    async fn log_in(&mut self, email: &str, password: &str) -> Reply {
        let form = json!({ "email": email, "password": password });
        self.call(Method::POST, "/login", Some(form)).await
    }

    async fn sign_up(&mut self, email: &str) {
        assert_eq!(
            self.register(email, PASSWORD).await.status,
            StatusCode::CREATED
        );
        assert_eq!(
            self.log_in(email, PASSWORD).await.status,
            StatusCode::NO_CONTENT
        );
    }

    async fn write_note(&mut self, title: &str, body: &str) -> Reply {
        let note = json!({ "title": title, "body": body });
        self.call(Method::POST, "/notes", Some(note)).await
    }

    async fn titles(&mut self) -> Vec<String> {
        let list = self.get("/notes").await;
        assert_eq!(list.status, StatusCode::OK);
        list.json()
            .as_array()
            .unwrap()
            .iter()
            .map(|note| note["title"].as_str().unwrap().to_owned())
            .collect()
    }
}

struct Harness {
    service: Service,
    client: Client,
    _database: ContainerAsync<Postgres>,
}

impl Harness {
    async fn start() -> Self {
        let (database, pool) = start_database().await;
        let service = app::build(pool, cheap_hasher()).await.unwrap();
        let client = Client::new(service.router.clone());
        Self {
            service,
            client,
            _database: database,
        }
    }

    async fn signed_up() -> Self {
        let mut harness = Self::start().await;
        harness.client.sign_up(EMAIL).await;
        harness
    }

    async fn project(&self) {
        let outcome = self.service.projector.catch_up().await.unwrap();
        assert!(outcome.is_completed());
    }

    async fn titles(&mut self) -> Vec<String> {
        self.project().await;
        self.client.titles().await
    }

    async fn another_member(&self, email: &str) -> Client {
        let mut client = self.client.another_browser();
        client.sign_up(email).await;
        client
    }
}

async fn start_database() -> (ContainerAsync<Postgres>, sqlx::PgPool) {
    let mut attempt = 1;
    let container = loop {
        match Postgres::default().with_tag("16-alpine").start().await {
            Ok(container) => break container,
            Err(_) if attempt < 5 => {
                tokio::time::sleep(Duration::from_millis(250) * attempt).await;
                attempt += 1;
            }
            Err(error) => panic!("start postgres: {error}"),
        }
    };
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&format!(
            "postgresql://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .unwrap();
    (container, pool)
}

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
        .call(
            Method::POST,
            "/register",
            Some(json!({ "nickname": " ", "email": EMAIL, "password": PASSWORD })),
        )
        .await;

    assert_eq!(bad_email.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(weak_password.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(no_nickname.status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn changing_the_password_ends_every_other_session() {
    let mut harness = Harness::signed_up().await;
    let mut other = harness.client.another_browser();
    other.log_in(EMAIL, PASSWORD).await;
    assert_eq!(other.get("/me").await.status, StatusCode::OK);

    let change = harness
        .client
        .call(
            Method::PUT,
            "/password",
            Some(json!({ "password": "battery-staple" })),
        )
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
        .call(
            Method::PUT,
            "/password",
            Some(json!({ "password": "short" })),
        )
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
    assert_eq!(
        created.header(LOCATION.as_str()),
        Some(format!("/notes/{id}").as_str())
    );
    assert_eq!(created.json()["title"], "Groceries");

    let read = harness.client.get(&format!("/notes/{id}")).await;
    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(read.json()["body"], "milk");
    assert_eq!(harness.titles().await, ["Groceries"]);

    let replaced = harness
        .client
        .call(
            Method::PUT,
            &format!("/notes/{id}"),
            Some(json!({ "title": "Shopping", "body": "milk, eggs" })),
        )
        .await;
    assert_eq!(replaced.status, StatusCode::OK);
    assert_eq!(replaced.json()["title"], "Shopping");

    let patched = harness
        .client
        .call(
            Method::PATCH,
            &format!("/notes/{id}"),
            Some(json!({ "body": "milk, eggs, flour" })),
        )
        .await;
    assert_eq!(patched.status, StatusCode::OK);
    assert_eq!(patched.json()["title"], "Shopping");
    assert_eq!(patched.json()["body"], "milk, eggs, flour");
    assert_eq!(harness.titles().await, ["Shopping"]);
    let listed = harness.client.get("/notes").await.json();
    assert_eq!(listed[0]["body"], "milk, eggs, flour");

    let deleted = harness
        .client
        .call(Method::DELETE, &format!("/notes/{id}"), None)
        .await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    assert_eq!(
        harness.client.get(&format!("/notes/{id}")).await.status,
        StatusCode::NOT_FOUND
    );
    assert!(harness.titles().await.is_empty());
}

#[tokio::test]
async fn a_deleted_note_cannot_be_deleted_or_edited_again() {
    let mut harness = Harness::signed_up().await;
    let id = harness.client.write_note("Groceries", "milk").await.id();
    harness
        .client
        .call(Method::DELETE, &format!("/notes/{id}"), None)
        .await;

    let again = harness
        .client
        .call(Method::DELETE, &format!("/notes/{id}"), None)
        .await;
    let edit = harness
        .client
        .call(
            Method::PATCH,
            &format!("/notes/{id}"),
            Some(json!({ "title": "Back" })),
        )
        .await;

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

    let blank = harness.client.write_note("  ", "milk").await;
    let too_long = harness.client.write_note(&"x".repeat(201), "milk").await;
    let empty_patch = harness
        .client
        .call(Method::PATCH, &format!("/notes/{id}"), Some(json!({})))
        .await;
    let incomplete_put = harness
        .client
        .call(
            Method::PUT,
            &format!("/notes/{id}"),
            Some(json!({ "title": "Only a title" })),
        )
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
    let attempt = Some(json!({ "title": "Mine", "body": "stolen" }));

    let replies = [
        grace.call(Method::GET, &uri, None).await,
        grace.call(Method::PUT, &uri, attempt).await,
        grace
            .call(Method::PATCH, &uri, Some(json!({ "title": "Mine" })))
            .await,
        grace.call(Method::DELETE, &uri, None).await,
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
        Some(me_id(&mut harness.client).await)
    );
}

async fn me_id(client: &mut Client) -> String {
    client.get("/me").await.id()
}

async fn first_event_of(
    store: &PostgresEventStore,
    aggregate: &'static str,
    id: uuid::Uuid,
) -> EventEnvelope {
    let stream = store
        .load_stream(StreamId::new(aggregate, id), AggregateVersion::initial())
        .await
        .unwrap();
    let events: Vec<_> = stream.collect().await;
    events.into_iter().next().unwrap().unwrap()
}

#[tokio::test]
async fn every_response_carries_a_request_id() {
    let mut harness = Harness::start().await;

    let reply = harness.client.get("/me").await;

    assert!(reply
        .header("x-request-id")
        .is_some_and(|id| !id.is_empty()));
}
