use std::collections::BTreeMap;
use std::time::Duration;

use axum::{
    body::Body,
    http::{
        header::{COOKIE, SET_COOKIE},
        HeaderMap, Method, Request, StatusCode,
    },
    Router,
};
use event_sauce::postgres::PostgresEventStore;
use event_sauce::prelude::*;
use futures::StreamExt;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use tower::ServiceExt;

use super::{cheap_hasher, EMAIL, PASSWORD};
use crate::app::{self, Service};

pub(super) struct Reply {
    pub(super) status: StatusCode,
    pub(super) headers: HeaderMap,
    pub(super) body: String,
}

impl Reply {
    pub(super) fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }

    pub(super) fn id(&self) -> String {
        self.json()["id"].as_str().unwrap().to_owned()
    }

    pub(super) fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|value| value.to_str().unwrap())
    }
}

pub(super) struct Client {
    pub(super) router: Router,
    pub(super) cookies: BTreeMap<String, String>,
}

impl Client {
    pub(super) fn new(router: Router) -> Self {
        Self {
            router,
            cookies: BTreeMap::new(),
        }
    }

    pub(super) fn another_browser(&self) -> Self {
        Self::new(self.router.clone())
    }

    pub(super) async fn send(
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

    pub(super) async fn call(&mut self, method: Method, uri: &str, body: Option<Value>) -> Reply {
        self.send(method, uri, body, None).await
    }

    pub(super) async fn get(&mut self, uri: &str) -> Reply {
        self.call(Method::GET, uri, None).await
    }

    pub(super) async fn register(&mut self, email: &str, password: &str) -> Reply {
        let form = json!({ "nickname": "ada", "email": email, "password": password });
        self.call(Method::POST, "/register", Some(form)).await
    }

    pub(super) async fn log_in(&mut self, email: &str, password: &str) -> Reply {
        let form = json!({ "email": email, "password": password });
        self.call(Method::POST, "/login", Some(form)).await
    }

    pub(super) async fn sign_up(&mut self, email: &str) {
        assert_eq!(
            self.register(email, PASSWORD).await.status,
            StatusCode::CREATED
        );
        assert_eq!(
            self.log_in(email, PASSWORD).await.status,
            StatusCode::NO_CONTENT
        );
    }

    pub(super) async fn write_note(&mut self, title: &str, body: &str) -> Reply {
        let note = json!({ "title": title, "body": body });
        self.call(Method::POST, "/notes", Some(note)).await
    }

    pub(super) async fn titles(&mut self) -> Vec<String> {
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

pub(super) struct Harness {
    pub(super) service: Service,
    pub(super) client: Client,
    pub(super) _database: ContainerAsync<Postgres>,
}

impl Harness {
    pub(super) async fn start() -> Self {
        let (database, pool) = start_database().await;
        let service = app::build(pool, cheap_hasher()).await.unwrap();
        let client = Client::new(service.router.clone());
        Self {
            service,
            client,
            _database: database,
        }
    }

    pub(super) async fn signed_up() -> Self {
        let mut harness = Self::start().await;
        harness.client.sign_up(EMAIL).await;
        harness
    }

    pub(super) async fn project(&self) {
        let outcome = self.service.projector.catch_up().await.unwrap();
        assert!(outcome.is_completed());
    }

    pub(super) async fn titles(&mut self) -> Vec<String> {
        self.project().await;
        self.client.titles().await
    }

    pub(super) async fn another_member(&self, email: &str) -> Client {
        let mut client = self.client.another_browser();
        client.sign_up(email).await;
        client
    }
}

pub(super) async fn start_database() -> (ContainerAsync<Postgres>, sqlx::PgPool) {
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

pub(super) async fn first_event_of(
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
