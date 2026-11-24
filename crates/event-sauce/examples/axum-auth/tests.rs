//! The example's core flow, driven through its router without a socket.

use std::collections::BTreeMap;

use argon2::Params;
use axum::{
    body::Body,
    http::{
        header::{COOKIE, SET_COOKIE},
        Method, Request, StatusCode,
    },
    Router,
};
use event_sauce::auth::Argon2Hasher;
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::app::router;

const EMAIL: &str = "ada@example.com";
const PASSWORD: &str = "correct-horse";

struct Reply {
    status: StatusCode,
    body: String,
    request_id: Option<String>,
}

struct Client {
    router: Router,
    cookies: BTreeMap<String, String>,
}

impl Client {
    fn new() -> Self {
        let hasher = Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap());
        Self {
            router: router(hasher),
            cookies: BTreeMap::new(),
        }
    }

    fn another_browser(&self) -> Self {
        Self {
            router: self.router.clone(),
            cookies: BTreeMap::new(),
        }
    }

    async fn send(&mut self, method: Method, uri: &str, body: Option<Value>) -> Reply {
        let mut builder = Request::builder().method(method).uri(uri);
        if !self.cookies.is_empty() {
            let cookies: Vec<_> = self
                .cookies
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect();
            builder = builder.header(COOKIE, cookies.join("; "));
        }
        let request = match body {
            Some(body) => builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string())),
            None => builder.body(Body::empty()),
        }
        .unwrap();
        let response = self.router.clone().oneshot(request).await.unwrap();
        for set_cookie in response.headers().get_all(SET_COOKIE) {
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
        let status = response.status();
        let request_id = response
            .headers()
            .get("x-request-id")
            .map(|id| id.to_str().unwrap().to_owned());
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        Reply {
            status,
            body: String::from_utf8(bytes.to_vec()).unwrap(),
            request_id,
        }
    }

    async fn register(&mut self, email: &str) -> Reply {
        let form = json!({ "nickname": "ada", "email": email, "password": PASSWORD });
        self.send(Method::POST, "/members", Some(form)).await
    }

    async fn log_in(&mut self, password: &str) -> Reply {
        let form = json!({ "email": EMAIL, "password": password });
        self.send(Method::POST, "/login", Some(form)).await
    }

    async fn me(&mut self) -> Reply {
        self.send(Method::GET, "/me", None).await
    }

    async fn registered_and_logged_in() -> Self {
        let mut client = Self::new();
        assert_eq!(client.register(EMAIL).await.status, StatusCode::CREATED);
        assert_eq!(client.log_in(PASSWORD).await.status, StatusCode::NO_CONTENT);
        client
    }
}

#[tokio::test]
async fn a_visitor_is_refused_on_a_guarded_route() {
    let mut client = Client::new();

    assert_eq!(client.me().await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_member_logs_in_and_reaches_a_guarded_route() {
    let mut client = Client::registered_and_logged_in().await;

    let reply = client.me().await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, "ada");
}

#[tokio::test]
async fn a_wrong_password_opens_no_session() {
    let mut client = Client::new();
    client.register(EMAIL).await;

    let login = client.log_in("wrong-horse").await;

    assert_eq!(login.status, StatusCode::UNAUTHORIZED);
    assert_eq!(client.me().await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_email_registers_only_once() {
    let mut client = Client::new();
    client.register(EMAIL).await;

    let again = client.register(EMAIL).await;

    assert_eq!(again.status, StatusCode::CONFLICT);
    assert_eq!(again.body, "conflict");
}

#[tokio::test]
async fn logging_out_ends_the_session() {
    let mut client = Client::registered_and_logged_in().await;

    let logout = client.send(Method::POST, "/logout", None).await;

    assert_eq!(logout.status, StatusCode::NO_CONTENT);
    assert_eq!(client.me().await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn changing_the_password_ends_every_session() {
    let mut client = Client::registered_and_logged_in().await;
    let mut other = client.another_browser();
    other.log_in(PASSWORD).await;

    let change = client
        .send(
            Method::POST,
            "/password",
            Some(json!({ "password": "battery-staple" })),
        )
        .await;

    assert_eq!(change.status, StatusCode::NO_CONTENT);
    assert_eq!(client.me().await.status, StatusCode::UNAUTHORIZED);
    assert_eq!(other.me().await.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        client.log_in(PASSWORD).await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client.log_in("battery-staple").await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(client.me().await.status, StatusCode::OK);
}

#[tokio::test]
async fn every_response_carries_a_request_id() {
    let mut client = Client::new();

    let reply = client.me().await;

    assert!(reply.request_id.is_some_and(|id| !id.is_empty()));
}
