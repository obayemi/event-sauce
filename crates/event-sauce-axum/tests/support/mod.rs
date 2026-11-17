//! A client that drives a router without a socket and keeps its cookies,
//! shared by the login and app integration tests.

use std::collections::BTreeMap;

use axum::{
    body::Body,
    http::{
        header::{COOKIE, SET_COOKIE},
        HeaderMap, Method, Request, StatusCode,
    },
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
}

pub struct Client {
    router: Router,
    pub cookies: BTreeMap<String, String>,
}

impl Client {
    pub fn new(router: Router) -> Self {
        Self {
            router,
            cookies: BTreeMap::new(),
        }
    }

    pub fn fork(&self) -> Self {
        Self {
            router: self.router.clone(),
            cookies: BTreeMap::new(),
        }
    }

    pub fn remember(&mut self, set_cookie: &str) {
        let pair = set_cookie.split(';').next().unwrap();
        let (name, value) = pair.split_once('=').unwrap();
        if value.is_empty() || set_cookie.contains("Max-Age=0") {
            self.cookies.remove(name);
        } else {
            self.cookies.insert(name.to_owned(), value.to_owned());
        }
    }

    pub fn cookie_header(&self) -> String {
        self.cookies
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    pub async fn request(&mut self, method: Method, uri: &str, body: Option<Value>) -> Reply {
        let mut builder = Request::builder().method(method).uri(uri);
        if !self.cookies.is_empty() {
            builder = builder.header(COOKIE, self.cookie_header());
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
            self.remember(set_cookie.to_str().unwrap());
        }
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        Reply {
            status: parts.status,
            headers: parts.headers,
            body: String::from_utf8(bytes.to_vec()).unwrap(),
        }
    }

    pub async fn get(&mut self, uri: &str) -> Reply {
        self.request(Method::GET, uri, None).await
    }

    pub async fn post(&mut self, uri: &str, body: Option<Value>) -> Reply {
        self.request(Method::POST, uri, body).await
    }

    pub async fn log_in_at(&mut self, uri: &str, email: &str, password: &str) -> Reply {
        self.post(uri, Some(json!({ "email": email, "password": password })))
            .await
    }

    pub async fn log_in(&mut self, email: &str, password: &str) -> Reply {
        self.log_in_at("/login", email, password).await
    }

    pub async fn send_code(&mut self, code: &str) -> Reply {
        self.post("/login/second-factor", Some(json!({ "code": code })))
            .await
    }
}
