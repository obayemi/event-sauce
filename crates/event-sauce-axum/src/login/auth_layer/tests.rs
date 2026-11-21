use std::sync::Arc;

use axum::{body::Body, routing::get, Router};
use axum_login::tower_sessions::{MemoryStore, SessionManagerLayer};
use event_sauce_auth::CachedUserLoader;
use event_sauce_memory::InMemoryEventStore;
use http::{Request, StatusCode};
use tower::ServiceExt;

use super::AuthLayer;
use crate::login::test_support::Member;
use crate::{AuthSession, MaybeUser};

fn layer() -> AuthLayer<Member> {
    AuthLayer::new(CachedUserLoader::new(Arc::new(InMemoryEventStore::new())))
}

async fn visitors(MaybeUser(user): MaybeUser<Member>) -> &'static str {
    if user.is_some() {
        "somebody"
    } else {
        "nobody"
    }
}

async fn session(session: AuthSession<Member>) -> &'static str {
    if session.user().is_some() {
        "somebody"
    } else {
        "nobody"
    }
}

async fn body(router: Router, uri: &str) -> (StatusCode, String) {
    let response = router
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

fn routes() -> Router {
    Router::new()
        .route("/visitors", get(visitors))
        .route("/session", get(session))
}

#[tokio::test]
async fn the_layer_installs_the_session_the_extractors_read() {
    let router = layer().layer(routes(), SessionManagerLayer::new(MemoryStore::default()));

    assert_eq!(
        body(router.clone(), "/visitors").await,
        (StatusCode::OK, "nobody".to_owned())
    );
    assert_eq!(
        body(router, "/session").await,
        (StatusCode::OK, "nobody".to_owned())
    );
}

#[tokio::test]
async fn a_clone_installs_the_same_layer() {
    let router = layer()
        .clone()
        .layer(routes(), SessionManagerLayer::new(MemoryStore::default()));

    assert_eq!(body(router, "/session").await.0, StatusCode::OK);
}

#[test]
fn debug_prints_nothing_of_the_backend() {
    assert_eq!(format!("{:?}", layer()), "AuthLayer { .. }");
}
