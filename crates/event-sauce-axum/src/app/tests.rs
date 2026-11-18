use std::sync::Arc;

use async_trait::async_trait;
use axum::{body::Body, routing::get, Router};
use axum_login::{
    tower_sessions::{MemoryStore, SessionManagerLayer},
    AuthnBackend,
};
use event_sauce_auth::{
    Advance, AlwaysDone, Argon2Hasher, Done, Entered, FirstFactor, Login, LoginKey, LoginStore,
    LoginStoreError, LoginTtl, PasswordLogin, Route,
};
use event_sauce_core::{EventStore, Repository};
use event_sauce_memory::InMemoryEventStore;
use http::{HeaderName, Request, StatusCode};
use sqlx::PgPool;
use tower::ServiceExt;

use super::{App, EventSauceApp, UserAppBuilder};
use crate::login::test_support::{registered, Member};
use crate::{EventSauceBackend, MaybeUser, DEFAULT_HEADER};

struct NoLoginStore;

#[async_trait]
impl LoginStore for NoLoginStore {
    async fn put(&self, _: &LoginKey, _: &[u8], _: LoginTtl) -> Result<(), LoginStoreError> {
        Ok(())
    }

    async fn take(&self, _: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        Ok(None)
    }

    async fn mark_once(&self, _: &LoginKey, _: LoginTtl) -> Result<bool, LoginStoreError> {
        Ok(true)
    }
}

type Method = PasswordLogin<Argon2Hasher>;
type Factor = FirstFactor<Method, AlwaysDone>;
type TestApp = App<Member, Factor, InMemoryEventStore>;
type Backend = EventSauceBackend<Member, InMemoryEventStore>;

pub(super) fn builder() -> UserAppBuilder<Member, Method> {
    let pool = PgPool::connect_lazy("postgres://nobody@localhost/none").unwrap();
    EventSauceApp::postgres(pool).user::<Member, _>(PasswordLogin::new(Argon2Hasher::default()))
}

fn assembled(builder: UserAppBuilder<Member, Method>) -> TestApp {
    builder.assemble(Arc::new(InMemoryEventStore::new()), Arc::new(NoLoginStore))
}

async fn whoami(MaybeUser(user): MaybeUser<Backend>) -> &'static str {
    if user.is_some() {
        "somebody"
    } else {
        "nobody"
    }
}

async fn serve(app: &TestApp) -> http::Response<Body> {
    let router = app.layer(
        Router::new().route("/whoami", get(whoami)),
        SessionManagerLayer::new(MemoryStore::default()),
    );
    router
        .oneshot(Request::get("/whoami").body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn the_layer_installs_the_authentication_and_echoes_the_default_header() {
    let response = serve(&assembled(builder())).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key(DEFAULT_HEADER));
}

#[tokio::test]
async fn the_layer_echoes_the_configured_header() {
    let response = serve(&assembled(
        builder().correlation_header(HeaderName::from_static("x-correlation-id")),
    ))
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-correlation-id"));
    assert!(!response.headers().contains_key(DEFAULT_HEADER));
}

#[tokio::test]
async fn the_backend_resolves_the_users_of_the_apps_store() {
    let app = assembled(builder());
    let mut user = registered();
    let id = user.entity_id();
    app.store()
        .arc()
        .repository::<Member>()
        .save(&mut user)
        .await
        .unwrap();

    let found = app.backend().get_user(&id).await.unwrap();

    assert_eq!(found.map(|user| user.entity_id()), Some(id));
}

#[tokio::test]
async fn the_logins_run_on_the_apps_store_and_end_in_a_session_user() {
    let app = assembled(builder());
    let root = registered();
    let id = root.entity_id();

    let authenticated = app
        .logins()
        .sign_up(root)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let session = app.backend().authenticate(authenticated).await.unwrap();

    assert_eq!(session.map(|user| user.entity_id()), Some(id));
    assert!(app
        .store()
        .arc()
        .repository::<Member>()
        .exists(id)
        .await
        .unwrap());
}

#[tokio::test]
async fn the_app_exposes_the_pieces_a_handler_needs() {
    let app = assembled(builder());

    let _pending: event_sauce_auth::PendingLogins<Member> = app.pending();
    let _first: Arc<Factor> = app.first_factor();
    let store: Arc<dyn LoginStore> = app.login_store();

    assert!(store
        .take(&LoginKey::new(
            "none",
            event_sauce_auth::PendingLoginId::new()
        ))
        .await
        .unwrap()
        .is_none());
}

struct StraightThrough;

impl Route<Member> for StraightThrough {
    type Next = Login<Member, Done>;

    fn route(&self, entered: Advance<Member, Entered>) -> Login<Member, Done> {
        entered.to(Done)
    }
}

#[tokio::test]
async fn the_route_replaces_the_default_one() {
    let app = builder()
        .route(StraightThrough)
        .assemble(Arc::new(InMemoryEventStore::new()), Arc::new(NoLoginStore));

    let _: &Arc<FirstFactor<Method, StraightThrough>> = &app.first_factor();
}
