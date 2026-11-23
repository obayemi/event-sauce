use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::State,
    routing::{get, post},
    Router,
};
use axum_login::tower_sessions::{
    cookie::{Cookie, CookieJar, Key},
    service::CookieController,
    MemoryStore, SessionManagerLayer,
};
use event_sauce_auth::{
    Advance, AlwaysDone, Argon2Hasher, Done, Entered, FirstFactor, Login, LoginKey, LoginStore,
    LoginStoreError, LoginTtl, Logins, PasswordLogin, Route,
};
use event_sauce_core::{EventStore, Repository};
use event_sauce_memory::InMemoryEventStore;
use http::{header::SET_COOKIE, HeaderName, Request, StatusCode};
use sqlx::PgPool;
use tower::ServiceExt;

use super::{App, EventSauceApp, UserAppBuilder};
use crate::login::test_support::{registered, Member};
use crate::{AuthSession, MaybeUser, DEFAULT_HEADER};

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

#[derive(Default)]
struct MemoryLoginStore(Mutex<HashMap<String, Vec<u8>>>);

impl MemoryLoginStore {
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

#[async_trait]
impl LoginStore for MemoryLoginStore {
    async fn put(&self, key: &LoginKey, value: &[u8], _: LoginTtl) -> Result<(), LoginStoreError> {
        self.0
            .lock()
            .unwrap()
            .insert(format!("{key:?}"), value.to_vec());
        Ok(())
    }

    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        Ok(self.0.lock().unwrap().remove(&format!("{key:?}")))
    }

    async fn mark_once(&self, _: &LoginKey, _: LoginTtl) -> Result<bool, LoginStoreError> {
        Ok(true)
    }
}

type Method = PasswordLogin<Argon2Hasher>;
type Factor = FirstFactor<Method, AlwaysDone>;
type TestApp = App<Member, Factor, InMemoryEventStore>;

pub(super) fn builder() -> UserAppBuilder<Member, Method> {
    let pool = PgPool::connect_lazy("postgres://nobody@localhost/none").unwrap();
    EventSauceApp::postgres(pool).user::<Member, _>(PasswordLogin::new(Argon2Hasher::default()))
}

fn assembled(builder: UserAppBuilder<Member, Method>) -> TestApp {
    builder.assemble(Arc::new(InMemoryEventStore::new()), Arc::new(NoLoginStore))
}

async fn whoami(MaybeUser(user): MaybeUser<Member>) -> &'static str {
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

async fn login_and_whoami(
    mut session: AuthSession<Member>,
    State(logins): State<Logins<Member>>,
) -> String {
    let authenticated = logins
        .sign_up(registered())
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    session.login(authenticated).await.unwrap();
    session.user().unwrap().entity_id().to_string()
}

#[tokio::test]
async fn a_session_resolves_the_users_of_the_apps_store() {
    let app = Arc::new(assembled(builder()));
    let mut user = registered();
    let id = user.entity_id();
    app.store()
        .arc()
        .repository::<Member>()
        .save(&mut user)
        .await
        .unwrap();
    let router = app.layer(
        Router::new()
            .route("/login", post(login_and_whoami))
            .with_state(app.logins()),
        SessionManagerLayer::new(MemoryStore::default()),
    );

    let response = router
        .oneshot(Request::post("/login").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
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

#[tokio::test]
async fn the_pending_logins_are_saved_in_the_apps_login_store() {
    let login_store = Arc::new(MemoryLoginStore::default());
    let app = builder().assemble(Arc::new(InMemoryEventStore::new()), login_store.clone());
    let root = registered();
    let id = root.entity_id();
    let login = app.logins().sign_up(root).await.unwrap();

    let pending_id = app.pending().save(login).await.unwrap();

    assert_eq!(login_store.len(), 1);
    let resumed = app
        .pending()
        .resume::<event_sauce_auth::Done>(pending_id)
        .await
        .unwrap();
    assert_eq!(resumed.user().entity_id(), id);
    assert_eq!(login_store.len(), 0);
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

fn key() -> Key {
    Key::from(&[7; 64])
}

async fn session_cookie<C: CookieController + Sync>(
    app: &TestApp,
    sessions: SessionManagerLayer<MemoryStore, C>,
) -> String {
    let router = app.layer(
        Router::new()
            .route("/login", post(login_and_whoami))
            .with_state(app.logins()),
        sessions,
    );
    let response = router
        .oneshot(Request::post("/login").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let set_cookie = response.headers()[SET_COOKIE].to_str().unwrap().to_owned();
    Cookie::parse(set_cookie).unwrap().value().to_owned()
}

fn signed_by(key: &Key, value: &str) -> bool {
    let mut jar = CookieJar::new();
    jar.add_original(Cookie::new("id", value.to_owned()));
    jar.signed(key).get("id").is_some()
}

#[tokio::test]
async fn the_layer_keeps_the_cookie_the_session_layer_signs() {
    let sessions = SessionManagerLayer::new(MemoryStore::default()).with_signed(key());

    let cookie = session_cookie(&assembled(builder()), sessions).await;

    assert!(signed_by(&key(), &cookie));
}
