//! Login, logout, second factor and session invalidation through a real
//! axum router, with tower-sessions' memory store and the in-memory event
//! store.

mod support;
mod user_fixture;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use axum::{
    extract::State,
    http::{header::SET_COOKIE, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum_login::{AuthManagerLayerBuilder, AuthSession};
use event_sauce_auth::{
    Argon2Hasher, ClaimAuthenticator, Email, LoginId, LoginKey, LoginOutcome, LoginStore,
    LoginStoreError, LoginTtl, MokaUserCache, PasswordAttempt, PasswordLogin, TwoFactor,
};
use event_sauce_axum::{
    BackendError, CurrentUser, EventSauceBackend, MaybeUser, PendingLogin, Registered, SessionUser,
};
use event_sauce_core::{AggregateRoot, EntityId, EventStore, Repository};
use event_sauce_memory::InMemoryEventStore;
use secrecy::SecretString;
use support::Client;
use tower_sessions::{MemoryStore, SessionManagerLayer};
use user_fixture::{
    change_password, cheap_hasher, register, CodeForm, FixedCode, LoginForm, User, UserCommands,
    ADA, CODE, PASSWORD,
};

#[derive(Default)]
struct MemoryLoginStore {
    entries: Mutex<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl LoginStore for MemoryLoginStore {
    async fn put(&self, key: &LoginKey, value: &[u8], _: LoginTtl) -> Result<(), LoginStoreError> {
        self.entries
            .lock()
            .unwrap()
            .insert(key.as_str().to_owned(), value.to_vec());
        Ok(())
    }

    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        Ok(self.entries.lock().unwrap().remove(key.as_str()))
    }

    async fn mark_once(&self, key: &LoginKey, _: LoginTtl) -> Result<bool, LoginStoreError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .insert(key.as_str().to_owned(), Vec::new())
            .is_none())
    }
}

type Authenticator =
    ClaimAuthenticator<User, PasswordLogin<Argon2Hasher>, InMemoryEventStore, TwoFactor<FixedCode>>;
type Backend = EventSauceBackend<User, Authenticator, InMemoryEventStore>;

fn attempt(form: LoginForm) -> PasswordAttempt<Email> {
    PasswordAttempt::new(
        Email::parse(&form.email).unwrap(),
        SecretString::from(form.password),
    )
}

fn describe(root: &AggregateRoot<User>) -> String {
    root.entity_id().to_string()
}

async fn login(mut auth: AuthSession<Backend>, Json(form): Json<LoginForm>) -> Response {
    match auth.backend.start(attempt(form)).await {
        Ok(LoginOutcome::Authenticated(root)) => {
            auth.login(&SessionUser::from(root)).await.unwrap();
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(LoginOutcome::SecondFactorRequired(id)) => (
            StatusCode::ACCEPTED,
            [(
                SET_COOKIE,
                PendingLogin::set_cookie(id, PendingLogin::DEFAULT_TTL),
            )],
        )
            .into_response(),
        Ok(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        Err(_) => StatusCode::UNAUTHORIZED.into_response(),
    }
}

async fn login_through_axum_login(
    mut auth: AuthSession<Backend>,
    Json(form): Json<LoginForm>,
) -> Response {
    match auth.authenticate(attempt(form)).await {
        Ok(Some(user)) => {
            auth.login(&user).await.unwrap();
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(None) => StatusCode::UNAUTHORIZED.into_response(),
        Err(axum_login::Error::Backend(BackendError::SecondFactorRequired(id))) => {
            (StatusCode::ACCEPTED, id.to_string()).into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn second_factor(
    mut auth: AuthSession<Backend>,
    pending: PendingLogin,
    Json(form): Json<CodeForm>,
) -> Response {
    let cleared = [(SET_COOKIE, PendingLogin::clear_cookie())];
    match auth.backend.complete(pending.id(), &form.code).await {
        Ok(user) => {
            auth.login(&user).await.unwrap();
            (StatusCode::NO_CONTENT, cleared).into_response()
        }
        Err(_) => (StatusCode::UNAUTHORIZED, cleared).into_response(),
    }
}

async fn logout(mut auth: AuthSession<Backend>) -> StatusCode {
    auth.logout().await.unwrap();
    StatusCode::NO_CONTENT
}

async fn become_anonymous(
    mut auth: AuthSession<Backend>,
    State(store): State<Arc<InMemoryEventStore>>,
) -> StatusCode {
    let mut user = User::anonymous().unwrap();
    store.repository::<User>().save(&mut user).await.unwrap();
    auth.login(&SessionUser::from(user)).await.unwrap();
    StatusCode::NO_CONTENT
}

async fn me(user: CurrentUser<Backend>) -> String {
    describe(&user)
}

async fn maybe(MaybeUser(user): MaybeUser<Backend>) -> String {
    user.map_or_else(|| "nobody".to_owned(), |user| describe(&user))
}

async fn registered(user: Registered<Backend>) -> String {
    describe(&user)
}

struct World {
    events: Arc<InMemoryEventStore>,
    client: Client,
}

impl World {
    fn new() -> Self {
        Self::with_backend(|backend| backend)
    }

    fn cached() -> Self {
        Self::with_backend(|backend| backend.with_cache(MokaUserCache::new(100)))
    }

    fn with_backend(configure: impl FnOnce(Backend) -> Backend) -> Self {
        let events = Arc::new(InMemoryEventStore::new());
        let authenticator =
            ClaimAuthenticator::new(events.clone(), PasswordLogin::new(cheap_hasher()))
                .with_second_factor(TwoFactor::new(
                    FixedCode,
                    Arc::new(MemoryLoginStore::default()),
                ));
        let backend = configure(EventSauceBackend::new(authenticator, events.clone()));
        let sessions = SessionManagerLayer::new(MemoryStore::default());
        let router = Router::new()
            .route("/login", post(login))
            .route("/login/axum-login", post(login_through_axum_login))
            .route("/login/second-factor", post(second_factor))
            .route("/logout", post(logout))
            .route("/anonymous", post(become_anonymous))
            .route("/me", get(me))
            .route("/maybe", get(maybe))
            .route("/registered", get(registered))
            .with_state(events.clone())
            .layer(AuthManagerLayerBuilder::new(backend, sessions).build());
        Self {
            events,
            client: Client::new(router),
        }
    }

    async fn register(&self, code: Option<&str>) -> EntityId {
        register(&self.events, code).await
    }

    async fn change_password(&self, id: EntityId, password: &str) {
        change_password(&self.events, id, password).await;
    }

    async fn enroll_second_factor(&self, id: EntityId) {
        self.events
            .repository::<User>()
            .modify(id, |user| user.enroll_second_factor(CODE.to_owned()))
            .await
            .unwrap();
    }

    async fn deactivate(&self, id: EntityId) {
        self.events
            .repository::<User>()
            .modify(id, |user| user.deactivate())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn a_visitor_is_unauthorized_where_a_user_is_required() {
    let mut world = World::new();

    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        world.client.get("/registered").await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_visitor_is_nobody_where_a_user_is_optional() {
    let mut world = World::new();

    let reply = world.client.get("/maybe").await;

    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, "nobody");
}

#[tokio::test]
async fn a_password_login_opens_a_session_for_the_user() {
    let mut world = World::new();
    let id = world.register(None).await;

    let login = world.client.log_in(ADA, PASSWORD).await;
    let me = world.client.get("/me").await;
    let maybe = world.client.get("/maybe").await;
    let registered = world.client.get("/registered").await;

    assert_eq!(login.status, StatusCode::NO_CONTENT);
    assert_eq!(me.status, StatusCode::OK);
    assert_eq!(me.body, id.to_string());
    assert_eq!(maybe.body, id.to_string());
    assert_eq!(registered.body, id.to_string());
}

#[tokio::test]
async fn a_wrong_password_opens_no_session() {
    let mut world = World::new();
    world.register(None).await;

    let login = world.client.log_in(ADA, "wrong-horse").await;

    assert_eq!(login.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn an_unknown_login_opens_no_session() {
    let mut world = World::new();

    let login = world.client.log_in("nobody@example.com", PASSWORD).await;

    assert_eq!(login.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn logging_out_ends_the_session_for_good() {
    let mut world = World::new();
    world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;
    let mut thief = world.client.fork();
    thief.cookies.clone_from(&world.client.cookies);

    let logout = world.client.post("/logout", None).await;

    assert_eq!(logout.status, StatusCode::NO_CONTENT);
    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(thief.get("/me").await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_session_id_changes_at_login() {
    let mut world = World::new();
    world.register(None).await;
    world.client.get("/maybe").await;
    let before = world.client.cookies.clone();

    world.client.log_in(ADA, PASSWORD).await;

    assert_ne!(world.client.cookies, before);
}

#[tokio::test]
async fn a_second_factor_holds_the_login_back_until_it_is_proven() {
    let mut world = World::new();
    world.register(Some(CODE)).await;

    let first = world.client.log_in(ADA, PASSWORD).await;
    let before = world.client.get("/me").await;
    let second = world.client.send_code(CODE).await;
    let after = world.client.get("/me").await;

    assert_eq!(first.status, StatusCode::ACCEPTED);
    assert!(first.headers.get_all(SET_COOKIE).iter().any(|cookie| cookie
        .as_bytes()
        .starts_with(PendingLogin::COOKIE.as_bytes())));
    assert_eq!(before.status, StatusCode::UNAUTHORIZED);
    assert_eq!(second.status, StatusCode::NO_CONTENT);
    assert_eq!(after.status, StatusCode::OK);
}

#[tokio::test]
async fn completing_the_login_removes_the_pending_cookie() {
    let mut world = World::new();
    world.register(Some(CODE)).await;
    world.client.log_in(ADA, PASSWORD).await;

    world.client.send_code(CODE).await;

    assert!(!world.client.cookies.contains_key(PendingLogin::COOKIE));
}

#[tokio::test]
async fn a_wrong_code_opens_no_session_and_ends_the_pending_login() {
    let mut world = World::new();
    world.register(Some(CODE)).await;
    world.client.log_in(ADA, PASSWORD).await;
    let stale = world.client.cookies.clone();

    let wrong = world.client.send_code("000000").await;
    world.client.cookies = stale;
    let retry = world.client.send_code(CODE).await;

    assert_eq!(wrong.status, StatusCode::UNAUTHORIZED);
    assert_eq!(retry.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_pending_login_completes_only_once() {
    let mut world = World::new();
    world.register(Some(CODE)).await;
    world.client.log_in(ADA, PASSWORD).await;
    let stale = world.client.cookies.clone();
    world.client.send_code(CODE).await;
    let mut replay = world.client.fork();
    replay.cookies = stale;

    let reply = replay.send_code(CODE).await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(replay.get("/me").await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_second_factor_needs_the_pending_cookie() {
    let mut world = World::new();
    world.register(Some(CODE)).await;

    let reply = world.client.send_code(CODE).await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_pending_login_cannot_be_completed_by_a_forged_cookie() {
    let mut world = World::new();
    world.register(Some(CODE)).await;
    world.client.cookies.insert(
        PendingLogin::COOKIE.to_owned(),
        event_sauce_auth::PendingLoginId::new().to_string(),
    );

    let reply = world.client.send_code(CODE).await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn axum_login_authenticate_reports_the_pending_login_as_an_error() {
    let mut world = World::new();
    world.register(Some(CODE)).await;

    let reply = world
        .client
        .log_in_at("/login/axum-login", ADA, PASSWORD)
        .await;

    assert_eq!(reply.status, StatusCode::ACCEPTED);
    assert!(reply
        .body
        .parse::<event_sauce_auth::PendingLoginId>()
        .is_ok());
    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn axum_login_authenticate_opens_a_session_without_a_second_factor() {
    let mut world = World::new();
    world.register(None).await;

    let reply = world
        .client
        .log_in_at("/login/axum-login", ADA, PASSWORD)
        .await;

    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert_eq!(world.client.get("/me").await.status, StatusCode::OK);
}

#[tokio::test]
async fn axum_login_authenticate_refuses_a_wrong_password_without_an_error() {
    let mut world = World::new();
    world.register(None).await;

    let reply = world
        .client
        .log_in_at("/login/axum-login", ADA, "wrong-horse")
        .await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_anonymous_user_is_current_but_not_registered() {
    let mut world = World::new();

    world.client.post("/anonymous", None).await;

    assert_eq!(world.client.get("/me").await.status, StatusCode::OK);
    assert_ne!(world.client.get("/maybe").await.body, "nobody");
    assert_eq!(
        world.client.get("/registered").await.status,
        StatusCode::FORBIDDEN
    );
}

async fn changing_the_password_ends_other_sessions(mut world: World) {
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;
    let mut other = world.client.fork();
    other.log_in(ADA, PASSWORD).await;
    assert_eq!(other.get("/me").await.status, StatusCode::OK);

    world.change_password(id, "new-horse").await;

    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(other.get("/me").await.status, StatusCode::UNAUTHORIZED);
    assert_eq!(world.client.get("/maybe").await.body, "nobody");
    assert_eq!(
        world.client.log_in(ADA, PASSWORD).await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        world.client.log_in(ADA, "new-horse").await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(world.client.get("/me").await.status, StatusCode::OK);
}

#[tokio::test]
async fn changing_the_password_ends_other_sessions_without_a_cache() {
    changing_the_password_ends_other_sessions(World::new()).await;
}

#[tokio::test]
async fn changing_the_password_ends_other_sessions_with_a_cache() {
    changing_the_password_ends_other_sessions(World::cached()).await;
}

#[tokio::test]
async fn enrolling_a_second_factor_ends_other_sessions() {
    let mut world = World::new();
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    world.enroll_second_factor(id).await;

    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

async fn deactivating_a_user_ends_their_sessions(mut world: World) {
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;
    world.client.get("/me").await;

    world.deactivate(id).await;

    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        world.client.log_in(ADA, PASSWORD).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn deactivating_a_user_ends_their_sessions_without_a_cache() {
    deactivating_a_user_ends_their_sessions(World::new()).await;
}

#[tokio::test]
async fn deactivating_a_user_ends_their_sessions_with_a_cache() {
    deactivating_a_user_ends_their_sessions(World::cached()).await;
}

#[tokio::test]
async fn a_cached_backend_serves_the_same_user() {
    let mut world = World::cached();
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    let first = world.client.get("/me").await;
    let second = world.client.get("/me").await;

    assert_eq!(first.body, id.to_string());
    assert_eq!(second.body, id.to_string());
}

#[tokio::test]
async fn the_extractors_report_a_missing_auth_layer_as_a_server_error() {
    let mut client = Client::new(
        Router::new()
            .route("/me", get(me))
            .route("/maybe", get(maybe))
            .route("/registered", get(registered)),
    );

    for uri in ["/me", "/maybe", "/registered"] {
        assert_eq!(
            client.get(uri).await.status,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
