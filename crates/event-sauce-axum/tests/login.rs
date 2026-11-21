//! Login, logout, multi-step logins and session invalidation through a real
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
    extract::{FromRef, State},
    http::{header::SET_COOKIE, StatusCode},
    routing::{get, post},
    Json, Router,
};
use event_sauce_auth::{
    CachedUserLoader, FirstFactor, LoginKey, LoginStore, LoginStoreError, LoginTtl, Logins,
    MokaUserCache, PasswordLogin, UserCache,
};
use event_sauce_axum::{AuthLayer, AuthSession, CurrentUser, MaybeUser, PendingLogin, Registered};
use event_sauce_core::{AggregateRoot, EntityId, EventStore, Repository};
use event_sauce_memory::InMemoryEventStore;
use serde::Deserialize;
use support::Client;
use tower_sessions::{MemoryStore, SessionManagerLayer};
use user_fixture::{
    change_password, cheap_hasher, login_routes, register, CodeIfEnrolled, Flow, User,
    UserCommands, ADA, CODE, MAIL_CODE, PASSWORD,
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

#[derive(Clone, FromRef)]
struct AppState {
    flow: Flow,
    events: Arc<InMemoryEventStore>,
}

#[derive(Deserialize)]
struct PasswordForm {
    password: String,
}

async fn logout(mut auth: AuthSession<User>) -> StatusCode {
    auth.logout().await.unwrap();
    StatusCode::NO_CONTENT
}

async fn become_anonymous(mut auth: AuthSession<User>, State(flow): State<Flow>) -> String {
    let authenticated = flow
        .logins
        .sign_up(User::anonymous().unwrap())
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    auth.login(authenticated).await.unwrap();
    auth.user().unwrap().entity_id().to_string()
}

async fn change_own_password(
    mut auth: AuthSession<User>,
    State(events): State<Arc<InMemoryEventStore>>,
    Json(form): Json<PasswordForm>,
) -> StatusCode {
    let id = auth.user().unwrap().entity_id();
    change_password(&events, id, &form.password).await;
    auth.renew().await.unwrap().unwrap();
    StatusCode::NO_CONTENT
}

async fn renew(mut auth: AuthSession<User>) -> String {
    describe(auth.renew().await.unwrap())
}

async fn deactivate_and_renew(
    mut auth: AuthSession<User>,
    State(events): State<Arc<InMemoryEventStore>>,
) -> String {
    let id = auth.user().unwrap().entity_id();
    events
        .repository::<User>()
        .modify(id, |user| user.deactivate())
        .await
        .unwrap();
    describe(auth.renew().await.unwrap())
}

async fn session(auth: AuthSession<User>) -> String {
    describe(auth.user().cloned())
}

fn describe(user: Option<Arc<AggregateRoot<User>>>) -> String {
    user.map_or_else(|| "nobody".to_owned(), |user| user.entity_id().to_string())
}

async fn me(user: CurrentUser<User>) -> String {
    user.entity_id().to_string()
}

async fn maybe(MaybeUser(user): MaybeUser<User>) -> String {
    describe(user)
}

async fn registered(user: Registered<User>) -> String {
    user.entity_id().to_string()
}

struct World {
    events: Arc<InMemoryEventStore>,
    client: Client,
}

impl World {
    fn new() -> Self {
        Self::with_cache(None)
    }

    fn cached() -> Self {
        Self::with_cache(Some(Arc::new(MokaUserCache::new(100))))
    }

    fn with_cache(cache: Option<Arc<dyn UserCache<User>>>) -> Self {
        let events = Arc::new(InMemoryEventStore::new());
        let mut loader = CachedUserLoader::new(events.clone());
        let mut logins = Logins::<User>::new(events.clone());
        if let Some(cache) = cache {
            loader = loader.with_shared_cache(cache.clone());
            logins = logins.with_shared_cache(cache);
        }
        let flow = Flow {
            pending: logins.pending(Arc::new(MemoryLoginStore::default())),
            logins,
            first: Arc::new(FirstFactor::new(
                PasswordLogin::new(cheap_hasher()),
                CodeIfEnrolled,
            )),
        };
        let sessions = SessionManagerLayer::new(MemoryStore::default());
        let router = Router::<AppState>::new()
            .merge(login_routes().with_state(flow.clone()))
            .route("/logout", post(logout))
            .route("/anonymous", post(become_anonymous))
            .route("/password", post(change_own_password))
            .route("/renew", post(renew))
            .route("/deactivate-and-renew", post(deactivate_and_renew))
            .route("/session", get(session))
            .route("/me", get(me))
            .route("/maybe", get(maybe))
            .route("/registered", get(registered))
            .with_state(AppState {
                flow,
                events: events.clone(),
            });
        let router = AuthLayer::new(loader).layer(router, sessions);
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

    async fn status(&mut self, uri: &str) -> StatusCode {
        self.client.get(uri).await.status
    }

    async fn pending_cookie_after_the_password(&mut self) -> String {
        self.register(Some(CODE)).await;
        self.client.log_in(ADA, PASSWORD).await;
        self.client.cookies[PendingLogin::COOKIE].clone()
    }
}

#[tokio::test]
async fn a_visitor_is_unauthorized_where_a_user_is_required() {
    let mut world = World::new();

    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
    assert_eq!(world.status("/registered").await, StatusCode::UNAUTHORIZED);
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
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
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
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
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
async fn a_session_opens_only_after_the_last_step() {
    let mut world = World::new();
    world.register(Some(CODE)).await;

    let password = world.client.log_in(ADA, PASSWORD).await;
    let after_password = world.status("/me").await;
    let first_cookie = world.client.cookies[PendingLogin::COOKIE].clone();
    let code = world.client.send_code(CODE).await;
    let after_code = world.status("/me").await;
    let second_cookie = world.client.cookies[PendingLogin::COOKIE].clone();
    let mail = world.client.send_mail(MAIL_CODE).await;
    let after_mail = world.status("/me").await;

    assert_eq!(password.status, StatusCode::ACCEPTED);
    assert_eq!(after_password, StatusCode::UNAUTHORIZED);
    assert_eq!(code.status, StatusCode::ACCEPTED);
    assert_eq!(after_code, StatusCode::UNAUTHORIZED);
    assert_ne!(first_cookie, second_cookie);
    assert_eq!(mail.status, StatusCode::NO_CONTENT);
    assert_eq!(after_mail, StatusCode::OK);
}

#[tokio::test]
async fn the_pending_cookie_lives_as_long_as_the_state_it_waits_in() {
    let mut world = World::new();
    world.register(Some(CODE)).await;

    let password = world.client.log_in(ADA, PASSWORD).await;

    let cookie = password
        .headers
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .find(|cookie| cookie.starts_with(PendingLogin::COOKIE))
        .unwrap();
    assert!(cookie.contains("Max-Age=300"));
}

#[tokio::test]
async fn completing_the_login_removes_the_pending_cookie() {
    let mut world = World::new();
    world.register(Some(CODE)).await;
    world.client.log_in(ADA, PASSWORD).await;
    world.client.send_code(CODE).await;

    world.client.send_mail(MAIL_CODE).await;

    assert!(!world.client.cookies.contains_key(PendingLogin::COOKIE));
}

#[tokio::test]
async fn a_wrong_code_opens_no_session_and_ends_the_pending_login() {
    let mut world = World::new();
    let stale = world.pending_cookie_after_the_password().await;

    let wrong = world.client.send_code("000000").await;
    world
        .client
        .cookies
        .insert(PendingLogin::COOKIE.to_owned(), stale);
    let retry = world.client.send_code(CODE).await;

    assert_eq!(wrong.status, StatusCode::UNAUTHORIZED);
    assert_eq!(retry.status, StatusCode::UNAUTHORIZED);
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_wrong_mailed_code_opens_no_session_and_ends_the_pending_login() {
    let mut world = World::new();
    world.pending_cookie_after_the_password().await;
    world.client.send_code(CODE).await;
    let stale = world.client.cookies[PendingLogin::COOKIE].clone();

    let wrong = world.client.send_mail("000000").await;
    world
        .client
        .cookies
        .insert(PendingLogin::COOKIE.to_owned(), stale);
    let retry = world.client.send_mail(MAIL_CODE).await;

    assert_eq!(wrong.status, StatusCode::UNAUTHORIZED);
    assert_eq!(retry.status, StatusCode::UNAUTHORIZED);
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_step_cannot_be_replayed() {
    let mut world = World::new();
    let first = world.pending_cookie_after_the_password().await;
    world.client.send_code(CODE).await;
    let mut replay = world.client.fork();
    replay
        .cookies
        .insert(PendingLogin::COOKIE.to_owned(), first);

    let reply = replay.send_code(CODE).await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        world.client.send_mail(MAIL_CODE).await.status,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn a_finished_login_cannot_be_replayed() {
    let mut world = World::new();
    world.pending_cookie_after_the_password().await;
    world.client.send_code(CODE).await;
    let last = world.client.cookies[PendingLogin::COOKIE].clone();
    world.client.send_mail(MAIL_CODE).await;
    let mut replay = world.client.fork();
    replay.cookies.insert(PendingLogin::COOKIE.to_owned(), last);

    let reply = replay.send_mail(MAIL_CODE).await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(replay.get("/me").await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_step_cannot_be_skipped() {
    let mut world = World::new();
    world.pending_cookie_after_the_password().await;

    let skipped = world.client.send_mail(MAIL_CODE).await;
    let honest = world.client.send_code(CODE).await;

    assert_eq!(skipped.status, StatusCode::UNAUTHORIZED);
    assert_eq!(honest.status, StatusCode::UNAUTHORIZED);
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_login_cannot_go_back_to_a_step_it_passed() {
    let mut world = World::new();
    world.pending_cookie_after_the_password().await;
    world.client.send_code(CODE).await;

    let again = world.client.send_code(CODE).await;

    assert_eq!(again.status, StatusCode::UNAUTHORIZED);
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_step_needs_the_pending_cookie() {
    let mut world = World::new();
    world.register(Some(CODE)).await;

    let code = world.client.send_code(CODE).await;
    let mail = world.client.send_mail(MAIL_CODE).await;

    assert_eq!(code.status, StatusCode::UNAUTHORIZED);
    assert_eq!(mail.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_pending_login_cannot_be_continued_with_a_forged_cookie() {
    let mut world = World::new();
    world.register(Some(CODE)).await;
    world.client.cookies.insert(
        PendingLogin::COOKIE.to_owned(),
        event_sauce_auth::PendingLoginId::new().to_string(),
    );

    let code = world.client.send_code(CODE).await;
    let mail = world.client.send_mail(MAIL_CODE).await;

    assert_eq!(code.status, StatusCode::UNAUTHORIZED);
    assert_eq!(mail.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn deactivating_the_user_after_the_password_ends_the_login() {
    let mut world = World::new();
    let id = world.register(Some(CODE)).await;
    world.client.log_in(ADA, PASSWORD).await;

    world.deactivate(id).await;
    let code = world.client.send_code(CODE).await;

    assert_eq!(code.status, StatusCode::UNAUTHORIZED);
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn deactivating_the_user_before_the_last_step_ends_the_login() {
    let mut world = World::new();
    let id = world.register(Some(CODE)).await;
    world.client.log_in(ADA, PASSWORD).await;
    world.client.send_code(CODE).await;

    world.deactivate(id).await;
    let mail = world.client.send_mail(MAIL_CODE).await;

    assert_eq!(mail.status, StatusCode::UNAUTHORIZED);
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn changing_the_password_during_the_login_ends_it() {
    let mut world = World::new();
    let id = world.register(Some(CODE)).await;
    world.client.log_in(ADA, PASSWORD).await;
    world.client.send_code(CODE).await;

    world.change_password(id, "new-horse").await;
    let mail = world.client.send_mail(MAIL_CODE).await;

    assert_eq!(mail.status, StatusCode::UNAUTHORIZED);
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_anonymous_user_is_current_but_not_registered() {
    let mut world = World::new();

    world.client.post("/anonymous", None).await;

    assert_eq!(world.status("/me").await, StatusCode::OK);
    assert_ne!(world.client.get("/maybe").await.body, "nobody");
    assert_eq!(world.status("/registered").await, StatusCode::FORBIDDEN);
}

async fn changing_the_password_ends_other_sessions(mut world: World) {
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;
    let mut other = world.client.fork();
    other.log_in(ADA, PASSWORD).await;
    assert_eq!(other.get("/me").await.status, StatusCode::OK);

    world.change_password(id, "new-horse").await;

    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
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
    assert_eq!(world.status("/me").await, StatusCode::OK);
}

#[tokio::test]
async fn changing_the_password_ends_other_sessions_without_a_cache() {
    changing_the_password_ends_other_sessions(World::new()).await;
}

#[tokio::test]
async fn changing_the_password_ends_other_sessions_with_a_cache() {
    changing_the_password_ends_other_sessions(World::cached()).await;
}

async fn renewing_keeps_the_session_that_changed_the_password(mut world: World) {
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;
    let mut other = world.client.fork();
    other.log_in(ADA, PASSWORD).await;

    let change = world
        .client
        .post(
            "/password",
            Some(serde_json::json!({ "password": "new-horse" })),
        )
        .await;

    assert_eq!(change.status, StatusCode::NO_CONTENT);
    assert_eq!(world.client.get("/me").await.body, id.to_string());
    assert_eq!(other.get("/me").await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn renewing_keeps_the_session_that_changed_the_password_without_a_cache() {
    renewing_keeps_the_session_that_changed_the_password(World::new()).await;
}

#[tokio::test]
async fn renewing_keeps_the_session_that_changed_the_password_with_a_cache() {
    renewing_keeps_the_session_that_changed_the_password(World::cached()).await;
}

#[tokio::test]
async fn enrolling_a_second_factor_ends_other_sessions() {
    let mut world = World::new();
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    world.enroll_second_factor(id).await;

    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

async fn deactivating_a_user_ends_their_sessions(mut world: World) {
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;
    world.client.get("/me").await;

    world.deactivate(id).await;

    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
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
            .route("/registered", get(registered))
            .route("/session", get(session)),
    );

    for uri in ["/me", "/maybe", "/registered", "/session"] {
        assert_eq!(
            client.get(uri).await.status,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}

#[tokio::test]
async fn the_session_of_a_visitor_holds_nobody() {
    let mut world = World::new();

    assert_eq!(world.client.get("/session").await.body, "nobody");
}

#[tokio::test]
async fn logging_in_through_the_session_makes_the_user_current_at_once() {
    let mut world = World::new();

    let reply = world.client.post("/anonymous", None).await;

    assert_eq!(reply.body, world.client.get("/session").await.body);
    assert_ne!(reply.body, "nobody");
}

#[tokio::test]
async fn a_finished_password_login_is_the_session_user() {
    let mut world = World::new();
    let id = world.register(None).await;

    world.client.log_in(ADA, PASSWORD).await;

    assert_eq!(world.client.get("/session").await.body, id.to_string());
}

#[tokio::test]
async fn renewing_with_nobody_logged_in_answers_nobody() {
    let mut world = World::new();

    assert_eq!(world.client.post("/renew", None).await.body, "nobody");
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

async fn renewing_without_a_change_keeps_the_session(mut world: World) {
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    let renewed = world.client.post("/renew", None).await;

    assert_eq!(renewed.body, id.to_string());
    assert_eq!(world.client.get("/me").await.body, id.to_string());
}

#[tokio::test]
async fn renewing_without_a_change_keeps_the_session_without_a_cache() {
    renewing_without_a_change_keeps_the_session(World::new()).await;
}

#[tokio::test]
async fn renewing_without_a_change_keeps_the_session_with_a_cache() {
    renewing_without_a_change_keeps_the_session(World::cached()).await;
}

#[tokio::test]
async fn renewing_a_user_deactivated_during_the_request_ends_the_session() {
    let mut world = World::new();
    world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    let renewed = world.client.post("/deactivate-and-renew", None).await;

    assert_eq!(renewed.body, "nobody");
    assert_eq!(world.status("/me").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn logging_out_leaves_nobody_in_the_session() {
    let mut world = World::new();
    world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    assert_eq!(
        world.client.post("/logout", None).await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(world.client.get("/session").await.body, "nobody");
}
