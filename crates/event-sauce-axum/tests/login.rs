//! Login, logout, multi-step logins and session invalidation through a real
//! axum router, with tower-sessions' memory store and the in-memory event
//! store.

mod broken_store;
mod memory_login_store;
mod note_fixture;
#[path = "support/password.rs"]
mod password;
mod support;
mod user_fixture;

use std::sync::Arc;

use axum::{
    extract::{FromRef, Query, State},
    http::{
        header::{LOCATION, SET_COOKIE},
        StatusCode,
    },
    routing::{get, post},
    Json, Router,
};
use broken_store::BrokenStore;
use event_sauce_auth::{
    CachedUserLoader, FirstFactor, Logins, MokaUserCache, PasswordLogin, UserCache,
};
use event_sauce_axum::{
    Acting, AuthLayer, AuthSession, Authorized, CurrentUser, FromUser, LoginError, LoginRedirect,
    LoginStep, MaybeUser, PendingLogin, Registered, Repo, ReturnTo, Store,
};
use event_sauce_core::{
    AggregateRoot, Command, CommandError, EntityId, EventStore, Repository, RetryPolicy, Root,
};
use event_sauce_memory::InMemoryEventStore;
use memory_login_store::MemoryLoginStore;
use note_fixture::{
    Note, NoteDeleteCommands, NoteError, NoteInitCommands, OpenOwner, Owner, RegisteredOwner,
};
use serde::Deserialize;
use support::Client;
use tower_sessions::{
    cookie::{Cookie, CookieJar, Key},
    service::CookieController,
    MemoryStore, SessionManagerLayer,
};
use user_fixture::{
    change_password, cheap_hasher, login_routes, register, CodeIfEnrolled, Flow, LoginForm,
    StepClient as _, User, UserCommands, ADA, CODE, MAIL_CODE, PASSWORD,
};

#[derive(Clone, FromRef)]
struct AppState {
    flow: Flow,
    events: Arc<InMemoryEventStore>,
    store: Store<InMemoryEventStore>,
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

struct Author {
    user: Arc<AggregateRoot<User>>,
}

impl FromUser for Author {
    type User = User;

    fn from_user(user: Arc<AggregateRoot<User>>) -> Self {
        Self { user }
    }
}

struct WriteNote;

impl Command<Note, Author> for WriteNote {
    fn handle(
        &self,
        root: Root<Note>,
        author: &Author,
    ) -> Result<Root<Note>, CommandError<NoteError>> {
        Ok(root.missing()?.write(author.user.entity_id())?.into())
    }
}

async fn acting(Acting(author): Acting<Author>) -> String {
    author.user.entity_id().to_string()
}

async fn write_note_as(
    Acting(author): Acting<Author>,
    Repo(notes): Repo<Note, InMemoryEventStore>,
) -> String {
    let id = EntityId::new();
    notes
        .execute(id, &WriteNote, &author, RetryPolicy::none())
        .await
        .unwrap();
    id.to_string()
}

type Owned<P> = Authorized<Note, P, InMemoryEventStore>;

async fn note(note: Owned<Owner>) -> String {
    note.owner.to_string()
}

async fn strict_note(note: Owned<OpenOwner>) -> String {
    note.owner.to_string()
}

async fn registered_note(note: Owned<RegisteredOwner>) -> String {
    note.user().entity_id().to_string()
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
        Self::with_sessions(cache, SessionManagerLayer::new(MemoryStore::default()))
    }

    fn with_sessions<C: CookieController + Sync>(
        cache: Option<Arc<dyn UserCache<User>>>,
        sessions: SessionManagerLayer<MemoryStore, C>,
    ) -> Self {
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
        let router = Router::<AppState>::new()
            .merge(login_routes().with_state(flow.clone()))
            .route("/logout", post(logout))
            .route("/anonymous", post(become_anonymous))
            .route("/password", post(change_own_password))
            .route("/renew", post(renew))
            .route("/deactivate-and-renew", post(deactivate_and_renew))
            .route("/session", get(session))
            .route("/me", get(me))
            .route("/acting", get(acting))
            .route("/acting/notes", post(write_note_as))
            .route("/maybe", get(maybe))
            .route("/registered", get(registered))
            .route("/notes/{id}", get(note))
            .route("/notes/{id}/strict", get(strict_note))
            .route("/notes/{id}/registered", get(registered_note))
            .nest("/app", pages())
            .with_state(AppState {
                flow,
                store: Store::from(events.clone()),
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

    async fn write_note(&self, owner: EntityId) -> EntityId {
        let mut note = Note::write(owner).unwrap();
        self.events
            .repository::<Note>()
            .save(&mut note)
            .await
            .unwrap();
        note.entity_id()
    }

    async fn remove_note(&self, id: EntityId) {
        let repository = self.events.repository::<Note>();
        let mut removed = repository.load(id).await.unwrap().remove().unwrap();
        repository.save_deleted(&mut removed).await.unwrap();
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

fn broken_world() -> Client {
    let events = Arc::new(BrokenStore);
    let logins = Logins::<User>::new(events.clone());
    let flow = Flow {
        pending: logins.pending(Arc::new(MemoryLoginStore::default())),
        logins,
        first: Arc::new(FirstFactor::new(
            PasswordLogin::new(cheap_hasher()),
            CodeIfEnrolled,
        )),
    };
    let router = Router::new()
        .merge(login_routes())
        .route("/me", get(me))
        .with_state(flow);
    let sessions = SessionManagerLayer::new(MemoryStore::default());
    Client::new(AuthLayer::<User>::new(CachedUserLoader::new(events)).layer(router, sessions))
}

#[tokio::test]
async fn a_login_against_a_failing_user_store_answers_a_server_error_and_opens_no_session() {
    let mut client = broken_world();

    let login = client.log_in(ADA, PASSWORD).await;
    let me = client.get("/me").await;

    assert_eq!(login.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(login.body, "internal error");
    assert_eq!(me.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_unknown_email_and_a_wrong_password_answer_the_same_response() {
    let mut world = World::new();
    world.register(None).await;

    let unknown = world.client.log_in("nobody@example.com", PASSWORD).await;
    let wrong = world.client.log_in(ADA, "wrong-horse").await;

    assert_eq!(unknown.status, StatusCode::UNAUTHORIZED);
    assert_eq!(unknown.status, wrong.status);
    assert_eq!(unknown.body, wrong.body);
    assert_eq!(unknown.headers, wrong.headers);
}

fn pending_cookie_of(reply: &support::Reply) -> String {
    reply
        .headers
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .find(|cookie| cookie.starts_with(PendingLogin::COOKIE))
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn each_pending_cookie_lives_as_long_as_the_state_its_login_waits_in() {
    let mut world = World::new();
    world.register(Some(CODE)).await;

    let password = world.client.log_in(ADA, PASSWORD).await;
    let code = world.client.send_code(CODE).await;

    assert!(pending_cookie_of(&password).contains("Max-Age=300"));
    assert!(pending_cookie_of(&code).contains("Max-Age=3600"));
}

#[tokio::test]
async fn a_malformed_body_does_not_burn_the_pending_login() {
    let mut world = World::new();
    world.pending_cookie_after_the_password().await;

    let malformed = world
        .client
        .post("/login/code", Some(serde_json::json!({ "wrong": 1 })))
        .await;
    let code = world.client.send_code(CODE).await;
    let mail = world.client.send_mail(MAIL_CODE).await;

    assert_eq!(malformed.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(code.status, StatusCode::ACCEPTED);
    assert_eq!(mail.status, StatusCode::NO_CONTENT);
    assert_eq!(world.status("/me").await, StatusCode::OK);
}

#[tokio::test]
async fn a_refused_step_clears_the_pending_cookie() {
    let mut world = World::new();
    world.pending_cookie_after_the_password().await;

    let wrong = world.client.send_code("000000").await;

    assert_eq!(wrong.status, StatusCode::UNAUTHORIZED);
    assert!(pending_cookie_of(&wrong).contains("Max-Age=0"));
}

#[tokio::test]
async fn the_owner_reads_the_note_the_path_names() {
    let mut world = World::new();
    let owner = world.register(None).await;
    let note = world.write_note(owner).await;
    world.client.log_in(ADA, PASSWORD).await;

    let reply = world.client.get(&format!("/notes/{note}")).await;

    assert_eq!(
        (reply.status, reply.body),
        (StatusCode::OK, owner.to_string())
    );
}

#[tokio::test]
async fn somebody_elses_note_is_not_found_like_a_missing_one() {
    let mut world = World::new();
    world.register(None).await;
    let theirs = world.write_note(EntityId::new()).await;
    world.client.log_in(ADA, PASSWORD).await;

    let other = world.client.get(&format!("/notes/{theirs}")).await;
    let missing = world
        .client
        .get(&format!("/notes/{}", EntityId::new()))
        .await;

    assert_eq!(other.status, StatusCode::NOT_FOUND);
    assert_eq!((other.status, other.body), (missing.status, missing.body));
}

#[tokio::test]
async fn a_policy_may_answer_forbidden_instead() {
    let mut world = World::new();
    world.register(None).await;
    let theirs = world.write_note(EntityId::new()).await;
    world.client.log_in(ADA, PASSWORD).await;

    let reply = world.client.get(&format!("/notes/{theirs}/strict")).await;

    assert_eq!(reply.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn nobody_logged_in_is_unauthorized_whether_or_not_the_note_exists() {
    let mut world = World::new();
    let note = world.write_note(EntityId::new()).await;

    let existing = world.status(&format!("/notes/{note}")).await;
    let missing = world.status(&format!("/notes/{}", EntityId::new())).await;

    assert_eq!(existing, StatusCode::UNAUTHORIZED);
    assert_eq!(missing, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_anonymous_user_is_forbidden_where_the_policy_wants_a_registered_one() {
    let mut world = World::new();
    world.client.post("/anonymous", None).await;
    let note = world.write_note(EntityId::new()).await;

    let reply = world.client.get(&format!("/notes/{note}/registered")).await;

    assert_eq!(reply.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_registered_user_reaches_the_note_under_a_registered_policy() {
    let mut world = World::new();
    let owner = world.register(None).await;
    let note = world.write_note(owner).await;
    world.client.log_in(ADA, PASSWORD).await;

    let reply = world.client.get(&format!("/notes/{note}/registered")).await;

    assert_eq!(
        (reply.status, reply.body),
        (StatusCode::OK, owner.to_string())
    );
}

#[tokio::test]
async fn a_deleted_note_is_hidden_by_a_policy_that_hides_existence() {
    let mut world = World::new();
    let owner = world.register(None).await;
    let note = world.write_note(owner).await;
    world.remove_note(note).await;
    world.client.log_in(ADA, PASSWORD).await;

    let reply = world.client.get(&format!("/notes/{note}")).await;

    assert_eq!(reply.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_deleted_note_is_gone_under_a_policy_that_denies_openly() {
    let mut world = World::new();
    let owner = world.register(None).await;
    let note = world.write_note(owner).await;
    world.remove_note(note).await;
    world.client.log_in(ADA, PASSWORD).await;

    let reply = world.client.get(&format!("/notes/{note}/strict")).await;

    assert_eq!(reply.status, StatusCode::GONE);
}

#[derive(Deserialize)]
struct Back {
    #[serde(default)]
    return_to: ReturnTo,
}

async fn page_login(
    auth: AuthSession<User>,
    state: State<Flow>,
    Query(back): Query<Back>,
    form: Json<LoginForm>,
) -> Result<(LoginStep, ReturnTo), LoginError> {
    Ok((
        user_fixture::login(auth, state, form).await?,
        back.return_to,
    ))
}

fn pages() -> Router<AppState> {
    Router::new()
        .route("/login", post(page_login))
        .route("/me", get(me))
        .route("/acting", get(acting))
        .route("/maybe", get(maybe))
        .route("/registered", get(registered))
        .route("/notes/{id}", get(note))
        .layer(
            LoginRedirect::to(ReturnTo::from_static("/login"))
                .register_at(ReturnTo::from_static("/register")),
        )
}

fn redirected(reply: &support::Reply) -> Option<&str> {
    reply
        .headers
        .get(LOCATION)
        .map(|location| location.to_str().unwrap())
}

#[tokio::test]
async fn a_visitor_of_a_page_is_sent_to_the_login_page_with_the_page_asked_for() {
    let mut world = World::new();

    let reply = world.client.get("/app/me?x=1").await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(
        redirected(&reply),
        Some("/login?return_to=%2Fapp%2Fme%3Fx%3D1")
    );
}

#[tokio::test]
async fn a_visitor_of_a_page_is_sent_to_the_login_page_whether_or_not_the_note_exists() {
    let mut world = World::new();
    let note = world.write_note(EntityId::new()).await;

    let existing = world.client.get(&format!("/app/notes/{note}")).await;
    let missing_id = EntityId::new();
    let missing = world.client.get(&format!("/app/notes/{missing_id}")).await;

    assert_eq!(existing.status, StatusCode::SEE_OTHER);
    assert_eq!(
        redirected(&existing),
        Some(format!("/login?return_to=%2Fapp%2Fnotes%2F{note}").as_str())
    );
    assert_eq!(missing.status, StatusCode::SEE_OTHER);
}

#[tokio::test]
async fn the_json_api_next_to_the_pages_still_answers_unauthorized() {
    let mut world = World::new();

    let reply = world.client.get("/me").await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(redirected(&reply), None);
}

#[tokio::test]
async fn an_anonymous_user_of_a_page_is_sent_to_the_registration_page() {
    let mut world = World::new();
    world.client.post("/anonymous", None).await;

    let reply = world.client.get("/app/registered").await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(
        redirected(&reply),
        Some("/register?return_to=%2Fapp%2Fregistered")
    );
}

#[tokio::test]
async fn an_anonymous_user_of_a_page_is_judged_by_the_policy_when_it_wants_no_registration() {
    let mut world = World::new();
    world.client.post("/anonymous", None).await;
    let note = world.write_note(EntityId::new()).await;

    let reply = world.client.get(&format!("/app/notes/{note}")).await;

    assert_eq!(reply.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_page_with_an_optional_user_is_never_redirected() {
    let mut world = World::new();

    let reply = world.client.get("/app/maybe").await;

    assert_eq!(
        (reply.status, reply.body.as_str()),
        (StatusCode::OK, "nobody")
    );
}

#[tokio::test]
async fn a_logged_in_user_reaches_the_page() {
    let mut world = World::new();
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    let reply = world.client.get("/app/me").await;

    assert_eq!((reply.status, reply.body), (StatusCode::OK, id.to_string()));
}

#[tokio::test]
async fn a_page_without_the_auth_layer_is_a_server_error_not_a_redirect() {
    let mut client = Client::new(
        Router::new().nest(
            "/app",
            Router::new()
                .route("/me", get(me))
                .layer(LoginRedirect::to(ReturnTo::from_static("/login"))),
        ),
    );

    let reply = client.get("/app/me").await;

    assert_eq!(reply.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(redirected(&reply), None);
}

#[tokio::test]
async fn logging_in_from_a_page_goes_back_to_the_page_asked_for() {
    let mut world = World::new();
    let id = world.register(None).await;

    let login = world
        .client
        .log_in_at("/app/login?return_to=%2Fapp%2Fme%3Fx%3D1", ADA, PASSWORD)
        .await;
    let me = world.client.get("/app/me").await;

    assert_eq!(login.status, StatusCode::SEE_OTHER);
    assert_eq!(redirected(&login), Some("/app/me?x=1"));
    assert_eq!((me.status, me.body), (StatusCode::OK, id.to_string()));
}

#[tokio::test]
async fn a_forged_return_target_goes_back_to_the_root() {
    let mut world = World::new();
    world.register(None).await;

    for query in [
        "return_to=%2F%2Fevil.com",
        "return_to=https%3A%2F%2Fevil.com",
        "",
    ] {
        let reply = world
            .client
            .log_in_at(&format!("/app/login?{query}"), ADA, PASSWORD)
            .await;

        assert_eq!(redirected(&reply), Some("/"), "{query}");
    }
}

#[tokio::test]
async fn a_refused_login_from_a_page_is_unauthorized_not_a_redirect() {
    let mut world = World::new();
    world.register(None).await;

    let reply = world
        .client
        .log_in_at("/app/login?return_to=%2Fapp%2Fme", ADA, "wrong")
        .await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(redirected(&reply), None);
}

#[tokio::test]
async fn a_context_is_built_from_the_logged_in_user() {
    let mut world = World::new();
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    let reply = world.client.get("/acting").await;

    assert_eq!((reply.status, reply.body), (StatusCode::OK, id.to_string()));
}

#[tokio::test]
async fn nobody_logged_in_has_no_context() {
    let mut world = World::new();

    assert_eq!(world.status("/acting").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_user_deactivated_since_login_has_no_context() {
    let mut world = World::new();
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    world.deactivate(id).await;

    assert_eq!(world.status("/acting").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn nobody_logged_in_on_a_page_is_sent_to_the_login_page_instead_of_acting() {
    let mut world = World::new();

    let reply = world.client.get("/app/acting").await;

    assert_eq!(reply.status, StatusCode::SEE_OTHER);
    assert_eq!(redirected(&reply), Some("/login?return_to=%2Fapp%2Facting"));
}

#[tokio::test]
async fn a_command_run_with_the_context_sees_the_acting_user() {
    let mut world = World::new();
    let id = world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;

    let reply = world.client.post("/acting/notes", None).await;

    let note = EntityId::from(uuid::Uuid::parse_str(&reply.body).unwrap());
    let note = world.events.repository::<Note>().load(note).await.unwrap();
    assert_eq!(note.owner, id);
}

const SESSION_COOKIE: &str = "id";

fn key(byte: u8) -> Key {
    Key::from(&[byte; 64])
}

#[derive(Clone, Copy)]
enum Protection {
    Signed,
    Private,
}

impl Protection {
    fn sessions(self, key: Key) -> World {
        let sessions = SessionManagerLayer::new(MemoryStore::default());
        match self {
            Self::Signed => World::with_sessions(None, sessions.with_signed(key)),
            Self::Private => World::with_sessions(None, sessions.with_private(key)),
        }
    }

    fn reveal(self, key: &Key, value: &str) -> Option<String> {
        let mut jar = CookieJar::new();
        jar.add_original(Cookie::new(SESSION_COOKIE, value.to_owned()));
        let cookie = match self {
            Self::Signed => jar.signed(key).get(SESSION_COOKIE),
            Self::Private => jar.private(key).get(SESSION_COOKIE),
        };
        cookie.map(|cookie| cookie.value().to_owned())
    }

    fn seal(self, key: &Key, id: &str) -> String {
        let mut jar = CookieJar::new();
        let cookie = Cookie::new(SESSION_COOKIE, id.to_owned());
        match self {
            Self::Signed => jar.signed_mut(key).add(cookie),
            Self::Private => jar.private_mut(key).add(cookie),
        }
        jar.get(SESSION_COOKIE).unwrap().value().to_owned()
    }
}

async fn check_a_protected_cookie_cannot_be_forged(protection: Protection) {
    let mut world = protection.sessions(key(1));
    world.register(None).await;
    world.client.log_in(ADA, PASSWORD).await;
    let issued = world.client.cookies[SESSION_COOKIE].clone();
    let id = protection.reveal(&key(1), &issued).unwrap();

    assert_eq!(world.client.get("/me").await.status, StatusCode::OK);
    for forged in [id.clone(), protection.seal(&key(2), &id)] {
        world
            .client
            .cookies
            .insert(SESSION_COOKIE.to_owned(), forged);
        assert_eq!(
            world.client.get("/me").await.status,
            StatusCode::UNAUTHORIZED
        );
    }
    world
        .client
        .cookies
        .insert(SESSION_COOKIE.to_owned(), protection.seal(&key(1), &id));
    assert_eq!(world.client.get("/me").await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_signed_session_refuses_an_unsigned_or_foreign_signed_cookie() {
    check_a_protected_cookie_cannot_be_forged(Protection::Signed).await;
}

#[tokio::test]
async fn an_encrypted_session_refuses_a_plain_or_foreign_encrypted_cookie() {
    check_a_protected_cookie_cannot_be_forged(Protection::Private).await;
}
