//! Login, logout, second factor and session invalidation through a real
//! axum router, with tower-sessions' memory store and the in-memory event
//! store.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use argon2::Params;
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::State,
    http::{
        header::{COOKIE, SET_COOKIE},
        Method, Request, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum_login::{AuthManagerLayerBuilder, AuthSession};
use event_sauce_auth::{
    Argon2Hasher, ClaimAuthenticator, Credentials, Digest, Email, HasPassword, HasSecondFactor,
    Identity, LoginId, LoginKey, LoginOutcome, LoginStore, LoginStoreError, MokaUserCache,
    Password, PasswordAttempt, PasswordHash, PasswordHasher, PasswordLogin, Rejected, SecondFactor,
    Sha256, TwoFactor, UserAggregate,
};
use event_sauce_axum::{
    BackendError, CurrentUser, EventSauceBackend, MaybeUser, PendingLogin, Registered, SessionUser,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, AggregateRoot,
    Entity, EntityId, EventStore, Repository,
};
use event_sauce_memory::InMemoryEventStore;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower::ServiceExt;
use tower_sessions::{MemoryStore, SessionManagerLayer};

const PASSWORD: &str = "correct-horse";
const CODE: &str = "424242";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Account {
    password: Password<Email>,
    code: Option<String>,
}

impl Credentials for Account {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.password.claims()
    }

    fn fingerprint(&self, digest: &mut Sha256) {
        self.password.fingerprint(digest);
        if let Some(code) = &self.code {
            digest.update(code.as_bytes());
        }
    }
}

impl HasPassword for Account {
    type Login = Email;

    fn password(&self) -> Option<&Password<Email>> {
        Some(&self.password)
    }
}

impl HasSecondFactor for Account {
    fn requires_second_factor(&self) -> bool {
        self.code.is_some()
    }
}

struct FixedCode;

#[async_trait]
impl SecondFactor<Account> for FixedCode {
    type Challenge = ();
    type Response = String;

    fn challenge(&self, _stored: &Account) {}

    async fn verify(
        &self,
        stored: &Account,
        _challenge: &(),
        response: &String,
        _store: &dyn LoginStore,
    ) -> Result<(), Rejected> {
        if stored.code.as_ref() == Some(response) {
            Ok(())
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum UserError {}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: EntityId,
    identity: Identity<Account>,
    active: bool,
}

impl Entity for User {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for User {
    type Event = UserEvent;
    type Error = UserError;
    type DeletedState = Self;

    fn claims(&self) -> Vec<AggregateClaim> {
        self.identity
            .credentials()
            .map(Credentials::claims)
            .unwrap_or_default()
    }
}

impl UserAggregate for User {
    type Credentials = Account;

    fn identity(&self) -> &Identity<Account> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

define_events! {
    enum UserEvent for User {
        Registered {
            credentials: Account,
        }
        @init
        => |id, event| {
            User {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                active: true,
            }
        },

        Anonymous {}
        @init
        => |id, _event| {
            User { id, identity: Identity::Anonymous, active: true }
        },

        PasswordChanged {
            hash: PasswordHash,
        }
        => |user, event| {
            if let Identity::Registered(account) = &mut user.identity {
                account.password.set_hash(event.hash.clone());
            }
        },

        SecondFactorEnrolled {
            code: String,
        }
        => |user, event| {
            if let Identity::Registered(account) = &mut user.identity {
                account.code = Some(event.code.clone());
            }
        },

        Deactivated {}
        => |user, _event| {
            user.active = false;
        },
    }
}

aggregate_command! {
    impl User {
        @clock @init fn register(credentials: Account) -> RegisteredEvent { credentials };
        @clock @init fn anonymous() -> AnonymousEvent { };
        @clock fn change_password(hash: PasswordHash) -> PasswordChangedEvent { hash };
        @clock fn enroll_second_factor(code: String) -> SecondFactorEnrolledEvent { code };
        @clock fn deactivate() -> DeactivatedEvent { };
    }
}

#[derive(Default)]
struct MemoryLoginStore {
    entries: Mutex<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl LoginStore for MemoryLoginStore {
    async fn put(&self, key: &LoginKey, value: &[u8], _: Duration) -> Result<(), LoginStoreError> {
        self.entries
            .lock()
            .unwrap()
            .insert(key.as_str().to_owned(), value.to_vec());
        Ok(())
    }

    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        Ok(self.entries.lock().unwrap().remove(key.as_str()))
    }

    async fn mark_once(&self, key: &LoginKey, _: Duration) -> Result<bool, LoginStoreError> {
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

fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

fn hash(password: &str) -> PasswordHash {
    cheap_hasher().hash(&SecretString::from(password)).unwrap()
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
}

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

#[derive(Deserialize)]
struct CodeForm {
    code: String,
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

struct Reply {
    status: StatusCode,
    set_cookies: Vec<String>,
    body: String,
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

    fn fork(&self) -> Self {
        Self {
            router: self.router.clone(),
            cookies: BTreeMap::new(),
        }
    }

    fn remember(&mut self, set_cookie: &str) {
        let pair = set_cookie.split(';').next().unwrap();
        let (name, value) = pair.split_once('=').unwrap();
        if value.is_empty() || set_cookie.contains("Max-Age=0") {
            self.cookies.remove(name);
        } else {
            self.cookies.insert(name.to_owned(), value.to_owned());
        }
    }

    fn cookie_header(&self) -> String {
        self.cookies
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    async fn request(&mut self, method: Method, uri: &str, body: Option<Value>) -> Reply {
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
        let status = response.status();
        let set_cookies: Vec<String> = response
            .headers()
            .get_all(SET_COOKIE)
            .iter()
            .map(|value| value.to_str().unwrap().to_owned())
            .collect();
        for set_cookie in &set_cookies {
            self.remember(set_cookie);
        }
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        Reply {
            status,
            set_cookies,
            body: String::from_utf8(bytes.to_vec()).unwrap(),
        }
    }

    async fn get(&mut self, uri: &str) -> Reply {
        self.request(Method::GET, uri, None).await
    }

    async fn post(&mut self, uri: &str, body: Option<Value>) -> Reply {
        self.request(Method::POST, uri, body).await
    }

    async fn log_in(&mut self, email: &str, password: &str) -> Reply {
        self.post(
            "/login",
            Some(json!({ "email": email, "password": password })),
        )
        .await
    }

    async fn send_code(&mut self, code: &str) -> Reply {
        self.post("/login/second-factor", Some(json!({ "code": code })))
            .await
    }
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

    async fn register(&self, email: &str, code: Option<&str>) -> EntityId {
        let account = Account {
            password: Password::new(Email::parse(email).unwrap(), hash(PASSWORD)),
            code: code.map(str::to_owned),
        };
        let mut user = User::register(account).unwrap();
        let id = user.entity_id();
        self.events
            .repository::<User>()
            .save(&mut user)
            .await
            .unwrap();
        id
    }

    async fn change_password(&self, id: EntityId, password: &str) {
        let hash = hash(password);
        self.events
            .repository::<User>()
            .modify(id, |user| user.change_password(hash.clone()))
            .await
            .unwrap();
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

const ADA: &str = "ada@example.com";

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
    let id = world.register(ADA, None).await;

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
    world.register(ADA, None).await;

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
    world.register(ADA, None).await;
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
    world.register(ADA, None).await;
    world.client.get("/maybe").await;
    let before = world.client.cookies.clone();

    world.client.log_in(ADA, PASSWORD).await;

    assert_ne!(world.client.cookies, before);
}

#[tokio::test]
async fn a_second_factor_holds_the_login_back_until_it_is_proven() {
    let mut world = World::new();
    world.register(ADA, Some(CODE)).await;

    let first = world.client.log_in(ADA, PASSWORD).await;
    let before = world.client.get("/me").await;
    let second = world.client.send_code(CODE).await;
    let after = world.client.get("/me").await;

    assert_eq!(first.status, StatusCode::ACCEPTED);
    assert!(first
        .set_cookies
        .iter()
        .any(|cookie| cookie.starts_with(PendingLogin::COOKIE)));
    assert_eq!(before.status, StatusCode::UNAUTHORIZED);
    assert_eq!(second.status, StatusCode::NO_CONTENT);
    assert_eq!(after.status, StatusCode::OK);
}

#[tokio::test]
async fn completing_the_login_removes_the_pending_cookie() {
    let mut world = World::new();
    world.register(ADA, Some(CODE)).await;
    world.client.log_in(ADA, PASSWORD).await;

    world.client.send_code(CODE).await;

    assert!(!world.client.cookies.contains_key(PendingLogin::COOKIE));
}

#[tokio::test]
async fn a_wrong_code_opens_no_session_and_ends_the_pending_login() {
    let mut world = World::new();
    world.register(ADA, Some(CODE)).await;
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
    world.register(ADA, Some(CODE)).await;
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
    world.register(ADA, Some(CODE)).await;

    let reply = world.client.send_code(CODE).await;

    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_pending_login_cannot_be_completed_by_a_forged_cookie() {
    let mut world = World::new();
    world.register(ADA, Some(CODE)).await;
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
    world.register(ADA, Some(CODE)).await;

    let reply = world
        .client
        .post(
            "/login/axum-login",
            Some(json!({ "email": ADA, "password": PASSWORD })),
        )
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
    world.register(ADA, None).await;

    let reply = world
        .client
        .post(
            "/login/axum-login",
            Some(json!({ "email": ADA, "password": PASSWORD })),
        )
        .await;

    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert_eq!(world.client.get("/me").await.status, StatusCode::OK);
}

#[tokio::test]
async fn axum_login_authenticate_refuses_a_wrong_password_without_an_error() {
    let mut world = World::new();
    world.register(ADA, None).await;

    let reply = world
        .client
        .post(
            "/login/axum-login",
            Some(json!({ "email": ADA, "password": "wrong-horse" })),
        )
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
    let id = world.register(ADA, None).await;
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
    let id = world.register(ADA, None).await;
    world.client.log_in(ADA, PASSWORD).await;

    world.enroll_second_factor(id).await;

    assert_eq!(
        world.client.get("/me").await.status,
        StatusCode::UNAUTHORIZED
    );
}

async fn deactivating_a_user_ends_their_sessions(mut world: World) {
    let id = world.register(ADA, None).await;
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
    let id = world.register(ADA, None).await;
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
