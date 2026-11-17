//! `EventSauceApp::postgres` against a real `PostgreSQL` container: the
//! schemas it creates and the application it wires, driven through an axum
//! router.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use argon2::Params;
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{
        header::{COOKIE, SET_COOKIE},
        Method, Request, StatusCode,
    },
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum_login::{AuthSession, AuthnBackend};
use event_sauce_auth::{
    Argon2Hasher, ClaimAuthenticator, Credentials, Digest, Email, HasPassword, HasSecondFactor,
    Identity, LoginId, LoginStore, MokaUserCache, Password, PasswordAttempt, PasswordHash,
    PasswordHasher, PasswordLogin, Rejected, SecondFactor, Sha256, TwoFactor, UserAggregate,
};
use event_sauce_axum::{
    BackendError, CurrentUser, EventSauceApp, EventSauceBackend, PendingLogin, SessionUser, Store,
    UserBackend,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, Entity, EntityId,
    EventStore, Repository,
};
use event_sauce_postgres::PostgresEventStore;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::{postgres::Postgres, testcontainers::ContainerAsync};
use tower::ServiceExt;
use tower_sessions::{MemoryStore, SessionManagerLayer};

const ADA: &str = "ada@example.com";
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
}

define_events! {
    enum UserEvent for User {
        Registered {
            credentials: Account,
        }
        @init
        => |id, event| {
            User { id, identity: Identity::Registered(event.credentials.clone()) }
        },

        PasswordChanged {
            hash: PasswordHash,
        }
        => |user, event| {
            if let Identity::Registered(account) = &mut user.identity {
                account.password.set_hash(event.hash.clone());
            }
        },
    }
}

aggregate_command! {
    impl User {
        @clock @init fn register(credentials: Account) -> RegisteredEvent { credentials };
        @clock fn change_password(hash: PasswordHash) -> PasswordChangedEvent { hash };
    }
}

type Plain = EventSauceBackend<
    User,
    ClaimAuthenticator<User, PasswordLogin<Argon2Hasher>, PostgresEventStore>,
    PostgresEventStore,
>;
type Guarded = EventSauceBackend<
    User,
    ClaimAuthenticator<User, PasswordLogin<Argon2Hasher>, PostgresEventStore, TwoFactor<FixedCode>>,
    PostgresEventStore,
>;

fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

fn method() -> PasswordLogin<Argon2Hasher> {
    PasswordLogin::new(cheap_hasher())
}

fn hash(password: &str) -> PasswordHash {
    cheap_hasher().hash(&SecretString::from(password)).unwrap()
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct CodeForm {
    code: String,
}

async fn login<B>(mut auth: AuthSession<B>, Json(form): Json<LoginForm>) -> Response
where
    B: AuthnBackend<
            Credentials = PasswordAttempt<Email>,
            User = SessionUser<User>,
            Error = BackendError,
        > + Send
        + Sync
        + 'static,
{
    let attempt = PasswordAttempt::new(
        Email::parse(&form.email).unwrap(),
        SecretString::from(form.password),
    );
    match auth.authenticate(attempt).await {
        Ok(Some(user)) => {
            auth.login(&user).await.unwrap();
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(None) => StatusCode::UNAUTHORIZED.into_response(),
        Err(axum_login::Error::Backend(BackendError::SecondFactorRequired(id))) => (
            StatusCode::ACCEPTED,
            [(
                SET_COOKIE,
                PendingLogin::set_cookie(id, PendingLogin::DEFAULT_TTL),
            )],
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn second_factor(
    mut auth: AuthSession<Guarded>,
    pending: PendingLogin,
    Json(form): Json<CodeForm>,
) -> Response {
    match auth.backend.complete(pending.id(), &form.code).await {
        Ok(user) => {
            auth.login(&user).await.unwrap();
            StatusCode::NO_CONTENT.into_response()
        }
        Err(_) => StatusCode::UNAUTHORIZED.into_response(),
    }
}

async fn me<B: UserBackend<Aggregate = User>>(user: CurrentUser<B>) -> String {
    user.entity_id().to_string()
}

fn routes<B>() -> Router
where
    B: UserBackend<Aggregate = User>
        + AuthnBackend<
            Credentials = PasswordAttempt<Email>,
            User = SessionUser<User>,
            Error = BackendError,
        > + Send
        + Sync
        + 'static,
{
    Router::new()
        .route("/login", post(login::<B>))
        .route("/me", get(me::<B>))
}

struct Reply {
    status: StatusCode,
    headers: http::HeaderMap,
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
        Self::new(self.router.clone())
    }

    fn remember(&mut self, set_cookie: &str) {
        let (name, value) = set_cookie
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

    async fn request(&mut self, method: Method, uri: &str, body: Option<Value>) -> Reply {
        let mut builder = Request::builder().method(method).uri(uri);
        if !self.cookies.is_empty() {
            let cookies = self
                .cookies
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("; ");
            builder = builder.header(COOKIE, cookies);
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
            self.remember(set_cookie.to_str().unwrap());
        }
        Reply {
            status: response.status(),
            headers: response.headers().clone(),
        }
    }

    async fn get(&mut self, uri: &str) -> Reply {
        self.request(Method::GET, uri, None).await
    }

    async fn log_in(&mut self, password: &str) -> Reply {
        self.request(
            Method::POST,
            "/login",
            Some(json!({ "email": ADA, "password": password })),
        )
        .await
    }

    async fn send_code(&mut self, code: &str) -> Reply {
        self.request(
            Method::POST,
            "/login/second-factor",
            Some(json!({ "code": code })),
        )
        .await
    }
}

async fn start_database() -> (ContainerAsync<Postgres>, PgPool) {
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

async fn schema_exists(pool: &PgPool, schema: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
    )
    .bind(schema)
    .fetch_one(pool)
    .await
    .unwrap()
}

fn sessions() -> SessionManagerLayer<MemoryStore> {
    SessionManagerLayer::new(MemoryStore::default())
}

async fn register(store: &Store<PostgresEventStore>, code: Option<&str>) -> EntityId {
    let account = Account {
        password: Password::new(Email::parse(ADA).unwrap(), hash(PASSWORD)),
        code: code.map(str::to_owned),
    };
    let mut user = User::register(account).unwrap();
    let id = user.entity_id();
    store
        .arc()
        .repository::<User>()
        .save(&mut user)
        .await
        .unwrap();
    id
}

async fn change_password(store: &Store<PostgresEventStore>, id: EntityId, password: &str) {
    let hash = hash(password);
    store
        .arc()
        .repository::<User>()
        .modify(id, |user| user.change_password(hash.clone()))
        .await
        .unwrap();
}

async fn check_password_change_ends_sessions(
    pool: PgPool,
    configure: impl FnOnce(
        event_sauce_axum::UserAppBuilder<User, PasswordLogin<Argon2Hasher>>,
    ) -> event_sauce_axum::UserAppBuilder<User, PasswordLogin<Argon2Hasher>>,
) {
    let app = configure(EventSauceApp::postgres(pool).user::<User, _>(method()))
        .build()
        .await
        .unwrap();
    let router = app.layer(routes::<Plain>(), sessions());
    let id = register(&app.store(), None).await;
    let mut client = Client::new(router);
    assert_eq!(client.log_in(PASSWORD).await.status, StatusCode::NO_CONTENT);
    let mut other = client.fork();
    other.log_in(PASSWORD).await;
    assert_eq!(client.get("/me").await.status, StatusCode::OK);

    change_password(&app.store(), id, "new-horse").await;

    assert_eq!(client.get("/me").await.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        client.log_in("new-horse").await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(client.get("/me").await.status, StatusCode::OK);
}

#[tokio::test]
async fn build_creates_the_login_schema_and_is_idempotent() {
    let (_guard, pool) = start_database().await;

    EventSauceApp::postgres(pool.clone())
        .user::<User, _>(method())
        .build()
        .await
        .unwrap();
    let again = EventSauceApp::postgres(pool.clone())
        .user::<User, _>(method())
        .build()
        .await
        .unwrap();

    assert!(schema_exists(&pool, "event_sauce_login").await);
    register(&again.store(), None).await;
}

#[tokio::test]
async fn the_login_store_lives_in_the_schema_it_is_given() {
    let (_guard, pool) = start_database().await;

    EventSauceApp::postgres(pool.clone())
        .user::<User, _>(method())
        .login_store_schema("auth_logins")
        .build()
        .await
        .unwrap();

    assert!(schema_exists(&pool, "auth_logins").await);
    assert!(!schema_exists(&pool, "event_sauce_login").await);
}

#[tokio::test]
async fn a_login_opens_a_session_and_the_response_carries_a_request_id() {
    let (_guard, pool) = start_database().await;
    let app = EventSauceApp::postgres(pool)
        .user::<User, _>(method())
        .build()
        .await
        .unwrap();
    register(&app.store(), None).await;
    let mut client = Client::new(app.layer(routes::<Plain>(), sessions()));

    let before = client.get("/me").await;
    let login = client.log_in(PASSWORD).await;
    let after = client.get("/me").await;
    let wrong = client.fork().log_in("wrong").await;

    assert_eq!(before.status, StatusCode::UNAUTHORIZED);
    assert_eq!(login.status, StatusCode::NO_CONTENT);
    assert_eq!(after.status, StatusCode::OK);
    assert_eq!(wrong.status, StatusCode::UNAUTHORIZED);
    assert!(after.headers.contains_key("x-request-id"));
}

#[tokio::test]
async fn the_correlation_header_is_configurable() {
    let (_guard, pool) = start_database().await;
    let app = EventSauceApp::postgres(pool)
        .user::<User, _>(method())
        .correlation_header("x-correlation-id")
        .build()
        .await
        .unwrap();
    let mut client = Client::new(app.layer(routes::<Plain>(), sessions()));

    let reply = client.get("/me").await;

    assert!(reply.headers.contains_key("x-correlation-id"));
    assert!(!reply.headers.contains_key("x-request-id"));
}

#[tokio::test]
async fn a_password_change_ends_sessions_with_the_default_cache() {
    let (_guard, pool) = start_database().await;
    check_password_change_ends_sessions(pool, |builder| builder).await;
}

#[tokio::test]
async fn a_password_change_ends_sessions_without_a_cache() {
    let (_guard, pool) = start_database().await;
    check_password_change_ends_sessions(pool, |builder| builder.no_user_cache()).await;
}

#[tokio::test]
async fn a_password_change_ends_sessions_with_a_sized_cache() {
    let (_guard, pool) = start_database().await;
    check_password_change_ends_sessions(pool, |builder| builder.user_cache_capacity(3)).await;
}

#[tokio::test]
async fn a_password_change_ends_sessions_with_a_custom_cache() {
    let (_guard, pool) = start_database().await;
    check_password_change_ends_sessions(pool, |builder| builder.user_cache(MokaUserCache::new(3)))
        .await;
}

#[tokio::test]
async fn a_second_factor_is_held_in_the_postgres_login_store() {
    let (_guard, pool) = start_database().await;
    let app = EventSauceApp::postgres(pool.clone())
        .user::<User, _>(method())
        .second_factor(FixedCode)
        .build()
        .await
        .unwrap();
    register(&app.store(), Some(CODE)).await;
    let router = app.layer(
        routes::<Guarded>().route("/login/second-factor", post(second_factor)),
        sessions(),
    );
    let mut client = Client::new(router);

    let first = client.log_in(PASSWORD).await;
    let held: i64 = sqlx::query_scalar("SELECT count(*) FROM event_sauce_login.entries")
        .fetch_one(&pool)
        .await
        .unwrap();
    let before = client.get("/me").await;
    let second = client.send_code(CODE).await;
    let after = client.get("/me").await;

    assert_eq!(first.status, StatusCode::ACCEPTED);
    assert_eq!(held, 1);
    assert_eq!(before.status, StatusCode::UNAUTHORIZED);
    assert_eq!(second.status, StatusCode::NO_CONTENT);
    assert_eq!(after.status, StatusCode::OK);
}

#[tokio::test]
async fn a_given_event_store_is_migrated_and_used() {
    let (_guard, pool) = start_database().await;
    let events = Arc::new(
        PostgresEventStore::builder()
            .pool(pool.clone())
            .schema("custom_events")
            .build()
            .unwrap(),
    );

    let app = EventSauceApp::postgres(pool.clone())
        .user::<User, _>(method())
        .event_store(events)
        .build()
        .await
        .unwrap();
    let id = register(&app.store(), None).await;

    assert!(schema_exists(&pool, "custom_events").await);
    assert!(app
        .store()
        .arc()
        .repository::<User>()
        .exists(id)
        .await
        .unwrap());
}
