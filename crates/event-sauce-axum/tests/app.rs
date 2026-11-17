//! `EventSauceApp::postgres` against a real `PostgreSQL` container: the
//! schemas it creates and the application it wires, driven through an axum
//! router.

mod support;
mod user_fixture;

use std::{sync::Arc, time::Duration};

use axum::{
    http::{header::SET_COOKIE, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum_login::{AuthSession, AuthnBackend};
use event_sauce_auth::{
    Argon2Hasher, ClaimAuthenticator, Email, LoginId, MokaUserCache, PasswordAttempt,
    PasswordLogin, TwoFactor,
};
use event_sauce_axum::{
    BackendError, CurrentUser, EventSauceApp, EventSauceBackend, PendingLogin, UserBackend,
};
use event_sauce_core::{EventStore, Repository};
use event_sauce_postgres::PostgresEventStore;
use secrecy::SecretString;
use sqlx::{postgres::PgPoolOptions, PgPool};
use support::Client;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::{postgres::Postgres, testcontainers::ContainerAsync};
use tower_sessions::{MemoryStore, SessionManagerLayer};
use user_fixture::{
    change_password, cheap_hasher, register, CodeForm, FixedCode, LoginForm, User, ADA, CODE,
    PASSWORD,
};

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

fn method() -> PasswordLogin<Argon2Hasher> {
    PasswordLogin::new(cheap_hasher())
}

async fn login<B>(mut auth: AuthSession<B>, Json(form): Json<LoginForm>) -> Response
where
    B: UserBackend<Aggregate = User>
        + AuthnBackend<Credentials = PasswordAttempt<Email>, Error = BackendError>,
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
        + AuthnBackend<Credentials = PasswordAttempt<Email>, Error = BackendError>,
{
    Router::new()
        .route("/login", post(login::<B>))
        .route("/me", get(me::<B>))
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
    let id = register(app.store().arc(), None).await;
    let mut client = Client::new(router);
    assert_eq!(
        client.log_in(ADA, PASSWORD).await.status,
        StatusCode::NO_CONTENT
    );
    let mut other = client.fork();
    other.log_in(ADA, PASSWORD).await;
    assert_eq!(client.get("/me").await.status, StatusCode::OK);

    change_password(app.store().arc(), id, "new-horse").await;

    assert_eq!(client.get("/me").await.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        client.log_in(ADA, "new-horse").await.status,
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
    register(again.store().arc(), None).await;
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
    let id = register(app.store().arc(), None).await;
    let mut client = Client::new(app.layer(routes::<Plain>(), sessions()));

    let before = client.get("/me").await;
    let login = client.log_in(ADA, PASSWORD).await;
    let after = client.get("/me").await;
    let wrong = client.fork().log_in(ADA, "wrong").await;

    assert_eq!(before.status, StatusCode::UNAUTHORIZED);
    assert_eq!(login.status, StatusCode::NO_CONTENT);
    assert_eq!(after.status, StatusCode::OK);
    assert_eq!(after.body, id.to_string());
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
    register(app.store().arc(), Some(CODE)).await;
    let router = app.layer(
        routes::<Guarded>().route("/login/second-factor", post(second_factor)),
        sessions(),
    );
    let mut client = Client::new(router);

    let first = client.log_in(ADA, PASSWORD).await;
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
    let id = register(app.store().arc(), None).await;

    assert!(schema_exists(&pool, "custom_events").await);
    assert!(app
        .store()
        .arc()
        .repository::<User>()
        .exists(id)
        .await
        .unwrap());
}
