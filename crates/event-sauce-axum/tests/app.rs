//! `EventSauceApp::postgres` against a database of its own on a shared
//! `PostgreSQL` container: the schemas it creates and the application it
//! wires, driven through an axum router.

#[path = "support/password.rs"]
mod password;
mod support;
mod user_fixture;

use std::sync::Arc;

use axum::{
    http::{HeaderName, StatusCode},
    routing::get,
    Router,
};
use event_sauce_auth::{Argon2Hasher, MokaUserCache, PasswordLogin};
use event_sauce_axum::{App, CurrentUser, EventSauceApp, UserAppBuilder};
use event_sauce_core::{EventStore, Repository};
use event_sauce_postgres::PostgresEventStore;
use sqlx::{postgres::PgPoolOptions, PgPool};
use support::Client;
use testcontainers_shared::postgres::{fresh_database, FreshDatabase};
use tower_sessions::{MemoryStore, SessionManagerLayer};
use user_fixture::{
    change_password, cheap_hasher, login_routes, register, CodeIfEnrolled, Flow, PasswordFactor,
    StepClient as _, User, ADA, CODE, MAIL_CODE, PASSWORD,
};

type Method = PasswordLogin<Argon2Hasher>;
type Built = App<User, PasswordFactor, PostgresEventStore>;

fn method() -> Method {
    PasswordLogin::new(cheap_hasher())
}

fn builder(pool: PgPool) -> UserAppBuilder<User, Method> {
    EventSauceApp::postgres(pool).user::<User, _>(method())
}

async fn built(pool: PgPool) -> Built {
    builder(pool).route(CodeIfEnrolled).build().await.unwrap()
}

async fn me(user: CurrentUser<User>) -> String {
    user.entity_id().to_string()
}

fn flow(app: &Built) -> Flow {
    Flow {
        logins: app.logins(),
        pending: app.pending(),
        first: app.first_factor(),
    }
}

fn router(app: &Built) -> Router {
    let routes = login_routes().route("/me", get(me)).with_state(flow(app));
    app.layer(routes, sessions())
}

async fn start_database() -> (FreshDatabase, PgPool) {
    let (database, url) = fresh_database().await;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await
        .unwrap();
    (database, pool)
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

async fn held_logins(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM event_sauce_login.entries")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn check_password_change_ends_sessions(
    pool: PgPool,
    configure: impl FnOnce(UserAppBuilder<User, Method>) -> UserAppBuilder<User, Method>,
) {
    let app = configure(builder(pool))
        .route(CodeIfEnrolled)
        .build()
        .await
        .unwrap();
    let id = register(app.store().arc(), None).await;
    let mut client = Client::new(router(&app));
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

    built(pool.clone()).await;
    let again = built(pool.clone()).await;

    assert!(schema_exists(&pool, "event_sauce_login").await);
    register(again.store().arc(), None).await;
}

#[tokio::test]
async fn the_login_store_lives_in_the_schema_it_is_given() {
    let (_guard, pool) = start_database().await;

    builder(pool.clone())
        .route(CodeIfEnrolled)
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
    let app = built(pool).await;
    let id = register(app.store().arc(), None).await;
    let mut client = Client::new(router(&app));

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
    let app = builder(pool)
        .route(CodeIfEnrolled)
        .correlation_header(HeaderName::from_static("x-correlation-id"))
        .build()
        .await
        .unwrap();
    let mut client = Client::new(router(&app));

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
    check_password_change_ends_sessions(pool, UserAppBuilder::no_user_cache).await;
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
async fn a_multi_step_login_waits_in_the_postgres_login_store() {
    let (_guard, pool) = start_database().await;
    let app = built(pool.clone()).await;
    register(app.store().arc(), Some(CODE)).await;
    let mut client = Client::new(router(&app));

    let password = client.log_in(ADA, PASSWORD).await;
    let held = held_logins(&pool).await;
    let before = client.get("/me").await;
    let code = client.send_code(CODE).await;
    let mail = client.send_mail(MAIL_CODE).await;
    let after = client.get("/me").await;

    assert_eq!(password.status, StatusCode::ACCEPTED);
    assert_eq!(held, 1);
    assert_eq!(before.status, StatusCode::UNAUTHORIZED);
    assert_eq!(code.status, StatusCode::ACCEPTED);
    assert_eq!(mail.status, StatusCode::NO_CONTENT);
    assert_eq!(after.status, StatusCode::OK);
    assert_eq!(held_logins(&pool).await, 0);
}

#[tokio::test]
async fn a_pending_login_resumes_once_across_instances() {
    let (_guard, pool) = start_database().await;
    let first = built(pool.clone()).await;
    let second = built(pool).await;
    register(first.store().arc(), Some(CODE)).await;
    let mut client = Client::new(router(&first));
    let mut elsewhere = Client::new(router(&second));
    client.log_in(ADA, PASSWORD).await;
    elsewhere.cookies.clone_from(&client.cookies);

    let there = elsewhere.send_code(CODE).await;
    let here = client.send_code(CODE).await;

    assert_eq!(there.status, StatusCode::ACCEPTED);
    assert_eq!(here.status, StatusCode::UNAUTHORIZED);
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

    let app = builder(pool.clone())
        .route(CodeIfEnrolled)
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
