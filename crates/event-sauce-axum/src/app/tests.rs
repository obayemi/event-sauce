use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::{body::Body, routing::get, Router};
use axum_login::{
    tower_sessions::{MemoryStore, SessionManagerLayer},
    AuthnBackend,
};
use event_sauce_auth::{
    Argon2Hasher, ClaimAuthenticator, LoginKey, LoginStore, LoginStoreError, MokaUserCache,
    PasswordLogin,
};
use event_sauce_core::{EventStore, Repository};
use event_sauce_memory::InMemoryEventStore;
use http::{Request, StatusCode};
use sqlx::PgPool;
use tower::ServiceExt;

use super::{App, BuildError, CacheChoice, EventSauceApp, UserAppBuilder, DEFAULT_CACHE_CAPACITY};
use crate::login::test_support::{registered, Member};
use crate::{EventSauceBackend, MaybeUser, DEFAULT_HEADER};

struct NoLoginStore;

#[async_trait]
impl LoginStore for NoLoginStore {
    async fn put(&self, _: &LoginKey, _: &[u8], _: Duration) -> Result<(), LoginStoreError> {
        Ok(())
    }

    async fn take(&self, _: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        Ok(None)
    }

    async fn mark_once(&self, _: &LoginKey, _: Duration) -> Result<bool, LoginStoreError> {
        Ok(true)
    }
}

type Method = PasswordLogin<Argon2Hasher>;
type Authenticator = ClaimAuthenticator<Member, Method, InMemoryEventStore>;
type TestApp = App<Member, Authenticator, InMemoryEventStore>;
type Backend = EventSauceBackend<Member, Authenticator, InMemoryEventStore>;

fn builder() -> UserAppBuilder<Member, Method> {
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
async fn the_user_cache_defaults_to_moka_with_ten_thousand_entries() {
    assert_eq!(DEFAULT_CACHE_CAPACITY, 10_000);
    assert!(matches!(
        builder().cache,
        CacheChoice::Moka(DEFAULT_CACHE_CAPACITY)
    ));
}

#[tokio::test]
async fn the_cache_capacity_is_overridden() {
    assert!(matches!(
        builder().user_cache_capacity(42).cache,
        CacheChoice::Moka(42)
    ));
}

#[tokio::test]
async fn the_cache_is_removed() {
    assert!(matches!(
        builder().no_user_cache().cache,
        CacheChoice::Disabled
    ));
}

#[tokio::test]
async fn a_custom_cache_replaces_the_default_and_the_last_choice_wins() {
    let custom = builder().user_cache(MokaUserCache::new(5));
    assert!(matches!(custom.cache, CacheChoice::Shared(_)));
    assert!(matches!(
        custom.user_cache_capacity(7).cache,
        CacheChoice::Moka(7)
    ));
}

#[tokio::test]
async fn an_invalid_login_schema_is_refused_before_touching_the_database() {
    let error = builder()
        .login_store_schema("Not A Schema")
        .build()
        .await
        .unwrap_err();

    assert!(matches!(error, BuildError::LoginSchema(_)));
}

#[tokio::test]
async fn the_layer_installs_the_authentication_and_echoes_the_default_header() {
    let response = serve(&assembled(builder())).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key(DEFAULT_HEADER));
}

#[tokio::test]
async fn the_layer_echoes_the_configured_header() {
    let response = serve(&assembled(builder().correlation_header("x-correlation-id"))).await;

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
