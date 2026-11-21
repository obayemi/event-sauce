//! The routes of the example.

use std::sync::Arc;

use crate::member::{Member, MemberCommands};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use event_sauce::auth::{
    AlwaysDone, Argon2Hasher, CachedUserLoader, Email, FirstFactor, LoginId, Logins, Password,
    PasswordAttempt, PasswordHash, PasswordHasher, PasswordLogin,
};
use event_sauce::axum::{
    AuthLayer, AuthSession, CorrelationLayer, CurrentUser, LoginError, LoginStep, SessionError,
    StoreError,
};
use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use secrecy::SecretString;
use serde::Deserialize;
use std::result::Result;
use tower_sessions::{MemoryStore, SessionManagerLayer};

type Factor = FirstFactor<PasswordLogin<Argon2Hasher>, AlwaysDone>;

#[derive(Clone)]
struct AppState {
    store: Arc<InMemoryEventStore>,
    hasher: Argon2Hasher,
    logins: Logins<Member>,
    first: Arc<Factor>,
}

#[derive(Deserialize)]
struct Registration {
    nickname: String,
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
}

#[derive(Deserialize)]
struct NewPassword {
    password: String,
}

async fn hash_password(hasher: &Argon2Hasher, password: String) -> Option<PasswordHash> {
    hasher.hash(&SecretString::from(password)).await.ok()
}

async fn register(
    State(state): State<AppState>,
    Json(form): Json<Registration>,
) -> Result<Response, StoreError> {
    let (Ok(email), Some(hash)) = (
        Email::parse(&form.email),
        hash_password(&state.hasher, form.password).await,
    ) else {
        return Ok(StatusCode::BAD_REQUEST.into_response());
    };
    let Ok(mut member) = Member::join(form.nickname, Password::new(email, hash));
    state.store.repository::<Member>().save(&mut member).await?;
    Ok((StatusCode::CREATED, member.entity_id().to_string()).into_response())
}

async fn login(
    mut auth: AuthSession<Member>,
    State(state): State<AppState>,
    Json(form): Json<LoginForm>,
) -> Result<LoginStep, LoginError> {
    let email = Email::parse(&form.email).map_err(|_| LoginError::refused())?;
    let attempt = PasswordAttempt::new(email, SecretString::from(form.password));
    let login = state.logins.begin(&state.first, attempt).await?;
    auth.finish(login).await
}

async fn logout(mut auth: AuthSession<Member>) -> Result<StatusCode, SessionError> {
    auth.logout().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn me(member: CurrentUser<Member>) -> String {
    member.nickname.clone()
}

async fn change_password(
    State(state): State<AppState>,
    member: CurrentUser<Member>,
    Json(form): Json<NewPassword>,
) -> Result<StatusCode, StoreError> {
    let Some(hash) = hash_password(&state.hasher, form.password).await else {
        return Ok(StatusCode::BAD_REQUEST);
    };
    state
        .store
        .repository::<Member>()
        .modify(member.entity_id(), |member| {
            member.change_password(hash.clone())
        })
        .await
        .map_err(Error::from)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Builds the application: an in-memory event store, in-memory sessions and
/// the routes above, behind the correlation, session and authentication
/// layers.
pub fn router(hasher: Argon2Hasher) -> Router {
    let store = Arc::new(InMemoryEventStore::builder().build());
    let first = Arc::new(FirstFactor::new(
        PasswordLogin::new(hasher.clone()),
        AlwaysDone,
    ));
    let auth = AuthLayer::<Member>::new(CachedUserLoader::new(store.clone()));
    let state = AppState {
        logins: Logins::new(store.clone()),
        store,
        hasher,
        first,
    };
    let sessions = SessionManagerLayer::new(MemoryStore::default());
    let routes = Router::new()
        .route("/members", post(register))
        .route("/login", post(login))
        .route("/logout", post(logout))
        .route("/me", get(me))
        .route("/password", post(change_password))
        .with_state(state);
    auth.layer(routes, sessions).layer(CorrelationLayer::new())
}
