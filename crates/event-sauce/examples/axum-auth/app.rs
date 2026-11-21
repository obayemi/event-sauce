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
    AlwaysDone, Argon2Hasher, AuthError, CachedUserLoader, Email, FirstFactor, LoginId, Logins,
    Password, PasswordAttempt, PasswordHash, PasswordHasher, PasswordLogin,
};
use event_sauce::axum::{AuthLayer, AuthSession, CorrelationLayer, CurrentUser};
use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use secrecy::SecretString;
use serde::Deserialize;
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

async fn register(State(state): State<AppState>, Json(form): Json<Registration>) -> Response {
    let (Ok(email), Some(hash)) = (
        Email::parse(&form.email),
        hash_password(&state.hasher, form.password).await,
    ) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(mut member) = Member::join(form.nickname, Password::new(email, hash));
    match state.store.repository::<Member>().save(&mut member).await {
        Ok(()) => (StatusCode::CREATED, member.entity_id().to_string()).into_response(),
        Err(error) if error.as_claim_conflict().is_some() => StatusCode::CONFLICT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn login(
    mut auth: AuthSession<Member>,
    State(state): State<AppState>,
    Json(form): Json<LoginForm>,
) -> StatusCode {
    let Ok(email) = Email::parse(&form.email) else {
        return StatusCode::UNAUTHORIZED;
    };
    let attempt = PasswordAttempt::new(email, SecretString::from(form.password));
    let login = match state.logins.begin(&state.first, attempt).await {
        Ok(login) => login,
        Err(AuthError::InvalidCredentials | AuthError::Inactive | AuthError::Unknown { .. }) => {
            return StatusCode::UNAUTHORIZED;
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    let Ok(authenticated) = login.finish().await else {
        return StatusCode::UNAUTHORIZED;
    };
    match auth.login(authenticated).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn logout(mut auth: AuthSession<Member>) -> StatusCode {
    match auth.logout().await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn me(member: CurrentUser<Member>) -> String {
    member.nickname.clone()
}

async fn change_password(
    State(state): State<AppState>,
    member: CurrentUser<Member>,
    Json(form): Json<NewPassword>,
) -> StatusCode {
    let Some(hash) = hash_password(&state.hasher, form.password).await else {
        return StatusCode::BAD_REQUEST;
    };
    let changed = state
        .store
        .repository::<Member>()
        .modify(member.entity_id(), |member| {
            member.change_password(hash.clone())
        })
        .await;
    match changed {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
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
