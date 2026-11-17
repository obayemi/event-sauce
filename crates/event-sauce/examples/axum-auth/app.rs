//! The aggregate, the backend and the routes of the example.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum_login::{AuthManagerLayerBuilder, AuthSession};
use event_sauce::auth::{
    Argon2Hasher, ClaimAuthenticator, Email, Identity, LoginId, Password, PasswordAttempt,
    PasswordHash, PasswordHasher, PasswordLogin,
};
use event_sauce::axum::{CorrelationLayer, CurrentUser, EventSauceBackend};
use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use tower_sessions::{MemoryStore, SessionManagerLayer};

#[derive(Debug, thiserror::Error)]
pub enum MemberError {}

impl AggregateError for MemberError {}

#[aggregate(event = "MemberEvent", error = "MemberError", init, user)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Member {
    #[id]
    id: EntityId,
    #[user(identity)]
    identity: Identity<Password<Email>>,
    nickname: String,
}

define_events! {
    pub enum MemberEvent for Member {
        Joined {
            nickname: String,
            credentials: Password<Email>,
        }
        @init
        => |id, event| {
            Member {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                nickname: event.nickname.clone(),
            }
        },

        PasswordChanged {
            hash: PasswordHash,
        }
        => |member, event| {
            if let Identity::Registered(credentials) = &mut member.identity {
                credentials.set_hash(event.hash.clone());
            }
        },
    }
}

aggregate_command! {
    impl Member {
        @clock @init fn join(nickname: String, credentials: Password<Email>) -> JoinedEvent {
            nickname, credentials
        };
        @clock fn change_password(hash: PasswordHash) -> PasswordChangedEvent { hash };
    }
}

type Authenticator = ClaimAuthenticator<Member, PasswordLogin<Argon2Hasher>, InMemoryEventStore>;
type Backend = EventSauceBackend<Member, Authenticator, InMemoryEventStore>;

#[derive(Clone)]
struct AppState {
    store: Arc<InMemoryEventStore>,
    hasher: Argon2Hasher,
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

fn hash_password(hasher: &Argon2Hasher, password: String) -> Option<PasswordHash> {
    hasher.hash(&SecretString::from(password)).ok()
}

async fn register(State(state): State<AppState>, Json(form): Json<Registration>) -> Response {
    let (Ok(email), Some(hash)) = (
        Email::parse(&form.email),
        hash_password(&state.hasher, form.password),
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

async fn login(mut auth: AuthSession<Backend>, Json(form): Json<LoginForm>) -> StatusCode {
    let Ok(email) = Email::parse(&form.email) else {
        return StatusCode::UNAUTHORIZED;
    };
    let attempt = PasswordAttempt::new(email, SecretString::from(form.password));
    match auth.authenticate(attempt).await {
        Ok(Some(member)) => match auth.login(&member).await {
            Ok(()) => StatusCode::NO_CONTENT,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        },
        Ok(None) => StatusCode::UNAUTHORIZED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn logout(mut auth: AuthSession<Backend>) -> StatusCode {
    match auth.logout().await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn me(member: CurrentUser<Backend>) -> String {
    member.nickname.clone()
}

async fn change_password(
    State(state): State<AppState>,
    member: CurrentUser<Backend>,
    Json(form): Json<NewPassword>,
) -> StatusCode {
    let Some(hash) = hash_password(&state.hasher, form.password) else {
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
    let authenticator: Authenticator =
        ClaimAuthenticator::new(store.clone(), PasswordLogin::new(hasher.clone()));
    let backend = EventSauceBackend::new(authenticator, store.clone());
    let sessions = SessionManagerLayer::new(MemoryStore::default());
    Router::new()
        .route("/members", post(register))
        .route("/login", post(login))
        .route("/logout", post(logout))
        .route("/me", get(me))
        .route("/password", post(change_password))
        .with_state(AppState { store, hasher })
        .layer(AuthManagerLayerBuilder::new(backend, sessions).build())
        .layer(CorrelationLayer::new())
}
