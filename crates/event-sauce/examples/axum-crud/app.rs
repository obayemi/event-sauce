//! The state, the backend and the router of the example.

use axum::{
    extract::FromRef,
    routing::{get, post, put},
    Router,
};
use event_sauce::auth::{Argon2Hasher, ClaimAuthenticator, PasswordLogin};
use event_sauce::axum::{EventSauceApp, EventSauceBackend, Store};
use event_sauce::postgres::PostgresEventStore;
use sqlx::PgPool;
use tower_sessions::{MemoryStore, SessionManagerLayer};

use crate::member::Member;
use crate::projection::Projector;
use crate::{auth_routes, note_routes};

type Authenticator = ClaimAuthenticator<Member, PasswordLogin<Argon2Hasher>, PostgresEventStore>;

/// The `axum-login` backend of the members, as the extractors name it.
pub type Backend = EventSauceBackend<Member, Authenticator, PostgresEventStore>;

/// What the handlers reach through `FromRef`: the event store behind `Repo`,
/// the pool the read model is queried on and the password hasher.
#[derive(Clone, FromRef)]
pub struct AppState {
    store: Store<PostgresEventStore>,
    pool: PgPool,
    hasher: Argon2Hasher,
}

/// The application: its router and the projector that feeds the list of
/// notes, to be run next to it.
pub struct Service {
    pub router: Router,
    pub projector: Projector,
}

fn routes() -> Router<AppState> {
    Router::new()
        .route("/register", post(auth_routes::register))
        .route("/login", post(auth_routes::login))
        .route("/logout", post(auth_routes::logout))
        .route("/password", put(auth_routes::change_password))
        .route("/me", get(auth_routes::me))
        .route("/notes", post(note_routes::create).get(note_routes::list))
        .route(
            "/notes/{id}",
            get(note_routes::read)
                .put(note_routes::replace)
                .patch(note_routes::amend)
                .delete(note_routes::remove),
        )
}

/// Builds the application on `pool`: runs the migrations of the event store,
/// of the login store and of the read model, then wires the routes behind
/// the correlation, session and authentication layers.
///
/// Sessions live in memory and their cookie is not marked `Secure`, which
/// suits a local run. Behind TLS, use a shared session store and
/// `with_secure(true)`.
///
/// # Errors
///
/// Whatever a migration fails with.
pub async fn build(pool: PgPool, hasher: Argon2Hasher) -> anyhow::Result<Service> {
    let projector = Projector::setup(pool.clone()).await?;
    let app = EventSauceApp::postgres(pool.clone())
        .user::<Member, _>(PasswordLogin::new(hasher.clone()))
        .event_store(projector.event_store())
        .build()
        .await?;
    let state = AppState {
        store: app.store(),
        pool,
        hasher,
    };
    let sessions = SessionManagerLayer::new(MemoryStore::default()).with_secure(false);
    Ok(Service {
        router: app.layer(routes(), sessions).with_state(state),
        projector,
    })
}
