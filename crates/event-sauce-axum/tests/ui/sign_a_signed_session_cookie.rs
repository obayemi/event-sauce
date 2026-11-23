use axum::Router;
use event_sauce_auth::{Argon2Hasher, Email, Password, PasswordLogin, UserAggregate};
use event_sauce_axum::EventSauceApp;
use serde::{de::DeserializeOwned, Serialize};
use tower_sessions::{cookie::Key, MemoryStore, SessionManagerLayer};

async fn router<U>(pool: sqlx::PgPool, key: Key) -> Router
where
    U: UserAggregate<Credentials = Password<Email>> + 'static,
    U::DeletedState: Serialize + DeserializeOwned,
{
    let app = EventSauceApp::postgres(pool)
        .user::<U, _>(PasswordLogin::new(Argon2Hasher::default()))
        .sign_cookie(key.clone())
        .build()
        .await
        .unwrap();
    let sessions = SessionManagerLayer::new(MemoryStore::default()).with_signed(key);
    app.layer(Router::new(), sessions)
}

fn main() {}
