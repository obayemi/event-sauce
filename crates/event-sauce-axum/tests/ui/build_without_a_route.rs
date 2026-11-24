use event_sauce_auth::{Argon2Hasher, HasPassword, PasswordLogin, UserAggregate};
use event_sauce_axum::EventSauceApp;
use serde::{de::DeserializeOwned, Serialize};

async fn build<U>(pool: sqlx::PgPool)
where
    U: UserAggregate,
    U::Credentials: HasPassword,
    U::DeletedState: Serialize + DeserializeOwned,
    PasswordLogin<Argon2Hasher>: event_sauce_auth::LoginMethod<U::Credentials>,
{
    let _ = EventSauceApp::postgres(pool)
        .user::<U, _>(PasswordLogin::new(Argon2Hasher::default()))
        .build()
        .await;
}

fn main() {}
