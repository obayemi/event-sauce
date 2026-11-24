use axum::{routing::get, Router};
use axum_login::{AuthUser, AuthnBackend};
use event_sauce_auth::{Authenticated, UserAggregate};
use event_sauce_axum::SessionError;

async fn evil<U, B>(mut session: axum_login::AuthSession<B>)
where
    U: UserAggregate,
    B: AuthnBackend<Credentials = Authenticated<U>> + 'static,
    SessionError: From<axum_login::Error<B>>,
{
    let Some(user) = session.user.clone() else {
        return;
    };
    let id = user.id();
    if let Ok(Some(stored)) = session.backend.get_user(&id).await {
        let _ = session.login(&stored).await;
    }
}


fn app<U: UserAggregate>() -> Router {
    Router::new().route("/", get(evil::<U, _>))
}

fn main() {}
