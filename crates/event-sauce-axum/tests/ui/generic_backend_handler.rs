use axum::{routing::get, Router};
use axum_login::{AuthSession, AuthnBackend};

async fn handler<B: AuthnBackend + 'static>(_session: AuthSession<B>) {}

fn main() {
    let _: Router = Router::new().route("/", get(handler));
}
