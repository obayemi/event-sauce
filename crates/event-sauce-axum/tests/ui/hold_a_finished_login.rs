use event_sauce_auth::{Done, Login, UserAggregate};
use event_sauce_axum::PendingLogin;
use event_sauce_auth::PendingLogins;

async fn park<U: UserAggregate>(pending: &PendingLogins<U>, login: Login<U, Done>) {
    let _ = PendingLogin::hold(pending, login).await;
}

fn main() {}
