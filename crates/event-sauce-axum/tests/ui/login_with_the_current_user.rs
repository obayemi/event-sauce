use event_sauce_auth::UserAggregate;
use event_sauce_axum::{AuthSession, CurrentUser};

async fn reopen<U: UserAggregate>(mut session: AuthSession<U>, user: CurrentUser<U>) {
    let _ = session.login(user.0).await;
}

fn main() {}
