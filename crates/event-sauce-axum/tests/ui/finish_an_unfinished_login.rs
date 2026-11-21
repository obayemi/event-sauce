use event_sauce_auth::{Login, LoginState, UserAggregate};
use event_sauce_axum::AuthSession;

async fn open<U: UserAggregate, S: LoginState>(mut session: AuthSession<U>, login: Login<U, S>) {
    let _ = session.finish(login).await;
}

fn main() {}
