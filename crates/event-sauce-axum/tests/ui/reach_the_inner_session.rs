use event_sauce_auth::UserAggregate;
use event_sauce_axum::AuthSession;

fn reach<U: UserAggregate>(session: AuthSession<U>) {
    let _ = session.inner;
    let _ = session.backend;
}

fn main() {}
