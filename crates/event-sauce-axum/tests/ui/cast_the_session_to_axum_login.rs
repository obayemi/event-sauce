use event_sauce_auth::UserAggregate;
use event_sauce_axum::AuthSession;

fn reach<U: UserAggregate>(session: AuthSession<U>) {
    let _: &axum_login::tower_sessions::Session = session.as_ref();
    let _ = session.clone();
}

fn main() {}
