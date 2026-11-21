use event_sauce_auth::UserAggregate;
use event_sauce_axum::App;

fn reach<U: UserAggregate, F, S>(app: &App<U, F, S>) {
    let _ = app.backend();
}

fn main() {}
