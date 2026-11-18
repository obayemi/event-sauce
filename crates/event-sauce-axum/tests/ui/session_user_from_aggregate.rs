use event_sauce_auth::UserAggregate;
use event_sauce_axum::SessionUser;
use event_sauce_core::AggregateRoot;

fn open<U: UserAggregate>(root: AggregateRoot<U>) -> SessionUser<U> {
    SessionUser::from(root)
}

fn main() {}
