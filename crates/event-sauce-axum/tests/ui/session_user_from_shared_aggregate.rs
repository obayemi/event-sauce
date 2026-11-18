use std::sync::Arc;

use event_sauce_auth::UserAggregate;
use event_sauce_axum::SessionUser;
use event_sauce_core::AggregateRoot;

fn open<U: UserAggregate>(root: Arc<AggregateRoot<U>>) -> SessionUser<U> {
    SessionUser::from(root)
}

fn main() {}
