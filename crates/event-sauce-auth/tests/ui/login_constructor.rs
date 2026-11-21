use std::sync::Arc;

use event_sauce_auth::{Done, Login, UserAggregate};
use event_sauce_core::AggregateRoot;

fn forge<U: UserAggregate>(user: Arc<AggregateRoot<U>>) -> Login<U, Done> {
    Login::resumed(user, [0; 32], chrono::Utc::now(), todo!(), Done)
}

fn main() {}
