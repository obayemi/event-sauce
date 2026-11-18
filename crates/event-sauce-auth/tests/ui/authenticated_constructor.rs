use event_sauce_auth::{Authenticated, UserAggregate};

fn forge<U: UserAggregate>(user: std::sync::Arc<event_sauce_core::AggregateRoot<U>>) -> Authenticated<U> {
    Authenticated::new(user, [0; 32])
}

fn main() {}
