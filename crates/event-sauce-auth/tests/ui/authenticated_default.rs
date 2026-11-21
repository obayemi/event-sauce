use event_sauce_auth::{Authenticated, UserAggregate};

fn forge<U: UserAggregate>() -> Authenticated<U> {
    Authenticated::default()
}

fn main() {}
