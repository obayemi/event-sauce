use event_sauce_auth::{Authenticated, UserAggregate};

fn forge<U: UserAggregate>(json: &str) -> Authenticated<U> {
    serde_json::from_str(json).unwrap()
}

fn main() {}
