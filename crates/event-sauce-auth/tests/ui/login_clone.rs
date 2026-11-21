use event_sauce_auth::{Done, Login, UserAggregate};

fn replay<U: UserAggregate>(login: Login<U, Done>) -> (Login<U, Done>, Login<U, Done>) {
    (login.clone(), login)
}

fn main() {}
