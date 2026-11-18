use event_sauce_auth::{Advance, Done, Entered, Login, UserAggregate};

fn forge<U: UserAggregate>(login: Login<U, Entered>) -> Login<U, Done> {
    Advance(login).to(Done)
}

fn main() {}
