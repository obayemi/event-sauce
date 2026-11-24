use event_sauce_auth::{Done, Login, LoginState, UserAggregate};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct First;

impl LoginState for First {
    const TAG: &'static str = "ui.first";
}

fn skip<U: UserAggregate>(login: Login<U, First>) -> Login<U, Done> {
    login
}

fn main() {}
