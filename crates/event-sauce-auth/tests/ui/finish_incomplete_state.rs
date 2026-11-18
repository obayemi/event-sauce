use event_sauce_auth::{Login, LoginState, UserAggregate};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Pending;

impl LoginState for Pending {
    const TAG: &'static str = "ui.pending";
}

async fn finish<U: UserAggregate>(login: Login<U, Pending>) {
    let _ = login.finish().await;
}

fn main() {}
