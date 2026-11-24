use event_sauce_auth::{Done, Login, UserAggregate};

async fn prove<U: UserAggregate>(login: Login<U, Done>) {
    let _ = login.prove(&(), ()).await;
}

fn main() {}
