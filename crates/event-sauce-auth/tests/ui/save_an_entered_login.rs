use event_sauce_auth::{Entered, Login, PendingLogins, UserAggregate};

async fn save<U: UserAggregate>(pending: PendingLogins<U>, login: Login<U, Entered>) {
    let _ = pending.save(login).await;
}

fn main() {}
