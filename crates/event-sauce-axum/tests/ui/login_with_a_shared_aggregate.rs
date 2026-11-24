use std::sync::Arc;

use event_sauce_auth::UserAggregate;
use event_sauce_axum::AuthSession;
use event_sauce_core::AggregateRoot;

async fn open<U: UserAggregate>(mut session: AuthSession<U>, root: Arc<AggregateRoot<U>>) {
    let _ = session.login(root).await;
}

fn main() {}
