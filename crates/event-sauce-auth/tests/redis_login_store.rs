//! `RedisLoginStore` against a logical database of its own on a shared Redis
//! container.

mod support;

use std::sync::Arc;

use event_sauce_auth::{LoginKey, LoginStore, LoginTtl, RedisLoginStore};
use testcontainers_shared::redis::{fresh_redis, FreshRedis};

async fn start_store() -> (FreshRedis, Arc<dyn LoginStore>) {
    let (database, url) = fresh_redis();
    let store = RedisLoginStore::connect(&url).await.expect("connect");
    (database, Arc::new(store))
}

login_store_contract!(start_store);

#[tokio::test]
async fn prefixes_isolate_stores_sharing_a_server() {
    let (_guard, url) = fresh_redis();
    let first = RedisLoginStore::connect(&url)
        .await
        .unwrap()
        .with_prefix("a:");
    let second = RedisLoginStore::connect(&url)
        .await
        .unwrap()
        .with_prefix("b:");
    let key = LoginKey::new("test", "scoped");

    first.put(&key, b"v", LoginTtl::ONE_MINUTE).await.unwrap();

    assert_eq!(second.take(&key).await.unwrap(), None);
    assert!(first.take(&key).await.unwrap().is_some());
}

#[tokio::test]
async fn connecting_to_an_unreachable_server_fails() {
    assert!(RedisLoginStore::connect("redis://127.0.0.1:1")
        .await
        .is_err());
}
