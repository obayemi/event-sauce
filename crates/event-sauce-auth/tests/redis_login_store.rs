//! `RedisLoginStore` against a real Redis container.

mod support;

use std::sync::Arc;
use std::time::Duration;

use event_sauce_auth::{LoginKey, LoginStore, RedisLoginStore};
use testcontainers::ImageExt;
use testcontainers_modules::{redis::Redis, testcontainers::ContainerAsync};

async fn start_container() -> (ContainerAsync<Redis>, String) {
    let container = support::start_with_retry(|| Redis::default().with_tag("7-alpine"))
        .await
        .expect("start redis");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    (container, format!("redis://{host}:{port}"))
}

async fn start_store() -> (ContainerAsync<Redis>, Arc<dyn LoginStore>) {
    let (container, url) = start_container().await;
    let store = RedisLoginStore::connect(&url).await.expect("connect");
    (container, Arc::new(store))
}

login_store_contract!(start_store);

#[tokio::test]
async fn prefixes_isolate_stores_sharing_a_server() {
    let (_guard, url) = start_container().await;
    let first = RedisLoginStore::connect(&url)
        .await
        .unwrap()
        .with_prefix("a:");
    let second = RedisLoginStore::connect(&url)
        .await
        .unwrap()
        .with_prefix("b:");
    let key = LoginKey::new("test", "scoped");

    first
        .put(&key, b"v", Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(second.take(&key).await.unwrap(), None);
    assert!(first.take(&key).await.unwrap().is_some());
}

#[tokio::test]
async fn connecting_to_an_unreachable_server_fails() {
    assert!(RedisLoginStore::connect("redis://127.0.0.1:1")
        .await
        .is_err());
}
