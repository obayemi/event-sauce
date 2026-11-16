//! `PostgresLoginStore` against a real `PostgreSQL` container.

mod support;

use std::sync::Arc;
use std::time::Duration;

use event_sauce_auth::{LoginKey, LoginStore, PostgresLoginStore};
use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::ContainerAsync};

async fn start_database() -> (ContainerAsync<Postgres>, PgPool) {
    let container = support::start_with_retry(|| Postgres::default().with_tag("16-alpine"))
        .await
        .expect("start postgres");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&format!(
            "postgresql://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("connect");
    (container, pool)
}

async fn start_store() -> (ContainerAsync<Postgres>, Arc<dyn LoginStore>) {
    let (container, pool) = start_database().await;
    let store = PostgresLoginStore::new(pool);
    store.migrate().await.expect("migrate");
    (container, Arc::new(store))
}

login_store_contract!(start_store);

#[tokio::test]
async fn migrate_creates_the_default_schema() {
    let (_guard, pool) = start_database().await;

    PostgresLoginStore::new(pool.clone())
        .migrate()
        .await
        .unwrap();

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.schemata \
         WHERE schema_name = 'event_sauce_login')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(exists);
}

#[tokio::test]
async fn migrate_is_idempotent_and_keeps_existing_entries() {
    let (_guard, pool) = start_database().await;
    let store = PostgresLoginStore::new(pool);
    store.migrate().await.unwrap();
    store
        .put(
            &LoginKey::new("test", "kept"),
            b"v",
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    store.migrate().await.unwrap();

    assert!(store
        .take(&LoginKey::new("test", "kept"))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_migrations_do_not_race() {
    let (_guard, pool) = start_database().await;

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = PostgresLoginStore::new(pool.clone());
            tokio::spawn(async move { store.migrate().await })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn a_custom_schema_holds_its_own_entries() {
    let (_guard, pool) = start_database().await;
    let first = PostgresLoginStore::new(pool.clone())
        .with_schema("login_a")
        .unwrap();
    let second = PostgresLoginStore::new(pool)
        .with_schema("login_b")
        .unwrap();
    first.migrate().await.unwrap();
    second.migrate().await.unwrap();
    let key = LoginKey::new("test", "scoped");

    first
        .put(&key, b"v", Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(second.take(&key).await.unwrap(), None);
    assert!(first.take(&key).await.unwrap().is_some());
}

#[tokio::test]
async fn purge_expired_removes_only_expired_entries() {
    let (_guard, pool) = start_database().await;
    let store = PostgresLoginStore::new(pool.clone());
    store.migrate().await.unwrap();
    let short = Duration::from_millis(200);
    store
        .put(&LoginKey::new("test", "a"), b"v", short)
        .await
        .unwrap();
    assert!(store
        .mark_once(&LoginKey::new("test", "b"), short)
        .await
        .unwrap());
    store
        .put(
            &LoginKey::new("test", "live"),
            b"v",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    tokio::time::sleep(short * 3).await;

    assert_eq!(store.purge_expired().await.unwrap(), 2);

    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM event_sauce_login.entries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 1);
    assert_eq!(store.purge_expired().await.unwrap(), 0);
}
