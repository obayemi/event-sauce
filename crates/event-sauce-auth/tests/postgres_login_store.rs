//! `PostgresLoginStore` against a database of its own on a shared
//! `PostgreSQL` container.

mod support;

use std::num::NonZeroU32;
use std::sync::Arc;

use event_sauce_auth::{LoginKey, LoginStore, LoginTtl, PostgresLoginStore};
use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_shared::postgres::{fresh_database, FreshDatabase};

async fn start_database() -> (FreshDatabase, PgPool) {
    let (database, url) = fresh_database().await;
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&url)
        .await
        .expect("connect");
    (database, pool)
}

async fn start_store() -> (FreshDatabase, Arc<dyn LoginStore>) {
    let (database, pool) = start_database().await;
    let store = PostgresLoginStore::new(pool);
    store.migrate().await.expect("migrate");
    (database, Arc::new(store))
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
        .put(&LoginKey::new("test", "kept"), b"v", LoginTtl::ONE_MINUTE)
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

    first.put(&key, b"v", LoginTtl::ONE_MINUTE).await.unwrap();

    assert_eq!(second.take(&key).await.unwrap(), None);
    assert!(first.take(&key).await.unwrap().is_some());
}

#[tokio::test]
async fn purge_expired_removes_only_expired_entries() {
    let (_guard, pool) = start_database().await;
    let store = PostgresLoginStore::new(pool.clone());
    store.migrate().await.unwrap();
    let short = LoginTtl::from_millis(NonZeroU32::new(200).unwrap());
    store
        .put(&LoginKey::new("test", "a"), b"v", short)
        .await
        .unwrap();
    assert!(store
        .mark_once(&LoginKey::new("test", "b"), short)
        .await
        .unwrap());
    store
        .put(&LoginKey::new("test", "live"), b"v", LoginTtl::ONE_MINUTE)
        .await
        .unwrap();
    tokio::time::sleep(short.as_duration() * 3).await;

    assert_eq!(store.purge_expired().await.unwrap(), 2);

    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM event_sauce_login.entries")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 1);
    assert_eq!(store.purge_expired().await.unwrap(), 0);
}
