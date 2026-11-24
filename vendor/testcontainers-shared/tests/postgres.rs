//! The shared `PostgreSQL` server against a real container.

use sqlx::{Connection, PgConnection};
use testcontainers_shared::postgres::{fresh_database, start_postgres_url};

fn database_of(url: &str) -> &str {
    url.rsplit('/').next().expect("url has a database")
}

async fn database_exists(connection: &mut PgConnection, name: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
        .bind(name)
        .fetch_one(connection)
        .await
        .unwrap()
}

#[tokio::test]
async fn fresh_databases_do_not_see_each_other() {
    let (_first, first_url) = fresh_database().await;
    let (_second, second_url) = fresh_database().await;
    let mut first = PgConnection::connect(&first_url).await.unwrap();
    let mut second = PgConnection::connect(&second_url).await.unwrap();

    sqlx::query("CREATE TABLE only_in_first (id INT)")
        .execute(&mut first)
        .await
        .unwrap();

    let visible: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_name = 'only_in_first')",
    )
    .fetch_one(&mut second)
    .await
    .unwrap();
    assert_ne!(first_url, second_url);
    assert!(!visible);
}

#[tokio::test]
async fn dropping_the_guard_drops_the_database() {
    let (_observer, observer_url) = fresh_database().await;
    let (guard, url) = fresh_database().await;
    let name = database_of(&url).to_owned();
    let mut observer = PgConnection::connect(&observer_url).await.unwrap();
    let mut held = PgConnection::connect(&url).await.unwrap();
    assert!(database_exists(&mut observer, &name).await);

    drop(guard);

    assert!(!database_exists(&mut observer, &name).await);
    assert!(held.ping().await.is_err());
}

#[tokio::test]
async fn a_dedicated_server_accepts_server_wide_statements() {
    let (_container, url) = start_postgres_url().await;
    let mut admin = PgConnection::connect(&url).await.unwrap();

    sqlx::query("CREATE DATABASE dedicated")
        .execute(&mut admin)
        .await
        .unwrap();

    assert!(database_exists(&mut admin, "dedicated").await);
}
