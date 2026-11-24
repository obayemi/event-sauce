//! The shared Redis server against a real container.

use redis::{Commands, Connection};
use testcontainers_shared::redis::{fresh_redis, start_redis_url};
use uuid::Uuid;

fn connect(url: &str) -> Connection {
    redis::Client::open(url).unwrap().get_connection().unwrap()
}

fn unique_key() -> String {
    format!("key:{}", Uuid::new_v4())
}

#[test]
fn fresh_databases_do_not_see_each_other() {
    let (_first, first_url) = fresh_redis();
    let (_second, second_url) = fresh_redis();
    let key = unique_key();

    let () = connect(&first_url).set(&key, "v").unwrap();

    let seen: Option<String> = connect(&second_url).get(&key).unwrap();
    assert_ne!(first_url, second_url);
    assert_eq!(seen, None);
}

#[test]
fn dropping_the_guard_empties_the_database() {
    let (guard, url) = fresh_redis();
    let key = unique_key();
    let mut connection = connect(&url);
    let () = connection.set(&key, "v").unwrap();

    drop(guard);

    let seen: Option<String> = connection.get(&key).unwrap();
    assert_eq!(seen, None);
}

#[test]
fn a_fresh_database_starts_empty() {
    let (_guard, url) = fresh_redis();

    let size: u64 = redis::cmd("DBSIZE").query(&mut connect(&url)).unwrap();

    assert_eq!(size, 0);
}

#[tokio::test]
async fn a_dedicated_server_takes_server_wide_commands_away_from_the_shared_one() {
    let (_shared, shared_url) = fresh_redis();
    let (_container, dedicated_url) = start_redis_url().await;
    let key = unique_key();
    let () = connect(&shared_url).set(&key, "v").unwrap();

    redis::cmd("FLUSHALL")
        .exec(&mut connect(&dedicated_url))
        .unwrap();

    let seen: Option<String> = connect(&shared_url).get(&key).unwrap();
    assert_eq!(seen.as_deref(), Some("v"));
}
