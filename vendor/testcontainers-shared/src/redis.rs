//! Redis test servers: a shared one handing every test a logical database of
//! its own, and dedicated ones for tests that need a server to themselves.
//!
//! Each test on the shared server gets a distinct database index, which
//! isolates its keys and the commands scoped to one database, such as
//! `FLUSHDB` or `DBSIZE`, whatever key names the code under test picks. The
//! rest of Redis is server-wide and reaches every test of the process: pub/sub
//! channels, `FLUSHALL`, `CONFIG SET` (keyspace notifications included),
//! `SCRIPT` and `FUNCTION`, `SWAPDB` and `CLIENT KILL`. A test relying on any
//! of those starts a dedicated server with [`start_redis_url`].
//!
//! The shared server is started with [`DATABASES`] logical databases, which
//! bounds how many tests can hold one at the same time; an index is handed
//! out again once its holder released it.

use std::sync::{Mutex, OnceLock, PoisonError};

use testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use testcontainers_modules::redis::Redis;

use crate::{
    index_pool::{Index, IndexPool},
    server::{self, start_with_retry, Address},
};

/// How many logical databases the shared server has, and so how many tests
/// can hold one at once.
pub const DATABASES: u16 = 1024;

const PORT: u16 = 6379;

static INDICES: Mutex<IndexPool> = Mutex::new(IndexPool::new(DATABASES));

/// Builds the container request every test server starts from.
///
/// Persistence is switched off, since a test server never outlives its run,
/// and the server gets [`DATABASES`] logical databases instead of 16.
pub fn test_redis(image: Redis) -> ContainerRequest<Redis> {
    image.with_tag("7-alpine").with_cmd(server_args())
}

/// Starts a dedicated Redis container and returns it alongside a connection
/// URL for its database 0.
///
/// Only for tests relying on server-wide commands; every other test uses
/// [`fresh_redis`]. The container must be kept alive for as long as the URL
/// is used, so callers hold on to it even where they never touch it directly.
///
/// # Panics
///
/// Panics if the container cannot be started.
pub async fn start_redis_url() -> (ContainerAsync<Redis>, String) {
    let container = start_with_retry(|_attempt| test_redis(Redis::default()))
        .await
        .expect("start redis");
    let url = database_url(&Address::of(&container, PORT).await, 0);
    (container, url)
}

/// Reserves an empty logical database on the shared server and returns a
/// guard that empties and releases it, alongside a connection URL selecting
/// it.
///
/// The first call in the test process starts the shared server.
///
/// # Panics
///
/// Panics if the shared server cannot be started, or if [`DATABASES`] tests
/// already hold a database.
#[must_use]
pub fn fresh_redis() -> (FreshRedis, String) {
    let address = shared_server();
    let index = INDICES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .acquire()
        .unwrap_or_else(|| panic!("all {DATABASES} shared Redis databases are held"));
    let url = database_url(address, index.get());
    let guard = FreshRedis {
        url: url.clone(),
        index: Some(index),
    };
    (guard, url)
}

/// A logical database reserved by [`fresh_redis`], emptied and released
/// together with the guard.
///
/// A database is only handed out again once `FLUSHDB` succeeded on it: if it
/// fails, the database stays reserved until the process exits, so no later
/// test can see the keys it holds.
#[derive(Debug)]
pub struct FreshRedis {
    url: String,
    index: Option<Index>,
}

impl Drop for FreshRedis {
    fn drop(&mut self) {
        let flushed = ::redis::Client::open(self.url.as_str())
            .and_then(|client| client.get_connection())
            .and_then(|mut connection| ::redis::cmd("FLUSHDB").exec(&mut connection));
        if let (Ok(()), Some(index)) = (flushed, self.index.take()) {
            INDICES
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .release(index);
        }
    }
}

/// Returns the shared server, starting it on first use.
fn shared_server() -> &'static Address {
    static SERVER: OnceLock<Address> = OnceLock::new();
    SERVER.get_or_init(|| server::start_shared("redis", PORT, || test_redis(Redis::default())))
}

fn server_args() -> Vec<String> {
    [
        "redis-server",
        "--databases",
        &DATABASES.to_string(),
        "--save",
        "",
        "--appendonly",
        "no",
    ]
    .map(str::to_owned)
    .to_vec()
}

fn database_url(address: &Address, index: u16) -> String {
    format!("redis://{address}/{index}")
}

#[cfg(test)]
#[path = "tests/redis_tests.rs"]
mod tests;
