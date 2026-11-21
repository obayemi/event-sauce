//! `PostgreSQL` test servers: a shared one handing every test a fresh
//! database, and dedicated ones for tests that need a server to themselves.

use std::{sync::OnceLock, thread};

use sqlx::{Connection, Executor, PgConnection};
use testcontainers::{core::Mount, ContainerAsync, ContainerRequest, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

use crate::server::{self, start_with_retry, Address};

const PORT: u16 = 5432;
const DATA_DIRECTORY: &str = "/var/lib/postgresql/data";
const SERVER_FLAGS: [&str; 8] = [
    "-c",
    "fsync=off",
    "-c",
    "synchronous_commit=off",
    "-c",
    "full_page_writes=off",
    "-c",
    "max_connections=500",
];

/// Builds the container request every test server starts from.
///
/// The data directory lives on a tmpfs and durability is switched off: a test
/// server never outlives its run, so crash safety buys nothing and disk
/// writes only slow the suite down. `max_connections` is raised so the
/// shared server can serve every concurrently running test's pool.
pub fn test_postgres(image: Postgres) -> ContainerRequest<Postgres> {
    image
        .with_tag("16-alpine")
        .with_mount(Mount::tmpfs_mount(DATA_DIRECTORY))
        .with_cmd(SERVER_FLAGS)
}

/// Starts a dedicated `PostgreSQL` container and returns it alongside a
/// connection URL for its default `postgres` database.
///
/// Only for tests whose side effects reach beyond their own database; every
/// other test uses [`fresh_database`]. The container must be kept alive for as
/// long as the URL is used, so callers hold on to it even where they never
/// touch it directly.
///
/// # Panics
///
/// Panics if the container cannot be started.
pub async fn start_postgres_url() -> (ContainerAsync<Postgres>, String) {
    let container = start_with_retry(|_attempt| test_postgres(Postgres::default()))
        .await
        .expect("start postgres");
    let url = database_url(&Address::of(&container, PORT).await, "postgres");
    (container, url)
}

/// Creates an empty database on the shared server and returns a guard that
/// drops it, alongside its connection URL.
///
/// The first call in the test process starts the shared server. Every pool
/// must be built from the returned URL inside the calling test, since a pool
/// is bound to the runtime of the test that created it.
///
/// # Panics
///
/// Panics if the shared server cannot be started or the database cannot be
/// created.
pub async fn fresh_database() -> (FreshDatabase, String) {
    let address = shared_server();
    let name = database_name(Uuid::new_v4());
    let admin_url = database_url(address, "postgres");
    let mut admin = PgConnection::connect(&admin_url)
        .await
        .expect("connect to the shared server");
    admin
        .execute(format!("CREATE DATABASE {name}").as_str())
        .await
        .expect("create test database");
    admin.close().await.expect("close admin connection");
    let url = database_url(address, &name);
    (FreshDatabase { name, admin_url }, url)
}

/// A database created by [`fresh_database`], dropped together with the guard.
///
/// Dropping it releases the space it takes on the shared server as soon as
/// its test ends. The drop is best effort: a failure only leaves the database
/// behind until the shared server itself is removed at process exit.
#[derive(Debug)]
pub struct FreshDatabase {
    name: String,
    admin_url: String,
}

impl Drop for FreshDatabase {
    fn drop(&mut self) {
        let statement = format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name);
        let admin_url = std::mem::take(&mut self.admin_url);
        let dropped = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async {
                let mut admin = PgConnection::connect(&admin_url).await?;
                admin.execute(statement.as_str()).await?;
                admin.close().await
            })?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .join();
        drop(dropped);
    }
}

/// Returns the shared server, starting it on first use.
fn shared_server() -> &'static Address {
    static SERVER: OnceLock<Address> = OnceLock::new();
    SERVER.get_or_init(|| {
        server::start_shared("postgres", PORT, || test_postgres(Postgres::default()))
    })
}

/// A database name unique to `id`, usable unquoted in SQL.
fn database_name(id: Uuid) -> String {
    format!("test_{}", id.simple())
}

fn database_url(address: &Address, database: &str) -> String {
    format!("postgresql://postgres:postgres@{address}/{database}")
}

#[cfg(test)]
#[path = "tests/postgres_tests.rs"]
mod tests;
