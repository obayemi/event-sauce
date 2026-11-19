//! Shared helpers for the crate's `PostgreSQL` integration tests.

use std::time::Duration;

use testcontainers::{
    core::Mount, runners::AsyncRunner, ContainerRequest, Image, ImageExt, TestcontainersError,
};
use testcontainers_modules::{postgres::Postgres, testcontainers::ContainerAsync};

const START_ATTEMPTS: u32 = 5;
const START_RETRY_DELAY: Duration = Duration::from_millis(250);
const DATA_DIRECTORY: &str = "/var/lib/postgresql/data";
const SERVER_FLAGS: [&str; 6] = [
    "-c",
    "fsync=off",
    "-c",
    "synchronous_commit=off",
    "-c",
    "full_page_writes=off",
];

/// Builds the container request every test server starts from.
///
/// The data directory lives on a tmpfs and durability is switched off: a test
/// server never outlives its run, so crash safety buys nothing and disk
/// writes only slow the suite down.
pub(crate) fn test_postgres(image: Postgres) -> ContainerRequest<Postgres> {
    image
        .with_tag("16-alpine")
        .with_mount(Mount::tmpfs_mount(DATA_DIRECTORY))
        .with_cmd(SERVER_FLAGS)
}

/// Starts a container built by `build`, retrying transient startup failures.
///
/// Every test in this crate gets its own container, so a full run races dozens
/// of them for host ports. Docker occasionally loses that race and reports
/// `address already in use`; retrying picks a different port instead of failing
/// the test for a reason that has nothing to do with the code under test.
///
/// `build` is called once per attempt (not just once up front) so a caller
/// that names its container can hand out a fresh name each time: retrying a
/// failed attempt under the same name can otherwise fail again deterministically
/// against a name Docker still considers taken.
pub(crate) async fn start_with_retry<I: Image>(
    mut build: impl FnMut(u32) -> ContainerRequest<I>,
) -> Result<ContainerAsync<I>, TestcontainersError> {
    let mut attempt = 1;
    loop {
        match build(attempt).start().await {
            Ok(container) => return Ok(container),
            Err(_) if attempt < START_ATTEMPTS => {
                tokio::time::sleep(START_RETRY_DELAY * attempt).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Starts a throwaway `PostgreSQL` container, retrying transient startup failures.
pub(crate) async fn start_postgres() -> Result<ContainerAsync<Postgres>, TestcontainersError> {
    start_with_retry(|_attempt| test_postgres(Postgres::default())).await
}

/// Starts a throwaway `PostgreSQL` container and returns it alongside a
/// connection URL for the default `postgres` database.
///
/// The container must be kept alive for as long as the URL is used, so
/// callers hold on to it even where they never touch it directly.
pub(crate) async fn start_postgres_url() -> (ContainerAsync<Postgres>, String) {
    let container = start_postgres().await.expect("start postgres");
    let host = container.get_host().await.expect("get host");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    (
        container,
        format!("postgresql://postgres:postgres@{host}:{port}/postgres"),
    )
}
