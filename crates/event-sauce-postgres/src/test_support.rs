//! Shared helpers for the crate's `PostgreSQL` integration tests.

use std::time::Duration;

use testcontainers::{
    runners::AsyncRunner, ContainerRequest, Image, ImageExt, TestcontainersError,
};
use testcontainers_modules::{postgres::Postgres, testcontainers::ContainerAsync};

const START_ATTEMPTS: u32 = 5;
const START_RETRY_DELAY: Duration = Duration::from_millis(250);

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
    start_with_retry(|_attempt| Postgres::default().with_tag("16-alpine")).await
}
