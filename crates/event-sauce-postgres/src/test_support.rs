//! Shared helpers for the crate's `PostgreSQL` unit tests.
//!
//! Most tests run against one `PostgreSQL` server shared by the whole test
//! binary: [`fresh_database`] starts it lazily on first use and hands every
//! test its own empty database on it, so tests stay isolated without paying
//! for a container start each. The server is removed when the test process
//! exits normally, whether the run passed or failed; a run killed by a
//! signal leaves it behind.
//!
//! Tests that need a server of their own (server-wide side effects such as
//! creating databases, or a custom container setup) start a dedicated
//! container with [`start_postgres_url`], or with [`start_with_retry`] on a
//! request built by [`test_postgres`].

use std::{
    sync::{Mutex, OnceLock, PoisonError},
    thread,
    time::Duration,
};

use sqlx::{Connection, Executor, PgConnection};
use testcontainers::{
    core::Mount, runners::AsyncRunner, ContainerRequest, Image, ImageExt, TestcontainersError,
};
use testcontainers_modules::{postgres::Postgres, testcontainers::ContainerAsync};
use tokio::runtime::Runtime;
use uuid::Uuid;

const START_ATTEMPTS: u32 = 5;
const START_RETRY_DELAY: Duration = Duration::from_millis(250);
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
pub(crate) fn test_postgres(image: Postgres) -> ContainerRequest<Postgres> {
    image
        .with_tag("16-alpine")
        .with_mount(Mount::tmpfs_mount(DATA_DIRECTORY))
        .with_cmd(SERVER_FLAGS)
}

/// Starts a container built by `build`, retrying transient startup failures.
///
/// Several containers can start at once (the shared server and the dedicated
/// ones), racing for host ports. Docker occasionally loses that race and
/// reports `address already in use`; retrying picks a different port instead
/// of failing the test for a reason that has nothing to do with the code under
/// test.
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

/// Starts a dedicated `PostgreSQL` container and returns it alongside a
/// connection URL for its default `postgres` database.
///
/// Only for tests whose side effects reach beyond their own database; every
/// other test uses [`fresh_database`]. The container must be kept alive for as
/// long as the URL is used, so callers hold on to it even where they never
/// touch it directly.
pub(crate) async fn start_postgres_url() -> (ContainerAsync<Postgres>, String) {
    let container = start_with_retry(|_attempt| test_postgres(Postgres::default()))
        .await
        .expect("start postgres");
    let url = ServerAddress::of(&container).await.url("postgres");
    (container, url)
}

/// Creates an empty database on the shared server and returns a guard that
/// drops it, alongside its connection URL.
///
/// The first call in the test process starts the shared server. Every pool
/// must be built from the returned URL inside the calling test, since a pool
/// is bound to the runtime of the test that created it.
pub(crate) async fn fresh_database() -> (FreshDatabase, String) {
    let address = &shared_server().address;
    let name = format!("test_{}", Uuid::new_v4().simple());
    let mut admin = PgConnection::connect(&address.url("postgres"))
        .await
        .expect("connect to the shared server");
    admin
        .execute(format!("CREATE DATABASE {name}").as_str())
        .await
        .expect("create test database");
    admin.close().await.expect("close admin connection");
    let url = address.url(&name);
    let database = FreshDatabase {
        name,
        admin_url: address.url("postgres"),
    };
    (database, url)
}

/// A database created by [`fresh_database`], dropped together with the guard.
///
/// Dropping it releases the space it takes on the shared server as soon as
/// its test ends. The drop is best effort: a failure only leaves the database
/// behind until the shared server itself is removed at process exit.
pub(crate) struct FreshDatabase {
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

/// Where a test server listens on the host.
struct ServerAddress {
    host: String,
    port: u16,
}

impl ServerAddress {
    async fn of(container: &ContainerAsync<Postgres>) -> Self {
        Self {
            host: container.get_host().await.expect("get host").to_string(),
            port: container.get_host_port_ipv4(5432).await.expect("get port"),
        }
    }

    fn url(&self, database: &str) -> String {
        format!(
            "postgresql://postgres:postgres@{}:{}/{database}",
            self.host, self.port
        )
    }
}

/// The `PostgreSQL` server shared by every [`fresh_database`] of the process.
struct SharedServer {
    address: ServerAddress,
    owner: Mutex<Option<ServerOwner>>,
}

/// The container together with the runtime its docker client is bound to:
/// the container can only be removed from that runtime, so they live and die
/// together, independently of any test's own runtime.
struct ServerOwner {
    runtime: Runtime,
    container: ContainerAsync<Postgres>,
}

static SHARED_SERVER: OnceLock<SharedServer> = OnceLock::new();

/// Returns the shared server, starting it on first use.
///
/// The start runs on a thread of its own because it drives its own runtime,
/// which cannot be entered from the calling test's runtime.
fn shared_server() -> &'static SharedServer {
    SHARED_SERVER.get_or_init(|| {
        thread::spawn(SharedServer::start)
            .join()
            .expect("start the shared postgres server")
    })
}

impl SharedServer {
    /// Starts the container under a name carrying the process id, so a
    /// leftover server is traceable to the test run that started it.
    fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build the shared server runtime");
        let (container, address) = runtime.block_on(async {
            let container = start_with_retry(|attempt| {
                test_postgres(Postgres::default()).with_container_name(format!(
                    "event_sauce_test_pg_{}_{attempt}",
                    std::process::id()
                ))
            })
            .await
            .expect("start postgres");
            let address = ServerAddress::of(&container).await;
            (container, address)
        });
        Self {
            address,
            owner: Mutex::new(Some(ServerOwner { runtime, container })),
        }
    }
}

/// Removes the shared server when the test process exits.
///
/// A `static` is never dropped, so without this hook the container would
/// outlive every test run. The removal runs on a fresh thread: by the time
/// process-exit hooks run, the exiting thread's thread-locals (which the
/// runtime relies on) are already torn down.
#[dtor::dtor(unsafe)]
fn remove_shared_server() {
    let Some(server) = SHARED_SERVER.get() else {
        return;
    };
    let owner = server
        .owner
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    if let Some(ServerOwner { runtime, container }) = owner {
        let removed =
            thread::spawn(move || runtime.block_on(async move { drop(container) })).join();
        drop(removed);
    }
}
