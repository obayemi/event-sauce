//! The machinery every shared server builds on: starting a container with
//! retries, running it on a runtime of its own, and removing it when the test
//! process exits.

use std::{
    fmt,
    sync::{Mutex, OnceLock, PoisonError},
    thread,
    time::Duration,
};

use bollard::{errors::Error as DockerError, query_parameters::RemoveContainerOptions, Docker};
use testcontainers::{
    core::error::ClientError, runners::AsyncRunner, ContainerAsync, ContainerRequest, Image,
    ImageExt, TestcontainersError,
};
use tokio::runtime::Runtime;
use uuid::Uuid;

const START_ATTEMPTS: u32 = 5;
const START_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Starts a container built by `build`, retrying transient startup failures.
///
/// Several containers can start at once (the shared servers and the dedicated
/// ones), racing for host ports. Docker occasionally loses that race and
/// reports `address already in use`; retrying picks a different port instead
/// of failing the test for a reason that has nothing to do with the code under
/// test.
///
/// A failed start leaves the container Docker created behind, never started,
/// so every attempt runs under a name and a failed one is removed by that
/// name, unless Docker refused the name as taken: that container is not ours.
/// An unnamed request gets a name unique to the attempt, carrying the process
/// id and a token drawn once per process
/// (`testcontainers_shared_dedicated_<id>_<pid>_<token>_<attempt>`).
///
/// `build` is called once per attempt (not just once up front), with the
/// attempt number starting at 1, so a caller that names its container can
/// hand out a fresh name each time: retrying a failed attempt under the same
/// name can otherwise fail again deterministically against a name Docker
/// still considers taken.
///
/// # Errors
///
/// Returns the error of the last attempt once every attempt failed.
pub async fn start_with_retry<I: Image>(
    mut build: impl FnMut(u32) -> ContainerRequest<I>,
) -> Result<ContainerAsync<I>, TestcontainersError> {
    let mut attempt = 1;
    loop {
        let request = build(attempt);
        let name = request_name(request.container_name().as_deref(), Uuid::new_v4(), attempt);
        match request.with_container_name(&name).start().await {
            Ok(container) => return Ok(container),
            Err(error) => {
                if may_have_left_a_container(&error) {
                    remove_leftover(&name).await;
                }
                if attempt == START_ATTEMPTS {
                    return Err(error);
                }
                tokio::time::sleep(START_RETRY_DELAY * attempt).await;
                attempt += 1;
            }
        }
    }
}

/// The name `attempt` runs under: the caller's, or else one unique to `id`.
fn request_name(requested: Option<&str>, id: Uuid, attempt: u32) -> String {
    requested.map_or_else(
        || {
            let id = id.simple().to_string();
            container_name(
                &format!("dedicated_{}", &id[..16]),
                Process::current(),
                attempt,
            )
        },
        str::to_owned,
    )
}

/// Whether a start that failed with `error` may have left a container of ours
/// behind.
///
/// A name conflict means Docker refused to create ours: the container under
/// that name belongs to someone else, maybe another run's live server, and is
/// never removed.
fn may_have_left_a_container(error: &TestcontainersError) -> bool {
    !matches!(
        error,
        TestcontainersError::Client(ClientError::CreateContainer(
            DockerError::DockerResponseServerError {
                status_code: 409,
                ..
            }
        ))
    )
}

/// Removes the never-started container a failed start left under `name`.
///
/// Best effort: the start already failed, so a failed removal is not worth
/// reporting over the start error.
async fn remove_leftover(name: &str) {
    let Ok(docker) = Docker::connect_with_defaults() else {
        return;
    };
    let options = RemoveContainerOptions {
        force: true,
        v: true,
        ..RemoveContainerOptions::default()
    };
    drop(docker.remove_container(name, Some(options)).await);
}

/// Where a server listens on the host.
#[derive(Debug)]
pub(crate) struct Address {
    host: String,
    port: u16,
}

impl Address {
    pub(crate) fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// The host side of `container_port` on `container`.
    pub(crate) async fn of<I: Image>(container: &ContainerAsync<I>, container_port: u16) -> Self {
        let host = container.get_host().await.expect("get host");
        let port = container
            .get_host_port_ipv4(container_port)
            .await
            .expect("get port");
        Self::new(host.to_string(), port)
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// The name of a shared `kind` server started by `process` on `attempt`, so
/// a leftover server is traceable to the test run that started it.
pub(crate) fn container_name(kind: &str, process: &Process, attempt: u32) -> String {
    format!("testcontainers_shared_{kind}_{process}_{attempt}")
}

/// The test process, as the names of the containers it starts identify it.
///
/// The process id keeps a leftover container traceable to the run that
/// started it. The random token tells apart processes that share an id: test
/// runs in different PID namespaces (CI jobs, devcontainers) can talk to the
/// same Docker daemon, and a name taken by another run's container must never
/// be mistaken for one of ours.
#[derive(Debug)]
pub(crate) struct Process {
    id: u32,
    token: String,
}

impl Process {
    /// The running process, with a token drawn once for its whole life.
    pub(crate) fn current() -> &'static Self {
        static CURRENT: OnceLock<Process> = OnceLock::new();
        CURRENT.get_or_init(|| Self {
            id: std::process::id(),
            token: Uuid::new_v4().simple().to_string()[..8].to_owned(),
        })
    }
}

impl fmt::Display for Process {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.id, self.token)
    }
}

/// A shared container together with the runtime its docker client is bound
/// to: the container can only be removed from that runtime, so they live and
/// die together, independently of any test's own runtime.
struct Owner {
    runtime: Runtime,
    container: Box<dyn Send>,
}

static OWNERS: Mutex<Vec<Owner>> = Mutex::new(Vec::new());

/// Starts the shared `kind` server built by `build` and returns the host
/// side of its `container_port`.
///
/// The start runs on a thread of its own because it drives its own runtime,
/// which cannot be entered from the calling test's runtime. The server is
/// removed at process exit.
pub(crate) fn start_shared<I: Image + 'static>(
    kind: &'static str,
    container_port: u16,
    build: impl Fn() -> ContainerRequest<I> + Send + 'static,
) -> Address {
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build the shared server runtime");
        let (container, address) = runtime.block_on(async {
            let container = start_with_retry(|attempt| {
                build().with_container_name(container_name(kind, Process::current(), attempt))
            })
            .await
            .unwrap_or_else(|error| panic!("start the shared {kind} server: {error}"));
            let address = Address::of(&container, container_port).await;
            (container, address)
        });
        OWNERS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Owner {
                runtime,
                container: Box::new(container),
            });
        address
    })
    .join()
    .expect("start the shared server")
}

/// Removes the shared servers when the test process exits.
///
/// A `static` is never dropped, so without this hook the containers would
/// outlive every test run. Each removal runs on a fresh thread: by the time
/// process-exit hooks run, the exiting thread's thread-locals (which the
/// runtime relies on) are already torn down.
#[dtor::dtor(unsafe)]
fn remove_shared_servers() {
    let owners = std::mem::take(&mut *OWNERS.lock().unwrap_or_else(PoisonError::into_inner));
    let removals: Vec<_> = owners
        .into_iter()
        .map(|Owner { runtime, container }| {
            thread::spawn(move || runtime.block_on(async move { drop(container) }))
        })
        .collect();
    for removal in removals {
        drop(removal.join());
    }
}

#[cfg(test)]
#[path = "tests/server_tests.rs"]
mod tests;
