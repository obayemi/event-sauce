//! The machinery every shared server builds on: starting a container with
//! retries, running it on a runtime of its own, and removing it when the test
//! process exits.

use std::{
    fmt,
    sync::{Mutex, PoisonError},
    thread,
    time::Duration,
};

use testcontainers::{
    runners::AsyncRunner, ContainerAsync, ContainerRequest, Image, ImageExt, TestcontainersError,
};
use tokio::runtime::Runtime;

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
pub(crate) fn container_name(kind: &str, process: u32, attempt: u32) -> String {
    format!("testcontainers_shared_{kind}_{process}_{attempt}")
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
                build().with_container_name(container_name(kind, std::process::id(), attempt))
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
