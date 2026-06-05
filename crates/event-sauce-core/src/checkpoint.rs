//! Checkpoint storage and leasing for durable event consumers.
//!
//! A [`CheckpointStore`] tracks the progress of a consumer (a policy or a
//! projection) so it can resume after a restart, and gates **leases** so that
//! at most one worker actively processes a given consumer at a time.

use async_trait::async_trait;
use std::time::Duration;

use crate::{Position, Result};

/// Trait for checkpoint storage.
///
/// Checkpoint stores track the progress of consumers, enabling resumption
/// after restarts or failures. They also gate **leases**: short-lived locks
/// that ensure at most one worker is the active processor for a given
/// consumer at a time. Leases are how multiple application instances can run
/// the same projection or policy without racing each other's checkpoint
/// updates — the worker that holds the lease is the active one, and other
/// workers either wait or fail over when the lease expires.
///
/// Implementations must make the lease methods atomic with respect to
/// concurrent acquirers (typically via a single conditional `UPSERT` /
/// `UPDATE` for SQL backends, or a single locked region in-memory).
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Saves a checkpoint.
    async fn save_checkpoint(&self, subscription_name: &str, position: Position) -> Result<()>;

    /// Loads a checkpoint.
    async fn load_checkpoint(&self, subscription_name: &str) -> Result<Option<Position>>;

    /// Deletes a checkpoint.
    async fn delete_checkpoint(&self, subscription_name: &str) -> Result<()>;

    /// Atomically tries to acquire (or refresh) the lease for `subscription_name`.
    ///
    /// Succeeds — returning `Ok(Some(position))` — when any of the following
    /// are true:
    /// - no lease record exists yet (it is created at `Position::start`);
    /// - the existing lease is unowned;
    /// - the existing lease has expired;
    /// - the existing lease is already held by `worker_id` (i.e. a renewal).
    ///
    /// In any other case (a different worker holds an unexpired lease),
    /// returns `Ok(None)`. Returns `Err(_)` only on backend errors.
    ///
    /// `lease_duration` is how long the lease is valid from the moment of the
    /// call; renew before it expires to keep ownership.
    async fn try_acquire_lease(
        &self,
        subscription_name: &str,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<Position>>;

    /// Extends the lease for `subscription_name` if held by `worker_id`.
    ///
    /// Returns an error if the lease is not currently held by this worker
    /// (typically `Error::custom("lease lost")`). Callers should treat this
    /// as a signal to stop processing and let another worker take over.
    async fn renew_lease(
        &self,
        subscription_name: &str,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<()>;

    /// Releases the lease for `subscription_name` if held by `worker_id`.
    ///
    /// Idempotent: releasing a lease we no longer hold (because it expired
    /// and was taken by another worker) is not an error.
    async fn release_lease(&self, subscription_name: &str, worker_id: &str) -> Result<()>;
}

/// Blocks until a subscription's checkpoint reaches `target` (read-your-writes).
///
/// After committing events, a caller often wants to read an
/// eventually-consistent read model only *after* the projection that builds it
/// has processed the write it just produced — otherwise it sees stale data.
/// This helper polls `store` for the `subscription_name` checkpoint until it is
/// at or past `target`, sleeping `poll_interval` between polls. A missing
/// checkpoint is treated as [`Position::start`].
///
/// Returns `Ok(true)` once the stored checkpoint is `>= target` within
/// `timeout`, or `Ok(false)` if the timeout elapses first. The check is always
/// performed at least once, so a `target` that is already reached returns
/// promptly even with a tiny `timeout`.
///
/// This is **polling**-based, which makes it portable across every backend. On
/// `PostgreSQL` a `LISTEN`/`NOTIFY`-driven variant could wake the poll for
/// lower latency; that is a future enhancement and not required for
/// correctness here.
///
/// # Read-your-writes pattern
///
/// ```no_run
/// # use std::time::Duration;
/// # use event_sauce_core::{wait_for_checkpoint, CheckpointStore, Position};
/// # async fn example(checkpoints: &dyn CheckpointStore, write_position: Position) -> event_sauce_core::Result<()> {
/// // After `commit`, obtain your write's position (e.g. EventStore::max_position()).
/// let caught_up = wait_for_checkpoint(
///     checkpoints,
///     "MyProjection",
///     write_position,
///     Duration::from_millis(10),
///     Duration::from_secs(5),
/// )
/// .await?;
///
/// if caught_up {
///     // The read model now reflects our write — safe to read.
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns any error produced by [`CheckpointStore::load_checkpoint`].
pub async fn wait_for_checkpoint(
    store: &dyn CheckpointStore,
    subscription_name: &str,
    target: Position,
    poll_interval: Duration,
    timeout: Duration,
) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let current = store
            .load_checkpoint(subscription_name)
            .await?
            .unwrap_or_else(Position::start);
        if current >= target {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(poll_interval).await;
    }
}
