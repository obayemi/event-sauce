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
