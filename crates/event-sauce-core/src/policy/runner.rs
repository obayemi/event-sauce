//! Routes events to registered policies, tracking each policy's own
//! checkpoint and applying its configured error-handling strategy.

use std::sync::Arc;
use std::time::Duration;

use super::context::PolicyContext;
use super::on_error::{OnError, OnRetryExhausted, RetryConfig, RetryLimit};
use super::Policy;
use crate::{CheckpointStoreRef, EventEnvelope, EventStore, Position, Result};

/// Default duration a [`PolicyRunner`] leases each policy for while draining
/// pending events (see [`PolicyRunner::with_lease_duration`]).
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

/// Default number of events [`PolicyRunner`] fetches per
/// [`EventStore::fetch_batch`] call (see [`PolicyRunner::with_batch_size`]).
const DEFAULT_BATCH_SIZE: usize = 500;

/// Generates a process- and run-unique worker id for a [`PolicyRunner`] that
/// was not given one explicitly via [`PolicyRunner::with_worker_id`].
fn default_worker_id() -> String {
    format!(
        "policy-runner-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    )
}

/// One policy whose lease this [`PolicyRunner`] currently holds for the
/// duration of a [`process_pending`](PolicyRunner::process_pending) call.
///
/// `checkpoint` is this policy's in-memory progress marker, advanced as
/// events are routed to it; `saved` is the position last persisted for it,
/// so a checkpoint save can be skipped once nothing has changed since.
struct HeldPolicy<S: EventStore> {
    policy: Arc<dyn Policy<S>>,
    checkpoint: Position,
    saved: Position,
}

/// Tracks when a held set of policy leases is next due for renewal.
///
/// Mirrors the `PostgreSQL` projection runner and policy dispatcher's own
/// `LeaseRenewal` (a third of the lease's duration) — duplicated here rather
/// than shared, since `event-sauce-core` cannot depend on a backend crate.
struct LeaseRenewal {
    interval: Duration,
    last: std::time::Instant,
}

impl LeaseRenewal {
    fn new(lease_duration: Duration) -> Self {
        Self {
            interval: lease_duration / 3,
            last: std::time::Instant::now(),
        }
    }

    /// Whether at least one renewal interval has passed since the last
    /// renewal (or since this was created, if none yet).
    fn due(&self) -> bool {
        self.last.elapsed() >= self.interval
    }

    fn renewed(&mut self) {
        self.last = std::time::Instant::now();
    }
}

/// Runs policies by routing events to matching handlers.
///
/// `PolicyRunner` manages a set of policies and provides methods for
/// processing events with checkpoint-based resumption and configurable
/// error handling.
///
/// # Builder Pattern
///
/// ```ignore
/// let runner = PolicyRunner::new(store, checkpoint_store)
///     .with_max_cascade_depth(5)
///     .on_error(OnError::Skip)
///     .register(Arc::new(MyPolicy));
/// ```
pub struct PolicyRunner<S: EventStore> {
    store: Arc<S>,
    policies: Vec<Arc<dyn Policy<S>>>,
    checkpoint_store: CheckpointStoreRef,
    max_cascade_depth: usize,
    on_error: OnError,
    worker_id: String,
    lease_duration: Duration,
    batch_size: usize,
    /// Serializes `process_pending` calls on this runner instance.
    ///
    /// A per-policy lease alone does not stop two concurrent calls on the
    /// *same* runner (same `worker_id`) from both acquiring it — a worker
    /// can always renew/re-acquire its own lease — so in-process exclusion
    /// needs this separate guard. See [`process_pending`](Self::process_pending)'s
    /// "# Concurrency" section.
    run_lock: tokio::sync::Mutex<()>,
}

impl<S: EventStore + 'static> PolicyRunner<S> {
    /// Creates a new policy runner.
    ///
    /// Checkpoint store is required — policies track their position
    /// to prevent duplicate processing on restart.
    ///
    /// The worker id defaults to a value unique to this process and this
    /// runner instance (see [`with_worker_id`](Self::with_worker_id)), and
    /// the lease duration defaults to 30 seconds (see
    /// [`with_lease_duration`](Self::with_lease_duration)).
    #[must_use]
    pub fn new(store: Arc<S>, checkpoint_store: CheckpointStoreRef) -> Self {
        Self {
            store,
            policies: Vec::new(),
            checkpoint_store,
            max_cascade_depth: 10,
            on_error: OnError::Fail,
            worker_id: default_worker_id(),
            lease_duration: DEFAULT_LEASE_DURATION,
            batch_size: DEFAULT_BATCH_SIZE,
            run_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Sets the maximum cascade depth (default: 10).
    #[must_use]
    pub fn with_max_cascade_depth(mut self, depth: usize) -> Self {
        self.max_cascade_depth = depth;
        self
    }

    /// Sets the error handling strategy (default: `OnError::Fail`).
    #[must_use]
    pub fn on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Sets this runner's worker id (default: generated, unique to this
    /// process and this runner instance).
    ///
    /// Every [`process_pending`](Self::process_pending) call acquires each
    /// registered policy's lease under this id. Set it to a stable value
    /// (e.g. derived from the pod name or hostname plus the process id) if
    /// you need lease ownership to be recognizable across this runner being
    /// dropped and recreated — the generated default does not survive that.
    #[must_use]
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = worker_id.into();
        self
    }

    /// Sets how long each policy's lease is held before it must be renewed
    /// (default: 30 seconds).
    ///
    /// A shorter duration lets another worker take over a policy sooner
    /// after this one stalls or crashes, at the cost of more frequent
    /// renewal traffic against the checkpoint store.
    #[must_use]
    pub fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    /// Sets how many events [`process_pending`](Self::process_pending) fetches
    /// per [`EventStore::fetch_batch`] call (default: 500).
    ///
    /// Each batch is fully handled before the next is fetched, so no
    /// connection or snapshot the underlying store holds for a fetch stays
    /// open while handlers run — only the current batch's worth of events
    /// is ever in flight. A larger batch means fewer round-trips to the
    /// store at the cost of more events to replay if this run is
    /// interrupted mid-batch.
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Registers a policy.
    #[must_use]
    pub fn register(mut self, policy: Arc<dyn Policy<S>>) -> Self {
        self.policies.push(policy);
        self
    }

    /// Returns the registered policies.
    #[must_use]
    pub fn policies(&self) -> &[Arc<dyn Policy<S>>] {
        &self.policies
    }

    /// Seeds a never-before-seen policy's checkpoint at the store's current
    /// max position, so it skips all existing history and only processes
    /// events appended from now on. A no-op if the policy already has a
    /// checkpoint. Called before the lease is acquired for this policy, so
    /// a fresh row exists for [`try_acquire_lease`](crate::CheckpointStore::try_acquire_lease)
    /// to return rather than defaulting to [`Position::start`].
    async fn seed_new_policy_checkpoint(&self, policy_name: &str) -> Result<()> {
        if self
            .checkpoint_store
            .load_checkpoint(policy_name)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let max = self.store.max_position().await?;
        self.checkpoint_store
            .save_checkpoint(policy_name, max)
            .await
    }

    /// Acquires every policy's lease this worker can currently hold.
    ///
    /// A policy whose lease is held by another worker is left out of the
    /// returned set entirely — it sits out this call without blocking the
    /// rest. If acquiring a later policy's lease errors, every lease
    /// acquired so far in this call is released before the error
    /// propagates.
    async fn acquire_policy_leases(&self) -> Result<Vec<HeldPolicy<S>>> {
        let mut held = Vec::with_capacity(self.policies.len());
        for policy in &self.policies {
            self.seed_new_policy_checkpoint(policy.name()).await?;
            match self
                .checkpoint_store
                .try_acquire_lease(policy.name(), &self.worker_id, self.lease_duration)
                .await
            {
                Ok(Some(position)) => held.push(HeldPolicy {
                    policy: Arc::clone(policy),
                    checkpoint: position,
                    saved: position,
                }),
                Ok(None) => {}
                Err(e) => {
                    self.release_leases(&held).await;
                    return Err(e);
                }
            }
        }
        Ok(held)
    }

    /// Releases every held policy's lease, best-effort (errors are ignored —
    /// releasing a lease already lost to another worker is a no-op anyway).
    async fn release_leases(&self, held: &[HeldPolicy<S>]) {
        for held_policy in held {
            let _ = self
                .checkpoint_store
                .release_lease(held_policy.policy.name(), &self.worker_id)
                .await;
        }
    }

    /// Renews every held policy's lease and resets the renewal clock.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LeaseLost`](crate::Error::LeaseLost) if any held
    /// policy's lease is no longer held by this worker.
    async fn renew_leases(&self, held: &[HeldPolicy<S>], renewal: &mut LeaseRenewal) -> Result<()> {
        for held_policy in held {
            self.checkpoint_store
                .renew_lease(
                    held_policy.policy.name(),
                    &self.worker_id,
                    self.lease_duration,
                )
                .await?;
        }
        renewal.renewed();
        Ok(())
    }

    /// Runs one policy against one event: builds its [`PolicyContext`], calls
    /// [`Policy::handle`], and — only on success — flushes the buffered
    /// commits, folding both steps into the single outcome callers react to.
    async fn handle_and_flush(
        &self,
        policy: &Arc<dyn Policy<S>>,
        envelope: &EventEnvelope,
    ) -> Result<()> {
        let ctx = PolicyContext::new(
            Arc::clone(&self.store),
            envelope.clone(),
            self.max_cascade_depth,
        );
        policy.handle(envelope, &ctx).await?;
        ctx.flush().await
    }

    /// Processes all pending events in one-shot mode with checkpoint support.
    ///
    /// For each registered policy:
    /// 1. Acquires its lease (seeding a brand-new policy's checkpoint at the
    ///    current max position first, so it skips existing history)
    /// 2. Fetches bounded batches from the earliest held policy's checkpoint
    /// 3. Routes matching events to held policies, respecting their individual checkpoints
    /// 4. Handles cascading reactions until no new events are produced
    /// 5. Saves updated checkpoints and releases every lease acquired
    ///
    /// Returns the total number of events processed by policies this call
    /// held the lease for. A policy whose lease is held by another worker
    /// is skipped entirely for this call (its events are left for whichever
    /// worker holds it).
    ///
    /// A flush conflict is treated exactly like a handler error: both go
    /// through the same [`OnError`] policy, so `Retry` retries the flush and
    /// `Skip` skips a reaction whose flush conflicted.
    ///
    /// # Concurrency
    ///
    /// Safe to call from multiple processes/instances sharing the same
    /// [`CheckpointStore`](crate::CheckpointStore): each registered policy is
    /// its own lease, so two workers calling this concurrently each process
    /// only the policies they hold the lease for — never the same policy at
    /// the same time. Checkpoint saves while holding a policy's lease are
    /// fenced (see [`CheckpointStore::save_checkpoint_fenced`](crate::CheckpointStore::save_checkpoint_fenced)),
    /// so a worker that stalls past its lease and loses it to another worker
    /// cannot keep advancing that policy's checkpoint.
    ///
    /// The lease alone does not stop two concurrent calls **on this same
    /// runner instance** (same worker id) from both acquiring it — a worker
    /// can always renew its own lease. This method therefore also serializes
    /// on an internal lock: a second concurrent call on the same `PolicyRunner`
    /// waits for the first to finish rather than racing it.
    ///
    /// Held leases are renewed when due before every matching policy's
    /// handler call, and a handler stuck in a long [`OnError::Retry`]
    /// backoff keeps them renewed throughout on top of that. Two limits
    /// remain, both covered in `docs/policies.md`'s Multi-Instance Safety
    /// section: a single `Policy::handle` call (with no retry backoff in
    /// between to renew from) longer than roughly two thirds of
    /// `lease_duration` can still lose the lease before the next renewal
    /// lands; and cancelling this future (a timeout, a `select!`) skips the
    /// cleanup that releases its held leases, which then stay held under
    /// this worker until they simply expire.
    ///
    /// # Errors
    ///
    /// Returns an error if event loading, policy handling, or committing fails
    /// (depending on the [`OnError`] configuration), or if a held policy's
    /// lease is lost mid-run.
    pub async fn process_pending(&self) -> Result<usize> {
        let _run_guard = self.run_lock.lock().await;

        if self.policies.is_empty() {
            return Ok(0);
        }

        let mut held = self.acquire_policy_leases().await?;
        if held.is_empty() {
            return Ok(0);
        }

        let result = self.drain_held_policies(&mut held).await;
        self.release_leases(&held).await;
        result
    }

    /// Drives the round/cascade loop over `held`'s policies, assuming every
    /// one of them has an active lease held by this worker.
    async fn drain_held_policies(&self, held: &mut [HeldPolicy<S>]) -> Result<usize> {
        let mut from_position = held
            .iter()
            .map(|h| h.checkpoint)
            .min()
            .unwrap_or_else(Position::start);

        let mut total_processed = 0;
        let mut renewal = LeaseRenewal::new(self.lease_duration);

        // Process events in rounds to handle cascading
        loop {
            let mut round_processed = 0;
            let mut position = from_position;

            // Fetch bounded batches rather than holding one open-ended
            // stream_all stream for the whole round: no connection or
            // snapshot the store holds open for a fetch outlives the
            // handlers that run between fetches.
            loop {
                let batch = self.store.fetch_batch(position, self.batch_size).await?;
                if batch.is_empty() {
                    break;
                }

                for entry in batch {
                    let envelope = entry.envelope;
                    position = entry.position;

                    // Route to matching held policies, respecting per-policy checkpoints
                    for i in 0..held.len() {
                        // Skip if this policy has already processed past this position
                        if position.as_i64() <= held[i].checkpoint.as_i64() {
                            continue;
                        }

                        if held[i].policy.event_filter().matches(&envelope) {
                            if renewal.due() {
                                self.renew_leases(held, &mut renewal).await?;
                            }
                            match self.handle_and_flush(&held[i].policy, &envelope).await {
                                Ok(()) => {
                                    round_processed += 1;
                                }
                                Err(e) => match &self.on_error {
                                    OnError::Fail => {
                                        // Persist every held policy's checkpoint up to the
                                        // last event it fully handled *before* failing.
                                        // This policy's checkpoint is intentionally NOT
                                        // advanced past the failing event (the loop-end
                                        // advance below is skipped by the early return),
                                        // so a re-run resumes at the failing event and
                                        // never replays the already-flushed effects of
                                        // events 1..N-1.
                                        self.save_checkpoints(held).await?;
                                        return Err(e);
                                    }
                                    OnError::Skip => {
                                        tracing::warn!(
                                            policy = held[i].policy.name(),
                                            event_id = %envelope.id,
                                            error = %e,
                                            "Skipping event due to policy error"
                                        );
                                    }
                                    OnError::Retry(config) => {
                                        let should_skip = match self
                                            .retry_handler(&held[i].policy, &envelope, config)
                                            .await
                                        {
                                            Ok(skip) => skip,
                                            Err(e) => {
                                                // Retries exhausted under
                                                // OnRetryExhausted::Fail. This is the
                                                // OnError::Fail path: persist progress up
                                                // to the last fully-handled event (this
                                                // failing event is NOT advanced) before
                                                // failing, so a re-run resumes here and
                                                // does not replay already-flushed effects.
                                                self.save_checkpoints(held).await?;
                                                return Err(e);
                                            }
                                        };
                                        if !should_skip {
                                            round_processed += 1;
                                        }
                                    }
                                },
                            }
                        }

                        // Advance this policy's checkpoint
                        held[i].checkpoint = position;
                    }
                }
            }

            total_processed += round_processed;

            // If no events were processed this round, we're done
            if round_processed == 0 {
                break;
            }

            // Move position forward for next round (to pick up cascaded events)
            from_position = position;
        }

        // Save checkpoints for every held policy
        self.save_checkpoints(held).await?;

        Ok(total_processed)
    }

    /// Persists each held policy's checkpoint at its current position,
    /// fenced on this worker still holding that policy's lease.
    ///
    /// A policy whose checkpoint has not moved since the last save is
    /// skipped, so a round that only advanced some policies doesn't write
    /// the others' unchanged position. Called on both the success and the
    /// `OnError::Fail` paths so that a mid-batch failure does not lose the
    /// progress of earlier events whose side effects were already flushed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LeaseLost`](crate::Error::LeaseLost) if this worker
    /// no longer holds a held policy's lease.
    async fn save_checkpoints(&self, held: &mut [HeldPolicy<S>]) -> Result<()> {
        for held_policy in held.iter_mut() {
            if held_policy.checkpoint == held_policy.saved {
                continue;
            }
            self.checkpoint_store
                .save_checkpoint_fenced(
                    held_policy.policy.name(),
                    &self.worker_id,
                    self.lease_duration,
                    held_policy.checkpoint,
                )
                .await?;
            held_policy.saved = held_policy.checkpoint;
        }
        Ok(())
    }

    /// Retries a failed handler with exponential backoff.
    ///
    /// Returns `Ok(false)` if the retry succeeded (event was processed),
    /// `Ok(true)` if the retries were exhausted and the event was skipped,
    /// or `Err` if the retries were exhausted and the policy is `OnRetryExhausted::Fail`.
    /// A flush conflict on this attempt is retried exactly like a handler
    /// error, on the next loop iteration.
    async fn retry_handler(
        &self,
        policy: &Arc<dyn Policy<S>>,
        envelope: &EventEnvelope,
        config: &RetryConfig,
    ) -> Result<bool> {
        let started = std::time::Instant::now();
        let mut attempt = 0;

        // Initial attempt already failed before this function is called.
        // Now we do retries.
        loop {
            attempt += 1;

            // Check limit
            let exhausted = match config.limit {
                RetryLimit::MaxRetries(n) => attempt > n,
                RetryLimit::MaxDuration(d) => started.elapsed() > d,
                RetryLimit::Indefinite => false,
            };

            if exhausted {
                return match config.on_exhausted {
                    OnRetryExhausted::Fail => Err(crate::Error::custom(format!(
                        "Policy '{}' retry exhausted for event {}",
                        policy.name(),
                        envelope.id
                    ))),
                    OnRetryExhausted::Skip => {
                        tracing::warn!(
                            policy = policy.name(),
                            event_id = %envelope.id,
                            "Retry exhausted, skipping event"
                        );
                        Ok(true) // skipped
                    }
                };
            }

            // Exponential backoff: base_delay * 2^(attempt-1), capped at max_delay
            let delay = config
                .base_delay
                .saturating_mul(1 << (attempt - 1).min(30))
                .min(config.max_delay);
            tokio::time::sleep(delay).await;

            match self.handle_and_flush(policy, envelope).await {
                Ok(()) => {
                    return Ok(false); // success
                }
                Err(e) => {
                    tracing::warn!(
                        policy = policy.name(),
                        attempt,
                        event_id = %envelope.id,
                        error = %e,
                        "Policy handler retry failed"
                    );
                }
            }
        }
    }

    /// Processes a single event envelope through matching policies.
    ///
    /// Returns the number of policies that handled the event.
    /// Error handling ([`OnError`]) applies per-event.
    ///
    /// Unlike [`process_pending`](Self::process_pending), this does not
    /// touch checkpoints or leases at all — it is a direct, one-off
    /// dispatch for callers that already know which envelope to route (e.g.
    /// re-delivering a single event from an outbox).
    ///
    /// # Errors
    ///
    /// Returns an error if policy handling fails (depending on [`OnError`] configuration).
    pub async fn process_event(&self, envelope: &EventEnvelope) -> Result<usize> {
        let mut handled = 0;

        for policy in &self.policies {
            if policy.event_filter().matches(envelope) {
                match self.handle_and_flush(policy, envelope).await {
                    Ok(()) => {
                        handled += 1;
                    }
                    Err(e) => match &self.on_error {
                        OnError::Fail => return Err(e),
                        OnError::Skip => {
                            tracing::warn!(
                                policy = policy.name(),
                                event_id = %envelope.id,
                                error = %e,
                                "Skipping event due to policy error"
                            );
                        }
                        OnError::Retry(config) => {
                            let skipped = self.retry_handler(policy, envelope, config).await?;
                            if !skipped {
                                handled += 1;
                            }
                        }
                    },
                }
            }
        }

        Ok(handled)
    }
}

impl<S: EventStore + 'static> std::fmt::Debug for PolicyRunner<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let policy_names: Vec<&str> = self.policies.iter().map(|p| p.name()).collect();
        f.debug_struct("PolicyRunner")
            .field("policies", &policy_names)
            .field("max_cascade_depth", &self.max_cascade_depth)
            .field("worker_id", &self.worker_id)
            .field("lease_duration", &self.lease_duration)
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{test_checkpoint_store, test_envelope};
    use super::*;
    use crate::test_fixtures::MockEventStore;
    use crate::{CheckpointStore, EventFilter};

    #[test]
    fn test_policy_runner_new() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp);

        assert!(runner.policies().is_empty());
    }

    #[test]
    fn test_policy_runner_with_max_cascade_depth() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp).with_max_cascade_depth(5);

        assert_eq!(runner.max_cascade_depth, 5);
    }

    #[test]
    fn test_policy_runner_default_on_error_is_fail() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp);

        assert!(matches!(runner.on_error, OnError::Fail));
    }

    #[test]
    fn test_policy_runner_on_error_builder() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp).on_error(OnError::Skip);

        assert!(matches!(runner.on_error, OnError::Skip));
    }

    #[test]
    fn test_policy_runner_default_worker_id_is_unique_per_instance() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let a = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef);
        let b = PolicyRunner::new(store, cp);

        assert_ne!(a.worker_id, b.worker_id);
    }

    #[test]
    fn test_policy_runner_with_worker_id_builder() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp).with_worker_id("fixed-worker");

        assert_eq!(runner.worker_id, "fixed-worker");
    }

    #[test]
    fn test_policy_runner_default_lease_duration() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp);

        assert_eq!(runner.lease_duration, DEFAULT_LEASE_DURATION);
    }

    #[test]
    fn test_policy_runner_with_lease_duration_builder() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner =
            PolicyRunner::new(store, cp).with_lease_duration(std::time::Duration::from_secs(5));

        assert_eq!(runner.lease_duration, std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_policy_runner_default_batch_size() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp);

        assert_eq!(runner.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn test_policy_runner_with_batch_size_builder() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp).with_batch_size(7);

        assert_eq!(runner.batch_size, 7);
    }

    #[test]
    fn test_policy_runner_with_batch_size_floors_at_one() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp).with_batch_size(0);

        assert_eq!(runner.batch_size, 1);
    }

    struct CountingPolicy {
        name: String,
        filter: EventFilter,
        count: Arc<std::sync::Mutex<usize>>,
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for CountingPolicy {
        fn name(&self) -> &str {
            &self.name
        }

        fn event_filter(&self) -> EventFilter {
            self.filter.clone()
        }

        async fn handle(&self, _event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            *self.count.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[test]
    fn test_policy_runner_register() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let count = Arc::new(std::sync::Mutex::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::all(),
            count,
        });

        let runner = PolicyRunner::new(store, cp).register(policy);
        assert_eq!(runner.policies().len(), 1);
    }

    #[tokio::test]
    async fn test_policy_runner_process_event() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let count = Arc::new(std::sync::Mutex::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::by_event_type("MatchMe"),
            count: Arc::clone(&count),
        });

        let runner = PolicyRunner::new(store, cp).register(policy);

        let envelope = test_envelope("MatchMe", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 1);
        assert_eq!(*count.lock().unwrap(), 1);

        let envelope = test_envelope("DontMatchMe", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 0);
        assert_eq!(*count.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_policy_runner_process_event_multiple_policies() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let count1 = Arc::new(std::sync::Mutex::new(0));
        let count2 = Arc::new(std::sync::Mutex::new(0));

        let policy1 = Arc::new(CountingPolicy {
            name: "policy1".to_string(),
            filter: EventFilter::by_event_type("TestEvent"),
            count: Arc::clone(&count1),
        });
        let policy2 = Arc::new(CountingPolicy {
            name: "policy2".to_string(),
            filter: EventFilter::by_event_type("TestEvent"),
            count: Arc::clone(&count2),
        });

        let runner = PolicyRunner::new(store, cp)
            .register(policy1)
            .register(policy2);

        let envelope = test_envelope("TestEvent", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();

        assert_eq!(handled, 2);
        assert_eq!(*count1.lock().unwrap(), 1);
        assert_eq!(*count2.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_policy_runner_process_pending_no_events() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner: PolicyRunner<MockEventStore> = PolicyRunner::new(store, cp);

        let processed = runner.process_pending().await.unwrap();
        assert_eq!(processed, 0);
    }

    #[tokio::test]
    async fn test_policy_runner_process_pending_with_events() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();

        // Pre-set checkpoint so policy processes from the beginning
        cp.save_checkpoint("test", Position::start()).await.unwrap();

        store.add_event(test_envelope("TestEvent", "TestAggregate"));
        store.add_event(test_envelope("OtherEvent", "TestAggregate"));
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let count = Arc::new(std::sync::Mutex::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::by_event_type("TestEvent"),
            count: Arc::clone(&count),
        });

        let runner = PolicyRunner::new(Arc::clone(&store), cp).register(policy);
        let processed = runner.process_pending().await.unwrap();

        // 2 matching events out of 3 total
        assert_eq!(processed, 2);
        assert_eq!(*count.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_policy_runner_process_pending_seeds_new_policy_at_max_position() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();

        store.add_event(test_envelope("TestEvent", "TestAggregate"));
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let count = Arc::new(std::sync::Mutex::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "fresh".to_string(),
            filter: EventFilter::all(),
            count: Arc::clone(&count),
        });

        let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy);
        let processed = runner.process_pending().await.unwrap();

        assert_eq!(processed, 0, "a new policy must skip existing history");
        assert_eq!(*count.lock().unwrap(), 0);
        assert_eq!(cp.get("fresh"), Some(Position::new(2)));

        store.add_event(test_envelope("TestEvent", "TestAggregate"));
        let processed = runner.process_pending().await.unwrap();
        assert_eq!(processed, 1, "a subsequent event is processed normally");
    }

    #[tokio::test]
    async fn test_policy_runner_process_pending_releases_lease_after_completion() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("test", Position::start()).await.unwrap();

        let count = Arc::new(std::sync::Mutex::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::all(),
            count,
        });

        let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy)
            .with_worker_id("worker-a");
        runner.process_pending().await.unwrap();

        let acquired = cp
            .try_acquire_lease("test", "worker-b", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            acquired.is_some(),
            "the lease must be released once process_pending returns"
        );
    }

    #[tokio::test]
    async fn test_policy_runner_process_pending_skips_a_policy_whose_lease_is_held_elsewhere() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("test", Position::start()).await.unwrap();
        cp.try_acquire_lease("test", "worker-other", std::time::Duration::from_secs(60))
            .await
            .unwrap();

        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let count = Arc::new(std::sync::Mutex::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::all(),
            count: Arc::clone(&count),
        });

        let runner = PolicyRunner::new(Arc::clone(&store), cp)
            .register(policy)
            .with_worker_id("worker-mine");
        let processed = runner.process_pending().await.unwrap();

        assert_eq!(
            processed, 0,
            "a policy whose lease is held elsewhere must be skipped, not processed"
        );
        assert_eq!(*count.lock().unwrap(), 0);
    }

    /// A policy that signals when its handler starts, then waits to be
    /// released, letting a test deterministically interleave two runners.
    struct GatedPolicy {
        name: String,
        entries: Arc<std::sync::atomic::AtomicUsize>,
        count: Arc<std::sync::atomic::AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
        can_continue: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for GatedPolicy {
        fn name(&self) -> &str {
            &self.name
        }

        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }

        async fn handle(&self, _event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            self.entries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.started.notify_one();
            self.can_continue.notified().await;
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// Drives both runners concurrently within this one task: worker A
    /// blocks inside its handler until signaled, and worker B only starts
    /// once worker A has signaled it is inside the handler (lease held) —
    /// so worker B's attempt is deterministically concurrent with worker A
    /// still holding the lease.
    #[tokio::test]
    async fn test_two_workers_never_process_the_same_policy_concurrently() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("gated", Position::start())
            .await
            .unwrap();
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let can_continue = Arc::new(tokio::sync::Notify::new());

        let policy_a = Arc::new(GatedPolicy {
            name: "gated".to_string(),
            entries: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            count: Arc::clone(&count),
            started: Arc::clone(&started),
            can_continue: Arc::clone(&can_continue),
        });

        let runner_a = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_a)
            .with_worker_id("worker-a");

        let count2 = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy_b = Arc::new(CountingPolicyNoWait {
            name: "gated".to_string(),
            count: Arc::clone(&count2),
        });
        let runner_b = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_b)
            .with_worker_id("worker-b");

        let run_a = runner_a.process_pending();
        let run_b = async {
            started.notified().await;
            let processed_b = runner_b.process_pending().await;
            can_continue.notify_one();
            processed_b
        };
        let (processed_a, processed_b) = futures::join!(run_a, run_b);

        assert_eq!(
            processed_b.unwrap(),
            0,
            "worker B must see the lease as busy while worker A holds it"
        );
        assert_eq!(count2.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(processed_a.unwrap(), 1);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    struct CountingPolicyNoWait {
        name: String,
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for CountingPolicyNoWait {
        fn name(&self) -> &str {
            &self.name
        }

        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }

        async fn handle(&self, _event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// A second `process_pending` call on the same runner must wait for the
    /// first rather than entering a policy's handler concurrently with it.
    /// `call_b` is polled exactly once (via [`futures::poll!`]) right after
    /// `call_a` has parked inside the handler, holding `run_lock`: with the
    /// lock in place that single poll can only register on the lock and
    /// must leave `entries` at 1; without it, `call_b` would run straight
    /// into the handler within that same poll and bump `entries` to 2, so
    /// the assertion catches the regression deterministically, with no
    /// reliance on real time or thread scheduling. `can_continue` is
    /// notified twice: under a regression where both calls enter the gate
    /// concurrently, a single notification could permanently strand
    /// whichever task was not already waiting.
    #[tokio::test]
    async fn test_same_runner_second_call_waits_for_the_first_to_finish() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("gated2", Position::start())
            .await
            .unwrap();
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let entries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let can_continue = Arc::new(tokio::sync::Notify::new());
        let policy = Arc::new(GatedPolicy {
            name: "gated2".to_string(),
            entries: Arc::clone(&entries),
            count: Arc::clone(&count),
            started: Arc::clone(&started),
            can_continue: Arc::clone(&can_continue),
        });
        let runner = PolicyRunner::new(store, cp).register(policy);

        let call_a = runner.process_pending();
        let run_b = async {
            started.notified().await;

            let call_b = runner.process_pending();
            futures::pin_mut!(call_b);
            let first_poll = futures::poll!(&mut call_b);
            assert!(
                first_poll.is_pending(),
                "a second concurrent call on the same runner must not \
                 complete while the first still holds the lock"
            );
            assert_eq!(
                entries.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "a second concurrent call on the same runner must wait for \
                 the first before entering the handler at all"
            );

            can_continue.notify_one();
            can_continue.notify_one();

            call_b.await
        };
        let (outcome_a, outcome_b) = futures::join!(call_a, run_b);

        assert_eq!(outcome_a.unwrap(), 1);
        assert_eq!(outcome_b.unwrap(), 0);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_process_pending_spans_multiple_batches() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("test", Position::start()).await.unwrap();
        for _ in 0..5 {
            store.add_event(test_envelope("TestEvent", "TestAggregate"));
        }

        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy = Arc::new(CountingPolicyNoWait {
            name: "test".to_string(),
            count: Arc::clone(&count),
        });
        let runner = PolicyRunner::new(store, cp)
            .register(policy)
            .with_batch_size(2);

        let processed = runner.process_pending().await.unwrap();

        assert_eq!(
            processed, 5,
            "every event must still be processed across several small batches"
        );
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 5);
    }

    /// Wraps [`MockEventStore`], recording each `fetch_batch` call's
    /// `(from_position, limit)` arguments, to pin that `PolicyRunner` drives
    /// its round through bounded [`EventStore::fetch_batch`] calls rather
    /// than one unbounded `stream_all` held open for the whole round.
    struct BatchRecordingStore {
        inner: MockEventStore,
        calls: std::sync::Mutex<Vec<(Position, usize)>>,
    }

    impl BatchRecordingStore {
        fn new(inner: MockEventStore) -> Self {
            Self {
                inner,
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl EventStore for BatchRecordingStore {
        async fn append(
            &self,
            stream_id: crate::StreamId,
            events: Vec<EventEnvelope>,
            expected_version: crate::AggregateVersion,
            claims: Vec<crate::AggregateClaim>,
            clear_claims: bool,
        ) -> Result<()> {
            self.inner
                .append(stream_id, events, expected_version, claims, clear_claims)
                .await
        }

        async fn load_stream(
            &self,
            stream_id: crate::StreamId,
            from_version: crate::AggregateVersion,
        ) -> Result<impl futures::Stream<Item = Result<EventEnvelope>> + Send> {
            self.inner.load_stream(stream_id, from_version).await
        }

        async fn stream_all(
            &self,
            from_position: Position,
        ) -> Result<impl futures::Stream<Item = Result<crate::EventLogEntry>> + Send> {
            self.inner.stream_all(from_position).await
        }

        async fn fetch_batch(
            &self,
            from_position: Position,
            limit: usize,
        ) -> Result<Vec<crate::EventLogEntry>> {
            self.calls.lock().unwrap().push((from_position, limit));
            self.inner.fetch_batch(from_position, limit).await
        }

        async fn get_version(&self, stream_id: crate::StreamId) -> Result<crate::AggregateVersion> {
            self.inner.get_version(stream_id).await
        }
    }

    #[tokio::test]
    async fn test_process_pending_fetches_bounded_batches_not_one_open_stream() {
        let inner = MockEventStore::new();
        for _ in 0..5 {
            inner.add_event(test_envelope("TestEvent", "TestAggregate"));
        }
        let store = Arc::new(BatchRecordingStore::new(inner));
        let cp = test_checkpoint_store();
        cp.save_checkpoint("test", Position::start()).await.unwrap();

        let policy = Arc::new(CountingPolicyNoWait {
            name: "test".to_string(),
            count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });
        let runner = PolicyRunner::new(Arc::clone(&store), cp)
            .register(policy)
            .with_batch_size(2);
        runner.process_pending().await.unwrap();

        let calls = store.calls.lock().unwrap();
        assert!(
            calls.len() >= 3,
            "5 events at batch_size 2 must take at least 3 bounded fetch_batch calls, got {calls:?}"
        );
        assert!(
            calls.iter().all(|(_, limit)| *limit == 2),
            "every call must respect the configured batch size, got {calls:?}"
        );
    }

    #[test]
    fn test_policy_runner_debug() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let count = Arc::new(std::sync::Mutex::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "my_policy".to_string(),
            filter: EventFilter::all(),
            count,
        });

        let runner = PolicyRunner::new(store, cp).register(policy);
        let debug = format!("{runner:?}");
        assert!(debug.contains("PolicyRunner"));
        assert!(debug.contains("my_policy"));
        assert!(debug.contains("max_cascade_depth: 10"));
    }

    /// A policy that signals when its handler starts, then waits to be
    /// released, letting a test deterministically interleave two runners.
    struct GatedHandlerPolicy {
        name: String,
        count: Arc<std::sync::atomic::AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
        can_continue: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for GatedHandlerPolicy {
        fn name(&self) -> &str {
            &self.name
        }

        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }

        async fn handle(&self, _event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            self.started.notify_one();
            self.can_continue.notified().await;
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// A handler's fenced checkpoint save must reject a worker that lost its
    /// lease while the handler was running, and must not overwrite whatever
    /// position the new holder already saved.
    #[tokio::test]
    async fn test_fenced_save_rejects_a_worker_whose_lease_expired_mid_handler() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("fence", Position::start())
            .await
            .unwrap();
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let can_continue = Arc::new(tokio::sync::Notify::new());
        let policy_a = Arc::new(GatedHandlerPolicy {
            name: "fence".to_string(),
            count: Arc::clone(&count),
            started: Arc::clone(&started),
            can_continue: Arc::clone(&can_continue),
        });

        let runner_a = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_a)
            .with_worker_id("worker-a")
            .with_lease_duration(Duration::from_millis(100));

        let call_a = runner_a.process_pending();
        let steal_and_release = async {
            started.notified().await;

            tokio::time::sleep(Duration::from_millis(150)).await;
            let stolen = cp
                .try_acquire_lease("fence", "worker-b", Duration::from_secs(60))
                .await
                .unwrap();
            assert!(
                stolen.is_some(),
                "worker-a's expired lease must be stealable"
            );
            cp.save_checkpoint_fenced(
                "fence",
                "worker-b",
                Duration::from_secs(60),
                Position::new(5),
            )
            .await
            .unwrap();

            can_continue.notify_one();
        };

        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            futures::join!(call_a, steal_and_release)
        })
        .await
        .expect("worker-a must not hang");

        assert!(
            result
                .as_ref()
                .is_err_and(|e: &crate::Error| e.is_lease_lost()),
            "a worker that lost its lease mid-handler must not be able to \
             advance the checkpoint, got {result:?}"
        );
        assert_eq!(
            cp.get("fence"),
            Some(Position::new(5)),
            "worker-b's checkpoint must survive worker-a's rejected save"
        );
    }

    /// A handler that sleeps for `delay` before completing, used to keep a
    /// batch running for longer than a single lease duration.
    struct SlowPolicy {
        name: String,
        delay: Duration,
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for SlowPolicy {
        fn name(&self) -> &str {
            &self.name
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(&self, _event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            tokio::time::sleep(self.delay).await;
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// A long batch (handlers together running well past `lease_duration`)
    /// must stay leased throughout via the per-event renewal check, not just
    /// at acquisition — otherwise another worker could take over mid-batch.
    #[tokio::test]
    async fn test_per_event_renewal_keeps_a_long_batch_leased() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("renewal", Position::start())
            .await
            .unwrap();
        for _ in 0..10 {
            store.add_event(test_envelope("TestEvent", "TestAggregate"));
        }

        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy = Arc::new(SlowPolicy {
            name: "renewal".to_string(),
            delay: Duration::from_millis(50),
            count: Arc::clone(&count),
        });
        let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy)
            .with_worker_id("worker-a")
            .with_lease_duration(Duration::from_millis(200));

        let call_a = runner.process_pending();
        let check_still_leased = async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let busy = cp
                .try_acquire_lease("renewal", "worker-b", Duration::from_secs(60))
                .await
                .unwrap();
            assert!(
                busy.is_none(),
                "per-event renewal must keep the lease held through a batch \
                 longer than its duration"
            );
        };

        let (outcome, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            futures::join!(call_a, check_still_leased)
        })
        .await
        .expect("worker-a must not hang");

        assert_eq!(outcome.unwrap(), 10);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 10);
    }

    /// `route` must renew a due lease before *every* matching policy's
    /// handler call, not just once per event: worker-a holds `p1`/`p2`/`p3`,
    /// each a 120ms handler (120ms is 0.4 of the 300ms lease), and one event
    /// matches all three. If renewal only happened once before `route` ran
    /// the whole event's policies, the 240ms it takes to reach `p3`'s
    /// handler alone already exceeds the lease, well before any handler
    /// individually breaches the documented two-thirds-of-lease_duration
    /// limit. Worker-b, holding only `p3`, must still see it busy at 320ms —
    /// after `p3`'s handler on worker-a has started (240ms) but before it
    /// finishes (360ms).
    #[tokio::test]
    async fn test_renewal_happens_before_every_matching_policys_handler() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        for name in ["p1", "p2", "p3"] {
            cp.save_checkpoint(name, Position::start()).await.unwrap();
        }
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let handler_delay = Duration::from_millis(120);
        let counts: Vec<_> = (0..3)
            .map(|_| Arc::new(std::sync::atomic::AtomicUsize::new(0)))
            .collect();
        let runner_a = ["p1", "p2", "p3"].into_iter().zip(&counts).fold(
            PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
                .with_worker_id("worker-a")
                .with_lease_duration(Duration::from_millis(300)),
            |runner, (name, count)| {
                runner.register(Arc::new(SlowPolicy {
                    name: name.to_string(),
                    delay: handler_delay,
                    count: Arc::clone(count),
                }))
            },
        );

        let count_b = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let policy_b = Arc::new(CountingPolicyNoWait {
            name: "p3".to_string(),
            count: Arc::clone(&count_b),
        });
        let runner_b = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_b)
            .with_worker_id("worker-b");

        let call_a = runner_a.process_pending();
        let call_b = async {
            tokio::time::sleep(Duration::from_millis(320)).await;
            runner_b.process_pending().await
        };

        let (outcome_a, outcome_b) = tokio::time::timeout(Duration::from_secs(5), async {
            futures::join!(call_a, call_b)
        })
        .await
        .expect("both workers must not hang");

        assert_eq!(
            outcome_b.unwrap(),
            0,
            "p3's lease must still be renewed (and so held by worker-a) at \
             320ms, while worker-a is still inside p3's handler"
        );
        assert_eq!(count_b.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(outcome_a.unwrap(), 3);
    }

    /// Losing a held lease partway through a batch must stop that worker
    /// from running any further events in it, and must not touch whatever
    /// checkpoint the new holder has.
    #[tokio::test]
    async fn test_lease_lost_mid_batch_stops_further_events_from_running() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("lost", Position::start()).await.unwrap();
        for _ in 0..3 {
            store.add_event(test_envelope("TestEvent", "TestAggregate"));
        }

        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let can_continue = Arc::new(tokio::sync::Notify::new());
        let policy_a = Arc::new(GatedHandlerPolicy {
            name: "lost".to_string(),
            count: Arc::clone(&count),
            started: Arc::clone(&started),
            can_continue: Arc::clone(&can_continue),
        });

        let runner_a = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_a)
            .with_worker_id("worker-a")
            .with_lease_duration(Duration::from_millis(100));

        let call_a = runner_a.process_pending();
        let steal_lease = async {
            started.notified().await;

            tokio::time::sleep(Duration::from_millis(150)).await;
            let stolen = cp
                .try_acquire_lease("lost", "worker-b", Duration::from_secs(60))
                .await
                .unwrap();
            assert!(
                stolen.is_some(),
                "worker-a's expired lease must be stealable"
            );

            can_continue.notify_one();
        };

        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            futures::join!(call_a, steal_lease)
        })
        .await
        .expect("worker-a must not hang");

        assert!(
            result
                .as_ref()
                .is_err_and(|e: &crate::Error| e.is_lease_lost()),
            "losing the lease mid-batch must surface as LeaseLost, got {result:?}"
        );
        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "events 2 and 3 must never run once the lease is lost"
        );
        assert_eq!(
            cp.get("lost"),
            Some(Position::start()),
            "worker-b's checkpoint must be untouched by worker-a's aborted run"
        );
    }
}
