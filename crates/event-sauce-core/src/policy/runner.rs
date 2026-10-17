//! Routes events to registered policies, tracking each policy's own
//! checkpoint and applying its configured error-handling strategy.

use std::sync::Arc;
use std::time::Duration;

use super::context::PolicyContext;
use super::on_error::{OnError, OnRetryExhausted, RetryConfig, RetryLimit};
use super::Policy;
use crate::event_context::reaction_context;
use crate::{CheckpointStoreRef, EventEnvelope, EventStore, Position, Result, WithEventContextExt};

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

/// Outcome of running one policy against one event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Handled {
    /// The handler ran (on the first attempt or after a retry) and flushed.
    Yes,
    /// The event was skipped: a non-retrying `OnError::Skip`, or retries
    /// exhausted under `OnRetryExhausted::Skip`.
    Skipped,
}

/// Outcome of one [`PolicyRunner::process_pending`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyRunOutcome {
    /// This call held the lease for at least one registered policy (even if
    /// zero events ultimately matched any of them), or no policies are
    /// registered at all — there is then no lease to hold and nothing to be
    /// busy about. Carries the number of policy reactions that ran (see
    /// [`processed`](Self::processed)).
    Completed(usize),
    /// Every registered policy's lease is currently held by another worker:
    /// this call processed nothing and held no lease. Distinguishes a
    /// standby node sitting out a lease it does not hold from one that is
    /// simply idle because nothing is pending.
    Busy,
}

impl PolicyRunOutcome {
    /// The number of policy reactions that ran: a handler that ran and
    /// flushed successfully, on the first attempt or after retries. Skipped
    /// reactions ([`OnError::Skip`], or an exhausted retry under
    /// [`OnRetryExhausted::Skip`]) are not counted, and an event handled by
    /// several policies counts once per policy. `0` for
    /// [`PolicyRunOutcome::Busy`].
    #[must_use]
    pub fn processed(&self) -> usize {
        match self {
            Self::Completed(n) => *n,
            Self::Busy => 0,
        }
    }

    /// Whether every registered policy's lease was held by another worker
    /// for the whole call.
    #[must_use]
    pub fn is_busy(&self) -> bool {
        matches!(self, Self::Busy)
    }
}

/// Tracks when a held set of policy leases is next due for renewal.
///
/// Mirrors the `PostgreSQL` projection runner and policy dispatcher's own
/// `LeaseRenewal` (a third of the lease's duration) — duplicated here rather
/// than shared, since `event-sauce-core` cannot depend on a backend crate.
struct LeaseRenewal {
    lease_duration: Duration,
    last: std::time::Instant,
}

impl LeaseRenewal {
    fn new(lease_duration: Duration) -> Self {
        Self {
            lease_duration,
            last: std::time::Instant::now(),
        }
    }

    /// A third of `lease_duration`: how long this waits between renewals.
    fn interval(&self) -> Duration {
        self.lease_duration / 3
    }

    /// Whether at least one renewal interval has passed since the last
    /// renewal (or since this was created, if none yet).
    fn due(&self) -> bool {
        self.last.elapsed() >= self.interval()
    }

    fn renewed(&mut self) {
        self.last = std::time::Instant::now();
    }
}

/// Renews a worker's held policy leases once a third of `lease_duration` has
/// elapsed: [`route`](PolicyRunner::route) checks it as it visits each held
/// policy, and [`retry_handler`](PolicyRunner::retry_handler) checks it
/// before every attempt and between backoff chunks.
///
/// The retry case matters because a lease whose `lease_duration` is shorter
/// than a retry's total backoff (e.g. [`RetryLimit::Indefinite`] or
/// [`RetryLimit::MaxDuration`] longer than the lease) would otherwise expire
/// while the node is still retrying, letting another worker acquire it and
/// run the same event concurrently. Threading this through the retry loop
/// closes that window: a lease lost mid-retry now aborts the attempt with
/// [`Error::LeaseLost`](crate::Error::LeaseLost) instead of silently
/// continuing to work under it.
struct LeaseKeeper<'a> {
    checkpoint_store: &'a CheckpointStoreRef,
    worker_id: &'a str,
    policy_names: &'a [String],
    renewal: LeaseRenewal,
}

impl<'a> LeaseKeeper<'a> {
    fn new(
        checkpoint_store: &'a CheckpointStoreRef,
        worker_id: &'a str,
        lease_duration: Duration,
        policy_names: &'a [String],
    ) -> Self {
        Self {
            checkpoint_store,
            worker_id,
            policy_names,
            renewal: LeaseRenewal::new(lease_duration),
        }
    }

    /// Renews every held policy's lease if at least one renewal interval has
    /// passed since the last renewal.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LeaseLost`](crate::Error::LeaseLost) if any held
    /// policy's lease is no longer held by this worker.
    async fn renew_if_due(&mut self) -> Result<()> {
        if !self.renewal.due() {
            return Ok(());
        }
        for name in self.policy_names {
            self.checkpoint_store
                .renew_lease(name, self.worker_id, self.renewal.lease_duration)
                .await?;
        }
        self.renewal.renewed();
        Ok(())
    }

    /// Sleeps for `delay` in chunks no longer than the renewal interval,
    /// renewing the held leases between chunks — so a `delay` longer than
    /// the renewal interval (e.g. a large `max_delay`) can never skip a
    /// renewal by sleeping through it in one uninterrupted call.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LeaseLost`](crate::Error::LeaseLost) if a renewal
    /// between chunks finds a held policy's lease no longer held by this
    /// worker.
    async fn sleep_renewing(&mut self, delay: Duration) -> Result<()> {
        let chunk = self.renewal.interval().max(Duration::from_millis(1));
        let mut remaining = delay;
        while remaining > chunk {
            tokio::time::sleep(chunk).await;
            remaining -= chunk;
            self.renew_if_due().await?;
        }
        tokio::time::sleep(remaining).await;
        Ok(())
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
    /// Checkpoint store is required — each policy tracks its own position
    /// there, so a restart resumes where it left off instead of replaying
    /// from the start.
    ///
    /// The worker id defaults to a value unique to this process and this
    /// runner instance (see [`with_worker_id`](Self::with_worker_id)), the
    /// lease duration defaults to 30 seconds (see
    /// [`with_lease_duration`](Self::with_lease_duration)), and the fetch
    /// batch size defaults to 500 (see
    /// [`with_batch_size`](Self::with_batch_size)).
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
    ///
    /// # Uniqueness
    ///
    /// The id must be unique to each **live** runner instance you expect to
    /// run concurrently: two runners sharing a worker id count as one lease
    /// holder, so they acquire and run every policy they both register,
    /// together, instead of splitting the work. Keep the generated default
    /// unless you specifically need ownership to survive this runner being
    /// recreated, and give a process running more than one runner over the
    /// same policies a distinct id per runner.
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
    ///
    /// A `batch_size` of `0` is treated as `1`.
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
    /// max position, then tries to acquire its lease. Seeding first ensures
    /// a fresh row exists for [`try_acquire_lease`](crate::CheckpointStore::try_acquire_lease)
    /// to return rather than defaulting to [`Position::start`]; it is a
    /// no-op if the policy already has a checkpoint, and, since
    /// [`CheckpointStore::seed_checkpoint`](crate::CheckpointStore::seed_checkpoint)
    /// is an atomic insert-if-absent, two workers racing to seed the same
    /// new policy can never have the slower one overwrite progress the
    /// faster one (now holding the lease) already made.
    async fn acquire_policy_lease(
        &self,
        policy: &Arc<dyn Policy<S>>,
    ) -> Result<Option<HeldPolicy<S>>> {
        let max = self.store.max_position().await?;
        self.checkpoint_store
            .seed_checkpoint(policy.name(), max)
            .await?;
        let position = self
            .checkpoint_store
            .try_acquire_lease(policy.name(), &self.worker_id, self.lease_duration)
            .await?;
        Ok(position.map(|position| HeldPolicy {
            policy: Arc::clone(policy),
            checkpoint: position,
            saved: position,
        }))
    }

    /// Acquires every policy's lease this worker can currently hold.
    ///
    /// A policy whose lease is held by another worker is left out of the
    /// returned set entirely — it sits out this call without blocking the
    /// rest. If seeding or acquiring a later policy's lease errors, every
    /// lease acquired so far in this call is released before the error
    /// propagates.
    async fn acquire_policy_leases(&self) -> Result<Vec<HeldPolicy<S>>> {
        let mut held = Vec::with_capacity(self.policies.len());
        for policy in &self.policies {
            match self.acquire_policy_lease(policy).await {
                Ok(acquired) => held.extend(acquired),
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
        policy
            .handle(envelope, &ctx)
            .with_event_context(reaction_context(&ctx.reaction_metadata(), policy.name()))
            .await?;
        ctx.flush().await
    }

    /// Runs one policy against one event and applies [`OnError`] to a
    /// failure: fail, log and skip, or retry with backoff. Folds
    /// `handle`+`flush`+error-handling into the single outcome every caller
    /// (`process_event`, [`route`](Self::route)) reacts to.
    ///
    /// `keeper` is `Some` only when called from [`drain_held_policies`](Self::drain_held_policies)
    /// (i.e. this worker holds leases to keep alive); [`process_event`](Self::process_event)
    /// passes `None` since it touches no checkpoints or leases at all.
    ///
    /// # Errors
    ///
    /// Returns an error when `on_error` is [`OnError::Fail`], or when it is
    /// [`OnError::Retry`] and retries exhaust into
    /// [`OnRetryExhausted::Fail`], or a lease `keeper` tracks is lost mid-retry.
    async fn run_policy(
        &self,
        policy: &Arc<dyn Policy<S>>,
        envelope: &EventEnvelope,
        keeper: Option<&mut LeaseKeeper<'_>>,
    ) -> Result<Handled> {
        let Err(error) = self.handle_and_flush(policy, envelope).await else {
            return Ok(Handled::Yes);
        };

        match &self.on_error {
            OnError::Fail => Err(error),
            OnError::Skip => {
                tracing::warn!(
                    policy = policy.name(),
                    event_id = %envelope.id,
                    error = %error,
                    "Skipping event due to policy error"
                );
                Ok(Handled::Skipped)
            }
            OnError::Retry(config) => self.retry_handler(policy, envelope, config, keeper).await,
        }
    }

    /// Processes all pending events in one-shot mode with checkpoint support.
    ///
    /// Seeds and acquires a lease for each registered policy (a brand-new
    /// policy's checkpoint is seeded at the current max position first, so
    /// it skips existing history), then fetches bounded batches from the
    /// earliest held policy's checkpoint and routes each batch's events to
    /// the held policies past their own checkpoint. Re-fetching after every
    /// batch, rather than once up front, means an event a cascading handler
    /// appends mid-call is picked up by this same call, the same way any
    /// other pending event is. Checkpoints are saved after every batch, and
    /// every held lease is released once the call is done, win or lose.
    ///
    /// Returns [`PolicyRunOutcome::Completed`] with the total number of
    /// policy reactions run by policies this call held the lease for (see
    /// [`PolicyRunOutcome::processed`]) — even when that number is `0`
    /// because nothing was pending. A policy whose lease is held by another
    /// worker is skipped entirely for this call (its events are left for
    /// whichever worker holds it); if *every* registered policy's lease is
    /// held elsewhere, this call held no lease at all and returns
    /// [`PolicyRunOutcome::Busy`] instead, so a caller can tell a busy
    /// standby apart from one that is idle because nothing is pending.
    ///
    /// A flush conflict is treated exactly like a handler error: both go
    /// through the same [`OnError`] policy, so `Retry` retries the flush and
    /// `Skip` skips a reaction whose flush conflicted.
    ///
    /// # Cancel safety
    ///
    /// Checkpoints are saved after every batch, so dropping this future (a
    /// timeout, a `select!`, a process crash) replays at most the batch that
    /// was in flight; every earlier batch is already checkpointed. Delivery
    /// is still at-least-once — a handler whose side effects flushed but
    /// whose batch hadn't finished checkpointing yet re-runs on the next
    /// call — so keep handlers idempotent (see `docs/policies.md`). A
    /// cancellation also skips releasing this call's held leases, which
    /// then stay held under this worker until they simply expire.
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
    /// A due lease is renewed on every fetched event — matched or not — and
    /// again before every matching policy's handler call, so a held policy
    /// with nothing to process can never starve for renewal while the scan
    /// passes it by. A handler stuck in a long [`OnError::Retry`] backoff
    /// keeps them renewed throughout on top of that. Two limits remain, both
    /// covered in `docs/policies.md`'s Multi-Instance Safety section: a
    /// single `Policy::handle` call (with no retry backoff in between to
    /// renew from) longer than roughly two thirds of `lease_duration` can
    /// still lose the lease before the next renewal lands; and cancelling
    /// this future skips the cleanup that releases its held leases (see
    /// "# Cancel safety" above).
    ///
    /// # Errors
    ///
    /// Returns an error if seeding a new policy's checkpoint, acquiring or
    /// renewing a lease, fetching events, routing (per [`OnError`]), or
    /// saving a checkpoint fails. A held policy's lease lost mid-run (a
    /// renewal failure, or a fenced save rejected by a new holder) also
    /// surfaces here.
    pub async fn process_pending(&self) -> Result<PolicyRunOutcome> {
        let _run_guard = self.run_lock.lock().await;

        if self.policies.is_empty() {
            return Ok(PolicyRunOutcome::Completed(0));
        }

        let mut held = self.acquire_policy_leases().await?;
        if held.is_empty() {
            return Ok(PolicyRunOutcome::Busy);
        }

        let result = self.drain_held_policies(&mut held).await;
        self.release_leases(&held).await;
        result.map(PolicyRunOutcome::Completed)
    }

    /// Fetches, routes and checkpoints `held`'s policies until the event log
    /// is drained, assuming every one of them has an active lease held by
    /// this worker.
    ///
    /// Fetches bounded batches ([`EventStore::fetch_batch`]) rather than
    /// holding one open-ended `stream_all` stream for the whole call: no
    /// connection or snapshot the store holds open for a fetch outlives the
    /// handlers that run between fetches. Each batch is routed and
    /// checkpointed before the next is fetched at the now-advanced position,
    /// so an event a cascading handler appends mid-call is picked up by a
    /// later fetch the same way any other pending event is — one pass
    /// already drains everything there is to drain, including cascades, once
    /// a fetch comes back empty.
    /// Checkpoints are saved after every batch, whether routing it succeeded
    /// or failed, before a [`route`](Self::route) error propagates — see
    /// [`process_pending`](Self::process_pending)'s "# Cancel safety"
    /// section for the replay guarantee this gives.
    async fn drain_held_policies(&self, held: &mut [HeldPolicy<S>]) -> Result<usize> {
        let mut position = held
            .iter()
            .map(|h| h.checkpoint)
            .min()
            .unwrap_or_else(Position::start);

        let policy_names: Vec<String> = held.iter().map(|h| h.policy.name().to_string()).collect();
        let mut keeper = LeaseKeeper::new(
            &self.checkpoint_store,
            &self.worker_id,
            self.lease_duration,
            &policy_names,
        );

        let mut total_processed = 0;
        loop {
            let batch = self.store.fetch_batch(position, self.batch_size).await?;
            let Some(last) = batch.last() else {
                break;
            };
            position = last.position;

            let routed = self.route_batch(&batch, held, &mut keeper).await;
            self.save_checkpoints(held).await?;
            total_processed += routed?;
        }

        Ok(total_processed)
    }

    /// Routes every entry in `batch`, in order, summing how many policy
    /// reactions ran (see [`route`](Self::route)).
    ///
    /// # Errors
    ///
    /// Returns the first error [`route`](Self::route) returns for an entry
    /// in `batch`, leaving the rest of the batch unrouted.
    async fn route_batch(
        &self,
        batch: &[crate::EventLogEntry],
        held: &mut [HeldPolicy<S>],
        keeper: &mut LeaseKeeper<'_>,
    ) -> Result<usize> {
        let mut processed = 0;
        for entry in batch {
            processed += self
                .route(entry.position, &entry.envelope, held, keeper)
                .await?;
        }
        Ok(processed)
    }

    /// Routes one global-log entry at `position` to every held policy whose
    /// checkpoint it is past: runs matching policies' handlers (via
    /// [`run_policy`](Self::run_policy)) and advances each iterated policy's
    /// checkpoint to `position` in place — including one this event didn't
    /// match, so a non-matching gap is skipped on the next call too.
    ///
    /// A due lease is renewed before considering each held policy, which
    /// covers both a policy sitting ahead of the scan (no event or
    /// checkpoint of its own to trigger a check) and several matching
    /// handlers run back to back (the sum of their durations, not any single
    /// one, is what the next renewal has to arrive within).
    ///
    /// Returns how many policies ran their handler for this event
    /// ([`Handled::Yes`]).
    ///
    /// # Errors
    ///
    /// Propagates the first [`OnError::Fail`]-dispatched error (including a
    /// retry that exhausts into [`OnRetryExhausted::Fail`], or a lease
    /// `keeper` tracks lost mid-retry). On an `Err`, every policy iterated
    /// *before* the failing one already has its checkpoint advanced in
    /// `held` — the caller persists that progress before propagating.
    async fn route(
        &self,
        position: Position,
        envelope: &EventEnvelope,
        held: &mut [HeldPolicy<S>],
        keeper: &mut LeaseKeeper<'_>,
    ) -> Result<usize> {
        let mut processed = 0;
        for held_policy in held.iter_mut() {
            keeper.renew_if_due().await?;

            if position <= held_policy.checkpoint {
                continue;
            }

            if held_policy.policy.event_filter().matches(envelope)
                && self
                    .run_policy(&held_policy.policy, envelope, Some(&mut *keeper))
                    .await?
                    == Handled::Yes
            {
                processed += 1;
            }

            held_policy.checkpoint = position;
        }
        Ok(processed)
    }

    /// Persists each held policy's checkpoint at its current position,
    /// fenced on this worker still holding that policy's lease.
    ///
    /// A policy whose checkpoint has not moved since the last save is
    /// skipped, so a call that only advanced some policies doesn't write
    /// the others' unchanged position. Called after every fetched batch, and
    /// again before any route error propagates (a `Fail`-dispatched error,
    /// an exhausted retry, or a lost lease) — see
    /// [`process_pending`](Self::process_pending)'s "# Cancel safety"
    /// section for the replay guarantee this gives.
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
    /// The initial attempt already failed before this is called; it only
    /// performs retries. A flush conflict on a retry attempt is retried
    /// exactly like a handler error, on the next loop iteration.
    ///
    /// When `keeper` is `Some`, its held leases are renewed before every
    /// attempt and through every backoff sleep (see [`LeaseKeeper`]).
    ///
    /// # Errors
    ///
    /// Returns an error once retries are exhausted under
    /// [`OnRetryExhausted::Fail`], or if `keeper` finds a held lease lost.
    async fn retry_handler(
        &self,
        policy: &Arc<dyn Policy<S>>,
        envelope: &EventEnvelope,
        config: &RetryConfig,
        mut keeper: Option<&mut LeaseKeeper<'_>>,
    ) -> Result<Handled> {
        let started = std::time::Instant::now();
        let mut attempt = 0;

        loop {
            attempt += 1;

            if let Some(keeper) = &mut keeper {
                keeper.renew_if_due().await?;
            }

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
                        Ok(Handled::Skipped)
                    }
                };
            }

            // Exponential backoff: base_delay * 2^(attempt-1), capped at max_delay
            let delay = config
                .base_delay
                .saturating_mul(1 << (attempt - 1).min(30))
                .min(config.max_delay);
            match &mut keeper {
                Some(keeper) => keeper.sleep_renewing(delay).await?,
                None => tokio::time::sleep(delay).await,
            }

            let Err(e) = self.handle_and_flush(policy, envelope).await else {
                return Ok(Handled::Yes);
            };
            tracing::warn!(
                policy = policy.name(),
                attempt,
                event_id = %envelope.id,
                error = %e,
                "Policy handler retry failed"
            );
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
            if policy.event_filter().matches(envelope)
                && matches!(self.run_policy(policy, envelope, None).await?, Handled::Yes)
            {
                handled += 1;
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
    use crate::test_fixtures::{MockCheckpointStore, MockEventStore};
    use crate::{CheckpointStore, EventFilter};
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        count: Arc<AtomicUsize>,
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
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_policy_runner_register() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let count = Arc::new(AtomicUsize::new(0));
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
        let count = Arc::new(AtomicUsize::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::by_event_type("MatchMe"),
            count: Arc::clone(&count),
        });

        let runner = PolicyRunner::new(store, cp).register(policy);

        let envelope = test_envelope("MatchMe", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let envelope = test_envelope("DontMatchMe", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 0);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_policy_runner_process_event_multiple_policies() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

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
        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_policy_runner_process_pending_no_events() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner: PolicyRunner<MockEventStore> = PolicyRunner::new(store, cp);

        let processed = runner.process_pending().await.unwrap();
        assert_eq!(processed, PolicyRunOutcome::Completed(0));
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

        let count = Arc::new(AtomicUsize::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::by_event_type("TestEvent"),
            count: Arc::clone(&count),
        });

        let runner = PolicyRunner::new(Arc::clone(&store), cp).register(policy);
        let processed = runner.process_pending().await.unwrap();

        // 2 matching events out of 3 total
        assert_eq!(processed, PolicyRunOutcome::Completed(2));
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_policy_runner_process_pending_seeds_new_policy_at_max_position() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();

        store.add_event(test_envelope("TestEvent", "TestAggregate"));
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let count = Arc::new(AtomicUsize::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "fresh".to_string(),
            filter: EventFilter::all(),
            count: Arc::clone(&count),
        });

        let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy);
        let processed = runner.process_pending().await.unwrap();

        assert_eq!(
            processed,
            PolicyRunOutcome::Completed(0),
            "a new policy must skip existing history"
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(cp.get("fresh"), Some(Position::new(2)));

        store.add_event(test_envelope("TestEvent", "TestAggregate"));
        let processed = runner.process_pending().await.unwrap();
        assert_eq!(
            processed,
            PolicyRunOutcome::Completed(1),
            "a subsequent event is processed normally"
        );
    }

    #[tokio::test]
    async fn test_policy_runner_process_pending_releases_lease_after_completion() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("test", Position::start()).await.unwrap();

        let count = Arc::new(AtomicUsize::new(0));
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

    /// Wraps [`MockCheckpointStore`], failing `seed_checkpoint` for one
    /// chosen subscription name — simulating a never-before-seen policy
    /// whose seed errors (e.g. a transient backend failure) after an earlier
    /// policy in the same call already seeded and acquired its lease.
    struct FailingSeedCheckpointStore {
        inner: Arc<MockCheckpointStore>,
        fail_for: String,
    }

    #[async_trait::async_trait]
    impl CheckpointStore for FailingSeedCheckpointStore {
        async fn save_checkpoint(&self, name: &str, position: Position) -> Result<()> {
            self.inner.save_checkpoint(name, position).await
        }
        async fn load_checkpoint(&self, name: &str) -> Result<Option<Position>> {
            self.inner.load_checkpoint(name).await
        }
        async fn delete_checkpoint(&self, name: &str) -> Result<()> {
            self.inner.delete_checkpoint(name).await
        }
        async fn try_acquire_lease(
            &self,
            name: &str,
            worker_id: &str,
            dur: Duration,
        ) -> Result<Option<Position>> {
            self.inner.try_acquire_lease(name, worker_id, dur).await
        }
        async fn renew_lease(&self, name: &str, worker_id: &str, dur: Duration) -> Result<()> {
            self.inner.renew_lease(name, worker_id, dur).await
        }
        async fn release_lease(&self, name: &str, worker_id: &str) -> Result<()> {
            self.inner.release_lease(name, worker_id).await
        }
        async fn seed_checkpoint(&self, name: &str, position: Position) -> Result<()> {
            if name == self.fail_for {
                return Err(crate::Error::custom("seed boom"));
            }
            self.inner.seed_checkpoint(name, position).await
        }
    }

    /// When a later policy's seed fails, every lease this call already
    /// acquired for an earlier policy must be released, not left dangling
    /// until it expires — the same guarantee a `try_acquire_lease` error
    /// already got.
    #[tokio::test]
    async fn test_acquire_policy_leases_releases_earlier_leases_when_a_later_seed_fails() {
        let store = Arc::new(MockEventStore::new());
        let inner = Arc::new(MockCheckpointStore::new());
        let cp: CheckpointStoreRef = Arc::new(FailingSeedCheckpointStore {
            inner: Arc::clone(&inner),
            fail_for: "second".to_string(),
        });

        let policy1 = Arc::new(CountingPolicy {
            name: "first".to_string(),
            filter: EventFilter::all(),
            count: Arc::new(AtomicUsize::new(0)),
        });
        let policy2 = Arc::new(CountingPolicy {
            name: "second".to_string(),
            filter: EventFilter::all(),
            count: Arc::new(AtomicUsize::new(0)),
        });

        let runner = PolicyRunner::new(store, cp)
            .register(policy1)
            .register(policy2)
            .with_worker_id("worker-a");

        let result = runner.process_pending().await;
        assert!(result.is_err(), "a seed failure must propagate");

        let acquired = inner
            .try_acquire_lease("first", "worker-b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            acquired.is_some(),
            "an earlier policy's lease acquired in the same call must be \
             released when a later policy's seed fails"
        );
    }

    /// Wraps [`MockCheckpointStore`], failing `try_acquire_lease` for one
    /// chosen subscription name — simulating a transient backend failure
    /// acquiring a later policy's lease after an earlier one in the same
    /// call already acquired its own.
    struct FailingAcquireCheckpointStore {
        inner: Arc<MockCheckpointStore>,
        fail_for: String,
    }

    #[async_trait::async_trait]
    impl CheckpointStore for FailingAcquireCheckpointStore {
        async fn save_checkpoint(&self, name: &str, position: Position) -> Result<()> {
            self.inner.save_checkpoint(name, position).await
        }
        async fn load_checkpoint(&self, name: &str) -> Result<Option<Position>> {
            self.inner.load_checkpoint(name).await
        }
        async fn delete_checkpoint(&self, name: &str) -> Result<()> {
            self.inner.delete_checkpoint(name).await
        }
        async fn try_acquire_lease(
            &self,
            name: &str,
            worker_id: &str,
            dur: Duration,
        ) -> Result<Option<Position>> {
            if name == self.fail_for {
                return Err(crate::Error::custom("acquire boom"));
            }
            self.inner.try_acquire_lease(name, worker_id, dur).await
        }
        async fn renew_lease(&self, name: &str, worker_id: &str, dur: Duration) -> Result<()> {
            self.inner.renew_lease(name, worker_id, dur).await
        }
        async fn release_lease(&self, name: &str, worker_id: &str) -> Result<()> {
            self.inner.release_lease(name, worker_id).await
        }
        async fn seed_checkpoint(&self, name: &str, position: Position) -> Result<()> {
            self.inner.seed_checkpoint(name, position).await
        }
    }

    /// When a later policy's `try_acquire_lease` itself fails (as opposed to
    /// its seed), every lease this call already acquired for an earlier
    /// policy must still be released, not left dangling until it expires.
    #[tokio::test]
    async fn test_acquire_policy_leases_releases_earlier_leases_when_a_later_acquire_fails() {
        let store = Arc::new(MockEventStore::new());
        let inner = Arc::new(MockCheckpointStore::new());
        let cp: CheckpointStoreRef = Arc::new(FailingAcquireCheckpointStore {
            inner: Arc::clone(&inner),
            fail_for: "second".to_string(),
        });

        let policy1 = Arc::new(CountingPolicy {
            name: "first".to_string(),
            filter: EventFilter::all(),
            count: Arc::new(AtomicUsize::new(0)),
        });
        let policy2 = Arc::new(CountingPolicy {
            name: "second".to_string(),
            filter: EventFilter::all(),
            count: Arc::new(AtomicUsize::new(0)),
        });

        let runner = PolicyRunner::new(store, cp)
            .register(policy1)
            .register(policy2)
            .with_worker_id("worker-a");

        let result = runner.process_pending().await;
        assert!(result.is_err(), "an acquire failure must propagate");

        let acquired = inner
            .try_acquire_lease("first", "worker-b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            acquired.is_some(),
            "an earlier policy's lease acquired in the same call must be \
             released when a later policy's try_acquire_lease fails"
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

        let count = Arc::new(AtomicUsize::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::all(),
            count: Arc::clone(&count),
        });

        let runner = PolicyRunner::new(Arc::clone(&store), cp)
            .register(policy)
            .with_worker_id("worker-mine");
        let processed = runner.process_pending().await.unwrap();

        assert!(
            processed.is_busy(),
            "a policy whose lease is held elsewhere must be skipped, not processed"
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    /// A policy that signals when its handler starts, then waits to be
    /// released, letting a test deterministically interleave two runners.
    struct GatedPolicy {
        name: String,
        entries: Arc<AtomicUsize>,
        count: Arc<AtomicUsize>,
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
            self.entries.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.can_continue.notified().await;
            self.count.fetch_add(1, Ordering::SeqCst);
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

        let count = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let can_continue = Arc::new(tokio::sync::Notify::new());

        let policy_a = Arc::new(GatedPolicy {
            name: "gated".to_string(),
            entries: Arc::new(AtomicUsize::new(0)),
            count: Arc::clone(&count),
            started: Arc::clone(&started),
            can_continue: Arc::clone(&can_continue),
        });

        let runner_a = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_a)
            .with_worker_id("worker-a");

        let count2 = Arc::new(AtomicUsize::new(0));
        let policy_b = Arc::new(CountingPolicy {
            name: "gated".to_string(),
            filter: EventFilter::all(),
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

        assert!(
            processed_b.unwrap().is_busy(),
            "worker B must see the lease as busy while worker A holds it"
        );
        assert_eq!(count2.load(Ordering::SeqCst), 0);
        assert_eq!(processed_a.unwrap(), PolicyRunOutcome::Completed(1));
        assert_eq!(count.load(Ordering::SeqCst), 1);
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

        let entries = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));
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
                entries.load(Ordering::SeqCst),
                1,
                "a second concurrent call on the same runner must wait for \
                 the first before entering the handler at all"
            );

            can_continue.notify_one();
            can_continue.notify_one();

            call_b.await
        };
        let (outcome_a, outcome_b) = futures::join!(call_a, run_b);

        assert_eq!(outcome_a.unwrap(), PolicyRunOutcome::Completed(1));
        assert_eq!(outcome_b.unwrap(), PolicyRunOutcome::Completed(0));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// The per-policy skip guard must be strict (`position <= checkpoint`
    /// skips, so only a position *past* the checkpoint is new): a policy
    /// whose checkpoint sits exactly on a position must not re-run that
    /// position's event. `behind` starts at [`Position::start`] and sees
    /// every one of 3 events; `ahead` starts at [`Position::new`]`(2)` and
    /// must see only the 3rd, not re-run the 2nd (the one its checkpoint
    /// already sits on).
    #[tokio::test]
    async fn test_per_policy_checkpoint_skip_guard_is_strict() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("behind", Position::start())
            .await
            .unwrap();
        cp.save_checkpoint("ahead", Position::new(2)).await.unwrap();
        for _ in 0..3 {
            store.add_event(test_envelope("TestEvent", "TestAggregate"));
        }

        let behind_count = Arc::new(AtomicUsize::new(0));
        let ahead_count = Arc::new(AtomicUsize::new(0));
        let runner = PolicyRunner::new(store, cp)
            .register(Arc::new(CountingPolicy {
                name: "behind".to_string(),
                filter: EventFilter::all(),
                count: Arc::clone(&behind_count),
            }))
            .register(Arc::new(CountingPolicy {
                name: "ahead".to_string(),
                filter: EventFilter::all(),
                count: Arc::clone(&ahead_count),
            }));

        runner.process_pending().await.unwrap();

        assert_eq!(
            behind_count.load(Ordering::SeqCst),
            3,
            "a policy starting at Position::start() must see every event"
        );
        assert_eq!(
            ahead_count.load(Ordering::SeqCst),
            1,
            "a policy whose checkpoint already sits on position 2 must not \
             re-run that event, only the one after it"
        );
    }

    #[tokio::test]
    async fn test_process_pending_spans_multiple_batches() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("test", Position::start()).await.unwrap();
        for _ in 0..5 {
            store.add_event(test_envelope("TestEvent", "TestAggregate"));
        }

        let count = Arc::new(AtomicUsize::new(0));
        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::all(),
            count: Arc::clone(&count),
        });
        let runner = PolicyRunner::new(store, cp)
            .register(policy)
            .with_batch_size(2);

        let processed = runner.process_pending().await.unwrap();

        assert_eq!(
            processed,
            PolicyRunOutcome::Completed(5),
            "every event must still be processed across several small batches"
        );
        assert_eq!(count.load(Ordering::SeqCst), 5);
    }

    /// Wraps [`MockEventStore`], recording each `fetch_batch` call's
    /// `(from_position, limit)` arguments, to pin that `PolicyRunner` drains
    /// through bounded [`EventStore::fetch_batch`] calls rather than one
    /// unbounded `stream_all` held open for the whole call.
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

        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::all(),
            count: Arc::new(AtomicUsize::new(0)),
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

    /// Wraps [`MockEventStore`], failing the `n`th (1-indexed) `fetch_batch`
    /// call to simulate a transient error partway through a call — e.g. a
    /// dropped connection on the *next* batch's fetch, after earlier
    /// batches in the same call already ran their handlers and flushed.
    struct FlakyEventStore {
        inner: MockEventStore,
        fail_on_call: usize,
        calls: AtomicUsize,
    }

    impl FlakyEventStore {
        fn new(inner: MockEventStore, fail_on_call: usize) -> Self {
            Self {
                inner,
                fail_on_call,
                calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl EventStore for FlakyEventStore {
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
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_on_call {
                return Err(crate::Error::custom("simulated transient fetch failure"));
            }
            self.inner.fetch_batch(from_position, limit).await
        }

        async fn get_version(&self, stream_id: crate::StreamId) -> Result<crate::AggregateVersion> {
            self.inner.get_version(stream_id).await
        }
    }

    /// CONC-4: a transient error on a later batch's fetch must not lose the
    /// checkpoint progress of batches already handled and flushed earlier in
    /// the same call. `batch_size` 2 over 6 events takes 3 `fetch_batch`
    /// calls plus a trailing empty-batch call to end the call; failing the
    /// 2nd means the 1st batch's 2 events were already handled and flushed.
    #[tokio::test]
    async fn test_checkpoint_survives_a_failure_on_a_later_batch() {
        let inner = MockEventStore::new();
        for _ in 0..6 {
            inner.add_event(test_envelope("TestEvent", "TestAggregate"));
        }
        let store = Arc::new(FlakyEventStore::new(inner, 2));
        let cp = test_checkpoint_store();
        cp.save_checkpoint("test", Position::start()).await.unwrap();

        let policy = Arc::new(CountingPolicy {
            name: "test".to_string(),
            filter: EventFilter::all(),
            count: Arc::new(AtomicUsize::new(0)),
        });
        let runner = PolicyRunner::new(Arc::clone(&store), cp.clone())
            .register(policy)
            .with_batch_size(2);

        let result = runner.process_pending().await;
        assert!(
            result.is_err(),
            "the simulated fetch failure must propagate"
        );

        assert_eq!(
            cp.get("test"),
            Some(Position::new(2)),
            "the first batch's progress must be checkpointed even though the \
             second batch's fetch failed, so a re-run does not replay it"
        );
    }

    #[test]
    fn test_policy_runner_debug() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let count = Arc::new(AtomicUsize::new(0));
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

        let count = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let can_continue = Arc::new(tokio::sync::Notify::new());
        let policy_a = Arc::new(GatedPolicy {
            name: "fence".to_string(),
            entries: Arc::new(AtomicUsize::new(0)),
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
        count: Arc<AtomicUsize>,
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
            self.count.fetch_add(1, Ordering::SeqCst);
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

        let count = Arc::new(AtomicUsize::new(0));
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

        assert_eq!(outcome.unwrap(), PolicyRunOutcome::Completed(10));
        assert_eq!(count.load(Ordering::SeqCst), 10);
    }

    /// Wraps [`MockEventStore`], sleeping `delay` before every `fetch_batch`
    /// call — stretches a scan over many events to longer than a lease
    /// duration, so a renewal gap on a held policy with nothing matching in
    /// that stretch shows up deterministically.
    struct SlowFetchStore {
        inner: MockEventStore,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl EventStore for SlowFetchStore {
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
            tokio::time::sleep(self.delay).await;
            self.inner.fetch_batch(from_position, limit).await
        }

        async fn get_version(&self, stream_id: crate::StreamId) -> Result<crate::AggregateVersion> {
            self.inner.get_version(stream_id).await
        }
    }

    /// A held policy sitting ahead of the scan position (its checkpoint past
    /// every event about to be scanned) matches nothing for a long stretch,
    /// so the per-handler renewal check in [`PolicyRunner::route`] never
    /// fires for it, and its checkpoint never moves either, so the implicit
    /// renewal that rides along `save_checkpoint_fenced` never fires for it
    /// either. `ahead`'s lease must still survive the scan on the per-event
    /// check alone: `replay` matches nothing ever (so its own per-handler
    /// renewal never fires either), but its checkpoint *does* advance on
    /// every event, keeping its lease alive as a side effect of
    /// `save_checkpoint_fenced` — isolating the gap to `ahead`, the policy
    /// with no matching event and no checkpoint movement of its own, with
    /// `batch_size(1)` so a save follows every single event.
    #[tokio::test]
    async fn test_per_event_renewal_covers_a_held_policy_sitting_ahead_of_the_scan() {
        let store = Arc::new(SlowFetchStore {
            inner: MockEventStore::new(),
            delay: Duration::from_millis(60),
        });
        for _ in 0..10 {
            store
                .inner
                .add_event(test_envelope("Other", "TestAggregate"));
        }
        store
            .inner
            .add_event(test_envelope("Target", "TestAggregate"));

        let cp = test_checkpoint_store();
        cp.save_checkpoint("replay", Position::start())
            .await
            .unwrap();
        cp.save_checkpoint("ahead", Position::new(10))
            .await
            .unwrap();

        let ahead_count = Arc::new(AtomicUsize::new(0));
        let runner = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(Arc::new(CountingPolicy {
                name: "replay".to_string(),
                filter: EventFilter::by_event_type("NeverMatches"),
                count: Arc::new(AtomicUsize::new(0)),
            }))
            .register(Arc::new(CountingPolicy {
                name: "ahead".to_string(),
                filter: EventFilter::by_event_type("Target"),
                count: Arc::clone(&ahead_count),
            }))
            .with_worker_id("solo")
            .with_lease_duration(Duration::from_millis(200))
            .with_batch_size(1);

        let outcome = tokio::time::timeout(Duration::from_secs(10), runner.process_pending())
            .await
            .expect("must not hang");

        assert_eq!(
            outcome.unwrap(),
            PolicyRunOutcome::Completed(1),
            "a held policy sitting ahead of the scan position must keep its \
             lease renewed by the per-event check alone, with no matching \
             event or checkpoint movement of its own to trigger a renewal"
        );
        assert_eq!(ahead_count.load(Ordering::SeqCst), 1);
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
        let counts: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
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

        let count_b = Arc::new(AtomicUsize::new(0));
        let policy_b = Arc::new(CountingPolicy {
            name: "p3".to_string(),
            filter: EventFilter::all(),
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

        assert!(
            outcome_b.unwrap().is_busy(),
            "p3's lease must still be renewed (and so held by worker-a) at \
             320ms, while worker-a is still inside p3's handler"
        );
        assert_eq!(count_b.load(Ordering::SeqCst), 0);
        assert_eq!(outcome_a.unwrap(), PolicyRunOutcome::Completed(3));
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

        let count = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let can_continue = Arc::new(tokio::sync::Notify::new());
        let policy_a = Arc::new(GatedPolicy {
            name: "lost".to_string(),
            entries: Arc::new(AtomicUsize::new(0)),
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
            count.load(Ordering::SeqCst),
            1,
            "events 2 and 3 must never run once the lease is lost"
        );
        assert_eq!(
            cp.get("lost"),
            Some(Position::start()),
            "worker-b's checkpoint must be untouched by worker-a's aborted run"
        );
    }

    /// A handler that fails until `fail_until` has elapsed since
    /// `started_at`, then succeeds — used to drive a long [`OnError::Retry`]
    /// backoff deterministically by wall-clock time.
    struct FlakyThenOkPolicy {
        name: String,
        fail_until: Duration,
        started_at: std::time::Instant,
        ok_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for FlakyThenOkPolicy {
        fn name(&self) -> &str {
            &self.name
        }

        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }

        async fn handle(&self, _event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            if self.started_at.elapsed() < self.fail_until {
                return Err(crate::Error::custom("still flaky"));
            }
            self.ok_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A handler stuck in a long [`OnError::Retry`] backoff must not lose
    /// its lease to another worker mid-retry. Reproduces the original bug
    /// exactly: a 300ms lease, [`RetryLimit::Indefinite`] with a 50ms base /
    /// 100ms max delay, a handler that fails for ~900ms before succeeding,
    /// and a second worker that tries to acquire the same lease at 500ms —
    /// partway through worker-a's retry, and well past the original 300ms
    /// lease duration. Before the fix the lease expired mid-retry and
    /// worker-b acquired it and ran the event too; after the fix the
    /// retry's chunked renewals keep the lease alive and worker-b always
    /// sees it as busy.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_retry_backoff_keeps_the_lease_renewed_past_its_duration() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("flaky", Position::start())
            .await
            .unwrap();
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let ok_a = Arc::new(AtomicUsize::new(0));
        let policy_a = Arc::new(FlakyThenOkPolicy {
            name: "flaky".to_string(),
            fail_until: Duration::from_millis(900),
            started_at: std::time::Instant::now(),
            ok_count: Arc::clone(&ok_a),
        });

        let retry = OnError::Retry(RetryConfig {
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(100),
            limit: RetryLimit::Indefinite,
            on_exhausted: OnRetryExhausted::Fail,
        });

        let runner_a = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_a)
            .with_worker_id("worker-a")
            .with_lease_duration(Duration::from_millis(300))
            .on_error(retry);

        let call_a = tokio::spawn(async move { runner_a.process_pending().await });

        tokio::time::sleep(Duration::from_millis(500)).await;

        let ok_b = Arc::new(AtomicUsize::new(0));
        let policy_b = Arc::new(CountingPolicy {
            name: "flaky".to_string(),
            filter: EventFilter::all(),
            count: Arc::clone(&ok_b),
        });
        let runner_b = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_b)
            .with_worker_id("worker-b")
            .with_lease_duration(Duration::from_millis(300));
        let outcome_b = runner_b.process_pending().await.unwrap();

        let outcome_a = call_a.await.unwrap().unwrap();

        assert_eq!(
            ok_b.load(Ordering::SeqCst),
            0,
            "worker-b must never run the handler while worker-a's retry still holds the lease"
        );
        assert!(
            outcome_b.is_busy(),
            "worker-b must see the still-renewed lease as busy, not free"
        );
        assert_eq!(outcome_a, PolicyRunOutcome::Completed(1));
        assert_eq!(ok_a.load(Ordering::SeqCst), 1);
    }

    /// A handler that always fails, used to drive deterministic retry
    /// backoff timing without depending on wall-clock success.
    struct AlwaysFailPolicy {
        name: String,
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for AlwaysFailPolicy {
        fn name(&self) -> &str {
            &self.name
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }
        async fn handle(&self, _event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            Err(crate::Error::custom("always fails"))
        }
    }

    /// XN-3/M8: a *single* retry backoff sleep longer than the lease
    /// duration must still renew the lease in chunks while sleeping through
    /// it, not just at the top of each retry attempt. `base_delay ==
    /// max_delay == 450ms` with a `lease_duration` of 300ms means the first
    /// backoff sleep alone outlasts the original lease: the only way it can
    /// still be held at 375ms (after the 300ms mark, before the next
    /// top-of-attempt renewal at 450ms) is if `sleep_renewing` renewed mid-
    /// sleep.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_long_single_retry_sleep_keeps_renewing_the_lease() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        cp.save_checkpoint("m8", Position::start()).await.unwrap();
        store.add_event(test_envelope("TestEvent", "TestAggregate"));

        let policy = Arc::new(AlwaysFailPolicy {
            name: "m8".to_string(),
        });
        let retry = OnError::Retry(RetryConfig {
            base_delay: Duration::from_millis(450),
            max_delay: Duration::from_millis(450),
            limit: RetryLimit::MaxRetries(2),
            on_exhausted: OnRetryExhausted::Fail,
        });

        let runner_a = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy)
            .with_worker_id("worker-a")
            .with_lease_duration(Duration::from_millis(300))
            .on_error(retry);

        let call_a = tokio::spawn(async move { runner_a.process_pending().await });

        tokio::time::sleep(Duration::from_millis(375)).await;

        let policy_b = Arc::new(CountingPolicy {
            name: "m8".to_string(),
            filter: EventFilter::all(),
            count: Arc::new(AtomicUsize::new(0)),
        });
        let runner_b = PolicyRunner::new(Arc::clone(&store), Arc::clone(&cp) as CheckpointStoreRef)
            .register(policy_b)
            .with_worker_id("worker-b")
            .with_lease_duration(Duration::from_millis(300));
        let outcome_b = runner_b.process_pending().await.unwrap();

        assert!(
            outcome_b.is_busy(),
            "a single retry backoff sleep longer than the lease must still \
             keep it renewed in chunks"
        );

        let outcome_a = tokio::time::timeout(Duration::from_secs(5), call_a)
            .await
            .expect("worker-a must not hang")
            .unwrap();
        assert!(
            outcome_a.is_err(),
            "retries exhausted under OnRetryExhausted::Fail must propagate"
        );
    }

    struct ReactPolicy {
        failures: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl<S: EventStore + 'static> Policy<S> for ReactPolicy {
        fn name(&self) -> &'static str {
            "react"
        }

        fn event_filter(&self) -> EventFilter {
            EventFilter::all()
        }

        async fn handle(&self, _event: &EventEnvelope, ctx: &PolicyContext<S>) -> Result<()> {
            let mut reaction = crate::AggregateRoot::<crate::test_fixtures::SimpleTestEntity>::new(
                crate::EntityId::new(),
            );
            reaction
                .apply(crate::test_fixtures::SimpleTestEvent::Created { value: 1 })
                .unwrap();
            ctx.commit(&mut reaction).await?;
            if self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(crate::Error::custom("transient"));
            }
            Ok(())
        }
    }

    fn parent_event() -> EventEnvelope {
        let mut metadata = crate::EventMetadata::new()
            .with_correlation_id("req-1")
            .with_correlation_path(vec!["req-1".to_string()])
            .with_additional_entry("parent", true);
        metadata.tenant = Some("acme".to_string());
        metadata.command = Some("ParentCommand".to_string());
        test_envelope("TestEvent", "TestAggregate").with_metadata(metadata)
    }

    async fn reaction_metadata(
        on_error: OnError,
        failures: usize,
    ) -> (EventEnvelope, crate::EventMetadata) {
        use crate::EventContext;

        let store = Arc::new(MockEventStore::new());
        let runner = PolicyRunner::new(Arc::clone(&store), test_checkpoint_store())
            .register(Arc::new(ReactPolicy {
                failures: AtomicUsize::new(failures),
            }))
            .on_error(on_error);
        let parent = parent_event();

        let handled = runner
            .process_event(&parent)
            .with_event_context(
                EventContext::root()
                    .tenant("other-tenant")
                    .correlation_id("ambient")
                    .command("AmbientCommand")
                    .with("ambient", true),
            )
            .await
            .unwrap();

        assert_eq!(handled, 1);
        let events = store.get_events();
        assert_eq!(events.len(), 1);
        let metadata = events[0].metadata.clone().expect("reaction is tagged");
        (parent, metadata)
    }

    fn assert_source_context(parent: &EventEnvelope, metadata: &crate::EventMetadata) {
        assert_eq!(metadata.tenant.as_deref(), Some("acme"));
        assert_eq!(metadata.correlation_id.as_deref(), Some("req-1"));
        assert_eq!(metadata.correlation_path, ["req-1"]);
        assert_eq!(metadata.command.as_deref(), Some("react"));
        assert_eq!(metadata.additional, None);
        assert_eq!(metadata.causation_id, Some(parent.id));
        assert_eq!(metadata.causation_chain, vec![parent.id]);
    }

    #[tokio::test]
    async fn reactions_run_in_the_source_context_whatever_the_ambient_one() {
        let (parent, metadata) = reaction_metadata(OnError::Fail, 0).await;

        assert_source_context(&parent, &metadata);
    }

    #[tokio::test]
    async fn retried_reactions_run_in_the_source_context() {
        let retry = OnError::Retry(RetryConfig {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            limit: RetryLimit::Indefinite,
            on_exhausted: OnRetryExhausted::Fail,
        });

        let (parent, metadata) = reaction_metadata(retry, 2).await;

        assert_source_context(&parent, &metadata);
    }

    #[tokio::test]
    async fn a_source_without_metadata_correlates_its_reactions_to_its_own_id() {
        let store = Arc::new(MockEventStore::new());
        let runner = PolicyRunner::new(Arc::clone(&store), test_checkpoint_store()).register(
            Arc::new(ReactPolicy {
                failures: AtomicUsize::new(0),
            }),
        );
        let parent = test_envelope("TestEvent", "TestAggregate");

        runner.process_event(&parent).await.unwrap();

        let metadata = store.get_events()[0].metadata.clone().unwrap();
        assert_eq!(metadata.correlation_id, Some(parent.id.to_string()));
        assert_eq!(metadata.correlation_path, [parent.id.to_string()]);
        assert_eq!(metadata.causation_id, Some(parent.id));
    }
}
