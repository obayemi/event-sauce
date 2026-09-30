//! Unified `PostgreSQL` backend setup.
//!
//! Provides a single entry point for setting up all `PostgreSQL` stores
//! (event store + checkpoint store) with migrations.

use std::sync::Arc;
use std::time::Duration;

use event_sauce_core::{CheckpointStore, Error, EventFilter, Position, Result, SnapshotConfig};
use sqlx::PgPool;

use crate::{LeaseOutcome, PostgresCheckpointStore, PostgresEventStore, PostgresProjection};

/// A fully configured `PostgreSQL` backend with event store and checkpoint store.
///
/// `PostgresBackend` simplifies setup by creating and migrating all required stores
/// from a database URL and optional configuration. This replaces the manual 6-step
/// process of creating pools, stores, running migrations, and wiring `Arc` references.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresBackend;
///
/// // One-liner setup with defaults
/// let backend = PostgresBackend::setup("postgresql://localhost/events", "event_sauce").await?;
///
/// // Access components
/// let event_store = backend.event_store();
/// let checkpoint_store = backend.checkpoint_store();
/// let pool = backend.pool();
/// ```
pub struct PostgresBackend {
    pool: PgPool,
    event_store: Arc<PostgresEventStore>,
    checkpoint_store: Arc<PostgresCheckpointStore>,
}

impl PostgresBackend {
    /// Sets up a fully configured `PostgreSQL` backend with the given database URL and schema.
    ///
    /// This is a convenience method that connects to the database, creates both stores
    /// in the specified schema, runs all migrations, and wires the checkpoint store
    /// into the event store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresBackend;
    ///
    /// let backend = PostgresBackend::setup("postgresql://localhost/events", "event_sauce").await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the database connection fails or migrations cannot be applied.
    pub async fn setup(database_url: &str, schema: &str) -> Result<Self> {
        Self::builder()
            .database_url(database_url)
            .schema(schema)
            .build()
            .await
    }

    /// Creates a builder for configuring the backend.
    ///
    /// Use the builder for custom snapshot configuration or when you want
    /// to set options incrementally.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_postgres::PostgresBackend;
    /// use event_sauce_core::SnapshotConfig;
    ///
    /// let backend = PostgresBackend::builder()
    ///     .database_url("postgresql://localhost/events")
    ///     .schema("my_schema")
    ///     .snapshot_config(SnapshotConfig::disabled())
    ///     .build()
    ///     .await?;
    /// ```
    #[must_use]
    pub fn builder() -> PostgresBackendBuilder {
        PostgresBackendBuilder::new()
    }

    /// Returns a reference to the underlying connection pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a shared reference to the event store.
    #[must_use]
    pub fn event_store(&self) -> Arc<PostgresEventStore> {
        Arc::clone(&self.event_store)
    }

    /// Returns a shared reference to the checkpoint store.
    #[must_use]
    pub fn checkpoint_store(&self) -> Arc<PostgresCheckpointStore> {
        Arc::clone(&self.checkpoint_store)
    }

    /// Creates a [`PostgresEventLogQuery`](crate::PostgresEventLogQuery) for this backend.
    ///
    /// The returned query object uses the same connection pool and schema as the event store.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_core::EventLogQuery;
    ///
    /// let log_query = backend.event_log_query();
    /// let page = log_query.query_events(Default::default()).await?;
    /// ```
    #[must_use]
    pub fn event_log_query(&self) -> crate::PostgresEventLogQuery {
        self.event_store.event_log_query()
    }

    /// Runs a [`PostgresProjection`](PostgresProjection) atomically.
    ///
    /// For every matched event the runner opens a transaction, calls
    /// [`PostgresProjection::handle`](PostgresProjection::handle) with
    /// it, advances the subscription checkpoint inside the same transaction,
    /// and commits. If any step fails the transaction is dropped (rolled back)
    /// and the error propagates — the checkpoint never advances past an event
    /// whose materialization didn't commit, so a re-run picks up exactly where
    /// the failure occurred. A fetched batch's unmatched tail still moves the
    /// checkpoint, in its own cheap write outside any transaction, so the
    /// next fetch doesn't re-scan events already known not to match.
    ///
    /// # Multi-instance safety
    ///
    /// This method does **not** acquire a lease. Running it from two
    /// processes (or two tasks) at the same time will race on the checkpoint
    /// and double-apply events. Use
    /// [`run_leased_projection`](Self::run_leased_projection) when more than
    /// one instance might run the same projection.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut projection = OrderTotalsProjection;
    /// backend.run_postgres_projection(&mut projection).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint loading fails, the event stream errors,
    /// the projection's `handle` returns an error, or the per-event
    /// transaction cannot be started or committed.
    pub async fn run_postgres_projection<P: PostgresProjection>(
        &self,
        projection: &mut P,
    ) -> Result<()> {
        let mut current_position = self
            .checkpoint_store
            .load_checkpoint(P::NAME)
            .await?
            .unwrap_or_else(Position::start);

        let filter = P::event_filter();

        loop {
            let batch = self
                .event_store
                .fetch_events_batch(current_position, PROJECTION_BATCH_SIZE)
                .await?;
            if batch.is_empty() {
                break;
            }

            let mut checkpointed_position = current_position;
            for entry in batch {
                current_position = entry.position;

                if !filter.matches(&entry.envelope) {
                    continue;
                }

                let mut tx = self.begin("projection").await?;

                projection.handle(&entry.envelope, &mut tx).await?;
                self.checkpoint_store
                    .save_checkpoint_tx(&mut tx, P::NAME, current_position)
                    .await?;

                tx.commit()
                    .await
                    .map_err(|e| Error::backend("Failed to commit projection transaction", e))?;
                checkpointed_position = current_position;
            }

            if checkpointed_position != current_position {
                self.checkpoint_store
                    .save_checkpoint(P::NAME, current_position)
                    .await?;
            }
        }

        Ok(())
    }

    /// Runs a [`PostgresProjection`](PostgresProjection) under a lease.
    ///
    /// Acquires the lease for `P::NAME` on behalf of `worker_id`, drains all
    /// currently-available events (atomically per event, identically to
    /// [`run_postgres_projection`](Self::run_postgres_projection)), then
    /// releases the lease and returns. If another worker holds an active
    /// lease, this is a no-op that returns
    /// [`LeaseOutcome::Busy`](LeaseOutcome::Busy) — the caller can
    /// retry later.
    ///
    /// While processing, the lease is renewed roughly every
    /// `lease_duration / 3` to keep ownership. If renewal fails — typically
    /// because the lease expired and was taken by another worker — the run
    /// stops with an error rather than risk concurrent processing.
    /// Additionally, every per-event checkpoint advance is *fenced* on lease
    /// ownership and monotonicity: if this worker stalled past `leased_until`
    /// and another worker took over while a transaction was in flight, the
    /// fenced write is rejected, the transaction is rolled back, and the run
    /// stops with [`Error::LeaseLost`](Error::LeaseLost) —
    /// never double-applying an event or regressing the checkpoint.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let worker_id = format!("{}-{}", hostname()?, std::process::id());
    /// let outcome = backend
    ///     .run_leased_projection(&mut OrderTotalsProjection, &worker_id, Duration::from_secs(30))
    ///     .await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if checkpoint operations fail, the event stream
    /// errors, the projection's `handle` returns an error, the per-event
    /// transaction cannot be started or committed, or the lease is lost
    /// during the run.
    pub async fn run_leased_projection<P: PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<LeaseOutcome> {
        self.with_lease(P::NAME, worker_id, lease_duration, |start_position| {
            self.run_under_lease(projection, worker_id, lease_duration, start_position)
        })
        .await
    }

    /// Runs a [`PostgresProjection`](PostgresProjection) under a lease
    /// that is held across idle ticks, instead of being released and
    /// re-acquired on every call like [`run_leased_projection`].
    ///
    /// [`run_leased_projection`] acquires the lease, drains whatever is
    /// currently available, and releases it — every single call. Driven from
    /// a `NOTIFY`-woken loop, every commit then wakes every worker on every
    /// node to race for that lease via a contended `INSERT ... ON CONFLICT DO
    /// UPDATE`, even though only one of them can ever win it, and only the
    /// winner had any work to do.
    ///
    /// This entry point instead acquires the lease once and holds it,
    /// sharing the same acquire/release wrapper [`run_leased_projection`]
    /// itself uses (the policy dispatcher manages its own per-policy leases
    /// separately): after each drain it sleeps `idle_interval`, checks
    /// `should_continue`, and — only if told to keep going — renews the
    /// lease and drains again, without releasing in between. Checking
    /// right after the sleep and before the renewal means a shutdown
    /// signaled during that sleep is noticed immediately, with no extra
    /// renewal or drain first. Once `should_continue` returns `false` the
    /// lease is released and the run returns
    /// [`LeaseOutcome::Completed`](LeaseOutcome::Completed). The lease
    /// is also released if it is ever lost (an unexpected renewal failure, or
    /// a fenced checkpoint save rejected mid-drain), in which case the error
    /// propagates.
    ///
    /// `idle_interval` must stay below `lease_duration / 3` — the same
    /// margin the drain's own per-event renewal uses — or the lease could
    /// lapse during the idle sleep itself,
    /// before there is a chance to renew
    /// it; this is rejected with
    /// [`Error::InvalidState`](Error::InvalidState) rather
    /// than left to fail with [`Error::LeaseLost`](Error::LeaseLost)
    /// on the first idle tick.
    ///
    /// This does not change how a **non-holder** waits — the checkpoint store
    /// does not currently expose a lease's expiry for a non-holder to sleep
    /// until, so every node still wakes on every `NOTIFY` and probes the
    /// lease. That remains a smaller cost than before (a probe that loses is
    /// one contended write the *previous* holder no longer also performs on
    /// every tick).
    ///
    /// # Errors
    ///
    /// Returns an error if `idle_interval` is not below `lease_duration / 3`,
    /// or under the same conditions as
    /// [`run_leased_projection`], plus if renewing the held lease between
    /// idle ticks fails.
    ///
    /// [`run_leased_projection`]: Self::run_leased_projection
    pub async fn run_sticky_projection<P: PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: Duration,
        idle_interval: Duration,
        mut should_continue: impl FnMut() -> bool + Send,
    ) -> Result<LeaseOutcome> {
        if idle_interval >= lease_duration / 3 {
            return Err(Error::invalid_state(format!(
                "idle_interval ({idle_interval:?}) must be below lease_duration / 3 \
                 ({:?}), or the lease could lapse before the next renewal",
                lease_duration / 3,
            )));
        }

        self.with_lease(
            P::NAME,
            worker_id,
            lease_duration,
            |start_position| async move {
                let mut position = start_position;
                loop {
                    self.run_under_lease(projection, worker_id, lease_duration, position)
                        .await?;

                    tokio::time::sleep(idle_interval).await;

                    if !should_continue() {
                        return Ok(());
                    }

                    self.checkpoint_store
                        .renew_lease(P::NAME, worker_id, lease_duration)
                        .await?;

                    position = self
                        .checkpoint_store
                        .load_checkpoint(P::NAME)
                        .await?
                        .unwrap_or(position);
                }
            },
        )
        .await
    }

    /// Acquires the lease for `name` on behalf of `worker_id`, runs `body`
    /// with the position the lease started from, and always releases the
    /// lease on the way out (including on error — releasing a lease we no
    /// longer hold is a no-op).
    ///
    /// Shared by every entry point that holds a single named lease
    /// ([`run_leased_projection`](Self::run_leased_projection),
    /// [`rebuild`](Self::rebuild)) so the
    /// acquire/Busy/release/[`LeaseOutcome`](LeaseOutcome) mapping is
    /// written once.
    /// [`dispatch_policies_to_outbox`](Self::dispatch_policies_to_outbox)
    /// holds one lease per policy instead — it may end up dispatching only
    /// a subset of the policies it was given — so it manages its own set of
    /// leases rather than using this helper.
    async fn with_lease<F, Fut>(
        &self,
        name: &str,
        worker_id: &str,
        lease_duration: Duration,
        body: F,
    ) -> Result<LeaseOutcome>
    where
        F: FnOnce(Position) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let Some(start_position) = self
            .checkpoint_store
            .try_acquire_lease(name, worker_id, lease_duration)
            .await?
        else {
            return Ok(LeaseOutcome::Busy);
        };

        let result = body(start_position).await;

        let _ = self.checkpoint_store.release_lease(name, worker_id).await;

        result.map(|()| LeaseOutcome::Completed)
    }

    /// Rebuilds a [`PostgresProjection`](PostgresProjection) from genesis
    /// under a lease.
    ///
    /// A rebuild wipes the projection's read-model, rewinds its checkpoint to
    /// the start, and re-derives the whole model by replaying every event from
    /// position 0. Unlike the old manual procedure (drop the table by hand,
    /// `delete_checkpoint`, re-run), this is **lease-guarded** and **atomic**:
    ///
    /// 1. Acquire the lease for `P::NAME` on behalf of `worker_id`. If another
    ///    worker holds an active lease, return
    ///    [`LeaseOutcome::Busy`](LeaseOutcome::Busy) **without touching**
    ///    the read-model or the checkpoint — a concurrent worker is still
    ///    running against the live model, so resetting it would corrupt its
    ///    view.
    /// 2. In a single transaction, call
    ///    [`reset`](PostgresProjection::reset) to clear the read-model
    ///    and rewind the checkpoint to
    ///    [`Position::start`](Position::start). Both commit or
    ///    roll back together, so a rebuild never leaves a wiped table paired
    ///    with a stale checkpoint.
    /// 3. Re-drain from genesis using the same fenced per-event loop as
    ///    [`run_leased_projection`](Self::run_leased_projection).
    /// 4. Release the lease on exit, including on error.
    ///
    /// The projection **must** override
    /// [`reset`](PostgresProjection::reset); the default implementation
    /// returns an error so a projection that has not opted in fails loudly here
    /// rather than being silently half-rebuilt.
    ///
    /// The step-2 rewind to position 0 is a backward checkpoint move, which the
    /// fenced save ([`save_checkpoint_fenced_tx`]) deliberately rejects. The
    /// rewind therefore uses the **unfenced** [`save_checkpoint_tx`] — safe
    /// because this call holds the lease and is the legitimate owner performing
    /// an intentional rewind. The subsequent forward drain uses the fenced save
    /// as usual, preserving the monotonicity guarantee for ordinary runs.
    ///
    /// [`save_checkpoint_fenced_tx`]: crate::PostgresCheckpointStore::save_checkpoint_fenced_tx
    /// [`save_checkpoint_tx`]: crate::PostgresCheckpointStore::save_checkpoint_tx
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let worker_id = format!("{}-{}", hostname()?, std::process::id());
    /// let outcome = backend
    ///     .rebuild(&mut OrderTotalsProjection, &worker_id, Duration::from_secs(30))
    ///     .await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the projection does not override
    /// [`reset`](PostgresProjection::reset), if the reset/checkpoint
    /// transaction fails, or for any reason
    /// [`run_leased_projection`](Self::run_leased_projection) would error during
    /// the re-drain.
    pub async fn rebuild<P: PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<LeaseOutcome> {
        self.with_lease(P::NAME, worker_id, lease_duration, |_start_position| {
            self.rebuild_under_lease(projection, worker_id, lease_duration)
        })
        .await
    }

    /// Performs the atomic reset (read-model wipe + checkpoint rewind) and the
    /// subsequent fenced re-drain. The caller already holds the lease and is
    /// responsible for releasing it.
    async fn rebuild_under_lease<P: PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<()> {
        // Atomic reset: clear the read-model and rewind the checkpoint to
        // genesis in one transaction. The rewind is a backward move, so it uses
        // the UNFENCED save (the fenced save would reject it) — safe because we
        // hold the lease and this is a deliberate rewind by the owner.
        let mut tx = self.begin("rebuild reset").await?;

        projection.reset(&mut tx).await?;
        self.checkpoint_store
            .save_checkpoint_tx(&mut tx, P::NAME, Position::start())
            .await?;

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit rebuild reset transaction", e))?;

        // Re-drain from genesis with the usual fenced per-event loop.
        self.run_under_lease(projection, worker_id, lease_duration, Position::start())
            .await
    }

    /// Reads new events from the log and fans them out into the policy
    /// outbox for each registered policy whose filter matches.
    ///
    /// Each policy in `policies` tracks its own checkpoint and lease, named
    /// `"__policy_outbox_dispatcher:{policy_name}"` — not one name shared by
    /// every caller, and not one derived from the whole set passed to a
    /// given call. Two calls that share a policy name track (and contend
    /// for the lease of) that policy identically regardless of what else is
    /// in their `policies` list, so a caller adding or removing a policy
    /// (e.g. a rolling deploy) never moves a different policy's progress. A
    /// policy seen for the first time starts from genesis, unless a legacy
    /// checkpoint from before per-policy tracking exists, in which case it
    /// seeds from that. Either way, that first call is that policy's first
    /// delivery of every event since its starting point, not a replay — its
    /// handler runs for that whole history, which can be substantial on a
    /// long-lived log. To start a newly added policy at the current head
    /// instead, pre-seed its `__policy_outbox_dispatcher:{policy_name}`
    /// checkpoint before the first call that names it; seeding here never
    /// overwrites an existing row.
    ///
    /// A *re*scan of a policy already at this checkpoint is harmless, since
    /// [`enqueue_tx`](crate::PostgresPolicyOutbox::enqueue_tx) is
    /// idempotent — but only while the resulting `done` rows are still in
    /// the outbox; a genesis rescan after
    /// [`prune_done`](crate::PostgresPolicyOutbox::prune_done) would
    /// re-deliver, which per-policy checkpoints avoid entirely.
    ///
    /// This call acquires the lease of every policy in `policies` and
    /// dispatches only the ones it successfully holds — another worker
    /// already dispatching a subset of them does not block the rest.
    /// Returns [`LeaseOutcome::Busy`](LeaseOutcome::Busy) only if
    /// none of them could be held.
    ///
    /// Fan-out and every held policy's checkpoint advance run inside one
    /// transaction per fetched batch of events: an event is enqueued for
    /// every matching held policy or for none, and the batch's checkpoint
    /// writes commit or roll back together — never partial. The lease of
    /// every held policy is renewed once per batch rather than once per
    /// event.
    ///
    /// Workers (typically separate processes) then drain the outbox via
    /// [`PostgresPolicyOutbox::claim_batch`](crate::PostgresPolicyOutbox::claim_batch).
    ///
    /// # Errors
    ///
    /// Returns an error if seeding or acquiring a lease fails, the event
    /// stream errors, or a held policy's fenced checkpoint write is
    /// rejected mid-batch (its lease was lost to another worker).
    pub async fn dispatch_policies_to_outbox(
        &self,
        outbox: &crate::PostgresPolicyOutbox,
        worker_id: &str,
        policies: &[PolicyDispatch],
        lease_duration: Duration,
    ) -> Result<LeaseOutcome> {
        let grouped = group_policies_by_name(policies);
        let mut held = Vec::with_capacity(grouped.len());

        let result = match self
            .acquire_policy_leases(grouped, worker_id, lease_duration, &mut held)
            .await
        {
            Ok(()) => match held.iter().map(|h| h.position).min() {
                None => Ok(LeaseOutcome::Busy),
                Some(start) => self
                    .dispatch_under_lease(outbox, worker_id, &mut held, lease_duration, start)
                    .await
                    .map(|()| LeaseOutcome::Completed),
            },
            Err(error) => Err(error),
        };

        for held_policy in &held {
            let _ = self
                .checkpoint_store
                .release_lease(&held_policy.checkpoint_name, worker_id)
                .await;
        }

        result
    }

    /// Acquires every held-able policy's lease, appending each one to
    /// `held` as it goes. `held` reflects every lease acquired so far even
    /// when this returns `Err` — seeding or acquiring a later policy's lease
    /// can fail after an earlier one already succeeded, and the caller
    /// releases everything in `held` regardless of where this returns.
    async fn acquire_policy_leases<'p>(
        &self,
        grouped: Vec<(&'p str, Vec<&'p EventFilter>)>,
        worker_id: &str,
        lease_duration: Duration,
        held: &mut Vec<HeldPolicy<'p>>,
    ) -> Result<()> {
        for (name, filters) in grouped {
            let checkpoint_name = policy_checkpoint_name(name);
            self.checkpoint_store
                .seed_checkpoint_from_legacy(&checkpoint_name, LEGACY_DISPATCHER_CHECKPOINT)
                .await?;
            if let Some(position) = self
                .checkpoint_store
                .try_acquire_lease(&checkpoint_name, worker_id, lease_duration)
                .await?
            {
                held.push(HeldPolicy {
                    name,
                    filters,
                    checkpoint_name,
                    position,
                });
            }
        }

        Ok(())
    }

    /// Drains events for every policy in `held`, one transaction per
    /// **fetched batch** (up to [`PROJECTION_BATCH_SIZE`] events), starting
    /// from `start_position` — the minimum of every held policy's own
    /// checkpoint, which the caller computes since an empty `held` has no
    /// minimum to start from.
    ///
    /// Inside one transaction, an event is enqueued for a held
    /// policy only when the event's position is past *that policy's* own
    /// checkpoint and its filter matches — a policy already ahead (ran
    /// under a different call more recently) skips events it has already
    /// seen. Every held policy the batch actually advances past then has
    /// its checkpoint written, fenced, before the transaction commits, so
    /// the batch's enqueues and every checkpoint advance land together or
    /// not at all.
    ///
    /// If any held policy's fenced write is rejected (its lease was lost
    /// mid-batch), the whole batch rolls back and this returns
    /// [`Error::LeaseLost`](Error::LeaseLost) for that
    /// policy — re-fetching from the last successfully committed batch's
    /// position is always idempotent (`enqueue_tx` is `ON CONFLICT DO
    /// NOTHING`).
    async fn dispatch_under_lease(
        &self,
        outbox: &crate::PostgresPolicyOutbox,
        worker_id: &str,
        held: &mut [HeldPolicy<'_>],
        lease_duration: Duration,
        start_position: Position,
    ) -> Result<()> {
        let mut current_position = start_position;
        let renew_interval = lease_duration / 3;
        let mut last_renew = std::time::Instant::now();

        loop {
            let batch = self
                .event_store
                .fetch_events_batch(current_position, PROJECTION_BATCH_SIZE)
                .await?;
            let Some(batch_end) = batch.last().map(|e| e.position) else {
                break;
            };

            if last_renew.elapsed() >= renew_interval {
                for held_policy in held.iter() {
                    self.checkpoint_store
                        .renew_lease(&held_policy.checkpoint_name, worker_id, lease_duration)
                        .await?;
                }
                last_renew = std::time::Instant::now();
            }

            let mut tx = self.begin("dispatcher").await?;

            for entry in &batch {
                for held_policy in held.iter() {
                    if entry.position > held_policy.position
                        && held_policy
                            .filters
                            .iter()
                            .any(|filter| filter.matches(&entry.envelope))
                    {
                        outbox
                            .enqueue_tx(
                                &mut tx,
                                held_policy.name,
                                entry.envelope.id,
                                entry.position.as_i64(),
                            )
                            .await?;
                    }
                }
            }

            let advancing: Vec<(&str, Position)> = held
                .iter()
                .filter(|held_policy| batch_end > held_policy.position)
                .map(|held_policy| (held_policy.checkpoint_name.as_str(), batch_end))
                .collect();
            self.commit_fenced(tx, worker_id, &advancing).await?;
            for held_policy in held.iter_mut() {
                held_policy.position = held_policy.position.max(batch_end);
            }
            current_position = batch_end;
        }

        Ok(())
    }

    /// Fenced per-event drain used under a held lease. A fetched batch's
    /// unmatched tail still advances the checkpoint (fenced, in its own
    /// small transaction) so a restart or a `wait_for_checkpoint` poll
    /// doesn't re-fetch it — unfenced would let a stalled worker move the
    /// checkpoint after losing the lease, the same risk the per-event
    /// fenced save guards against.
    async fn run_under_lease<P: PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: Duration,
        start_position: Position,
    ) -> Result<()> {
        let filter = P::event_filter();
        let mut current_position = start_position;
        let renew_interval = lease_duration / 3;
        let mut last_renew = std::time::Instant::now();

        loop {
            let batch = self
                .event_store
                .fetch_events_batch(current_position, PROJECTION_BATCH_SIZE)
                .await?;
            if batch.is_empty() {
                break;
            }

            let mut checkpointed_position = current_position;
            for entry in batch {
                current_position = entry.position;

                if last_renew.elapsed() >= renew_interval {
                    self.checkpoint_store
                        .renew_lease(P::NAME, worker_id, lease_duration)
                        .await?;
                    last_renew = std::time::Instant::now();
                }

                if !filter.matches(&entry.envelope) {
                    continue;
                }

                let mut tx = self.begin("projection").await?;

                projection.handle(&entry.envelope, &mut tx).await?;
                self.commit_fenced(tx, worker_id, &[(P::NAME, current_position)])
                    .await?;
                checkpointed_position = current_position;
            }

            if checkpointed_position != current_position {
                let tx = self.begin("projection").await?;
                self.commit_fenced(tx, worker_id, &[(P::NAME, current_position)])
                    .await?;
            }
        }

        Ok(())
    }

    /// Starts a transaction on the pool, mapping a failure to
    /// `Failed to start {what} transaction`.
    async fn begin(&self, what: &str) -> Result<sqlx::Transaction<'_, sqlx::Postgres>> {
        self.pool
            .begin()
            .await
            .map_err(|e| Error::backend(format!("Failed to start {what} transaction"), e))
    }

    /// Applies every `(name, position)` fenced checkpoint write inside `tx`;
    /// if all land, commits and returns `Ok(())`. If any is rejected by the
    /// fence — the caller no longer holds that lease, or the position does
    /// not strictly advance — rolls back the whole transaction and returns
    /// [`Error::LeaseLost`](Error::LeaseLost) for that
    /// name, so a partially-applied batch never lands.
    ///
    /// Shared by every fenced commit site: the per-event and tail advances
    /// of [`run_under_lease`](Self::run_under_lease), and every held
    /// policy's advance in
    /// [`dispatch_under_lease`](Self::dispatch_under_lease).
    async fn commit_fenced(
        &self,
        mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
        worker_id: &str,
        checkpoints: &[(&str, Position)],
    ) -> Result<()> {
        for (name, position) in checkpoints {
            let landed = self
                .checkpoint_store
                .save_checkpoint_fenced_tx(&mut tx, name, worker_id, *position)
                .await?;
            if !landed {
                tx.rollback()
                    .await
                    .map_err(|e| Error::backend("Failed to roll back transaction", e))?;
                return Err(Error::lease_lost(*name, worker_id));
            }
        }

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit transaction", e))
    }
}

/// Maximum number of events fetched per batch by the projection runners and
/// the policy dispatcher.
///
/// Bounds memory and ensures the streaming connection is released between
/// batches, so the connection pool isn't held captive while a projection or
/// a dispatch call catches up. The lease is renewed inside each batch loop,
/// so this also bounds how long a projection runner goes between pool
/// connections, and, for the dispatcher, the longest stretch between lease
/// renewals.
const PROJECTION_BATCH_SIZE: i64 = 500;

/// Name of the shared checkpoint/lease every dispatch call wrote before
/// per-policy checkpoints existed. No longer written; kept only as a seed
/// source for a policy's first per-policy checkpoint (see
/// [`PostgresBackend::dispatch_policies_to_outbox`]). Frozen at this literal
/// regardless of [`DISPATCHER_CHECKPOINT_PREFIX`]'s current value — an
/// existing deployment's legacy row must stay findable even if the live
/// naming scheme ever changes.
const LEGACY_DISPATCHER_CHECKPOINT: &str = "__policy_outbox_dispatcher";

/// Prefix of every live `{prefix}:{policy}` per-policy checkpoint name (see
/// [`policy_checkpoint_name`]). Shares [`LEGACY_DISPATCHER_CHECKPOINT`]'s
/// value today, but the two are free to diverge: this one names an ongoing
/// scheme, that one a fixed historical row.
const DISPATCHER_CHECKPOINT_PREFIX: &str = LEGACY_DISPATCHER_CHECKPOINT;

/// Derives the checkpoint (and lease) name for one policy passed to
/// [`PostgresBackend::dispatch_policies_to_outbox`].
///
/// Every dispatch call that names this policy shares its progress and
/// contends for its lease, regardless of what else is in that call's
/// `policies` list.
fn policy_checkpoint_name(policy_name: &str) -> String {
    format!("{DISPATCHER_CHECKPOINT_PREFIX}:{policy_name}")
}

/// Groups `policies` by name, preserving first-seen order. A name repeated
/// under different filters (e.g. one call fanning out to the same
/// downstream handler through two `EventFilter`s) collapses into one entry
/// whose event matches the union of its filters — the same outbox row and
/// checkpoint a caller would get by registering it once with an `Or` filter.
fn group_policies_by_name(policies: &[PolicyDispatch]) -> Vec<(&str, Vec<&EventFilter>)> {
    let mut grouped: Vec<(&str, Vec<&EventFilter>)> = Vec::new();
    for policy in policies {
        match grouped.iter_mut().find(|(name, _)| *name == policy.name) {
            Some((_, filters)) => filters.push(&policy.filter),
            None => grouped.push((&policy.name, vec![&policy.filter])),
        }
    }
    grouped
}

/// One policy name [`PostgresBackend::dispatch_policies_to_outbox`]
/// currently holds the lease for, tracked with its own checkpoint name and
/// the position that checkpoint held when the lease was acquired.
struct HeldPolicy<'a> {
    name: &'a str,
    filters: Vec<&'a EventFilter>,
    checkpoint_name: String,
    position: Position,
}

/// One entry in the policy registry passed to
/// [`PostgresBackend::dispatch_policies_to_outbox`].
///
/// Pairs a policy name with the filter the dispatcher uses to decide which
/// events should produce outbox rows for that policy. Names must match what
/// drainer workers will pass to
/// [`PostgresPolicyOutbox::claim_batch`](crate::PostgresPolicyOutbox::claim_batch).
#[derive(Debug, Clone)]
pub struct PolicyDispatch {
    /// Policy name; used as the outbox row's `policy_name` and as the
    /// dispatcher's checkpoint/lease name for this policy. A call's
    /// `policies` may repeat a name under different filters — they collapse
    /// into one checkpoint whose match is the union of those filters.
    pub name: String,
    /// Which events trigger an outbox row for this policy.
    pub filter: EventFilter,
}

impl PolicyDispatch {
    /// Convenience constructor.
    #[must_use]
    pub fn new(name: impl Into<String>, filter: EventFilter) -> Self {
        Self {
            name: name.into(),
            filter,
        }
    }
}

/// Builder for configuring a [`PostgresBackend`].
///
/// # Examples
///
/// ```ignore
/// use event_sauce_postgres::PostgresBackend;
///
/// let backend = PostgresBackend::builder()
///     .database_url("postgresql://localhost/events")
///     .build()
///     .await?;
/// ```
pub struct PostgresBackendBuilder {
    database_url: Option<String>,
    pool: Option<PgPool>,
    schema: Option<String>,
    snapshot_config: Option<SnapshotConfig>,
    crypto_key_store: Option<Arc<dyn event_sauce_core::CryptoKeyStore>>,
    crypto_provider: Option<Arc<dyn event_sauce_core::CryptoProvider>>,
}

impl PostgresBackendBuilder {
    /// Creates a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            database_url: None,
            pool: None,
            schema: None,
            snapshot_config: None,
            crypto_key_store: None,
            crypto_provider: None,
        }
    }

    /// Sets the database connection URL.
    ///
    /// Either this or [`pool`](Self::pool) must be set before calling `build()`.
    /// When `database_url` is set, `build()` opens a default `PgPool::connect`
    /// to the URL. Use [`pool`](Self::pool) instead to share a pre-configured
    /// pool with the rest of the application (e.g. with a custom
    /// `max_connections`).
    #[must_use]
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
        self
    }

    /// Sets a pre-built `PgPool` to use for all stores.
    ///
    /// Use this when the application already owns a connection pool (e.g.
    /// configured with `PgPoolOptions::max_connections(...)`) so the event
    /// store, checkpoint store, and the rest of the app share a single pool.
    ///
    /// Either this or [`database_url`](Self::database_url) must be set; if both
    /// are provided, `pool` wins.
    #[must_use]
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Sets the schema name for all stores.
    ///
    /// Defaults to `"event_sauce"`.
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Sets the snapshot configuration for the event store.
    ///
    /// Defaults to `SnapshotConfig::builder().build()` (`EveryNEvents(100)` with
    /// snapshots-on-load enabled).
    #[must_use]
    pub fn snapshot_config(mut self, config: SnapshotConfig) -> Self {
        self.snapshot_config = Some(config);
        self
    }

    /// Sets the crypto key store for encrypted aggregate encryption.
    #[must_use]
    pub fn crypto_key_store(mut self, store: Arc<dyn event_sauce_core::CryptoKeyStore>) -> Self {
        self.crypto_key_store = Some(store);
        self
    }

    /// Sets the crypto provider for encrypted aggregate encryption.
    #[must_use]
    pub fn crypto_provider(mut self, provider: Arc<dyn event_sauce_core::CryptoProvider>) -> Self {
        self.crypto_provider = Some(provider);
        self
    }

    /// Builds the `PostgresBackend` by connecting to the database and running migrations.
    ///
    /// # Errors
    ///
    /// Returns an error if neither `database_url` nor `pool` has been set, or
    /// if the database connection fails or migrations cannot be applied.
    pub async fn build(self) -> Result<PostgresBackend> {
        let schema = self.schema.unwrap_or_else(|| "event_sauce".to_string());
        let snapshot_config = self
            .snapshot_config
            .unwrap_or_else(|| SnapshotConfig::builder().build());

        let pool = match (self.pool, self.database_url) {
            (Some(pool), _) => pool,
            (None, Some(url)) => PgPool::connect(&url)
                .await
                .map_err(|e| Error::backend("Failed to connect", e))?,
            (None, None) => {
                return Err(Error::invalid_state(
                    "either database_url or pool is required",
                ));
            }
        };

        let checkpoint_store = PostgresCheckpointStore::builder()
            .pool(pool.clone())
            .schema(&schema)
            .build()?;
        checkpoint_store.migrate().await?;

        let checkpoint_store = Arc::new(checkpoint_store);

        let mut event_store_builder = PostgresEventStore::builder()
            .pool(pool.clone())
            .schema(&schema)
            .snapshot_config(snapshot_config)
            .checkpoint_store(Arc::clone(&checkpoint_store) as Arc<dyn CheckpointStore>);

        if let Some(key_store) = self.crypto_key_store {
            event_store_builder = event_store_builder.crypto_key_store(key_store);
        }
        if let Some(provider) = self.crypto_provider {
            event_store_builder = event_store_builder.crypto_provider(provider);
        }

        let event_store = event_store_builder.build()?;
        event_store.migrate().await?;

        Ok(PostgresBackend {
            pool,
            event_store: Arc::new(event_store),
            checkpoint_store,
        })
    }
}

impl Default for PostgresBackendBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{AggregateVersion, EventEnvelope, EventStore, EventVersion, StreamId};
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    /// Pins the persisted format: this name is written to the checkpoint
    /// table, so a change here would silently re-seed or reset every
    /// policy's progress.
    #[test]
    fn policy_checkpoint_name_is_stable_per_policy() {
        assert_eq!(
            policy_checkpoint_name("send-order-confirmation"),
            "__policy_outbox_dispatcher:send-order-confirmation"
        );
    }

    /// Starts a `PostgreSQL` testcontainer and returns a connection URL.
    async fn start_test_db() -> (
        String,
        testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    ) {
        let (container, url) = crate::test_support::start_postgres_url().await;
        (url, container)
    }

    fn create_test_envelope(aggregate_id: Uuid) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            "TestAggregate".to_string(),
            "TestEvent".to_string(),
            EventVersion::new(1),
            json!({"data": "test"}),
        )
    }

    #[tokio::test]
    async fn test_setup_creates_working_backend() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        // Verify stores are functional by checking schema
        assert_eq!(backend.event_store().schema(), "event_sauce");
    }

    #[tokio::test]
    async fn test_builder_with_defaults() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::builder()
            .database_url(&url)
            .build()
            .await
            .expect("builder with defaults should succeed");

        assert_eq!(backend.event_store().schema(), "event_sauce");
    }

    #[tokio::test]
    async fn test_builder_with_custom_schema() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::builder()
            .database_url(&url)
            .schema("custom_schema")
            .build()
            .await
            .expect("builder with custom schema should succeed");

        assert_eq!(backend.event_store().schema(), "custom_schema");
    }

    #[tokio::test]
    async fn test_builder_with_snapshot_config() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::builder()
            .database_url(&url)
            .snapshot_config(SnapshotConfig::disabled())
            .build()
            .await
            .expect("builder with snapshot config should succeed");

        assert!(!backend
            .event_store()
            .snapshot_config()
            .use_snapshots_on_load());
    }

    #[tokio::test]
    async fn test_event_store_has_checkpoint_store() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        assert!(backend.event_store().checkpoint_store().is_some());
    }

    #[tokio::test]
    async fn test_migrations_run_automatically() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "test_migrations")
            .await
            .expect("setup should succeed");

        // Verify schema and tables exist by querying them
        let result: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1",
        )
        .bind("test_migrations")
        .fetch_one(backend.pool())
        .await
        .expect("should be able to query information_schema");

        // events, snapshots, checkpoints, + 2 migration tracking tables
        assert!(
            result.0 >= 3,
            "expected at least 3 tables, got {}",
            result.0
        );
    }

    #[tokio::test]
    async fn test_event_store_is_functional() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);
        let event = create_test_envelope(aggregate_id);

        let store = backend.event_store();

        store
            .append(
                stream_id.clone(),
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .expect("append should succeed");

        let mut stream = store
            .load_stream(stream_id, AggregateVersion::initial())
            .await
            .expect("load_stream should succeed");

        let loaded = stream.next().await;
        assert!(loaded.is_some(), "should have loaded one event");
        let envelope = loaded.unwrap().expect("event should be Ok");
        assert_eq!(envelope.event_type, "TestEvent");
    }

    #[tokio::test]
    async fn test_checkpoint_store_is_functional() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        backend
            .checkpoint_store()
            .save_checkpoint("test-sub", Position::new(42))
            .await
            .expect("save_checkpoint should succeed");

        let loaded = backend
            .checkpoint_store()
            .load_checkpoint("test-sub")
            .await
            .expect("load_checkpoint should succeed");

        assert_eq!(loaded, Some(Position::new(42)));
    }

    #[tokio::test]
    async fn test_pool_is_accessible() {
        let (url, _container) = start_test_db().await;

        let backend = PostgresBackend::setup(&url, "event_sauce")
            .await
            .expect("setup should succeed");

        let result: (i32,) = sqlx::query_as("SELECT 1")
            .fetch_one(backend.pool())
            .await
            .expect("pool should be functional");

        assert_eq!(result.0, 1);
    }

    #[tokio::test]
    async fn test_builder_errors_without_url_or_pool() {
        let result = PostgresBackend::builder().build().await;
        let err = result.err().expect("should be an error");
        let msg = err.to_string();
        assert!(
            msg.contains("either database_url or pool is required"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn test_builder_with_pool() {
        let (url, _container) = start_test_db().await;

        let pool = PgPool::connect(&url)
            .await
            .expect("pool connect should succeed");

        let backend = PostgresBackend::builder()
            .pool(pool.clone())
            .schema("pool_backend")
            .build()
            .await
            .expect("builder with pre-built pool should succeed");

        assert_eq!(backend.event_store().schema(), "pool_backend");

        // Both backend and the original pool handle should reference the
        // same underlying pool — confirm by querying through both.
        let count: (i64,) = sqlx::query_as("SELECT 1::bigint")
            .fetch_one(backend.pool())
            .await
            .expect("backend pool should be functional");
        assert_eq!(count.0, 1);
        let count: (i64,) = sqlx::query_as("SELECT 2::bigint")
            .fetch_one(&pool)
            .await
            .expect("original pool should still work");
        assert_eq!(count.0, 2);
    }

    /// Counts every matched event in a postgres-backed table.
    struct CountingProjection {
        fail_after: Option<u64>,
        seen: u64,
    }

    impl CountingProjection {
        async fn migrate(pool: &PgPool, schema: &str) {
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {schema}.counting_projection (
                    id INTEGER PRIMARY KEY,
                    n BIGINT NOT NULL
                )"
            ))
            .execute(pool)
            .await
            .unwrap();
            sqlx::query(&format!(
                "INSERT INTO {schema}.counting_projection (id, n) VALUES (1, 0)
                 ON CONFLICT (id) DO NOTHING"
            ))
            .execute(pool)
            .await
            .unwrap();
        }

        async fn read(pool: &PgPool, schema: &str) -> i64 {
            sqlx::query_scalar(&format!(
                "SELECT n FROM {schema}.counting_projection WHERE id = 1"
            ))
            .fetch_one(pool)
            .await
            .unwrap()
        }
    }

    #[async_trait::async_trait]
    impl PostgresProjection for CountingProjection {
        const NAME: &'static str = "CountingProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            _envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> Result<()> {
            self.seen += 1;
            if let Some(limit) = self.fail_after {
                if self.seen > limit {
                    return Err(Error::custom("forced failure"));
                }
            }
            sqlx::query("UPDATE event_sauce.counting_projection SET n = n + 1 WHERE id = 1")
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::custom(format!("update failed: {e}")))?;
            Ok(())
        }
    }

    async fn append_test_event(store: &PostgresEventStore, version: AggregateVersion) -> Uuid {
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);
        let event = create_test_envelope(aggregate_id);
        store
            .append(stream_id, vec![event], version, vec![], false)
            .await
            .expect("append should succeed");
        aggregate_id
    }

    #[tokio::test]
    async fn test_run_postgres_projection_atomic_success() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_postgres_projection(&mut projection)
            .await
            .unwrap();

        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            3
        );

        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(<CountingProjection as PostgresProjection>::NAME)
            .await
            .unwrap();
        assert_eq!(checkpoint, Some(Position::new(3)));
    }

    #[tokio::test]
    async fn test_run_postgres_projection_rolls_back_on_failure() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = CountingProjection {
            fail_after: Some(2),
            seen: 0,
        };
        let err = backend.run_postgres_projection(&mut projection).await;
        assert!(err.is_err(), "expected forced failure");

        // First two events committed; the third was rolled back.
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            2
        );

        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(<CountingProjection as PostgresProjection>::NAME)
            .await
            .unwrap();
        assert_eq!(checkpoint, Some(Position::new(2)));
    }

    #[tokio::test]
    async fn test_run_postgres_projection_resumes_from_checkpoint() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..2 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_postgres_projection(&mut projection)
            .await
            .unwrap();
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            2
        );

        // Append two more events and re-run; only the new ones should be applied.
        for _ in 0..2 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }
        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_postgres_projection(&mut projection)
            .await
            .unwrap();
        assert_eq!(projection.seen, 2);
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            4
        );
    }

    /// Two matched `TestEvent`s, then an unmatched event type as the LAST
    /// event of the (single-fetch) batch.
    #[tokio::test]
    async fn test_run_postgres_projection_checkpoint_advances_past_unmatched_tail() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        append_test_event(&store, AggregateVersion::initial()).await;
        append_test_event(&store, AggregateVersion::initial()).await;
        append_typed(
            &store,
            "TestAggregate".to_string(),
            "OtherEvent".to_string(),
        )
        .await;

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_postgres_projection(&mut projection)
            .await
            .unwrap();

        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            2,
            "only the matched events are applied"
        );
        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(<CountingProjection as PostgresProjection>::NAME)
            .await
            .unwrap();
        assert_eq!(
            checkpoint,
            Some(Position::new(3)),
            "the checkpoint must advance past the unmatched tail event, not \
             stall at the last matched one — otherwise every tick re-fetches \
             the whole unmatched tail again"
        );
    }

    /// Records the event UUID of every envelope it handles into a postgres
    /// table — one row per `handle` call. A duplicate UUID row therefore means
    /// the same event was handled more than once within a run.
    struct RecordingProjection;

    impl RecordingProjection {
        async fn migrate(pool: &PgPool, schema: &str) {
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {schema}.recording_projection (
                    seq BIGSERIAL PRIMARY KEY,
                    event_id UUID NOT NULL
                )"
            ))
            .execute(pool)
            .await
            .unwrap();
        }

        /// Number of `handle` rows recorded for `event_id`.
        async fn count_for(pool: &PgPool, schema: &str, event_id: Uuid) -> i64 {
            sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {schema}.recording_projection WHERE event_id = $1"
            ))
            .bind(event_id)
            .fetch_one(pool)
            .await
            .unwrap()
        }
    }

    #[async_trait::async_trait]
    impl PostgresProjection for RecordingProjection {
        const NAME: &'static str = "RecordingProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> Result<()> {
            sqlx::query("INSERT INTO event_sauce.recording_projection (event_id) VALUES ($1)")
                .bind(envelope.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::custom(format!("insert failed: {e}")))?;
            Ok(())
        }
    }

    /// Appends a single `TestEvent` to its own fresh stream and returns its
    /// event UUID. Each call uses a distinct aggregate, so every append is the
    /// first (version 0) event of its stream.
    async fn append_recorded_event(store: &PostgresEventStore) -> Uuid {
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("TestAggregate", aggregate_id);
        let event = create_test_envelope(aggregate_id);
        let event_id = event.id;
        store
            .append(
                stream_id,
                vec![event],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .expect("append should succeed");
        event_id
    }

    /// Regression test for F1 (C1/C2/C3): the projection runner reconstructs a
    /// SYNTHETIC ordinal (`current_position += 1` per event) instead of using
    /// the real global BIGSERIAL `events.id`. When the id sequence has a GAP —
    /// e.g. a unique-violation conflict burns a `nextval` — that ordinal lags
    /// the real id, so the checkpoint it saves is BELOW the id it just
    /// processed and `WHERE id > checkpoint` re-returns an already-processed
    /// event WITHIN THE SAME drain loop.
    ///
    /// Layout (real committed events keep their ids; a gap forms in between):
    /// - first event   -> id 1            (kept)
    /// - second event  -> id 2            (kept)
    /// - a seeder row + a losing append each consume ids, then both vanish
    ///   (the seeder row is deleted, the loser is aborted) -> a GAP in the ids
    /// - third event   -> a higher id     (kept, AFTER the gap)
    ///
    /// Three events are committed but their ids are NOT dense (e.g. {1, 2, 5}).
    /// Driving a recording projection ONCE must handle each event UUID exactly
    /// once. TODAY it fails: the synthetic `current_position += 1` counter only
    /// reaches 3 after processing all three events, so it checkpoints 3 while
    /// the last real id is 5. The batch loop then re-fetches `WHERE id > 3`,
    /// re-returning the last event AGAIN (and again, until the counter crawls
    /// past the real id) — handling it multiple times in a single run. After
    /// the fix (checkpoint = the real `entry.position`), the loop terminates
    /// immediately and each event is handled exactly once.
    #[tokio::test]
    async fn test_gap_in_ids_does_not_reapply_event_in_single_run() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        RecordingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        // A throwaway aggregate used only to force the conflict that burns an
        // id; none of its rows survive into the committed log.
        let agg_c = Uuid::new_v4();
        let stream_c = StreamId::new("TestAggregate", agg_c);

        // First two committed events (each on its own stream).
        let first_event = append_recorded_event(&store).await;
        let second_event = append_recorded_event(&store).await;

        // Burn a BIGSERIAL id to create a GAP. A held-open seeder transaction
        // reserves (agg_c, "TestAggregate", stream_version 0) on the unique
        // index but stays invisible to the real append's MAX(stream_version)
        // precheck. The real append to the (empty, committed) stream C passes
        // its precheck, its INSERT consumes a BIGSERIAL id (nextval), then
        // blocks on (and ultimately loses) the unique index — aborting the
        // transaction AFTER the sequence value is already burned. Using a fresh
        // aggregate keeps the recorded streams untouched.
        {
            let mut seeder = backend.pool().begin().await.unwrap();
            sqlx::query(
                "INSERT INTO event_sauce.events (
                    event_id, aggregate_id, aggregate_type, event_type, event_version,
                    event_data, stream_version
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(Uuid::new_v4())
            .bind(agg_c)
            .bind("TestAggregate")
            .bind("TestEvent")
            .bind(1_i64)
            .bind(json!({"data": "seed"}))
            .bind(0_i64)
            .execute(&mut *seeder)
            .await
            .unwrap();

            let store_for_loser = backend.event_store();
            let loser_stream = stream_c.clone();
            let loser_agg = agg_c;
            let loser = tokio::spawn(async move {
                let loser_event = create_test_envelope(loser_agg);
                store_for_loser
                    .append(
                        loser_stream,
                        vec![loser_event],
                        AggregateVersion::initial(),
                        vec![],
                        false,
                    )
                    .await
            });

            // Let the loser reach (and block on) its INSERT, then release it.
            tokio::time::sleep(Duration::from_millis(200)).await;
            seeder.commit().await.unwrap();

            let result = loser.await.unwrap();
            let err = result.expect_err("loser append must fail on the unique violation");
            assert!(
                err.is_concurrency_conflict(),
                "expected ConcurrencyConflict (and a burned sequence value), got: {err:?}"
            );
        }

        // The seeder's row committed for agg_c (consuming its own BIGSERIAL id);
        // remove all of agg_c so the only committed events are the recorded
        // ones, now with a gap in their ids (the burned loser id, and the
        // deleted seeder id, both sit between the surviving rows).
        sqlx::query("DELETE FROM event_sauce.events WHERE aggregate_id = $1")
            .bind(agg_c)
            .execute(backend.pool())
            .await
            .unwrap();

        // A third real, committed event, AFTER the gap.
        let third_event = append_recorded_event(&store).await;

        // Drive the projection to completion exactly once.
        let mut projection = RecordingProjection;
        backend
            .run_postgres_projection(&mut projection)
            .await
            .unwrap();

        // Every committed event must be handled EXACTLY once.
        for (label, id) in [
            ("first", first_event),
            ("second", second_event),
            ("third (after the gap)", third_event),
        ] {
            let count = RecordingProjection::count_for(backend.pool(), "event_sauce", id).await;
            assert_eq!(
                count, 1,
                "event {label} ({id}) must be handled exactly once, was handled {count} times"
            );
        }
    }

    #[tokio::test]
    async fn test_run_leased_projection_completes_under_lease() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(&mut projection, "worker-1", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(outcome, LeaseOutcome::Completed);
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            3
        );
    }

    #[tokio::test]
    async fn test_run_leased_projection_checkpoint_advances_past_unmatched_tail() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        append_test_event(&store, AggregateVersion::initial()).await;
        append_test_event(&store, AggregateVersion::initial()).await;
        append_typed(
            &store,
            "TestAggregate".to_string(),
            "OtherEvent".to_string(),
        )
        .await;

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(&mut projection, "worker-1", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(outcome, LeaseOutcome::Completed);
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            2
        );

        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(<CountingProjection as PostgresProjection>::NAME)
            .await
            .unwrap();
        assert_eq!(
            checkpoint,
            Some(Position::new(3)),
            "the fenced checkpoint must advance past the unmatched tail too"
        );
    }

    #[tokio::test]
    async fn test_run_leased_projection_busy_when_lease_held() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        // Worker A grabs the lease and holds it.
        backend
            .checkpoint_store()
            .try_acquire_lease(
                <CountingProjection as PostgresProjection>::NAME,
                "worker-a",
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        // Worker B should see Busy.
        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(&mut projection, "worker-b", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(outcome, LeaseOutcome::Busy);
        // No work was done.
        assert_eq!(projection.seen, 0);
    }

    #[tokio::test]
    async fn test_run_leased_projection_releases_on_exit() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        backend
            .run_leased_projection(&mut projection, "worker-a", Duration::from_secs(30))
            .await
            .unwrap();

        // Worker B should now be able to acquire the lease (worker A released it).
        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(&mut projection, "worker-b", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(outcome, LeaseOutcome::Completed);
    }

    #[tokio::test]
    async fn test_run_leased_projection_releases_on_failure() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        // Worker A fails partway through.
        let mut projection = CountingProjection {
            fail_after: Some(2),
            seen: 0,
        };
        let result = backend
            .run_leased_projection(&mut projection, "worker-a", Duration::from_secs(30))
            .await;
        assert!(result.is_err());

        // Lease should have been released even on failure.
        let acquired = backend
            .checkpoint_store()
            .try_acquire_lease(
                <CountingProjection as PostgresProjection>::NAME,
                "worker-b",
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert!(acquired.is_some(), "lease should be released after failure");
    }

    /// The probe below waits long enough for the runner to acquire the
    /// lease and enter its first idle sleep before checking it.
    #[tokio::test]
    async fn test_run_sticky_projection_holds_lease_across_idle_ticks() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let keep_going = Arc::new(AtomicBool::new(true));
        let keep_going_for_runner = Arc::clone(&keep_going);

        let sticky = backend.run_sticky_projection(
            &mut projection,
            "worker-a",
            Duration::from_secs(30),
            Duration::from_millis(30),
            move || keep_going_for_runner.load(Ordering::SeqCst),
        );

        let probe = async {
            tokio::time::sleep(Duration::from_millis(60)).await;
            let still_busy = backend
                .checkpoint_store()
                .try_acquire_lease(
                    <CountingProjection as PostgresProjection>::NAME,
                    "worker-b",
                    Duration::from_secs(30),
                )
                .await
                .unwrap();
            assert!(
                still_busy.is_none(),
                "the lease must still be held across an idle tick, not \
                 released and re-acquired every drain"
            );
            keep_going.store(false, Ordering::SeqCst);
        };

        let (outcome, ()) = tokio::join!(sticky, probe);
        assert_eq!(outcome.unwrap(), LeaseOutcome::Completed);

        let now_free = backend
            .checkpoint_store()
            .try_acquire_lease(
                <CountingProjection as PostgresProjection>::NAME,
                "worker-b",
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert!(
            now_free.is_some(),
            "the lease must be released once should_continue reports shutdown"
        );
    }

    /// Polls `CountingProjection::read` until it reaches `target`, or panics
    /// after `timeout` — a bounded wait so a regression fails the test
    /// instead of hanging the suite.
    async fn poll_count_reaches(pool: &PgPool, schema: &str, target: i64, timeout: Duration) {
        tokio::time::timeout(timeout, async {
            loop {
                if CountingProjection::read(pool, schema).await >= target {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("count never reached {target} within {timeout:?}"));
    }

    /// Pins where the next tick resumes: it must be the position
    /// [`run_under_lease`](PostgresBackend::run_under_lease) actually
    /// reached, not whatever the checkpoint store happens to hold — an
    /// event appended between two ticks must still be picked up by the
    /// very next one.
    #[tokio::test]
    async fn test_run_sticky_projection_resumes_where_previous_tick_left_off() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        append_test_event(&store, AggregateVersion::initial()).await;
        append_test_event(&store, AggregateVersion::initial()).await;

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let keep_going = Arc::new(AtomicBool::new(true));
        let keep_going_for_runner = Arc::clone(&keep_going);

        let sticky = backend.run_sticky_projection(
            &mut projection,
            "worker-a",
            Duration::from_secs(30),
            Duration::from_millis(30),
            move || keep_going_for_runner.load(Ordering::SeqCst),
        );

        let driver = async {
            poll_count_reaches(backend.pool(), "event_sauce", 2, Duration::from_secs(5)).await;

            append_test_event(&store, AggregateVersion::initial()).await;

            poll_count_reaches(backend.pool(), "event_sauce", 3, Duration::from_secs(5)).await;

            keep_going.store(false, Ordering::SeqCst);
        };

        let (outcome, ()) = tokio::join!(sticky, driver);
        assert_eq!(outcome.unwrap(), LeaseOutcome::Completed);
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            3,
            "the event appended between ticks must have been handled too"
        );
    }

    #[tokio::test]
    async fn test_run_sticky_projection_rejects_idle_interval_too_close_to_lease_duration() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };

        let err = backend
            .run_sticky_projection(
                &mut projection,
                "worker-a",
                Duration::from_secs(30),
                Duration::from_secs(11),
                || true,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidState(_)),
            "idle_interval >= lease_duration / 3 must be rejected up front: {err:?}"
        );

        let still_free = backend
            .checkpoint_store()
            .try_acquire_lease(
                <CountingProjection as PostgresProjection>::NAME,
                "worker-b",
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert!(
            still_free.is_some(),
            "a rejected idle_interval must never acquire the lease"
        );
    }

    #[tokio::test]
    async fn test_dispatch_policies_to_outbox_routes_matching_events() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        // Two streams: one matches the policy, one doesn't.
        for _ in 0..3 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("Order", aggregate_id);
            let envelope = EventEnvelope::new(
                Uuid::new_v4(),
                aggregate_id,
                "Order".to_string(),
                "Order.Created".to_string(),
                EventVersion::new(1),
                json!({}),
            );
            store
                .append(
                    stream_id,
                    vec![envelope],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }
        for _ in 0..2 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("User", aggregate_id);
            let envelope = EventEnvelope::new(
                Uuid::new_v4(),
                aggregate_id,
                "User".to_string(),
                "User.Created".to_string(),
                EventVersion::new(1),
                json!({}),
            );
            store
                .append(
                    stream_id,
                    vec![envelope],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        let policies = vec![PolicyDispatch::new(
            "send-order-confirmation",
            EventFilter::by_aggregate_type("Order"),
        )];

        let outcome = backend
            .dispatch_policies_to_outbox(
                &outbox,
                "dispatcher-1",
                &policies,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, LeaseOutcome::Completed);

        // Three Order events should be in the outbox; two User events should not.
        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            3
        );
    }

    /// Dispatching several matching events in one fetched batch must enqueue
    /// each outbox row with *that event's own* log position, not some
    /// batch-wide value — `claim_batch` orders by `event_position ASC`, so a
    /// shared position would make claim order (and `OutboxClaim::event_position`
    /// itself) arbitrary.
    #[tokio::test]
    async fn test_dispatch_enqueues_each_events_own_log_position() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        let mut event_ids = Vec::new();
        for _ in 0..4 {
            let aggregate_id = Uuid::new_v4();
            let event_id = Uuid::new_v4();
            let stream_id = StreamId::new("Order", aggregate_id);
            let envelope = EventEnvelope::new(
                event_id,
                aggregate_id,
                "Order".to_string(),
                "Order.Created".to_string(),
                EventVersion::new(1),
                json!({}),
            );
            store
                .append(
                    stream_id,
                    vec![envelope],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
            event_ids.push(event_id);
        }

        let log = store
            .fetch_events_batch(Position::start(), 10)
            .await
            .unwrap();
        let expected_positions: std::collections::HashMap<Uuid, i64> = log
            .iter()
            .map(|entry| (entry.envelope.id, entry.position.as_i64()))
            .collect();
        assert_eq!(
            expected_positions.len(),
            4,
            "the four appended events must land at four distinct log positions"
        );

        let policies = vec![PolicyDispatch::new(
            "send-order-confirmation",
            EventFilter::by_aggregate_type("Order"),
        )];
        backend
            .dispatch_policies_to_outbox(
                &outbox,
                "dispatcher-1",
                &policies,
                Duration::from_secs(30),
            )
            .await
            .unwrap();

        let claims = outbox
            .claim_batch(
                "send-order-confirmation",
                "worker-1",
                10,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(claims.len(), 4);

        for claim in &claims {
            assert_eq!(
                claim.event_position, expected_positions[&claim.event_id],
                "each outbox row must carry its own event's log position"
            );
        }

        let positions: Vec<i64> = claims.iter().map(|c| c.event_position).collect();
        let mut sorted_positions = positions.clone();
        sorted_positions.sort_unstable();
        assert_eq!(
            positions, sorted_positions,
            "claim_batch orders by event_position ASC"
        );
        let mut deduped = positions.clone();
        deduped.dedup();
        assert_eq!(
            deduped.len(),
            positions.len(),
            "every claimed row must have a distinct event_position"
        );
    }

    /// Every event in the log is a User event; the policy only cares about
    /// Order events, so this whole (single-fetch) batch is a miss.
    #[tokio::test]
    async fn test_dispatch_checkpoint_advances_over_wholly_unmatched_batch() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        for _ in 0..5 {
            append_typed(&store, "User".to_string(), "User.Created".to_string()).await;
        }

        let policies = vec![PolicyDispatch::new(
            "send-order-confirmation",
            EventFilter::by_aggregate_type("Order"),
        )];
        backend
            .dispatch_policies_to_outbox(
                &outbox,
                "dispatcher-1",
                &policies,
                Duration::from_secs(30),
            )
            .await
            .unwrap();

        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            0,
            "nothing in this batch matches the policy"
        );
        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(&policy_checkpoint_name(&policies[0].name))
            .await
            .unwrap();
        assert_eq!(
            checkpoint,
            Some(Position::new(5)),
            "the batch checkpoint must still advance to the last scanned \
             position even though nothing in it was enqueued"
        );
    }

    #[tokio::test]
    async fn test_dispatch_then_drain_via_skip_locked() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        // Publish 6 matching events.
        let store = backend.event_store();
        for _ in 0..6 {
            let aggregate_id = Uuid::new_v4();
            let stream_id = StreamId::new("Order", aggregate_id);
            let envelope = EventEnvelope::new(
                Uuid::new_v4(),
                aggregate_id,
                "Order".to_string(),
                "Order.Created".to_string(),
                EventVersion::new(1),
                json!({}),
            );
            store
                .append(
                    stream_id,
                    vec![envelope],
                    AggregateVersion::initial(),
                    vec![],
                    false,
                )
                .await
                .unwrap();
        }

        let policies = vec![PolicyDispatch::new(
            "send-order-confirmation",
            EventFilter::by_aggregate_type("Order"),
        )];
        backend
            .dispatch_policies_to_outbox(
                &outbox,
                "dispatcher-1",
                &policies,
                Duration::from_secs(30),
            )
            .await
            .unwrap();

        // Two parallel drain workers.
        let outbox_a = outbox.clone();
        let outbox_b = outbox.clone();
        let (claims_a, claims_b) = tokio::join!(
            tokio::spawn(async move {
                outbox_a
                    .claim_batch(
                        "send-order-confirmation",
                        "drainer-a",
                        100,
                        Duration::from_secs(30),
                    )
                    .await
            }),
            tokio::spawn(async move {
                outbox_b
                    .claim_batch(
                        "send-order-confirmation",
                        "drainer-b",
                        100,
                        Duration::from_secs(30),
                    )
                    .await
            })
        );

        let claims_a = claims_a.unwrap().unwrap();
        let claims_b = claims_b.unwrap().unwrap();
        assert_eq!(claims_a.len() + claims_b.len(), 6);

        // Mark all as done; pending should hit zero.
        for c in claims_a.iter().chain(claims_b.iter()) {
            let worker = if claims_a.iter().any(|a| a.id == c.id) {
                "drainer-a"
            } else {
                "drainer-b"
            };
            let _ = outbox.mark_done(c.id, worker).await.unwrap();
        }
        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            0
        );
    }

    /// Appends one event of `event_type` on a fresh aggregate of
    /// `aggregate_type` to its own stream.
    async fn append_typed(store: &PostgresEventStore, aggregate_type: String, event_type: String) {
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new(aggregate_type.clone(), aggregate_id);
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            aggregate_type,
            event_type,
            EventVersion::new(1),
            json!({}),
        );
        store
            .append(
                stream_id,
                vec![envelope],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await
            .unwrap();
    }

    /// Order and User events interleave in the log, but an "old" deployment
    /// only knows the Order policy, and calls dispatch with just that one. A
    /// rolling deploy then adds a User policy: the caller now dispatches a
    /// WIDER set, in the SAME call as the existing Order policy. Each
    /// policy's checkpoint is its own, so the new policy sees every User
    /// event from genesis while the existing one's checkpoint (and the rows
    /// it already produced) are untouched by sharing a call with it.
    #[tokio::test]
    async fn test_dispatch_a_policys_progress_is_independent_of_the_set_it_ran_in() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();

        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;
        append_typed(&store, "User".to_string(), "User.Created".to_string()).await;
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;
        append_typed(&store, "User".to_string(), "User.Created".to_string()).await;
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;

        let order_only = vec![PolicyDispatch::new(
            "send-order-confirmation",
            EventFilter::by_aggregate_type("Order"),
        )];
        backend
            .dispatch_policies_to_outbox(
                &outbox,
                "old-deploy",
                &order_only,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            3
        );

        let order_and_user = vec![
            PolicyDispatch::new(
                "send-order-confirmation",
                EventFilter::by_aggregate_type("Order"),
            ),
            PolicyDispatch::new("send-welcome-email", EventFilter::by_aggregate_type("User")),
        ];
        backend
            .dispatch_policies_to_outbox(
                &outbox,
                "new-deploy",
                &order_and_user,
                Duration::from_secs(30),
            )
            .await
            .unwrap();

        assert_eq!(
            outbox.pending_count("send-welcome-email").await.unwrap(),
            2,
            "the new policy must see every matching event, including ones \
             predating its first dispatch"
        );
        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            3,
            "re-scanning under the new checkpoint must not double-enqueue \
             the old policy (enqueue is idempotent)"
        );

        let checkpoint_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM event_sauce.checkpoints
             WHERE subscription_name LIKE '__policy_outbox_dispatcher:%'",
        )
        .fetch_one(backend.pool())
        .await
        .unwrap();
        assert_eq!(
            checkpoint_count, 2,
            "each distinct policy gets its own checkpoint row"
        );
    }

    /// A policy joins in a LATER call alongside the one whose rows were just
    /// pruned. Without a per-policy checkpoint, re-scanning from genesis to
    /// catch the new policy up would re-enqueue (and re-deliver) every event
    /// the pruned policy already handled.
    #[tokio::test]
    async fn test_dispatch_adding_a_policy_after_prune_does_not_redeliver_to_existing_ones() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;
        append_typed(&store, "User".to_string(), "User.Created".to_string()).await;
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;

        let order_only = vec![PolicyDispatch::new(
            "send-order-confirmation",
            EventFilter::by_aggregate_type("Order"),
        )];
        backend
            .dispatch_policies_to_outbox(&outbox, "w-1", &order_only, Duration::from_secs(30))
            .await
            .unwrap();

        let claims = outbox
            .claim_batch(
                "send-order-confirmation",
                "w-1",
                10,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(claims.len(), 2);
        for claim in &claims {
            assert_eq!(
                outbox.mark_done(claim.id, "w-1").await.unwrap(),
                crate::AckOutcome::Acked
            );
        }
        outbox.prune_done(Duration::ZERO, 100).await.unwrap();

        let order_and_user = vec![
            PolicyDispatch::new(
                "send-order-confirmation",
                EventFilter::by_aggregate_type("Order"),
            ),
            PolicyDispatch::new("send-welcome-email", EventFilter::by_aggregate_type("User")),
        ];
        backend
            .dispatch_policies_to_outbox(&outbox, "w-1", &order_and_user, Duration::from_secs(30))
            .await
            .unwrap();

        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            0,
            "a pruned policy must not be redelivered just because a new policy joined its call"
        );
        assert_eq!(
            outbox.pending_count("send-welcome-email").await.unwrap(),
            1,
            "the new policy must still see every matching event"
        );
    }

    /// Simulates the pre-upgrade shared checkpoint already having scanned
    /// past all 3 events.
    #[tokio::test]
    async fn test_dispatch_seeds_new_policy_checkpoint_from_legacy_shared_row() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        for _ in 0..3 {
            append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;
        }

        backend
            .checkpoint_store()
            .save_checkpoint("__policy_outbox_dispatcher", Position::new(3))
            .await
            .unwrap();

        let policies = vec![PolicyDispatch::new(
            "send-order-confirmation",
            EventFilter::by_aggregate_type("Order"),
        )];
        backend
            .dispatch_policies_to_outbox(&outbox, "w-1", &policies, Duration::from_secs(30))
            .await
            .unwrap();

        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            0,
            "the new per-policy checkpoint must resume from the seeded \
             legacy position, not genesis"
        );
    }

    /// A caller may register the same policy name under two different
    /// filters, e.g. one dispatch call fanning out to two separate
    /// `EventFilter`s that both feed the same downstream handler.
    /// `enqueue_tx` is `ON CONFLICT DO NOTHING`, so the duplicate is
    /// harmless — the call must still complete and make progress for every
    /// distinct name.
    #[tokio::test]
    async fn test_dispatch_tolerates_a_repeated_policy_name() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;
        append_typed(&store, "Invoice".to_string(), "Invoice.Created".to_string()).await;

        let policies = vec![
            PolicyDispatch::new("notify", EventFilter::by_aggregate_type("Order")),
            PolicyDispatch::new("notify", EventFilter::by_aggregate_type("Invoice")),
        ];

        let outcome = backend
            .dispatch_policies_to_outbox(&outbox, "w-1", &policies, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(outcome, LeaseOutcome::Completed);
        assert_eq!(
            outbox.pending_count("notify").await.unwrap(),
            2,
            "both filters' matches must land under the one shared name"
        );
    }

    /// Worker-b already holds policy B's checkpoint lease. Worker-a
    /// dispatches `[A, B]`: it must still complete, dispatching only A —
    /// enqueuing A's rows and advancing A's checkpoint — while B is left
    /// entirely untouched (no rows, no checkpoint movement) and its lease
    /// stays with worker-b throughout. Once worker-a returns, A's own lease
    /// must be free again for another worker to acquire.
    #[tokio::test]
    async fn test_dispatch_runs_only_the_policies_whose_lease_it_holds() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;
        append_typed(&store, "User".to_string(), "User.Created".to_string()).await;

        let checkpoint_b = policy_checkpoint_name("policy-b");
        backend
            .checkpoint_store()
            .try_acquire_lease(&checkpoint_b, "worker-b", Duration::from_secs(30))
            .await
            .unwrap();

        let policies = vec![
            PolicyDispatch::new("policy-a", EventFilter::by_aggregate_type("Order")),
            PolicyDispatch::new("policy-b", EventFilter::by_aggregate_type("User")),
        ];
        let outcome = backend
            .dispatch_policies_to_outbox(&outbox, "worker-a", &policies, Duration::from_secs(30))
            .await
            .unwrap();

        assert_eq!(outcome, LeaseOutcome::Completed);
        assert_eq!(
            outbox.pending_count("policy-a").await.unwrap(),
            1,
            "the policy whose lease worker-a holds must be dispatched"
        );
        assert_eq!(
            outbox.pending_count("policy-b").await.unwrap(),
            0,
            "a policy whose lease is held elsewhere must not be dispatched"
        );
        assert_eq!(
            backend
                .checkpoint_store()
                .load_checkpoint(&checkpoint_b)
                .await
                .unwrap(),
            Some(Position::start()),
            "a policy left undispatched must not have its checkpoint moved \
             past the position it held when worker-b acquired the lease"
        );

        let checkpoint_a = policy_checkpoint_name("policy-a");
        let reacquired = backend
            .checkpoint_store()
            .try_acquire_lease(&checkpoint_a, "worker-c", Duration::from_secs(30))
            .await
            .unwrap();
        assert!(
            reacquired.is_some(),
            "worker-a must release policy A's lease once dispatch returns"
        );
    }

    /// Every policy in the call has its lease held by another worker: the
    /// whole call must report `Busy` and enqueue nothing, rather than
    /// silently completing with zero progress.
    #[tokio::test]
    async fn test_dispatch_busy_only_when_every_policy_lease_is_held_elsewhere() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;

        let policies = vec![PolicyDispatch::new(
            "policy-a",
            EventFilter::by_aggregate_type("Order"),
        )];
        for policy in &policies {
            backend
                .checkpoint_store()
                .try_acquire_lease(
                    &policy_checkpoint_name(&policy.name),
                    "worker-b",
                    Duration::from_secs(30),
                )
                .await
                .unwrap();
        }

        let outcome = backend
            .dispatch_policies_to_outbox(&outbox, "worker-a", &policies, Duration::from_secs(30))
            .await
            .unwrap();

        assert_eq!(outcome, LeaseOutcome::Busy);
        assert_eq!(outbox.pending_count("policy-a").await.unwrap(), 0);
    }

    /// Acquiring policy-b's lease errors after policy-a's was already
    /// acquired: policy-a's lease must still be released rather than left
    /// held until it expires, since the call only returns `Err` and the
    /// caller has no `held` list of its own to release from.
    #[tokio::test]
    async fn test_dispatch_error_while_acquiring_releases_earlier_leases() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;

        let checkpoint_b = policy_checkpoint_name("policy-b");
        sqlx::query(&format!(
            "CREATE FUNCTION event_sauce.boom() RETURNS trigger AS $$
             BEGIN
                 IF NEW.subscription_name = '{checkpoint_b}' THEN
                     RAISE EXCEPTION 'boom';
                 END IF;
                 RETURN NEW;
             END $$ LANGUAGE plpgsql"
        ))
        .execute(backend.pool())
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER boom BEFORE INSERT ON event_sauce.checkpoints
             FOR EACH ROW EXECUTE FUNCTION event_sauce.boom()",
        )
        .execute(backend.pool())
        .await
        .unwrap();

        let policies = vec![
            PolicyDispatch::new("policy-a", EventFilter::by_aggregate_type("Order")),
            PolicyDispatch::new("policy-b", EventFilter::by_aggregate_type("User")),
        ];
        let err = backend
            .dispatch_policies_to_outbox(&outbox, "worker-a", &policies, Duration::from_secs(30))
            .await
            .unwrap_err();
        assert!(
            err.is_backend(),
            "policy-b's lease acquisition must surface the trigger's error: {err:?}"
        );

        let checkpoint_a = policy_checkpoint_name("policy-a");
        let reacquired = backend
            .checkpoint_store()
            .try_acquire_lease(&checkpoint_a, "worker-c", Duration::from_secs(30))
            .await
            .unwrap();
        assert!(
            reacquired.is_some(),
            "worker-a must release policy-a's lease even though acquiring \
             policy-b's failed"
        );
    }

    /// `commit_fenced` applies every checkpoint in one transaction together
    /// with whatever else that transaction did — here, an enqueue. One of
    /// the two checkpoints belongs to another worker: the whole transaction
    /// must roll back, so neither checkpoint moves and the enqueue never
    /// lands, not just the checkpoint whose fence was rejected.
    #[tokio::test]
    async fn test_commit_fenced_rolls_back_the_whole_transaction_on_one_rejection() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        backend
            .checkpoint_store()
            .try_acquire_lease("cp-held", "worker-a", Duration::from_secs(30))
            .await
            .unwrap();
        backend
            .checkpoint_store()
            .try_acquire_lease("cp-foreign", "worker-b", Duration::from_secs(30))
            .await
            .unwrap();

        let mut tx = backend.pool().begin().await.unwrap();
        outbox
            .enqueue_tx(&mut tx, "policy-a", Uuid::new_v4(), 1)
            .await
            .unwrap();

        let err = backend
            .commit_fenced(
                tx,
                "worker-a",
                &[
                    ("cp-held", Position::new(1)),
                    ("cp-foreign", Position::new(1)),
                ],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::LeaseLost { .. }),
            "the name worker-a does not hold must be reported as LeaseLost: {err:?}"
        );

        assert_eq!(
            outbox.pending_count("policy-a").await.unwrap(),
            0,
            "the enqueue sharing the rolled-back transaction must not land"
        );
        assert_eq!(
            backend
                .checkpoint_store()
                .load_checkpoint("cp-held")
                .await
                .unwrap(),
            Some(Position::start()),
            "even the name whose fence would have passed must not move, \
             since it shared the rolled-back transaction"
        );
        assert_eq!(
            backend
                .checkpoint_store()
                .load_checkpoint("cp-foreign")
                .await
                .unwrap(),
            Some(Position::start()),
            "the foreign checkpoint must not move either"
        );
    }

    /// One held policy's lease is stolen mid-batch (an `AFTER INSERT`
    /// trigger on `policy_outbox` reassigns its checkpoint row to another
    /// worker as soon as any row lands): the whole batch's transaction
    /// must roll back, so no other held policy's enqueues or checkpoint
    /// advance from that same batch survive either — partial application
    /// would let a still-held policy silently skip events its own
    /// checkpoint claims to have seen.
    #[tokio::test]
    async fn test_dispatch_lease_stolen_mid_batch_rolls_back_every_policy() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        let outbox = crate::PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
        outbox.migrate().await.unwrap();

        let store = backend.event_store();
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;
        append_typed(&store, "User".to_string(), "User.Created".to_string()).await;
        append_typed(&store, "Order".to_string(), "Order.Created".to_string()).await;

        let checkpoint_a = policy_checkpoint_name("policy-a");
        let checkpoint_b = policy_checkpoint_name("policy-b");
        sqlx::query(&format!(
            "CREATE FUNCTION event_sauce.steal() RETURNS trigger AS $$
             BEGIN
                 UPDATE event_sauce.checkpoints SET worker_id = 'thief'
                 WHERE subscription_name = '{checkpoint_b}';
                 RETURN NEW;
             END $$ LANGUAGE plpgsql"
        ))
        .execute(backend.pool())
        .await
        .unwrap();
        sqlx::query(
            "CREATE TRIGGER steal AFTER INSERT ON event_sauce.policy_outbox
             FOR EACH ROW EXECUTE FUNCTION event_sauce.steal()",
        )
        .execute(backend.pool())
        .await
        .unwrap();

        let policies = vec![
            PolicyDispatch::new("policy-a", EventFilter::by_aggregate_type("Order")),
            PolicyDispatch::new("policy-b", EventFilter::by_aggregate_type("User")),
        ];
        let err = backend
            .dispatch_policies_to_outbox(&outbox, "worker-a", &policies, Duration::from_secs(30))
            .await
            .unwrap_err();
        assert!(
            err.is_lease_lost(),
            "expected LeaseLost for the stolen policy: {err:?}"
        );

        assert_eq!(
            outbox.pending_count("policy-a").await.unwrap(),
            0,
            "policy-a's enqueues must roll back with the batch"
        );
        assert_eq!(outbox.pending_count("policy-b").await.unwrap(), 0);
        assert_eq!(
            backend
                .checkpoint_store()
                .load_checkpoint(&checkpoint_a)
                .await
                .unwrap(),
            Some(Position::start()),
            "policy-a's checkpoint must not move either, since it shared \
             the rolled-back transaction"
        );
        assert_eq!(
            backend
                .checkpoint_store()
                .load_checkpoint(&checkpoint_b)
                .await
                .unwrap(),
            Some(Position::start())
        );
        assert!(backend
            .checkpoint_store()
            .try_acquire_lease(&checkpoint_a, "worker-c", Duration::from_secs(30))
            .await
            .unwrap()
            .is_some());
        assert!(backend
            .checkpoint_store()
            .try_acquire_lease(&checkpoint_b, "worker-c", Duration::from_secs(30))
            .await
            .unwrap()
            .is_some());
    }

    /// A projection whose `handle` reassigns its own checkpoint row to
    /// another worker while handling the 2nd event, so that event's
    /// fenced commit is rejected mid-drain.
    struct StealingProjection;

    #[async_trait::async_trait]
    impl PostgresProjection for StealingProjection {
        const NAME: &'static str = "StealingProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            _envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> Result<()> {
            sqlx::query("UPDATE event_sauce.counting_projection SET n = n + 1 WHERE id = 1")
                .execute(&mut **tx)
                .await
                .unwrap();
            let n: i64 =
                sqlx::query_scalar("SELECT n FROM event_sauce.counting_projection WHERE id = 1")
                    .fetch_one(&mut **tx)
                    .await
                    .unwrap();
            if n == 2 {
                sqlx::query(
                    "UPDATE event_sauce.checkpoints SET worker_id = 'thief' \
                     WHERE subscription_name = 'StealingProjection'",
                )
                .execute(&mut **tx)
                .await
                .unwrap();
            }
            Ok(())
        }
    }

    /// The lease is stolen mid-drain, on the 2nd of 3 fetched events: that
    /// event's fenced checkpoint write must be rejected, rolling back its
    /// own handler's work, while the 1st event's earlier commit (its own,
    /// already-landed transaction) stands.
    #[tokio::test]
    async fn test_run_leased_projection_lease_stolen_mid_drain_rolls_back_the_stolen_event() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..3 {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let err = backend
            .run_leased_projection(&mut StealingProjection, "worker-a", Duration::from_secs(30))
            .await
            .unwrap_err();
        assert!(
            err.is_lease_lost(),
            "expected LeaseLost once the lease is stolen mid-drain: {err:?}"
        );

        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            1,
            "only the 1st event's handler work, committed before the \
             lease was stolen, must land"
        );
        assert_eq!(
            backend
                .checkpoint_store()
                .load_checkpoint("StealingProjection")
                .await
                .unwrap(),
            Some(Position::new(1)),
            "the checkpoint must stop at the last event whose fenced \
             commit actually landed"
        );
    }

    // === Rebuild tests (L11): lease-guarded table+checkpoint reset + re-drain ===

    /// A read model that records one row per handled event into its own table.
    /// `reset()` truncates that table so a rebuild starts from an empty model.
    struct RebuildProjection;

    impl RebuildProjection {
        async fn migrate(pool: &PgPool, schema: &str) {
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {schema}.rebuild_projection (
                    seq BIGSERIAL PRIMARY KEY,
                    event_id UUID NOT NULL
                )"
            ))
            .execute(pool)
            .await
            .unwrap();
        }

        /// Total number of rows currently materialized.
        async fn row_count(pool: &PgPool, schema: &str) -> i64 {
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {schema}.rebuild_projection"))
                .fetch_one(pool)
                .await
                .unwrap()
        }

        /// Number of rows recorded for a specific event UUID.
        async fn count_for(pool: &PgPool, schema: &str, event_id: Uuid) -> i64 {
            sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {schema}.rebuild_projection WHERE event_id = $1"
            ))
            .bind(event_id)
            .fetch_one(pool)
            .await
            .unwrap()
        }

        /// Seeds a STALE row that does not correspond to any committed event.
        /// A correct rebuild must wipe it before re-deriving from genesis.
        async fn seed_stale(pool: &PgPool, schema: &str, event_id: Uuid) {
            sqlx::query(&format!(
                "INSERT INTO {schema}.rebuild_projection (event_id) VALUES ($1)"
            ))
            .bind(event_id)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    #[async_trait::async_trait]
    impl PostgresProjection for RebuildProjection {
        const NAME: &'static str = "RebuildProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> Result<()> {
            sqlx::query("INSERT INTO event_sauce.rebuild_projection (event_id) VALUES ($1)")
                .bind(envelope.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::custom(format!("insert failed: {e}")))?;
            Ok(())
        }

        async fn reset(&mut self, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<()> {
            sqlx::query("TRUNCATE event_sauce.rebuild_projection")
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::custom(format!("truncate failed: {e}")))?;
            Ok(())
        }
    }

    /// A projection that does NOT override `reset()`, so it relies on the
    /// trait default. Used to lock in the default-impl decision (rebuild must
    /// refuse to run for a projection that has not opted in).
    struct NoResetProjection;

    impl NoResetProjection {
        async fn migrate(pool: &PgPool, schema: &str) {
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {schema}.no_reset_projection (
                    seq BIGSERIAL PRIMARY KEY,
                    event_id UUID NOT NULL
                )"
            ))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    #[async_trait::async_trait]
    impl PostgresProjection for NoResetProjection {
        const NAME: &'static str = "NoResetProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> Result<()> {
            sqlx::query("INSERT INTO event_sauce.no_reset_projection (event_id) VALUES ($1)")
                .bind(envelope.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::custom(format!("insert failed: {e}")))?;
            Ok(())
        }
        // intentionally no `reset()` override — uses the trait default.
    }

    #[tokio::test]
    async fn test_rebuild_resets_and_redrains() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        RebuildProjection::migrate(backend.pool(), "event_sauce").await;

        // Commit three real source events.
        let store = backend.event_store();
        let e1 = append_recorded_event(&store).await;
        let e2 = append_recorded_event(&store).await;
        let e3 = append_recorded_event(&store).await;

        // Poison the world: a STALE read-model row that maps to no committed
        // event, and a checkpoint already advanced PAST all real events. A
        // naive re-run (without reset) would see "nothing new" and leave the
        // stale row in place.
        let stale = Uuid::new_v4();
        RebuildProjection::seed_stale(backend.pool(), "event_sauce", stale).await;
        backend
            .checkpoint_store()
            .save_checkpoint(
                <RebuildProjection as PostgresProjection>::NAME,
                Position::new(999),
            )
            .await
            .unwrap();

        // Rebuild: lease-guarded, atomic table+checkpoint reset, re-drain from 0.
        let mut projection = RebuildProjection;
        let outcome = backend
            .rebuild(&mut projection, "worker-1", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(outcome, LeaseOutcome::Completed);

        // (a) The stale row is gone and the table reflects a clean re-derivation:
        //     exactly one row per real event, no extras.
        assert_eq!(
            RebuildProjection::count_for(backend.pool(), "event_sauce", stale).await,
            0,
            "stale row must be wiped by the reset"
        );
        assert_eq!(
            RebuildProjection::row_count(backend.pool(), "event_sauce").await,
            3,
            "table must hold exactly one row per committed event"
        );
        for (label, id) in [("first", e1), ("second", e2), ("third", e3)] {
            assert_eq!(
                RebuildProjection::count_for(backend.pool(), "event_sauce", id).await,
                1,
                "event {label} ({id}) must be re-derived exactly once"
            );
        }

        // (b) The checkpoint was reset off 999 and advanced to the real max (3).
        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(<RebuildProjection as PostgresProjection>::NAME)
            .await
            .unwrap();
        assert_eq!(
            checkpoint,
            Some(Position::new(3)),
            "checkpoint must be reset then re-advanced to the real max position"
        );
    }

    #[tokio::test]
    async fn test_rebuild_busy_when_leased() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        RebuildProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        append_recorded_event(&store).await;

        // Seed a stale row and a high checkpoint; if rebuild WRONGLY proceeded
        // it would wipe/reset these. A Busy outcome must leave them untouched.
        let stale = Uuid::new_v4();
        RebuildProjection::seed_stale(backend.pool(), "event_sauce", stale).await;
        backend
            .checkpoint_store()
            .save_checkpoint(
                <RebuildProjection as PostgresProjection>::NAME,
                Position::new(777),
            )
            .await
            .unwrap();

        // Another worker holds the lease.
        backend
            .checkpoint_store()
            .try_acquire_lease(
                <RebuildProjection as PostgresProjection>::NAME,
                "other-worker",
                Duration::from_secs(60),
            )
            .await
            .unwrap();

        let mut projection = RebuildProjection;
        let outcome = backend
            .rebuild(&mut projection, "worker-1", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            LeaseOutcome::Busy,
            "rebuild must report Busy when another worker holds the lease"
        );

        // The read model and checkpoint must be UNTOUCHED (no reset happened).
        assert_eq!(
            RebuildProjection::count_for(backend.pool(), "event_sauce", stale).await,
            1,
            "stale row must survive a Busy rebuild (no reset)"
        );
        let checkpoint = backend
            .checkpoint_store()
            .load_checkpoint(<RebuildProjection as PostgresProjection>::NAME)
            .await
            .unwrap();
        assert_eq!(
            checkpoint,
            Some(Position::new(777)),
            "checkpoint must not be reset on a Busy rebuild"
        );
    }

    #[tokio::test]
    async fn test_rebuild_errors_when_reset_not_implemented() {
        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        NoResetProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        append_recorded_event(&store).await;

        // A projection that did not override `reset()` must NOT be silently
        // half-rebuilt: rebuild must fail loudly via the default `reset()`.
        let mut projection = NoResetProjection;
        let result = backend
            .rebuild(&mut projection, "worker-1", Duration::from_secs(30))
            .await;
        assert!(
            result.is_err(),
            "rebuild must error for a projection whose reset() is the default (not overridden)"
        );
    }
}
