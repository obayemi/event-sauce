//! Projection runners: [`PostgresBackend::run_postgres_projection`],
//! [`PostgresBackend::run_leased_projection`],
//! [`PostgresBackend::run_sticky_projection`] and
//! [`PostgresBackend::rebuild`], and the fenced/unfenced drain loop they
//! share.

use std::time::Duration;

use event_sauce_core::{CheckpointStore, Error, Position, Result};

use crate::{LeaseOutcome, PostgresProjection};

use super::{LeaseRenewal, PostgresBackend, PROJECTION_BATCH_SIZE};

impl PostgresBackend {
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
        let start_position = self
            .checkpoint_store
            .load_checkpoint(P::NAME)
            .await?
            .unwrap_or_else(Position::start);

        self.drain(projection, start_position, Checkpointing::Unfenced)
            .await?;
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

    /// Runs a [`PostgresProjection`](PostgresProjection) under a lease held
    /// across idle ticks, rather than released and re-acquired on every call
    /// like [`run_leased_projection`].
    ///
    /// Acquires the lease once, then repeats: drain, sleep `idle_interval`,
    /// check `should_continue`, renew — without releasing in between.
    /// Checking right after the sleep and before the renewal means a
    /// shutdown signaled during that sleep is noticed immediately, with no
    /// extra renewal or drain first. Once `should_continue` returns `false`
    /// the lease is released and the run returns
    /// [`LeaseOutcome::Completed`](LeaseOutcome::Completed); it is also
    /// released if ever lost (a renewal failure, or a fenced checkpoint
    /// save rejected mid-drain), in which case the error propagates.
    ///
    /// `idle_interval` must stay below the lease's renewal interval
    /// (`lease_duration / 3`), or the lease could lapse during the idle
    /// sleep itself, before there is a chance to renew it; this is rejected
    /// with [`Error::InvalidState`](Error::InvalidState).
    ///
    /// This does not change how a **non-holder** waits — the checkpoint store
    /// does not currently expose a lease's expiry for a non-holder to sleep
    /// until, so every node still wakes on every `NOTIFY` and probes the
    /// lease. That remains a smaller cost than before (a probe that loses is
    /// one contended write the *previous* holder no longer also performs on
    /// every tick).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::sync::atomic::{AtomicBool, Ordering};
    /// use std::time::Duration;
    ///
    /// let shutdown = AtomicBool::new(false);
    /// let worker_id = format!("{}-{}", hostname()?, std::process::id());
    /// let outcome = backend
    ///     .run_sticky_projection(
    ///         &mut OrderTotalsProjection,
    ///         &worker_id,
    ///         Duration::from_secs(30),
    ///         Duration::from_secs(1),
    ///         || !shutdown.load(Ordering::Relaxed),
    ///     )
    ///     .await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if `idle_interval` is not below the lease's renewal
    /// interval, or under the same conditions as
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
        let renew_interval = LeaseRenewal::interval_for(lease_duration);
        if idle_interval >= renew_interval {
            return Err(Error::invalid_state(format!(
                "idle_interval ({idle_interval:?}) must be below lease_duration / 3 \
                 ({renew_interval:?}), or the lease could lapse before the next renewal",
            )));
        }

        self.with_lease(
            P::NAME,
            worker_id,
            lease_duration,
            |start_position| async move {
                let mut position = start_position;
                loop {
                    position = self
                        .run_under_lease(projection, worker_id, lease_duration, position)
                        .await?;

                    tokio::time::sleep(idle_interval).await;

                    if !should_continue() {
                        return Ok(position);
                    }

                    self.checkpoint_store
                        .renew_lease(P::NAME, worker_id, lease_duration)
                        .await?;
                }
            },
        )
        .await
    }

    /// Acquires the lease for `name` on behalf of `worker_id`, runs `body`
    /// from the position the lease started at, and always releases the
    /// lease on the way out (including on error — releasing a lease we no
    /// longer hold is a no-op), mapping `body`'s outcome to [`LeaseOutcome`].
    async fn with_lease<F, Fut, T>(
        &self,
        name: &str,
        worker_id: &str,
        lease_duration: Duration,
        body: F,
    ) -> Result<LeaseOutcome>
    where
        F: FnOnce(Position) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
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

        result.map(|_| LeaseOutcome::Completed)
    }

    /// Rebuilds a [`PostgresProjection`](PostgresProjection) from genesis
    /// under a lease.
    ///
    /// A rebuild wipes the projection's read-model, rewinds its checkpoint to
    /// the start, and re-derives the whole model by replaying every event
    /// from position 0. It is **lease-guarded** and **atomic**:
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
    ) -> Result<Position> {
        let mut tx = self.begin("rebuild reset").await?;

        projection.reset(&mut tx).await?;
        self.checkpoint_store
            .save_checkpoint_tx(&mut tx, P::NAME, Position::start())
            .await?;

        tx.commit()
            .await
            .map_err(|e| Error::backend("Failed to commit rebuild reset transaction", e))?;

        self.run_under_lease(projection, worker_id, lease_duration, Position::start())
            .await
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
    ) -> Result<Position> {
        self.drain(
            projection,
            start_position,
            Checkpointing::Fenced {
                worker_id,
                lease_duration,
            },
        )
        .await
    }

    /// Drains every event batch fetched from `start_position` onward into
    /// `projection`, advancing its checkpoint past each matched event —
    /// fenced on a held lease, or as a plain write — then catching up an
    /// unmatched batch tail the same way. Shared by
    /// [`run_postgres_projection`](Self::run_postgres_projection) and
    /// [`run_under_lease`](Self::run_under_lease), which differ only in
    /// `checkpointing`. Returns the position reached.
    async fn drain<P: PostgresProjection>(
        &self,
        projection: &mut P,
        start_position: Position,
        checkpointing: Checkpointing<'_>,
    ) -> Result<Position> {
        let filter = P::event_filter();
        let mut current_position = start_position;
        let mut renewal = match &checkpointing {
            Checkpointing::Fenced { lease_duration, .. } => {
                Some(LeaseRenewal::new(*lease_duration))
            }
            Checkpointing::Unfenced => None,
        };

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

                if let (
                    Checkpointing::Fenced {
                        worker_id,
                        lease_duration,
                    },
                    Some(renewal),
                ) = (&checkpointing, &mut renewal)
                {
                    if renewal.due() {
                        self.checkpoint_store
                            .renew_lease(P::NAME, worker_id, *lease_duration)
                            .await?;
                        renewal.renewed();
                    }
                }

                if !filter.matches(&entry.envelope) {
                    continue;
                }

                let mut tx = self.begin("projection").await?;
                projection.handle(&entry.envelope, &mut tx).await?;

                match &checkpointing {
                    Checkpointing::Unfenced => {
                        self.checkpoint_store
                            .save_checkpoint_tx(&mut tx, P::NAME, current_position)
                            .await?;
                        tx.commit().await.map_err(|e| {
                            Error::backend("Failed to commit projection transaction", e)
                        })?;
                    }
                    Checkpointing::Fenced { worker_id, .. } => {
                        self.commit_fenced(tx, worker_id, &[(P::NAME, current_position)])
                            .await?;
                    }
                }
                checkpointed_position = current_position;
            }

            if checkpointed_position != current_position {
                match &checkpointing {
                    Checkpointing::Unfenced => {
                        self.checkpoint_store
                            .save_checkpoint(P::NAME, current_position)
                            .await?;
                    }
                    Checkpointing::Fenced { worker_id, .. } => {
                        let tx = self.begin("projection").await?;
                        self.commit_fenced(tx, worker_id, &[(P::NAME, current_position)])
                            .await?;
                    }
                }
            }
        }

        Ok(current_position)
    }
}

/// How [`PostgresBackend::drain`] writes a checkpoint: a plain update, or
/// one fenced on still holding `worker_id`'s lease, which `drain` also
/// renews roughly every `lease_duration / 3`.
enum Checkpointing<'a> {
    /// Checkpoint writes are ordinary, unfenced updates.
    Unfenced,
    /// Checkpoint writes are fenced on still holding `worker_id`'s lease,
    /// renewed roughly every `lease_duration / 3`.
    Fenced {
        worker_id: &'a str,
        lease_duration: Duration,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{
        AggregateVersion, CheckpointStore, EventEnvelope, EventStore, StreamId,
    };
    use serde_json::json;
    use sqlx::PgPool;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use uuid::Uuid;

    use crate::backend::tests::fixtures::{append_typed, create_test_envelope, start_test_db};
    use crate::PostgresEventStore;

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

    /// A projection whose `handle` takes a fixed, configurable delay, so a
    /// test can make one drain run longer than a given lease duration.
    struct SlowCountingProjection {
        delay: Duration,
        seen: u64,
    }

    #[async_trait::async_trait]
    impl PostgresProjection for SlowCountingProjection {
        const NAME: &'static str = "SlowCountingProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            _envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> Result<()> {
            tokio::time::sleep(self.delay).await;
            self.seen += 1;
            sqlx::query("UPDATE event_sauce.counting_projection SET n = n + 1 WHERE id = 1")
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::custom(format!("update failed: {e}")))?;
            Ok(())
        }
    }

    /// A single fenced drain that outlasts its own lease duration must keep
    /// renewing as it goes — every per-event commit is fenced on
    /// `leased_until > NOW()`, so without renewal a drain this long would
    /// lose the lease partway through and fail with
    /// [`Error::LeaseLost`](Error::LeaseLost).
    #[tokio::test]
    async fn test_run_leased_projection_renews_across_a_drain_longer_than_the_lease() {
        const EVENT_COUNT: u64 = 10;

        let (url, _container) = start_test_db().await;
        let backend = PostgresBackend::setup(&url, "event_sauce").await.unwrap();
        CountingProjection::migrate(backend.pool(), "event_sauce").await;

        let store = backend.event_store();
        for _ in 0..EVENT_COUNT {
            append_test_event(&store, AggregateVersion::initial()).await;
        }

        let mut projection = SlowCountingProjection {
            delay: Duration::from_millis(100),
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(&mut projection, "worker-1", Duration::from_millis(600))
            .await
            .unwrap();
        assert_eq!(outcome, LeaseOutcome::Completed);
        assert_eq!(projection.seen, EVENT_COUNT);
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            i64::try_from(EVENT_COUNT).unwrap()
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
