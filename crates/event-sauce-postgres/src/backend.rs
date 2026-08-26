//! Unified `PostgreSQL` backend setup.
//!
//! Provides a single entry point for setting up all `PostgreSQL` stores
//! (event store + checkpoint store) with migrations.

use std::sync::Arc;

use event_sauce_core::SnapshotConfig;
use sqlx::PgPool;

use crate::{PostgresCheckpointStore, PostgresEventStore};

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
    pub async fn setup(database_url: &str, schema: &str) -> event_sauce_core::Result<Self> {
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

    /// Runs a [`PostgresProjection`](crate::PostgresProjection) atomically.
    ///
    /// For every matched event the runner opens a transaction, calls
    /// [`PostgresProjection::handle`](crate::PostgresProjection::handle) with
    /// it, advances the subscription checkpoint inside the same transaction,
    /// and commits. If any step fails the transaction is dropped (rolled back)
    /// and the error propagates — the checkpoint never advances past an event
    /// whose materialization didn't commit, so a re-run picks up exactly where
    /// the failure occurred.
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
    pub async fn run_postgres_projection<P: crate::PostgresProjection>(
        &self,
        projection: &mut P,
    ) -> event_sauce_core::Result<()> {
        use event_sauce_core::{CheckpointStore, Position};

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

            for entry in batch {
                current_position = entry.position;

                if !filter.matches(&entry.envelope) {
                    continue;
                }

                let mut tx = self.pool.begin().await.map_err(|e| {
                    event_sauce_core::Error::backend("Failed to start projection transaction", e)
                })?;

                projection.handle(&entry.envelope, &mut tx).await?;
                self.checkpoint_store
                    .save_checkpoint_tx(&mut tx, P::NAME, current_position)
                    .await?;

                tx.commit().await.map_err(|e| {
                    event_sauce_core::Error::backend("Failed to commit projection transaction", e)
                })?;
            }
        }

        Ok(())
    }

    /// Runs a [`PostgresProjection`](crate::PostgresProjection) under a lease.
    ///
    /// Acquires the lease for `P::NAME` on behalf of `worker_id`, drains all
    /// currently-available events (atomically per event, identically to
    /// [`run_postgres_projection`](Self::run_postgres_projection)), then
    /// releases the lease and returns. If another worker holds an active
    /// lease, this is a no-op that returns
    /// [`LeaseOutcome::Busy`](crate::LeaseOutcome::Busy) — the caller can
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
    /// stops with [`Error::LeaseLost`](event_sauce_core::Error::LeaseLost) —
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
    pub async fn run_leased_projection<P: crate::PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: std::time::Duration,
    ) -> event_sauce_core::Result<crate::LeaseOutcome> {
        use event_sauce_core::CheckpointStore;

        let Some(start_position) = self
            .checkpoint_store
            .try_acquire_lease(P::NAME, worker_id, lease_duration)
            .await?
        else {
            return Ok(crate::LeaseOutcome::Busy);
        };

        let result = self
            .run_under_lease(projection, worker_id, lease_duration, start_position)
            .await;

        // Always try to release on exit, including on error. Releasing a
        // lease we no longer hold is a no-op.
        let _ = self
            .checkpoint_store
            .release_lease(P::NAME, worker_id)
            .await;

        result.map(|()| crate::LeaseOutcome::Completed)
    }

    /// Rebuilds a [`PostgresProjection`](crate::PostgresProjection) from genesis
    /// under a lease.
    ///
    /// A rebuild wipes the projection's read-model, rewinds its checkpoint to
    /// the start, and re-derives the whole model by replaying every event from
    /// position 0. Unlike the old manual procedure (drop the table by hand,
    /// `delete_checkpoint`, re-run), this is **lease-guarded** and **atomic**:
    ///
    /// 1. Acquire the lease for `P::NAME` on behalf of `worker_id`. If another
    ///    worker holds an active lease, return
    ///    [`LeaseOutcome::Busy`](crate::LeaseOutcome::Busy) **without touching**
    ///    the read-model or the checkpoint — a concurrent worker is still
    ///    running against the live model, so resetting it would corrupt its
    ///    view.
    /// 2. In a single transaction, call
    ///    [`reset`](crate::PostgresProjection::reset) to clear the read-model
    ///    and rewind the checkpoint to
    ///    [`Position::start`](event_sauce_core::Position::start). Both commit or
    ///    roll back together, so a rebuild never leaves a wiped table paired
    ///    with a stale checkpoint.
    /// 3. Re-drain from genesis using the same fenced per-event loop as
    ///    [`run_leased_projection`](Self::run_leased_projection).
    /// 4. Release the lease on exit, including on error.
    ///
    /// The projection **must** override
    /// [`reset`](crate::PostgresProjection::reset); the default implementation
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
    /// [`reset`](crate::PostgresProjection::reset), if the reset/checkpoint
    /// transaction fails, or for any reason
    /// [`run_leased_projection`](Self::run_leased_projection) would error during
    /// the re-drain.
    pub async fn rebuild<P: crate::PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: std::time::Duration,
    ) -> event_sauce_core::Result<crate::LeaseOutcome> {
        use event_sauce_core::CheckpointStore;

        let Some(_held_position) = self
            .checkpoint_store
            .try_acquire_lease(P::NAME, worker_id, lease_duration)
            .await?
        else {
            // Another worker owns the lease: leave the read-model and
            // checkpoint untouched and let the caller retry later.
            return Ok(crate::LeaseOutcome::Busy);
        };

        let result = self
            .rebuild_under_lease(projection, worker_id, lease_duration)
            .await;

        // Always try to release on exit, including on error. Releasing a lease
        // we no longer hold is a no-op.
        let _ = self
            .checkpoint_store
            .release_lease(P::NAME, worker_id)
            .await;

        result.map(|()| crate::LeaseOutcome::Completed)
    }

    /// Performs the atomic reset (read-model wipe + checkpoint rewind) and the
    /// subsequent fenced re-drain. The caller already holds the lease and is
    /// responsible for releasing it.
    async fn rebuild_under_lease<P: crate::PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: std::time::Duration,
    ) -> event_sauce_core::Result<()> {
        // Atomic reset: clear the read-model and rewind the checkpoint to
        // genesis in one transaction. The rewind is a backward move, so it uses
        // the UNFENCED save (the fenced save would reject it) — safe because we
        // hold the lease and this is a deliberate rewind by the owner.
        let mut tx = self.pool.begin().await.map_err(|e| {
            event_sauce_core::Error::backend("Failed to start rebuild reset transaction", e)
        })?;

        projection.reset(&mut tx).await?;
        self.checkpoint_store
            .save_checkpoint_tx(&mut tx, P::NAME, event_sauce_core::Position::start())
            .await?;

        tx.commit().await.map_err(|e| {
            event_sauce_core::Error::backend("Failed to commit rebuild reset transaction", e)
        })?;

        // Re-drain from genesis with the usual fenced per-event loop.
        self.run_under_lease(
            projection,
            worker_id,
            lease_duration,
            event_sauce_core::Position::start(),
        )
        .await
    }

    /// Reads new events from the log and fans them out into the policy
    /// outbox for each registered policy whose filter matches.
    ///
    /// The dispatcher uses its own subscription checkpoint
    /// (`__policy_outbox_dispatcher`) under a lease, so running it from
    /// multiple instances is safe — only one is active at a time. The
    /// fan-out itself runs inside a per-event transaction together with the
    /// checkpoint advance, so an event is enqueued for every matching
    /// policy or for none — never partial.
    ///
    /// Workers (typically separate processes) then drain the outbox via
    /// [`PostgresPolicyOutbox::claim_batch`](crate::PostgresPolicyOutbox::claim_batch).
    ///
    /// # Errors
    ///
    /// Returns an error if the lease cannot be acquired/renewed, the event
    /// stream errors, or the per-event transaction fails.
    pub async fn dispatch_policies_to_outbox(
        &self,
        outbox: &crate::PostgresPolicyOutbox,
        worker_id: &str,
        policies: &[PolicyDispatch],
        lease_duration: std::time::Duration,
    ) -> event_sauce_core::Result<crate::LeaseOutcome> {
        use event_sauce_core::CheckpointStore;

        const DISPATCHER_NAME: &str = "__policy_outbox_dispatcher";

        let Some(start_position) = self
            .checkpoint_store
            .try_acquire_lease(DISPATCHER_NAME, worker_id, lease_duration)
            .await?
        else {
            return Ok(crate::LeaseOutcome::Busy);
        };

        let result = self
            .dispatch_under_lease(outbox, worker_id, policies, lease_duration, start_position)
            .await;

        let _ = self
            .checkpoint_store
            .release_lease(DISPATCHER_NAME, worker_id)
            .await;

        result.map(|()| crate::LeaseOutcome::Completed)
    }

    async fn dispatch_under_lease(
        &self,
        outbox: &crate::PostgresPolicyOutbox,
        worker_id: &str,
        policies: &[PolicyDispatch],
        lease_duration: std::time::Duration,
        start_position: event_sauce_core::Position,
    ) -> event_sauce_core::Result<()> {
        use event_sauce_core::CheckpointStore;

        const DISPATCHER_NAME: &str = "__policy_outbox_dispatcher";

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

            for entry in batch {
                current_position = entry.position;
                let event = entry.envelope;

                if last_renew.elapsed() >= renew_interval {
                    self.checkpoint_store
                        .renew_lease(DISPATCHER_NAME, worker_id, lease_duration)
                        .await?;
                    last_renew = std::time::Instant::now();
                }

                // Find which policies care about this event before opening a
                // transaction — keep the tx narrow.
                let matched: Vec<&PolicyDispatch> = policies
                    .iter()
                    .filter(|p| p.filter.matches(&event))
                    .collect();

                let mut tx = self.pool.begin().await.map_err(|e| {
                    event_sauce_core::Error::backend("Failed to start dispatcher transaction", e)
                })?;

                for dispatch in matched {
                    outbox
                        .enqueue_tx(&mut tx, &dispatch.name, event.id, current_position.as_i64())
                        .await?;
                }
                let landed = self
                    .checkpoint_store
                    .save_checkpoint_fenced_tx(
                        &mut tx,
                        DISPATCHER_NAME,
                        worker_id,
                        current_position,
                    )
                    .await?;

                if !landed {
                    // Lease lost mid-run: roll back the fan-out + checkpoint
                    // advance instead of risking duplicate enqueues / a
                    // regressed dispatcher checkpoint.
                    tx.rollback().await.map_err(|e| {
                        event_sauce_core::Error::backend(
                            "Failed to roll back dispatcher transaction",
                            e,
                        )
                    })?;
                    return Err(event_sauce_core::Error::lease_lost(
                        DISPATCHER_NAME,
                        worker_id,
                    ));
                }

                tx.commit().await.map_err(|e| {
                    event_sauce_core::Error::backend("Failed to commit dispatcher transaction", e)
                })?;
            }
        }

        Ok(())
    }

    async fn run_under_lease<P: crate::PostgresProjection>(
        &self,
        projection: &mut P,
        worker_id: &str,
        lease_duration: std::time::Duration,
        start_position: event_sauce_core::Position,
    ) -> event_sauce_core::Result<()> {
        use event_sauce_core::CheckpointStore;

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

                let mut tx = self.pool.begin().await.map_err(|e| {
                    event_sauce_core::Error::backend("Failed to start projection transaction", e)
                })?;

                projection.handle(&entry.envelope, &mut tx).await?;
                let landed = self
                    .checkpoint_store
                    .save_checkpoint_fenced_tx(&mut tx, P::NAME, worker_id, current_position)
                    .await?;

                if !landed {
                    // We no longer own an active lease (a stalled worker that
                    // was taken over). Roll the in-flight tx back rather than
                    // double-apply / regress the checkpoint.
                    tx.rollback().await.map_err(|e| {
                        event_sauce_core::Error::backend(
                            "Failed to roll back projection transaction",
                            e,
                        )
                    })?;
                    return Err(event_sauce_core::Error::lease_lost(P::NAME, worker_id));
                }

                tx.commit().await.map_err(|e| {
                    event_sauce_core::Error::backend("Failed to commit projection transaction", e)
                })?;
            }
        }

        Ok(())
    }
}

/// Maximum number of events fetched per batch by the projection runners.
///
/// Bounds memory and ensures the streaming connection is released between
/// batches, so the connection pool isn't held captive while a projection
/// catches up. The lease is renewed inside the batch loop, so this only
/// affects how often the projection runner re-acquires a pool connection.
const PROJECTION_BATCH_SIZE: i64 = 500;

/// One entry in the policy registry passed to
/// [`PostgresBackend::dispatch_policies_to_outbox`].
///
/// Pairs a policy name with the filter the dispatcher uses to decide which
/// events should produce outbox rows for that policy. Names must match what
/// drainer workers will pass to
/// [`PostgresPolicyOutbox::claim_batch`](crate::PostgresPolicyOutbox::claim_batch).
#[derive(Debug, Clone)]
pub struct PolicyDispatch {
    /// Unique policy name; used as the outbox row's `policy_name`.
    pub name: String,
    /// Which events trigger an outbox row for this policy.
    pub filter: event_sauce_core::EventFilter,
}

impl PolicyDispatch {
    /// Convenience constructor.
    #[must_use]
    pub fn new(name: impl Into<String>, filter: event_sauce_core::EventFilter) -> Self {
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
    pub async fn build(self) -> event_sauce_core::Result<PostgresBackend> {
        let schema = self.schema.unwrap_or_else(|| "event_sauce".to_string());
        let snapshot_config = self
            .snapshot_config
            .unwrap_or_else(|| SnapshotConfig::builder().build());

        let pool = match (self.pool, self.database_url) {
            (Some(pool), _) => pool,
            (None, Some(url)) => PgPool::connect(&url)
                .await
                .map_err(|e| event_sauce_core::Error::backend("Failed to connect", e))?,
            (None, None) => {
                return Err(event_sauce_core::Error::invalid_state(
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
            .checkpoint_store(
                Arc::clone(&checkpoint_store) as Arc<dyn event_sauce_core::CheckpointStore>
            );

        // Pass through user-provided crypto overrides; otherwise
        // the event store builder defaults to PostgresCryptoKeyStore + Aes256GcmProvider.
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
    use event_sauce_core::{
        AggregateVersion, CheckpointStore, EventEnvelope, EventStore, EventVersion, Position,
        StreamId,
    };
    use futures::StreamExt;
    use serde_json::json;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    /// Starts a `PostgreSQL` testcontainer and returns a connection URL.
    async fn start_test_db() -> (
        String,
        testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    ) {
        let container = crate::test_support::start_postgres()
            .await
            .expect("Failed to start PostgreSQL container");

        let host = container.get_host().await.expect("Failed to get host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get port");

        let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
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
    impl crate::PostgresProjection for CountingProjection {
        const NAME: &'static str = "CountingProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            _envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> event_sauce_core::Result<()> {
            self.seen += 1;
            if let Some(limit) = self.fail_after {
                if self.seen > limit {
                    return Err(event_sauce_core::Error::custom("forced failure"));
                }
            }
            sqlx::query("UPDATE event_sauce.counting_projection SET n = n + 1 WHERE id = 1")
                .execute(&mut **tx)
                .await
                .map_err(|e| event_sauce_core::Error::custom(format!("update failed: {e}")))?;
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
            .load_checkpoint(<CountingProjection as crate::PostgresProjection>::NAME)
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
            .load_checkpoint(<CountingProjection as crate::PostgresProjection>::NAME)
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
    impl crate::PostgresProjection for RecordingProjection {
        const NAME: &'static str = "RecordingProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> event_sauce_core::Result<()> {
            sqlx::query("INSERT INTO event_sauce.recording_projection (event_id) VALUES ($1)")
                .bind(envelope.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| event_sauce_core::Error::custom(format!("insert failed: {e}")))?;
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
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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
            .run_leased_projection(
                &mut projection,
                "worker-1",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, crate::LeaseOutcome::Completed);
        assert_eq!(
            CountingProjection::read(backend.pool(), "event_sauce").await,
            3
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
                <CountingProjection as crate::PostgresProjection>::NAME,
                "worker-a",
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();

        // Worker B should see Busy.
        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(
                &mut projection,
                "worker-b",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, crate::LeaseOutcome::Busy);
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
            .run_leased_projection(
                &mut projection,
                "worker-a",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();

        // Worker B should now be able to acquire the lease (worker A released it).
        let mut projection = CountingProjection {
            fail_after: None,
            seen: 0,
        };
        let outcome = backend
            .run_leased_projection(
                &mut projection,
                "worker-b",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, crate::LeaseOutcome::Completed);
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
            .run_leased_projection(
                &mut projection,
                "worker-a",
                std::time::Duration::from_secs(30),
            )
            .await;
        assert!(result.is_err());

        // Lease should have been released even on failure.
        let acquired = backend
            .checkpoint_store()
            .try_acquire_lease(
                <CountingProjection as crate::PostgresProjection>::NAME,
                "worker-b",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert!(acquired.is_some(), "lease should be released after failure");
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
            event_sauce_core::EventFilter::by_aggregate_type("Order"),
        )];

        let outcome = backend
            .dispatch_policies_to_outbox(
                &outbox,
                "dispatcher-1",
                &policies,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, crate::LeaseOutcome::Completed);

        // Three Order events should be in the outbox; two User events should not.
        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            3
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
            event_sauce_core::EventFilter::by_aggregate_type("Order"),
        )];
        backend
            .dispatch_policies_to_outbox(
                &outbox,
                "dispatcher-1",
                &policies,
                std::time::Duration::from_secs(30),
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
                        std::time::Duration::from_secs(30),
                    )
                    .await
            }),
            tokio::spawn(async move {
                outbox_b
                    .claim_batch(
                        "send-order-confirmation",
                        "drainer-b",
                        100,
                        std::time::Duration::from_secs(30),
                    )
                    .await
            })
        );

        let claims_a = claims_a.unwrap().unwrap();
        let claims_b = claims_b.unwrap().unwrap();
        assert_eq!(claims_a.len() + claims_b.len(), 6);

        // Mark all as done; pending should hit zero.
        for c in claims_a.iter().chain(claims_b.iter()) {
            outbox.mark_done(c.id).await.unwrap();
        }
        assert_eq!(
            outbox
                .pending_count("send-order-confirmation")
                .await
                .unwrap(),
            0
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
    impl crate::PostgresProjection for RebuildProjection {
        const NAME: &'static str = "RebuildProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> event_sauce_core::Result<()> {
            sqlx::query("INSERT INTO event_sauce.rebuild_projection (event_id) VALUES ($1)")
                .bind(envelope.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| event_sauce_core::Error::custom(format!("insert failed: {e}")))?;
            Ok(())
        }

        async fn reset(
            &mut self,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> event_sauce_core::Result<()> {
            sqlx::query("TRUNCATE event_sauce.rebuild_projection")
                .execute(&mut **tx)
                .await
                .map_err(|e| event_sauce_core::Error::custom(format!("truncate failed: {e}")))?;
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
    impl crate::PostgresProjection for NoResetProjection {
        const NAME: &'static str = "NoResetProjection";

        fn handled_event_types() -> Option<Vec<&'static str>> {
            Some(vec!["TestEvent"])
        }

        async fn handle(
            &mut self,
            envelope: &EventEnvelope,
            tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ) -> event_sauce_core::Result<()> {
            sqlx::query("INSERT INTO event_sauce.no_reset_projection (event_id) VALUES ($1)")
                .bind(envelope.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| event_sauce_core::Error::custom(format!("insert failed: {e}")))?;
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
                <RebuildProjection as crate::PostgresProjection>::NAME,
                Position::new(999),
            )
            .await
            .unwrap();

        // Rebuild: lease-guarded, atomic table+checkpoint reset, re-drain from 0.
        let mut projection = RebuildProjection;
        let outcome = backend
            .rebuild(
                &mut projection,
                "worker-1",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(outcome, crate::LeaseOutcome::Completed);

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
            .load_checkpoint(<RebuildProjection as crate::PostgresProjection>::NAME)
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
                <RebuildProjection as crate::PostgresProjection>::NAME,
                Position::new(777),
            )
            .await
            .unwrap();

        // Another worker holds the lease.
        backend
            .checkpoint_store()
            .try_acquire_lease(
                <RebuildProjection as crate::PostgresProjection>::NAME,
                "other-worker",
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();

        let mut projection = RebuildProjection;
        let outcome = backend
            .rebuild(
                &mut projection,
                "worker-1",
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            crate::LeaseOutcome::Busy,
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
            .load_checkpoint(<RebuildProjection as crate::PostgresProjection>::NAME)
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
            .rebuild(
                &mut projection,
                "worker-1",
                std::time::Duration::from_secs(30),
            )
            .await;
        assert!(
            result.is_err(),
            "rebuild must error for a projection whose reset() is the default (not overridden)"
        );
    }
}
