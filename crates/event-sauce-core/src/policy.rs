//! Policy system for cross-aggregate event reactions.
//!
//! Provides the [`Policy`] trait for defining event handlers that react to events
//! by issuing commands on other aggregates, and [`PolicyContext`] for safe event
//! store access with automatic causation tracking.
//!
//! # Overview
//!
//! In an event-sourced system, a single command may produce events that should
//! trigger effects on other aggregates. For example, kicking a user may need to
//! update a group aggregate. Policies provide this cross-aggregate orchestration.
//!
//! # Design Principles
//!
//! - **Async/eventually consistent**: Policies process events outside the original
//!   transaction.
//! - **Forced causation tracking**: All interactions go through [`PolicyContext`],
//!   which guarantees causation metadata is always set.
//! - **Cascade depth limits**: Reactions can trigger further reactions; depth is
//!   bounded to prevent infinite loops.
//! - **Checkpoint-based resumption**: Policies track their position via
//!   [`CheckpointStore`](crate::CheckpointStore), preventing duplicate processing
//!   on restart.
//! - **Configurable error handling**: [`OnError`] controls whether failures abort,
//!   skip, or retry with exponential backoff.
//!
//! # Examples
//!
//! ```ignore
//! use event_sauce_core::policy::{Policy, PolicyContext, PolicyRunner};
//!
//! struct MyPolicy;
//!
//! #[async_trait::async_trait]
//! impl<S: EventStore + 'static> Policy<S> for MyPolicy {
//!     fn name(&self) -> &str { "MyPolicy" }
//!     fn event_filter(&self) -> EventFilter { EventFilter::by_event_type("User.Kicked") }
//!     async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<S>) -> Result<()> {
//!         // Load, modify, and commit another aggregate
//!         Ok(())
//!     }
//! }
//! ```

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::{
    event_store::PreparedCommit, Aggregate, AggregateId, AggregateRoot, CheckpointStoreRef,
    DeletedAggregateRoot, EntityIdFor, EventEnvelope, EventFilter, EventMetadata, EventStore,
    Loaded, Position, Result,
};

/// A policy handles events by issuing commands on other aggregates.
///
/// Policies are the mechanism for cross-aggregate event orchestration
/// (known as "process managers" or "sagas" in some literature).
/// Each policy declares which events it handles via [`event_filter()`](Policy::event_filter)
/// and processes matching events in [`handle()`](Policy::handle).
///
/// The type parameter `S` is the event store type, allowing the policy to
/// work with any `EventStore` implementation while remaining dyn-compatible.
///
/// # Implementing a Policy
///
/// ```ignore
/// struct NotifyOnKick;
///
/// #[async_trait]
/// impl<S: EventStore + 'static> Policy<S> for NotifyOnKick {
///     fn name(&self) -> &str { "NotifyOnKick" }
///
///     fn event_filter(&self) -> EventFilter {
///         EventFilter::by_event_type("User.Kicked")
///     }
///
///     async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<S>) -> Result<()> {
///         // React to the event by loading and modifying other aggregates
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Policy<S: EventStore + 'static>: Send + Sync {
    /// Unique name for this policy.
    ///
    /// Used for checkpoint tracking, so each policy resumes
    /// from where it left off after restarts.
    fn name(&self) -> &str;

    /// Which events this policy handles.
    ///
    /// Only events matching this filter will be passed to [`handle()`](Self::handle).
    fn event_filter(&self) -> EventFilter;

    /// Handle a single event.
    ///
    /// Use `ctx` to load aggregates and commit changes. The context
    /// automatically injects causation metadata into all produced events.
    ///
    /// # Errors
    ///
    /// Returns an error if loading, applying, or committing fails.
    async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<S>) -> Result<()>;
}

/// Provides event store access with automatic causation tracking.
///
/// `PolicyContext` wraps an event store and a source event. All load operations
/// delegate to the store, while commit operations automatically inject causation
/// metadata (correlation ID, causation ID, causation chain) into every pending
/// event before persisting.
///
/// # Causation Logic
///
/// When committing:
/// 1. `correlation_id` = source event's `correlation_id`, or source event's id if none
/// 2. `causation_id` = source event's id
/// 3. `causation_chain` = source event's chain + source event's id
/// 4. Depth check: `chain.len()` < `max_cascade_depth`
///
/// # Examples
///
/// ```ignore
/// async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<impl EventStore>) -> Result<()> {
///     let mut group = ctx.load_as::<Group>(some_entity_id).await?;
///     group.apply(SomeEvent { ... })?;
///     ctx.commit(&mut group).await?;
///     Ok(())
/// }
/// ```
pub struct PolicyContext<S: EventStore> {
    store: Arc<S>,
    source_event: EventEnvelope,
    max_cascade_depth: usize,
    pending_commits: std::sync::Mutex<Vec<PreparedCommit>>,
}

impl<S: EventStore + 'static> PolicyContext<S> {
    /// Creates a new policy context.
    ///
    /// # Arguments
    ///
    /// - `store`: The event store for loading and committing aggregates.
    /// - `source_event`: The event being reacted to.
    /// - `max_cascade_depth`: Maximum allowed depth in the causation chain.
    #[must_use]
    pub fn new(store: Arc<S>, source_event: EventEnvelope, max_cascade_depth: usize) -> Self {
        Self {
            store,
            source_event,
            max_cascade_depth,
            pending_commits: std::sync::Mutex::new(Vec::new()),
        }
    }

    // === Load methods (read-only, delegate to store) ===

    /// Loads an aggregate using a typed ID.
    ///
    /// The aggregate type is inferred from the ID type.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails.
    pub async fn load<I: AggregateId>(&self, id: I) -> Result<AggregateRoot<I::Aggregate>>
    where
        I::Aggregate: Aggregate + serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::DeletedState: serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load(&*self.store, id.as_entity_id()).await
    }

    /// Loads an aggregate using a raw `EntityId` with explicit type.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails.
    pub async fn load_as<A: Aggregate + serde::de::DeserializeOwned>(
        &self,
        id: impl EntityIdFor<A>,
    ) -> Result<AggregateRoot<A>>
    where
        A::DeletedState: serde::de::DeserializeOwned,
        A::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load(&*self.store, id.entity_id()).await
    }

    /// Loads an aggregate returning its lifecycle state, using a typed ID.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails.
    pub async fn load_any<I: AggregateId>(&self, id: I) -> Result<Loaded<I::Aggregate>>
    where
        I::Aggregate: Aggregate + serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::DeletedState: serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load_any(&*self.store, id.as_entity_id()).await
    }

    /// Loads an aggregate returning its lifecycle state, with explicit type.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails.
    pub async fn load_any_as<A: Aggregate + serde::de::DeserializeOwned>(
        &self,
        id: impl EntityIdFor<A>,
    ) -> Result<Loaded<A>>
    where
        A::DeletedState: serde::de::DeserializeOwned,
        A::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load_any(&*self.store, id.entity_id()).await
    }

    /// Loads a deleted aggregate using a typed ID.
    ///
    /// # Errors
    ///
    /// Returns an error if loading fails or the aggregate is not deleted.
    pub async fn load_deleted<I: AggregateId>(
        &self,
        id: I,
    ) -> Result<DeletedAggregateRoot<I::Aggregate>>
    where
        I::Aggregate: Aggregate + serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::DeletedState: serde::de::DeserializeOwned,
        <I::Aggregate as Aggregate>::Event: serde::de::DeserializeOwned,
    {
        crate::event_store::load_deleted(&*self.store, id.as_entity_id()).await
    }

    /// Checks if an aggregate exists, using a typed ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the store operation fails.
    pub async fn exists<I: AggregateId>(&self, id: I) -> Result<bool>
    where
        I::Aggregate: Aggregate,
    {
        let stream_id =
            crate::StreamId::new(I::Aggregate::aggregate_type(), id.as_entity_id().as_uuid());
        self.store.stream_exists(stream_id).await
    }

    // === Commit methods (inject causation metadata) ===

    /// Commits an aggregate with automatic causation tracking.
    ///
    /// Injects causation metadata into all pending events and buffers the
    /// prepared commit. The buffered events are only persisted when
    /// [`flush()`](Self::flush) is called (typically by [`PolicyRunner`] after
    /// the handler returns `Ok`). If the handler returns `Err`, the buffer is
    /// dropped and nothing is persisted.
    ///
    /// # Errors
    ///
    /// Returns `Error::CascadeDepthExceeded` if the cascade depth exceeds the limit.
    /// Returns errors from serialization or encryption during preparation.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (only possible if a prior
    /// panic occurred while holding the lock).
    pub async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::Event: serde::Serialize,
    {
        let metadata = self.build_causation_metadata()?;
        aggregate.set_pending_metadata(&metadata);
        if let Some(prepared) = crate::event_store::prepare_commit(&*self.store, aggregate).await? {
            self.pending_commits.lock().unwrap().push(prepared);
        }
        Ok(())
    }

    /// Commits a deleted aggregate with automatic causation tracking.
    ///
    /// Like [`commit()`](Self::commit), buffers the prepared commit for later
    /// flushing.
    ///
    /// # Errors
    ///
    /// Returns `Error::CascadeDepthExceeded` if the cascade depth exceeds the limit.
    /// Returns errors from serialization or encryption during preparation.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (only possible if a prior
    /// panic occurred while holding the lock).
    pub async fn commit_deleted<A>(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>
    where
        A: Aggregate + serde::Serialize,
        A::DeletedState: serde::Serialize,
        A::Event: serde::Serialize,
    {
        let metadata = self.build_causation_metadata()?;
        aggregate.set_pending_metadata(&metadata);
        if let Some(prepared) =
            crate::event_store::prepare_commit_deleted(&*self.store, aggregate).await?
        {
            self.pending_commits.lock().unwrap().push(prepared);
        }
        Ok(())
    }

    /// Flushes all buffered commits to the event store.
    ///
    /// Called by [`PolicyRunner`] after a handler returns `Ok(())`. If the
    /// handler returns `Err`, this method is never called and the buffered
    /// commits are silently dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if appending events or saving snapshots fails.
    pub(crate) async fn flush(&self) -> Result<()> {
        let commits: Vec<PreparedCommit> = self.pending_commits.lock().unwrap().drain(..).collect();
        for prepared in commits {
            crate::event_store::flush_prepared(&*self.store, prepared).await?;
        }
        Ok(())
    }

    // === Introspection ===

    /// Returns the event being reacted to.
    #[must_use]
    pub fn source_event(&self) -> &EventEnvelope {
        &self.source_event
    }

    /// Returns the current cascade depth.
    #[must_use]
    pub fn cascade_depth(&self) -> usize {
        self.source_event
            .metadata
            .as_ref()
            .map_or(0, EventMetadata::cascade_depth)
    }

    // === Internal ===

    /// Builds causation metadata from the source event.
    ///
    /// Returns `Error::CascadeDepthExceeded` if the chain would exceed the limit.
    fn build_causation_metadata(&self) -> Result<EventMetadata> {
        let source_id = self.source_event.id;

        // Get or create parent metadata
        let parent_metadata = self.source_event.metadata.clone().unwrap_or_default();

        let child = parent_metadata.child_metadata(source_id);

        // Check depth limit
        let depth = child.cascade_depth();
        if depth > self.max_cascade_depth {
            return Err(crate::Error::cascade_depth_exceeded(
                depth,
                self.max_cascade_depth,
            ));
        }

        Ok(child)
    }
}

impl<S: EventStore + 'static> std::fmt::Debug for PolicyContext<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pending_count = self.pending_commits.lock().unwrap().len();
        f.debug_struct("PolicyContext")
            .field("source_event_id", &self.source_event.id)
            .field("max_cascade_depth", &self.max_cascade_depth)
            .field("cascade_depth", &self.cascade_depth())
            .field("pending_commits", &pending_count)
            .finish_non_exhaustive()
    }
}

/// What to do when a policy handler returns an error.
///
/// The variants differ in how they affect the per-policy checkpoint, and hence
/// what a subsequent [`PolicyRunner::process_pending`] call re-attempts:
///
/// - [`OnError::Fail`] **stops and resumes at the failing event**. Checkpoints
///   are persisted up to the last event each policy fully handled, but never
///   past the failing event — so a re-run resumes at it and never replays the
///   already-flushed effects of earlier events in the batch.
/// - [`OnError::Skip`] **permanently skips the failing event**: the checkpoint
///   advances past it and the source event is never retried. Use this only when
///   dropping the event is acceptable.
/// - [`OnError::Retry`] retries the same event with backoff; on exhaustion it
///   either fails (resume at the event) or skips it, per [`OnRetryExhausted`].
#[derive(Debug, Clone)]
pub enum OnError {
    /// Stop immediately and resume at the failing event on the next run.
    ///
    /// Checkpoints are persisted up to the last successfully-handled event but
    /// NOT advanced past the failing event.
    Fail,
    /// Log a warning and permanently skip the event: advance the checkpoint past
    /// it and continue. The source event is not retried on subsequent runs.
    Skip,
    /// Retry with exponential backoff before giving up.
    Retry(RetryConfig),
}

/// Configuration for exponential backoff retry.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Base delay between retries — doubles each attempt (default: 100ms).
    pub base_delay: Duration,
    /// Maximum delay cap (default: 30s).
    pub max_delay: Duration,
    /// When to stop retrying.
    pub limit: RetryLimit,
    /// What to do when all retries are exhausted.
    pub on_exhausted: OnRetryExhausted,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            limit: RetryLimit::MaxRetries(3),
            on_exhausted: OnRetryExhausted::Fail,
        }
    }
}

/// When to stop retrying.
#[derive(Debug, Clone)]
pub enum RetryLimit {
    /// Stop after N retry attempts.
    MaxRetries(usize),
    /// Stop after total elapsed time exceeds this duration.
    MaxDuration(Duration),
    /// Retry indefinitely until success (blocks processing on this event).
    Indefinite,
}

/// What to do when retry limit is reached.
#[derive(Debug, Clone)]
pub enum OnRetryExhausted {
    /// Return error, stop processing. Checkpoint NOT advanced.
    Fail,
    /// Log warning, skip the event, advance checkpoint, continue.
    Skip,
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
}

impl<S: EventStore + 'static> PolicyRunner<S> {
    /// Creates a new policy runner.
    ///
    /// Checkpoint store is required — policies track their position
    /// to prevent duplicate processing on restart.
    #[must_use]
    pub fn new(store: Arc<S>, checkpoint_store: CheckpointStoreRef) -> Self {
        Self {
            store,
            policies: Vec::new(),
            checkpoint_store,
            max_cascade_depth: 10,
            on_error: OnError::Fail,
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

    /// Resolves the starting position for a policy from its checkpoint.
    ///
    /// If a checkpoint exists, returns it. If no checkpoint exists (new policy),
    /// resolves to the store's current max position so all existing history is
    /// skipped and only future events are processed.
    async fn resolve_checkpoint(&self, policy_name: &str) -> Result<Position> {
        if let Some(pos) = self.checkpoint_store.load_checkpoint(policy_name).await? {
            return Ok(pos);
        }

        // New policy: skip all existing events by starting from the current max
        // global position.
        self.store.max_position().await
    }

    /// Processes all pending events in one-shot mode with checkpoint support.
    ///
    /// For each registered policy:
    /// 1. Loads the checkpoint (or resolves to current max for new policies)
    /// 2. Streams events from the earliest checkpoint position
    /// 3. Routes matching events to policies, respecting their individual checkpoints
    /// 4. Handles cascading reactions until no new events are produced
    /// 5. Saves updated checkpoints
    ///
    /// Returns the total number of events processed.
    ///
    /// # Errors
    ///
    /// Returns an error if event loading, policy handling, or committing fails
    /// (depending on the [`OnError`] configuration).
    #[allow(clippy::too_many_lines)]
    pub async fn process_pending(&self) -> Result<usize> {
        use futures::StreamExt;

        if self.policies.is_empty() {
            return Ok(0);
        }

        // Resolve checkpoints for all policies
        let mut policy_checkpoints: Vec<Position> = Vec::with_capacity(self.policies.len());
        for policy in &self.policies {
            let checkpoint = self.resolve_checkpoint(policy.name()).await?;
            policy_checkpoints.push(checkpoint);
        }

        // Start from the minimum checkpoint across all policies
        let mut from_position = policy_checkpoints
            .iter()
            .copied()
            .min()
            .unwrap_or_else(Position::start);

        let mut total_processed = 0;

        // Process events in rounds to handle cascading
        loop {
            let event_stream = self.store.stream_all(from_position).await?;
            futures::pin_mut!(event_stream);

            let mut round_processed = 0;
            let mut last_position = from_position;

            while let Some(entry_result) = event_stream.next().await {
                let entry = entry_result?;
                let envelope = entry.envelope;

                // Track position for next round
                last_position = entry.position;

                // Route to matching policies, respecting per-policy checkpoints
                for (i, policy) in self.policies.iter().enumerate() {
                    // Skip if this policy has already processed past this position
                    if last_position.as_i64() <= policy_checkpoints[i].as_i64() {
                        continue;
                    }

                    if policy.event_filter().matches(&envelope) {
                        let ctx = PolicyContext::new(
                            Arc::clone(&self.store),
                            envelope.clone(),
                            self.max_cascade_depth,
                        );

                        match policy.handle(&envelope, &ctx).await {
                            Ok(()) => {
                                ctx.flush().await?;
                                round_processed += 1;
                            }
                            Err(e) => match &self.on_error {
                                OnError::Fail => {
                                    // Persist every policy's checkpoint up to the
                                    // last event it fully handled *before* failing.
                                    // This policy's checkpoint is intentionally NOT
                                    // advanced past the failing event (the loop-end
                                    // advance at the bottom of this iteration is
                                    // skipped by the early return), so a re-run
                                    // resumes at the failing event and never replays
                                    // the already-flushed effects of events 1..N-1.
                                    self.save_checkpoints(&policy_checkpoints).await?;
                                    return Err(e);
                                }
                                OnError::Skip => {
                                    tracing::warn!(
                                        policy = policy.name(),
                                        event_id = %envelope.id,
                                        error = %e,
                                        "Skipping event due to policy error"
                                    );
                                }
                                OnError::Retry(config) => {
                                    let should_skip = match self
                                        .retry_handler(policy, &envelope, config)
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
                                            self.save_checkpoints(&policy_checkpoints).await?;
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
                    policy_checkpoints[i] = last_position;
                }
            }

            total_processed += round_processed;

            // If no events were processed this round, we're done
            if round_processed == 0 {
                break;
            }

            // Move position forward for next round (to pick up cascaded events)
            from_position = last_position;
        }

        // Save checkpoints for all policies
        self.save_checkpoints(&policy_checkpoints).await?;

        Ok(total_processed)
    }

    /// Persists each policy's checkpoint at its current position.
    ///
    /// `checkpoints[i]` is the highest position policy `i` has fully processed
    /// (handled-`Ok`, skipped because non-matching, or `OnError::Skip`'d). This
    /// is called on both the success and the `OnError::Fail` paths so that a
    /// mid-batch failure does not lose the progress of earlier events whose
    /// side effects were already flushed.
    async fn save_checkpoints(&self, checkpoints: &[Position]) -> Result<()> {
        for (i, policy) in self.policies.iter().enumerate() {
            self.checkpoint_store
                .save_checkpoint(policy.name(), checkpoints[i])
                .await?;
        }
        Ok(())
    }

    /// Retries a failed handler with exponential backoff.
    ///
    /// Returns `Ok(false)` if the retry succeeded (event was processed),
    /// `Ok(true)` if the retries were exhausted and the event was skipped,
    /// or `Err` if the retries were exhausted and the policy is `OnRetryExhausted::Fail`.
    async fn retry_handler(
        &self,
        policy: &Arc<dyn Policy<S>>,
        envelope: &EventEnvelope,
        config: &RetryConfig,
    ) -> Result<bool> {
        let started = std::time::Instant::now();
        let mut attempt = 0;
        let mut last_error;

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

            let ctx = PolicyContext::new(
                Arc::clone(&self.store),
                envelope.clone(),
                self.max_cascade_depth,
            );

            match policy.handle(envelope, &ctx).await {
                Ok(()) => {
                    ctx.flush().await?;
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
                    last_error = e;
                    let _ = last_error; // suppress unused warning
                }
            }
        }
    }

    /// Processes a single event envelope through matching policies.
    ///
    /// Returns the number of policies that handled the event.
    /// Error handling ([`OnError`]) applies per-event.
    ///
    /// # Errors
    ///
    /// Returns an error if policy handling fails (depending on [`OnError`] configuration).
    pub async fn process_event(&self, envelope: &EventEnvelope) -> Result<usize> {
        let mut handled = 0;

        for policy in &self.policies {
            if policy.event_filter().matches(envelope) {
                let ctx = PolicyContext::new(
                    Arc::clone(&self.store),
                    envelope.clone(),
                    self.max_cascade_depth,
                );

                match policy.handle(envelope, &ctx).await {
                    Ok(()) => {
                        ctx.flush().await?;
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
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{
        MockCheckpointStore, MockEventStore, SimpleTestEntity, SimpleTestEvent,
    };
    use crate::{AggregateRoot, AggregateVersion, CheckpointStore, EventVersion};
    use serde_json::json;
    use uuid::Uuid;

    // Helper to create a test envelope
    fn test_envelope(event_type: &str, aggregate_type: &str) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            aggregate_type.to_string(),
            event_type.to_string(),
            EventVersion::new(1),
            json!({}),
        )
    }

    fn test_checkpoint_store() -> Arc<MockCheckpointStore> {
        Arc::new(MockCheckpointStore::new())
    }

    // === PolicyContext tests ===

    #[test]
    fn test_policy_context_new() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(store, envelope.clone(), 10);

        assert_eq!(ctx.source_event().id, envelope.id);
        assert_eq!(ctx.cascade_depth(), 0);
    }

    #[test]
    fn test_policy_context_cascade_depth_from_metadata() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(), Uuid::new_v4()];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = PolicyContext::new(store, envelope, 10);

        assert_eq!(ctx.cascade_depth(), 2);
    }

    #[test]
    fn test_policy_context_build_causation_metadata() {
        let store = Arc::new(MockEventStore::new());
        let correlation_id = Uuid::new_v4();
        let metadata = EventMetadata::new().with_correlation_id(correlation_id);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let source_id = envelope.id;
        let ctx = PolicyContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        assert_eq!(child.correlation_id, Some(correlation_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain.len(), 1);
        assert_eq!(child.causation_chain[0], source_id);
    }

    #[test]
    fn test_policy_context_build_causation_metadata_no_parent_metadata() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let source_id = envelope.id;
        let ctx = PolicyContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        // Without parent correlation_id, uses parent event ID
        assert_eq!(child.correlation_id, Some(source_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain, vec![source_id]);
    }

    #[test]
    fn test_policy_context_build_causation_metadata_extends_chain() {
        let store = Arc::new(MockEventStore::new());
        let root_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let correlation_id = Uuid::new_v4();

        let metadata = EventMetadata::new()
            .with_correlation_id(correlation_id)
            .with_causation_id(parent_id)
            .with_causation_chain(vec![root_id, parent_id]);

        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let source_id = envelope.id;
        let ctx = PolicyContext::new(store, envelope, 10);

        let child = ctx.build_causation_metadata().unwrap();

        assert_eq!(child.correlation_id, Some(correlation_id));
        assert_eq!(child.causation_id, Some(source_id));
        assert_eq!(child.causation_chain.len(), 3);
        assert_eq!(child.causation_chain[0], root_id);
        assert_eq!(child.causation_chain[1], parent_id);
        assert_eq!(child.causation_chain[2], source_id);
    }

    #[test]
    fn test_policy_context_cascade_depth_exceeded() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 5];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = PolicyContext::new(store, envelope, 5);

        // Chain has 5 elements, adding 1 more = 6, which exceeds max of 5
        let result = ctx.build_causation_metadata();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cascade_depth_exceeded());
    }

    #[test]
    fn test_policy_context_cascade_depth_at_limit_succeeds() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 4];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = PolicyContext::new(store, envelope, 5);

        // Chain has 4 elements, adding 1 more = 5, which equals max of 5
        let result = ctx.build_causation_metadata();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().cascade_depth(), 5);
    }

    #[tokio::test]
    async fn test_policy_context_commit_injects_metadata() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let source_id = envelope.id;
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();
        ctx.flush().await.unwrap();

        // Load and verify the committed event has causation metadata
        let loaded = crate::event_store::load::<_, SimpleTestEntity>(&*store, id)
            .await
            .unwrap();
        assert_eq!(loaded.value, 42);

        // Verify events were committed (version should be 1)
        let stream_id = crate::StreamId::new("SimpleTestEntity", id.as_uuid());
        let version = store.get_version(stream_id).await.unwrap();
        assert_eq!(version, AggregateVersion::new(1));

        // Verify pending events were cleared
        assert!(agg.pending_events().is_empty());

        // Verify the stored envelope has metadata
        let stored = store.get_events();
        let last_envelope = stored.last().unwrap();
        let meta = last_envelope
            .metadata
            .as_ref()
            .expect("Should have metadata");
        assert_eq!(meta.causation_id, Some(source_id));
        assert_eq!(meta.correlation_id, Some(source_id)); // No parent correlation, uses source id
        assert_eq!(meta.causation_chain, vec![source_id]);
    }

    #[tokio::test]
    async fn test_policy_context_commit_cascade_depth_exceeded() {
        let store = Arc::new(MockEventStore::new());
        let chain = vec![Uuid::new_v4(); 3];
        let metadata = EventMetadata::new().with_causation_chain(chain);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(metadata);
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 3);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();

        let result = ctx.commit(&mut agg).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_cascade_depth_exceeded());
    }

    #[tokio::test]
    async fn test_policy_context_load_as() {
        let store = Arc::new(MockEventStore::new());

        // Commit an aggregate first
        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 99 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        // Load via policy context
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let loaded = ctx.load_as::<SimpleTestEntity>(id).await.unwrap();
        assert_eq!(loaded.value, 99);
    }

    #[test]
    fn test_policy_context_debug() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(store, envelope, 10);

        let debug = format!("{ctx:?}");
        assert!(debug.contains("PolicyContext"));
        assert!(debug.contains("max_cascade_depth: 10"));
        assert!(debug.contains("cascade_depth: 0"));
        assert!(debug.contains("pending_commits: 0"));
    }

    // === PolicyRunner tests ===

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
    fn test_retry_config_default() {
        let config = RetryConfig::default();

        assert_eq!(config.base_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert!(matches!(config.limit, RetryLimit::MaxRetries(3)));
        assert!(matches!(config.on_exhausted, OnRetryExhausted::Fail));
    }

    #[test]
    fn test_retry_limit_variants() {
        let max_retries = RetryLimit::MaxRetries(5);
        assert!(matches!(max_retries, RetryLimit::MaxRetries(5)));

        let max_duration = RetryLimit::MaxDuration(Duration::from_secs(60));
        assert!(matches!(max_duration, RetryLimit::MaxDuration(_)));

        let indefinite = RetryLimit::Indefinite;
        assert!(matches!(indefinite, RetryLimit::Indefinite));
    }

    struct CountingPolicy {
        name: String,
        filter: EventFilter,
        count: Arc<std::sync::Mutex<usize>>,
    }

    #[async_trait]
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

        // Matching event
        let envelope = test_envelope("MatchMe", "TestAggregate");
        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 1);
        assert_eq!(*count.lock().unwrap(), 1);

        // Non-matching event
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

        // Add some events to the store
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

    #[tokio::test]
    async fn test_policy_context_load_any_as() {
        let store = Arc::new(MockEventStore::new());

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 55 }).unwrap();
        store.commit(&mut agg).await.unwrap();

        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let loaded = ctx.load_any_as::<SimpleTestEntity>(id).await.unwrap();
        assert!(loaded.is_active());
    }

    #[tokio::test]
    async fn test_policy_context_commit_preserves_existing_metadata() {
        let store = Arc::new(MockEventStore::new());
        let correlation_id = Uuid::new_v4();
        let parent_metadata = EventMetadata::new().with_correlation_id(correlation_id);
        let envelope = test_envelope("TestEvent", "TestAggregate").with_metadata(parent_metadata);
        let source_id = envelope.id;
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);

        // Apply with existing metadata
        let existing = EventMetadata::new().with_causation_id(Uuid::new_v4());
        agg.apply_with_metadata(SimpleTestEvent::Created { value: 1 }, existing.clone())
            .unwrap();
        // Apply without metadata
        agg.apply(SimpleTestEvent::Updated { value: 2 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();
        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 2);

        // First event keeps its existing metadata (set_pending_metadata doesn't overwrite)
        let meta0 = stored[0].metadata.as_ref().unwrap();
        assert_eq!(meta0.causation_id, existing.causation_id);

        // Second event gets policy causation metadata
        let meta1 = stored[1].metadata.as_ref().unwrap();
        assert_eq!(meta1.causation_id, Some(source_id));
        assert_eq!(meta1.correlation_id, Some(correlation_id));
        assert_eq!(meta1.causation_chain, vec![source_id]);
    }

    // === policy! macro tests ===

    // Test event structs implementing EventType + Deserialize
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FooCreatedEvent {
        pub name: String,
    }

    impl crate::EventType for FooCreatedEvent {
        const EVENT_TYPE: &'static str = "Foo.Created";
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FooUpdatedEvent {
        pub value: i32,
    }

    impl crate::EventType for FooUpdatedEvent {
        const EVENT_TYPE: &'static str = "Foo.Updated";
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct BarDeletedEvent {
        pub reason: String,
    }

    impl crate::EventType for BarDeletedEvent {
        const EVENT_TYPE: &'static str = "Bar.Deleted";
    }

    // Test: policy! generates a struct
    crate::policy! {
        TestPolicy {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_policy_macro_generates_struct() {
        // Verify the policy! macro generates a usable struct
        let policy: &dyn Policy<MockEventStore> = &TestPolicy;
        assert_eq!(policy.name(), "TestPolicy");
    }

    #[test]
    fn test_policy_macro_name() {
        let store = Arc::new(MockEventStore::new());
        let policy: &dyn Policy<MockEventStore> = &TestPolicy;
        let _ = store; // suppress unused
        assert_eq!(policy.name(), "TestPolicy");
    }

    #[test]
    fn test_policy_macro_event_filter_single() {
        let policy: &dyn Policy<MockEventStore> = &TestPolicy;
        let filter = policy.event_filter();

        let matching = test_envelope("Foo.Created", "Foo");
        let non_matching = test_envelope("Foo.Updated", "Foo");

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    // Test: policy! with multiple event handlers
    crate::policy! {
        MultiEventPolicy {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
            on FooUpdatedEvent |_event, _ctx| {
                Ok(())
            },
            on BarDeletedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_policy_macro_event_filter_multiple() {
        let policy: &dyn Policy<MockEventStore> = &MultiEventPolicy;
        let filter = policy.event_filter();

        assert!(filter.matches(&test_envelope("Foo.Created", "Foo")));
        assert!(filter.matches(&test_envelope("Foo.Updated", "Foo")));
        assert!(filter.matches(&test_envelope("Bar.Deleted", "Bar")));
        assert!(!filter.matches(&test_envelope("Other.Event", "Other")));
    }

    // A policy that captures deserialized event data for testing
    struct CapturingPolicy(Arc<std::sync::Mutex<Option<String>>>);

    #[async_trait]
    impl<S: EventStore + 'static> Policy<S> for CapturingPolicy {
        fn name(&self) -> &'static str {
            "CapturingPolicy"
        }
        fn event_filter(&self) -> EventFilter {
            EventFilter::by_event_type("Foo.Created")
        }
        async fn handle(&self, event: &EventEnvelope, _ctx: &PolicyContext<S>) -> Result<()> {
            let foo: FooCreatedEvent = serde_json::from_value(event.event_data.clone())?;
            *self.0.lock().unwrap() = Some(foo.name);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_policy_macro_handle_deserializes_event() {
        let store = Arc::new(MockEventStore::new());
        let received = Arc::new(std::sync::Mutex::new(None::<String>));

        let policy = CapturingPolicy(Arc::clone(&received));
        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "hello"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);
        policy.handle(&envelope, &ctx).await.unwrap();

        assert_eq!(*received.lock().unwrap(), Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_policy_macro_handle_routes_to_correct_handler() {
        // Verify the generated handle() method deserializes and routes correctly
        let store = Arc::new(MockEventStore::new());

        // FooCreatedEvent envelope
        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "test_name"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        // The generated handle should succeed (just returns Ok)
        let result =
            <TestPolicy as Policy<MockEventStore>>::handle(&TestPolicy, &envelope, &ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_policy_macro_handle_deserialization_error() {
        let store = Arc::new(MockEventStore::new());

        // Envelope with wrong data shape for FooCreatedEvent (missing "name" field)
        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"wrong_field": 42});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <TestPolicy as Policy<MockEventStore>>::handle(&TestPolicy, &envelope, &ctx).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_serialization());
    }

    #[tokio::test]
    async fn test_policy_macro_handle_unmatched_event() {
        let store = Arc::new(MockEventStore::new());

        // Envelope with event type that doesn't match any handler
        let envelope = test_envelope("Unknown.Event", "Unknown");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        // Should succeed (no-op for unmatched events)
        let result =
            <TestPolicy as Policy<MockEventStore>>::handle(&TestPolicy, &envelope, &ctx).await;
        assert!(result.is_ok());
    }

    // Test: policy! with doc comment
    crate::policy! {
        /// A documented policy for testing.
        DocPolicy {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_policy_macro_with_doc_comment() {
        let policy: &dyn Policy<MockEventStore> = &DocPolicy;
        assert_eq!(policy.name(), "DocPolicy");
    }

    // Test: policy! with pub visibility
    crate::policy! {
        pub PubPolicy {
            on FooCreatedEvent |_event, _ctx| {
                Ok(())
            },
        }
    }

    #[test]
    fn test_policy_macro_with_visibility() {
        let policy: &dyn Policy<MockEventStore> = &PubPolicy;
        assert_eq!(policy.name(), "PubPolicy");
    }

    // Test: policy! registers with PolicyRunner
    #[test]
    fn test_policy_macro_with_runner() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(store, cp).register(Arc::new(TestPolicy));
        assert_eq!(runner.policies().len(), 1);
        assert_eq!(runner.policies()[0].name(), "TestPolicy");
    }

    #[tokio::test]
    async fn test_policy_macro_process_event_via_runner() {
        let store = Arc::new(MockEventStore::new());
        let cp = test_checkpoint_store();
        let runner = PolicyRunner::new(Arc::clone(&store), cp).register(Arc::new(TestPolicy));

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "via_runner"});

        let handled = runner.process_event(&envelope).await.unwrap();
        assert_eq!(handled, 1);
    }

    // Test: policy! with handler that accesses event fields
    crate::policy! {
        FieldAccessPolicy {
            on FooCreatedEvent |event, _ctx| {
                assert_eq!(event.name, "expected_name");
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_policy_macro_handler_accesses_event_fields() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "expected_name"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result = <FieldAccessPolicy as Policy<MockEventStore>>::handle(
            &FieldAccessPolicy,
            &envelope,
            &ctx,
        )
        .await;
        assert!(result.is_ok());
    }

    // Test: policy! with handler that accesses ctx
    crate::policy! {
        CtxAccessPolicy {
            on FooCreatedEvent |_event, ctx| {
                assert_eq!(ctx.cascade_depth(), 0);
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_policy_macro_handler_accesses_ctx() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "test"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <CtxAccessPolicy as Policy<MockEventStore>>::handle(&CtxAccessPolicy, &envelope, &ctx)
                .await;
        assert!(result.is_ok());
    }

    // Test: policy! with async handler that loads and commits
    crate::policy! {
        AsyncPolicy {
            on FooCreatedEvent |_event, ctx| {
                // Create a new aggregate reacting to the event
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                agg.apply(SimpleTestEvent::Created { value: 42 })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_policy_macro_async_handler() {
        let store = Arc::new(MockEventStore::new());

        let mut envelope = test_envelope("Foo.Created", "Foo");
        envelope.event_data = serde_json::json!({"name": "hello"});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <AsyncPolicy as Policy<MockEventStore>>::handle(&AsyncPolicy, &envelope, &ctx).await;
        assert!(result.is_ok());
        ctx.flush().await.unwrap();

        // Verify the aggregate was committed
        let stored = store.get_events();
        assert_eq!(stored.len(), 1);

        // Verify causation metadata was injected
        let meta = stored[0].metadata.as_ref().expect("Should have metadata");
        assert_eq!(meta.causation_id, Some(envelope.id));
    }

    // Test: policy! multiple handlers route correctly
    crate::policy! {
        RoutingPolicy {
            on FooCreatedEvent |_event, ctx| {
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                agg.apply(SimpleTestEvent::Created { value: 99 })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
            on FooUpdatedEvent |event, ctx| {
                let id = crate::EntityId::new();
                let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
                let val = event.value * 10;
                agg.apply(SimpleTestEvent::Created { value: val })
                    .map_err(|e| crate::Error::invalid_state(format!("{e:?}")))?;
                ctx.commit(&mut agg).await?;
                Ok(())
            },
        }
    }

    #[tokio::test]
    async fn test_policy_macro_routes_to_correct_async_handler() {
        let store = Arc::new(MockEventStore::new());

        // Send FooUpdatedEvent — should go to the second handler
        let mut envelope = test_envelope("Foo.Updated", "Foo");
        envelope.event_data = serde_json::json!({"value": 7});

        let ctx = PolicyContext::new(Arc::clone(&store), envelope.clone(), 10);

        let result =
            <RoutingPolicy as Policy<MockEventStore>>::handle(&RoutingPolicy, &envelope, &ctx)
                .await;
        assert!(result.is_ok());
        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 1);

        // The second handler creates SimpleTestEntity with value = event.value * 10 = 70
        let data: serde_json::Value = stored[0].event_data.clone();
        assert_eq!(data["Created"]["value"], 70);
    }

    // === Buffered commit tests ===

    #[tokio::test]
    async fn test_policy_context_commit_without_flush_does_not_persist() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();

        // Events should NOT be in the store yet (buffered only)
        let stored = store.get_events();
        assert!(
            stored.is_empty(),
            "Events should be buffered, not persisted"
        );

        // But pending events should be cleared from aggregate
        assert!(agg.pending_events().is_empty());
    }

    #[tokio::test]
    async fn test_policy_context_commit_then_flush_persists() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 42 }).unwrap();

        ctx.commit(&mut agg).await.unwrap();
        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 1, "Events should be persisted after flush");
    }

    #[tokio::test]
    async fn test_policy_context_multiple_commits_then_flush() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        // First commit
        let id1 = crate::EntityId::new();
        let mut agg1 = AggregateRoot::<SimpleTestEntity>::new(id1);
        agg1.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        ctx.commit(&mut agg1).await.unwrap();

        // Second commit (different aggregate)
        let id2 = crate::EntityId::new();
        let mut agg2 = AggregateRoot::<SimpleTestEntity>::new(id2);
        agg2.apply(SimpleTestEvent::Created { value: 2 }).unwrap();
        ctx.commit(&mut agg2).await.unwrap();

        // Nothing persisted yet
        assert!(store.get_events().is_empty());

        // Flush all
        ctx.flush().await.unwrap();

        let stored = store.get_events();
        assert_eq!(stored.len(), 2, "Both commits should be persisted");
    }

    #[tokio::test]
    async fn test_policy_context_multiple_commits_dropped_on_error() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");

        {
            let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

            let id = crate::EntityId::new();
            let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
            agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
            ctx.commit(&mut agg).await.unwrap();

            // Simulate handler error — ctx is dropped without flush
        }

        let stored = store.get_events();
        assert!(
            stored.is_empty(),
            "Events should not be persisted when ctx is dropped without flush"
        );
    }

    #[tokio::test]
    async fn test_policy_context_flush_without_commits_is_noop() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        // Flush with no commits should succeed without error
        ctx.flush().await.unwrap();

        assert!(store.get_events().is_empty());
    }

    #[tokio::test]
    async fn test_policy_context_debug_shows_pending_count() {
        let store = Arc::new(MockEventStore::new());
        let envelope = test_envelope("TestEvent", "TestAggregate");
        let ctx = PolicyContext::new(Arc::clone(&store), envelope, 10);

        let id = crate::EntityId::new();
        let mut agg = AggregateRoot::<SimpleTestEntity>::new(id);
        agg.apply(SimpleTestEvent::Created { value: 1 }).unwrap();
        ctx.commit(&mut agg).await.unwrap();

        let debug = format!("{ctx:?}");
        assert!(
            debug.contains("pending_commits: 1"),
            "Debug should show 1 pending commit, got: {debug}"
        );
    }
}
