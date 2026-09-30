//! Routes events to registered policies, tracking each policy's own
//! checkpoint and applying its configured error-handling strategy.

use std::sync::Arc;

use super::context::PolicyContext;
use super::on_error::{OnError, OnRetryExhausted, RetryConfig, RetryLimit};
use super::Policy;
use crate::{CheckpointStoreRef, EventEnvelope, EventStore, Position, Result};

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

        self.store.max_position().await
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
    /// 1. Loads the checkpoint (or resolves to current max for new policies)
    /// 2. Streams events from the earliest checkpoint position
    /// 3. Routes matching events to policies, respecting their individual checkpoints
    /// 4. Handles cascading reactions until no new events are produced
    /// 5. Saves updated checkpoints
    ///
    /// Returns the total number of events processed.
    ///
    /// A flush conflict is treated exactly like a handler error: both go
    /// through the same [`OnError`] policy, so `Retry` retries the flush and
    /// `Skip` skips a reaction whose flush conflicted.
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
                        match self.handle_and_flush(policy, &envelope).await {
                            Ok(()) => {
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
                                    let should_skip =
                                        match self.retry_handler(policy, &envelope, config).await {
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
}
