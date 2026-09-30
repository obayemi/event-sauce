//! Policy dispatch: [`PostgresBackend::dispatch_policies_to_outbox`] and the
//! per-policy checkpoint/lease machinery it uses.

use std::time::Duration;

use event_sauce_core::{CheckpointStore, EventFilter, Position, Result};

use crate::LeaseOutcome;

use super::{LeaseRenewal, PostgresBackend, PROJECTION_BATCH_SIZE};

impl PostgresBackend {
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
    /// writes commit or roll back together — never partial. Every held
    /// policy's lease is renewed on the same cadence as the projection
    /// runners (a third of `lease_duration`, see `LeaseRenewal`), checked
    /// once per fetched batch rather than once per event.
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
    /// [`Error::LeaseLost`](event_sauce_core::Error::LeaseLost) for that
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
        let mut renewal = LeaseRenewal::new(lease_duration);

        loop {
            let batch = self
                .event_store
                .fetch_events_batch(current_position, PROJECTION_BATCH_SIZE)
                .await?;
            let Some(batch_end) = batch.last().map(|e| e.position) else {
                break;
            };

            if renewal.due() {
                for held_policy in held.iter() {
                    self.checkpoint_store
                        .renew_lease(&held_policy.checkpoint_name, worker_id, lease_duration)
                        .await?;
                }
                renewal.renewed();
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
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{
        AggregateVersion, CheckpointStore, EventEnvelope, EventStore, EventVersion, StreamId,
    };
    use serde_json::json;
    use uuid::Uuid;

    use crate::backend::tests::fixtures::{append_typed, start_test_db};

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
}
