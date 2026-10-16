# Policies - Cross-Aggregate Event Orchestration

Policies handle events by issuing aggregate commands on other aggregates. When one aggregate produces an event that should trigger effects on other aggregates, policies provide the orchestration.

## Overview

In an event-sourced system, a single command may produce events that should trigger effects elsewhere. For example:

```text
User.Kicked event
    |
    v
KickUserPolicy -------> Group.MemberRemoved event
                              |
                              v
                      NotifyOnRemovalPolicy -------> Notification.Sent event
```

Policies are **async/eventually consistent** -- they process events outside the original transaction. Every event produced by a policy carries full causation metadata, enabling complete traceability.

## Defining Policies

### Using the `policy!` macro

The `policy!` macro is the recommended way to define simple policies:

```rust
use event_sauce::{policy, EntityId, AggregateRoot};

policy! {
    /// Removes kicked users from their groups.
    KickUserPolicy {
        on KickedEvent |event, ctx| {
            let user_id = EntityId::from(ctx.source_event().aggregate_id);
            let mut group = ctx.load_as::<Group>(event.group_id).await?;
            group.remove_member(user_id, event.reason.clone())
                .map_err(|e| event_sauce::Error::invalid_state(format!("{e}")))?;
            ctx.commit(&mut group).await?;
            Ok(())
        },
    }
}
```

Each `on` clause specifies:
- An **event struct** (e.g., `KickedEvent`) that implements `EventType + DeserializeOwned`
- A **handler closure** receiving the deserialized event and a `PolicyContext`

When using `define_events!`, each variant `Foo` automatically generates a `FooEvent` struct with the required traits.

### Using the `Policy` trait directly

For more complex scenarios, implement the trait manually:

```rust
use event_sauce::{EventStore, EventEnvelope, EventFilter, PolicyContext, Result};

struct ComplexPolicy;

#[async_trait::async_trait]
impl<S: EventStore + 'static> Policy<S> for ComplexPolicy {
    fn name(&self) -> &str { "ComplexPolicy" }

    fn event_filter(&self) -> EventFilter {
        EventFilter::any_of_event_types(["User.Kicked", "User.Banned"])
    }

    async fn handle(&self, event: &EventEnvelope, ctx: &PolicyContext<S>) -> Result<()> {
        match event.event_type.as_str() {
            "User.Kicked" => { /* handle kick */ },
            "User.Banned" => { /* handle ban */ },
            _ => {},
        }
        Ok(())
    }
}
```

## PolicyContext

The `PolicyContext` provides event store access with **automatic causation tracking**. All interactions go through the context to guarantee metadata is always set.

### Loading aggregates

```rust
// With explicit type:
let group = ctx.load_as::<Group>(entity_id).await?;

// With typed ID (type inferred):
let group = ctx.load(group_id).await?;  // GroupId -> AggregateRoot<Group>

// Active or deleted:
let loaded = ctx.load_any_as::<Group>(entity_id).await?;
```

### Committing changes

```rust
ctx.commit(&mut aggregate).await?;
ctx.commit_deleted(&mut deleted_aggregate).await?;
```

Commits through `PolicyContext` are **buffered**: they are only persisted to the event store when the handler returns `Ok(())`. If the handler returns `Err`, all buffered commits are discarded. This prevents partially-committed events on handler failure, and avoids duplicate events when the handler is retried.

A handler may `ctx.commit` to **several aggregates** in one reaction — the canonical "move funds from account A to account B". When the handler returns `Ok(())`, all buffered commits are flushed through a single `EventStore::append_batch`:

The whole reaction is **atomic** on every built-in backend: a `ConcurrencyConflict` (or claim conflict) on any aggregate rolls back the entire reaction — so a partial failure can never leave aggregate A debited while aggregate B's credit is lost, and the at-least-once redelivery of the source event re-drives a clean (not double-applied) reaction.

- On **PostgreSQL** every aggregate's events commit in one transaction, or none do.
- On the **in-memory** backend every buffered commit is checked under one pair of write locks before any of them are applied, so either all are applied or none are.

A custom `EventStore` that keeps `append_batch`'s looping default is not atomic: a mid-flush failure can leave earlier aggregates written.

### Introspection

```rust
let source = ctx.source_event();   // The event being reacted to
let depth = ctx.cascade_depth();    // Current depth in causation chain
```

## Causation Tracking

Every event produced by a policy carries metadata:

- **`causation_id`**: The ID of the event that directly caused this one
- **`correlation_id`**: A string identifying the whole chain: the incoming correlation id, or the root event ID (as a string) when there is none
- **`causation_chain`**: Full list of event IDs from root to direct parent

For example, in a cascade `A -> B -> C`:
- Event C has `causation_id = B.id`, `correlation_id = A.id`, `causation_chain = [A.id, B.id]`

A reaction also inherits the [event context](event-context.md) of its source
event. It runs in a root context seeded with the source event's tenant,
correlation id and correlation path, so a cascade stays in one tenant and one
flow without any code in the handler. The events it produces record the
policy's `name()` as their `command`.

## PolicyRunner

The `PolicyRunner` manages policy registration, event routing, and checkpoint-based resumption:

```rust
use event_sauce::PolicyRunner;

let runner = PolicyRunner::new(store, checkpoint_store)
    .with_max_cascade_depth(5)  // Default: 10
    // worker_id defaults to a value already unique per process and per
    // instance (pid + a UUID) — leave it unset unless you need lease
    // ownership to be recognizable across this runner being recreated.
    .with_lease_duration(Duration::from_secs(30))  // Default: 30s
    .with_batch_size(500)  // Default: 500
    .register(Arc::new(KickUserPolicy))
    .register(Arc::new(NotifyOnRemovalPolicy));

// One-shot: process all pending events
let outcome = runner.process_pending().await?;
let processed = outcome.processed(); // 0 also when outcome.is_busy()

// Single event: route to matching policies
let handled = runner.process_event(&envelope).await?;
```

### Checkpoint-Based Resumption

`PolicyRunner` uses a `CheckpointStore` to track which events each policy has already processed. This ensures:

- **Resumes where it left off**: a restart replays at most the batch in flight (at-least-once — keep handlers idempotent)
- **No duplicate cascades**: Events produced by policies are not re-processed on restart
- **New policy skip**: A newly registered policy (no checkpoint) starts from the current max position, skipping all historical events
- **Resume at the failing event on `Fail`**: If a handler fails mid-batch under `OnError::Fail`, each policy's checkpoint is persisted up to the **last event it fully handled** before the error — never past the failing event. A subsequent `process_pending()` resumes at the failing event and does **not** re-run the already-flushed handlers of earlier events in the batch.

> **At-least-once on the consume side.** The in-process `PolicyRunner` advances each policy's checkpoint *after* a fetched batch's handlers' buffered commits are flushed, in a separate write — fenced on the policy's lease, and saved after every batch (not just once at the end of the call), so a cancellation or a transient error replays at most the batch in flight when it happened, never the whole call. These are still not a single transaction with the flush, so a crash between a batch's last flush and its checkpoint save can re-deliver that batch's events on restart. Keep handlers idempotent. The Postgres outbox dispatcher (see [Two ways to dispatch](#two-ways-to-dispatch-in-process-runner-vs-queue)) adds per-event retry and a DLQ, but its delivery is **also** at-least-once — it does not make handlers exactly-once.

### Multi-Instance Safety

Every registered policy is its own lease, acquired through the configured
`CheckpointStore` (the same `try_acquire_lease`/`renew_lease`/`release_lease`
machinery the Postgres projection runners use). This makes
`process_pending()` **safe to call from more than one process or pod**: each
call acquires the lease of every policy it can, processes only those, renews
leases as it goes, and releases them before returning. A policy whose lease
is already held by another worker is simply left out of that call — its
events are picked up next time by whichever worker holds it, not duplicated.
If *every* registered policy's lease is held elsewhere, the call returns
`PolicyRunOutcome::Busy` rather than an empty `Completed(0)`, so a caller
or a metrics dashboard can tell a standby sitting out a held lease apart
from a node that is simply idle.

A per-policy lease does **not** stop two concurrent `process_pending()`
calls on the *same* `PolicyRunner` instance (same worker id) from both
succeeding — a worker can always renew its own lease. `process_pending()`
therefore also serializes internally: a second concurrent call on the same
instance waits for the first to finish rather than racing it. If you need
policies processed concurrently within one process, register **disjoint**
sets of policies on separate `PolicyRunner` instances (each with their own
`worker_id`, see below). Instances that register the *same* policies never
run the same policy at the same time: overlapping calls split those
policies' leases between them, and a call that gets none of them reports
`Busy`.

**A `worker_id` must be unique per running instance.** Two live runners that
share one `worker_id` (for example, a fixed string hard-coded across every
replica) are indistinguishable to `try_acquire_lease`: it treats a matching
`worker_id` as the *same* holder renewing its own lease, so every such
replica acquires every policy's lease at once and all of them run the same
reactions, with the fence accepting every one of their saves. Derive it from
something unique to the process — a hostname or pod name plus the process
id — or leave it at the generated default.

**Two known limits**, from how the lease is kept alive:

- A **single handler call** (one `Policy::handle` invocation, with no
  retry backoff in between to renew from) longer than roughly **two
  thirds of `lease_duration`** can lose the lease before the next renewal
  lands; size `lease_duration` well above your slowest expected handler
  call. This is the practical limit, because renewal itself is checked
  often: on every fetched event (matched or not), before every matching
  policy's handler call, and through every `OnError::Retry` backoff and
  before every retry attempt — in chunks no longer than the renewal
  interval (`lease_duration / 3`) — but the lease store only actually sees
  a write once that interval has elapsed since the last one. A held policy
  with no matching event in a long stretch the runner scans (e.g. a
  checkpoint reset for a replay) is covered by the same per-event check,
  not left to starve.
- A **cancelled `process_pending()`** (a timeout, a `select!`, a dropped
  task) does not run its cleanup: whatever leases it had acquired stay
  held under this worker until they simply expire, rather than being
  released immediately. Another worker only picks up those policies once
  `lease_duration` has elapsed.

### Cascade Depth Limits

Reactions can trigger further reactions. To prevent infinite loops, the runner enforces a maximum cascade depth (default: 10). When exceeded, `Error::CascadeDepthExceeded` is returned.

## Error Handling

`PolicyRunner` supports configurable error handling via `OnError`:

```rust
use event_sauce::{PolicyRunner, OnError, RetryConfig, RetryLimit, OnRetryExhausted};
use std::time::Duration;

// Fail immediately on error (default)
let runner = PolicyRunner::new(store, cp)
    .on_error(OnError::Fail);

// Skip failed events and continue processing
let runner = PolicyRunner::new(store, cp)
    .on_error(OnError::Skip);

// Retry with exponential backoff
let runner = PolicyRunner::new(store, cp)
    .on_error(OnError::Retry(RetryConfig {
        base_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(30),
        limit: RetryLimit::MaxRetries(3),
        on_exhausted: OnRetryExhausted::Fail,
    }));
```

### Error Strategies

| Strategy | Checkpoint | Processing |
|----------|-----------|------------|
| `OnError::Fail` | Advanced to last fully-handled event, **not** past the failing one | Stops with error; resumes at the failing event |
| `OnError::Skip` | Advanced **past** the failing event (event permanently skipped, never retried) | Continues |
| `Retry` → success | Advanced | Continues |
| `Retry` → exhausted + `Fail` | Advanced to last fully-handled event, not past the failing one | Stops with error; resumes at the failing event |
| `Retry` → exhausted + `Skip` | Advanced past the failing event | Continues |
| `Retry` → `Indefinite` | Blocks until success | Continues after success |

`OnError::Fail` and `OnError::Skip` differ in what a re-run does with the failing event: `Fail` stops and **resumes at it** on the next `process_pending()`; `Skip` advances past it and **never retries** it.

### RetryLimit Options

- **`MaxRetries(n)`**: Stop after `n` retry attempts
- **`MaxDuration(d)`**: Stop after total elapsed time exceeds `d`
- **`Indefinite`**: Keep retrying until success (blocks processing on this event)

### Retry Backoff

Exponential backoff: `delay = base_delay * 2^(attempt-1)`, capped at `max_delay`.

## Convenience Method

If your event store has a checkpoint store configured, use the convenience method:

```rust
let runner = store.policy_runner()?  // auto-wires checkpoint store
    .register(Arc::new(MyPolicy));
```

## Example

See [`examples/policy.rs`](../crates/event-sauce/examples/policy.rs) for a complete example with:
- Three aggregates (User, Group, Notification)
- Two policies forming a cascade chain
- Full causation tracking visible in the output

## Two ways to dispatch: in-process runner vs. queue

The default `PolicyRunner` runs in-process: it reads from the event log via
a checkpoint and calls each registered handler directly. That's the right
shape for **same-aggregate orchestration** — emitting follow-up events into
event-sauce itself, with strict cascade depth tracking and causation
propagation. It is *not* the right shape for fanning side-effects out to
many parallel workers: each policy has its own checkpoint and lease, but
they all run in the same runner instance, so a hung handler stalls every
policy that runner holds the lease for.

For side-effects (email, payment APIs, webhooks — anything where per-event
retry and a DLQ matter more than ordering) event-sauce provides a
queue-shaped alternative built on `FOR UPDATE SKIP LOCKED`:
**`PostgresPolicyOutbox`**.

> **Delivery is at-least-once — handlers MUST be idempotent.** Draining is
> three separate steps (`claim_batch` → run handler → `mark_done`), not one
> transaction. A crash or lease expiry *after* the side effect runs but
> *before* `mark_done` commits re-delivers the row, so the side effect runs
> again. The outbox is the right home for effects that are **safe to repeat**
> (idempotent API calls keyed by `event_id`, upserts, dedup-on-the-caller
> sends) — it does **not** make a non-idempotent effect safe. Dedupe on
> `claim.event_id`, or, for an effect that is a write to *this same Postgres
> DB*, fold it into the same transaction as the projection runner so it
> commits atomically with the triggering event. (A built-in transactional
> drain is future work.)

### When to pick which

| Concern | In-process `PolicyRunner` | Queue (`PostgresPolicyOutbox`) |
|---|---|---|
| Source of work | Event log directly | Outbox table populated from log |
| Ordering | Strict | Per-claim only (workers parallelize) |
| Per-event retry / DLQ | No (whole policy stalls on failure) | Yes (`status='failed'`, `failures` vs `max_attempts`, `last_error`) |
| Scale-out | Lease-safe across instances, but one policy is handled serially by whichever instance holds its lease | N parallel workers per policy |
| Replay | Reset checkpoint | Reset checkpoint + truncate outbox |
| Best for | Cascading domain logic | External side effects |

You can use both side by side: the same policy logic can be triggered by
the in-process runner *or* drained from the outbox, depending on which
fits the job.

### Outbox in three pieces

```text
                ┌──────────────┐
   commit ──▶   │  events      │  (source of truth, never mutated)
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐    Any instance may call dispatch:
                │ checkpoint:  │    fetches events in batches, applies each
                │  __policy_   │    registered policy's filter, INSERTs
                │  outbox_     │    matching rows into policy_outbox inside
                │  dispatcher: │    the same tx as its checkpoint advance.
                │  {policy}    │    Each policy's checkpoint and lease are
                │  (one per    │    its own — one instance dispatching
                │   policy)    │    policy A does not block another
                └──────┬───────┘    instance dispatching policy B.
                       │
                       ▼
                ┌──────────────┐    Worker processes (1..N per policy):
                │ policy_outbox│    SELECT … FOR UPDATE SKIP LOCKED LIMIT $batch
                │              │    → run handler → mark_done (or mark_failed)
                └──────────────┘
```

### Setting up the outbox

```rust
use event_sauce::postgres::{BackoffPolicy, PolicyDispatch, PostgresPolicyOutbox};
use event_sauce::EventFilter;
use std::time::Duration;

// Migrate the outbox table once at startup.
let outbox = PostgresPolicyOutbox::new(backend.pool().clone(), "event_sauce");
outbox.migrate().await?;

// Register the policies the dispatcher should fan out for.
let policies = vec![
    PolicyDispatch::new(
        "send-order-confirmation",
        EventFilter::by_event_type("Order.Created"),
    ),
    PolicyDispatch::new(
        "notify-on-cancel",
        EventFilter::by_event_type("Order.Cancelled"),
    ),
];
```

### Dispatching

Every instance may call this — each policy is dispatched by whichever
instance holds its lease, independently of the others. A `Busy` result
means none of `policies` could be held right now, not that dispatch
failed. It reads new events from the log and inserts outbox rows for each
registered policy whose filter matches:

```rust
backend
    .dispatch_policies_to_outbox(
        &outbox,
        "dispatcher-1",
        &policies,
        Duration::from_secs(30),
    )
    .await?;
```

Run it on a tick or wake on `listen_for_events()` for low-latency dispatch.

### Draining (N parallel workers per policy)

```rust
loop {
    let claims = outbox
        .claim_batch(
            "send-order-confirmation",
            "drainer-1",
            32,
            Duration::from_secs(60),
        )
        .await?;

    if claims.is_empty() {
        tokio::time::sleep(Duration::from_millis(500)).await;
        continue;
    }

    for claim in claims {
        // `AckOutcome::Fenced` means another worker's lease already
        // reclaimed this row — this worker's ack lost the race and must
        // not be treated as ours.
        match send_email_for(claim.event_id).await {
            Ok(_) => {
                let _ = outbox.mark_done(claim.id, "drainer-1").await?;
            }
            Err(e) => {
                let _ = outbox
                    .mark_failed(
                        claim.id,
                        "drainer-1",
                        &e.to_string(),
                        Some(5),
                        BackoffPolicy::default(),
                    )
                    .await?;
            }
        }
    }
}
```

`claim_batch` uses `FOR UPDATE SKIP LOCKED` so multiple drainer workers
never see the same row in the same claim. The DLQ keys off the row's
`failures` counter — the number of real handler errors reported via
`mark_failed` — **not** `attempts`, which counts claims (including
lease-expiry reclaims of a crashed worker). So a worker that claims, runs
the side effect, then crashes before `mark_done` bumps `attempts` on the
next reclaim but never `failures`; it can never push the row to the DLQ
without the handler actually failing. Once `failures` reaches
`max_attempts` the row lands in `status='failed'` for ops to inspect; reset
it to `'pending'` to retry.
