# Policies - Cross-Aggregate Event Orchestration

Policies handle events by issuing commands on other aggregates. When one aggregate produces an event that should trigger effects on other aggregates, policies provide the orchestration.

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

- On **PostgreSQL** the whole reaction is **atomic**: every aggregate's events commit in one transaction, or none do. A `ConcurrencyConflict` (or claim conflict) on any aggregate rolls back the entire reaction — so a partial failure can never leave aggregate A debited while aggregate B's credit is lost, and the at-least-once redelivery of the source event re-drives a clean (not double-applied) reaction.
- On the **in-memory** backend the buffered commits are applied per-stream (not transactional), so a mid-flush failure can leave earlier aggregates written. Use the PostgreSQL backend when multi-aggregate reactions must be atomic.

### Introspection

```rust
let source = ctx.source_event();   // The event being reacted to
let depth = ctx.cascade_depth();    // Current depth in causation chain
```

## Causation Tracking

Every event produced by a policy carries metadata:

- **`causation_id`**: The ID of the event that directly caused this one
- **`correlation_id`**: The ID of the root event that started the chain
- **`causation_chain`**: Full list of event IDs from root to direct parent

For example, in a cascade `A -> B -> C`:
- Event C has `causation_id = B.id`, `correlation_id = A.id`, `causation_chain = [A.id, B.id]`

## PolicyRunner

The `PolicyRunner` manages policy registration, event routing, and checkpoint-based resumption:

```rust
use event_sauce::PolicyRunner;

let runner = PolicyRunner::new(store, checkpoint_store)
    .with_max_cascade_depth(5)  // Default: 10
    .register(Arc::new(KickUserPolicy))
    .register(Arc::new(NotifyOnRemovalPolicy));

// One-shot: process all pending events (useful for testing)
let processed = runner.process_pending().await?;

// Single event: route to matching policies
let handled = runner.process_event(&envelope).await?;
```

### Checkpoint-Based Resumption

`PolicyRunner` uses a `CheckpointStore` to track which events each policy has already processed. This ensures:

- **No duplicate processing**: Restarting `process_pending()` picks up where it left off
- **No duplicate cascades**: Events produced by policies are not re-processed on restart
- **New policy skip**: A newly registered policy (no checkpoint) starts from the current max position, skipping all historical events
- **Resume at the failing event on `Fail`**: If a handler fails mid-batch under `OnError::Fail`, each policy's checkpoint is persisted up to the **last event it fully handled** before the error — never past the failing event. A subsequent `process_pending()` resumes at the failing event and does **not** re-run the already-flushed handlers of earlier events in the batch.

> **At-least-once on the consume side.** The in-process `PolicyRunner` advances the per-policy checkpoint *after* a handler's buffered commits are flushed, in a separate write. These are not a single transaction, so a crash between the flush and the checkpoint save can re-deliver the last handled event on restart. Keep handlers idempotent. The Postgres outbox dispatcher (see [Two ways to dispatch](#two-ways-to-dispatch-in-process-runner-vs-queue)) adds per-event retry and a DLQ, but its delivery is **also** at-least-once — it does not make handlers exactly-once.

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
many parallel workers, because every policy shares one checkpoint and one
process; a hung handler stalls everyone behind it.

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
| Scale-out | One active runner per process | N parallel workers per policy |
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
                ┌──────────────┐    Dispatcher process:
                │ checkpoint:  │    leased; reads log via stream_all,
                │  __policy_   │    applies each registered policy filter,
                │  outbox_     │    INSERTs matching rows into policy_outbox
                │  dispatcher  │    inside the same tx as the checkpoint advance.
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐    Worker processes (1..N per policy):
                │ policy_outbox│    SELECT … FOR UPDATE SKIP LOCKED LIMIT $batch
                │              │    → run handler → mark_done (or mark_failed)
                └──────────────┘
```

### Setting up the outbox

```rust
use event_sauce::postgres::{PolicyDispatch, PostgresPolicyOutbox};
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

### Dispatching (one process)

Run this in **one** worker — the lease ensures it. It reads new events from
the log and inserts outbox rows for each registered policy whose filter
matches:

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
