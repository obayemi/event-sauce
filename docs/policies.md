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
| `OnError::Fail` | NOT advanced | Stops with error |
| `OnError::Skip` | Advanced | Continues |
| `Retry` → success | Advanced | Continues |
| `Retry` → exhausted + `Fail` | NOT advanced | Stops with error |
| `Retry` → exhausted + `Skip` | Advanced | Continues |
| `Retry` → `Indefinite` | Blocks until success | Continues after success |

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
