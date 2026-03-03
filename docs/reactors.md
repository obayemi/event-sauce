# Reactors - Cross-Aggregate Event Orchestration

Reactors handle events by issuing commands on other aggregates. When one aggregate produces an event that should trigger effects on other aggregates, reactors provide the orchestration.

## Overview

In an event-sourced system, a single command may produce events that should trigger effects elsewhere. For example:

```text
User.Kicked event
    |
    v
KickUserReactor -------> Group.MemberRemoved event
                              |
                              v
                      NotifyOnRemovalReactor -------> Notification.Sent event
```

Reactors are **async/eventually consistent** -- they process events outside the original transaction. Every event produced by a reactor carries full causation metadata, enabling complete traceability.

## Defining Reactors

### Using the `reactor!` macro

The `reactor!` macro is the recommended way to define simple reactors:

```rust
use event_sauce::{reactor, EntityId, AggregateRoot};

reactor! {
    /// Removes kicked users from their groups.
    KickUserReactor {
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
- A **handler closure** receiving the deserialized event and a `ReactorContext`

When using `define_events!`, each variant `Foo` automatically generates a `FooEvent` struct with the required traits.

### Using the `Reactor` trait directly

For more complex scenarios, implement the trait manually:

```rust
use event_sauce::{EventStore, EventEnvelope, EventFilter, ReactorContext, Result};

struct ComplexReactor;

#[async_trait::async_trait]
impl<S: EventStore + 'static> Reactor<S> for ComplexReactor {
    fn name(&self) -> &str { "ComplexReactor" }

    fn event_filter(&self) -> EventFilter {
        EventFilter::any_of_event_types(["User.Kicked", "User.Banned"])
    }

    async fn handle(&self, event: &EventEnvelope, ctx: &ReactorContext<S>) -> Result<()> {
        match event.event_type.as_str() {
            "User.Kicked" => { /* handle kick */ },
            "User.Banned" => { /* handle ban */ },
            _ => {},
        }
        Ok(())
    }
}
```

## ReactorContext

The `ReactorContext` provides event store access with **automatic causation tracking**. All interactions go through the context to guarantee metadata is always set.

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

Every event produced by a reactor carries metadata:

- **`causation_id`**: The ID of the event that directly caused this one
- **`correlation_id`**: The ID of the root event that started the chain
- **`causation_chain`**: Full list of event IDs from root to direct parent

For example, in a cascade `A -> B -> C`:
- Event C has `causation_id = B.id`, `correlation_id = A.id`, `causation_chain = [A.id, B.id]`

## ReactorRunner

The `ReactorRunner` manages reactor registration and event routing:

```rust
use event_sauce::ReactorRunner;

let runner = ReactorRunner::new(store)
    .with_max_cascade_depth(5)  // Default: 10
    .register(Arc::new(KickUserReactor))
    .register(Arc::new(NotifyOnRemovalReactor));

// One-shot: process all pending events (useful for testing)
let processed = runner.process_pending().await?;

// Single event: route to matching reactors
let handled = runner.process_event(&envelope).await?;
```

### Cascade Depth Limits

Reactions can trigger further reactions. To prevent infinite loops, the runner enforces a maximum cascade depth (default: 10). When exceeded, `Error::CascadeDepthExceeded` is returned.

## Example

See [`examples/reactor.rs`](../crates/event-sauce/examples/reactor.rs) for a complete example with:
- Three aggregates (User, Group, Notification)
- Two reactors forming a cascade chain
- Full causation tracking visible in the output
