# Getting Started with event-sauce

This guide will walk you through creating your first event-sourced application with event-sauce.

## Quick Overview

Here's a complete counter aggregate in ~30 lines:

```rust
use event_sauce::prelude::*;
use event_sauce_macros::{AggregateError, AggregateId};
use thiserror::Error;
use uuid::Uuid;

// 1. Define ID and errors
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
struct CounterId(Uuid);

#[derive(AggregateError, Debug, Error)]
enum CounterError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i32),
}

// 2. Define aggregate
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
#[derive(Default)]
struct Counter {
    value: i32,
}

// 3. Define events with validation
define_events! {
    pub enum CounterEvent for Counter {
        Incremented { amount: i32 }
        @validate {
            if self.amount <= 0 {
                return Err(CounterError::InvalidAmount(self.amount));
            }
            return Ok(());
        }
        => |counter, event| {
            counter.state.value += event.amount;
        },

        Decremented { amount: i32 }
        => |counter, event| {
            counter.state.value -= event.amount;
        },
    }
}

// 4. Implement commands
impl Counter {
    fn create() -> Self {
        Self::new(CounterId(Uuid::new_v4()))
    }
}

command_handler! {
    impl Counter {
        fn increment(amount: i32) -> IncrementedEvent { amount };
        fn decrement(amount: i32) -> DecrementedEvent { amount };
    }
}
```

That's it! You now have a fully functional event-sourced aggregate with validation, event replay, and type safety.

## Complete Examples

Want to see full working examples? Check out:

- **[postgres-quickstart.rs](../crates/event-sauce/examples/postgres-quickstart.rs)** - Demonstrates User and Order aggregates with projections, PostgreSQL backend, and the subscription system. Uses `define_events!` macro (recommended for most use cases).
  ```bash
  cargo run --example postgres-quickstart --all-features
  ```

- **[apply-event.rs](../crates/event-sauce/examples/apply-event.rs)** - Shows manual event implementation using the `ApplyEvent` trait for fine-grained control. Includes complex validation logic with a bank account example.
  ```bash
  cargo run --example apply-event --all-features
  ```

## Table of Contents

1. [Installation](#installation)
2. [Your First Aggregate](#your-first-aggregate)
3. [Storing and Loading](#storing-and-loading)
4. [Building Projections](#building-projections-read-models)
5. [Testing](#testing)
6. [Next Steps](#next-steps)

## Installation

Add event-sauce to your `Cargo.toml`:

```toml
[dependencies]
event-sauce = { version = "0.1", features = ["macros", "memory"] }
tokio = { version = "1.48", features = ["full"] }
uuid = { version = "1.18", features = ["v4", "serde"] }
serde = { version = "1.0", features = ["derive"] }
```

Or use the CLI to scaffold a new project:

```bash
cargo install event-sauce-cli
event-sauce init my-app --backend memory
cd my-app
```

## Your First Aggregate

Let's create a simple counter aggregate. An **aggregate** is a consistency boundary in your domain that processes commands and produces events.

### Step 1: Define the Aggregate ID

```rust
use event_sauce::prelude::*;
use event_sauce_macros::AggregateId;
use uuid::Uuid;

#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
struct CounterId(Uuid);
```

The `#[derive(AggregateId)]` macro automatically implements the `AggregateId` trait and `Display`. Use `CounterId(Uuid::new_v4())` when you need a new ID.

### Step 2: Define Domain Errors

```rust
use event_sauce_macros::AggregateError;
use thiserror::Error;

#[derive(AggregateError, Debug, Error)]
enum CounterError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i32),
}
```

### Step 3: Define the Aggregate

```rust
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
#[derive(Default)]
struct Counter {
    value: i32,
}
```

The `#[aggregate(...)]` macro generates everything you need:
- Infrastructure wrapper with `id`, `version`, and `pending_events`
- Implements the `Aggregate` trait
- Provides transparent field access

### Step 4: Define Events with Validation

```rust
use event_sauce::define_events;

define_events! {
    pub enum CounterEvent for Counter {
        Incremented {
            amount: i32,
        }
        @validate {
            if self.amount <= 0 {
                return Err(CounterError::InvalidAmount(self.amount));
            }
            return Ok(());
        }
        => |counter, event| {
            counter.state.value += event.amount;
        },

        Decremented {
            amount: i32,
        }
        => |counter, event| {
            counter.state.value -= event.amount;
        },
    }
}
```

**What this generates:**
- Individual event structs (`IncrementedEvent`, `DecrementedEvent`) with automatic `timestamp` fields
- Event enum `CounterEvent` with all variants
- `ApplyEvent` trait implementations with inline validation and apply logic
- Proper event type names and versioning

**Benefits:** 60% less boilerplate, inline validation, type-safe apply logic.

### Step 5: Implement Business Logic

```rust
use event_sauce::command_handler;

impl Counter {
    fn create() -> Self {
        Self::new(CounterId(Uuid::new_v4()))
    }
}

command_handler! {
    impl Counter {
        fn increment(amount: i32) -> IncrementedEvent { amount };
        fn decrement(amount: i32) -> DecrementedEvent { amount };
    }
}
```

That's it! Your aggregate is ready with full event sourcing capabilities:
- ✅ **70% less boilerplate** - Automatic event creation and timestamp handling
- ✅ **Type-safe** - Compile-time validation
- ✅ **Inline validation** - Business rules enforced during command execution

## Storing and Loading

### Using the In-Memory Store

Perfect for testing and development:

```rust
use event_sauce::prelude::*;
use event_sauce_memory::InMemoryEventStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Using builder pattern (recommended)
    let store = InMemoryEventStore::builder()
        .build();

    // Or use the simple constructor
    // let store = InMemoryEventStore::new();

    // Create and use the counter
    let mut counter = Counter::create();
    counter.increment(5)?;
    counter.increment(3)?;
    counter.decrement(2)?;

    // Save to store
    let id = counter.id();
    save_aggregate(&store, &counter).await?;

    // Load from store
    let loaded: Counter = load_aggregate(&store, id).await?;
    println!("Counter value: {}", loaded.value);

    Ok(())
}
```

### Custom Configuration with Builder

For more control over store behavior:

```rust
use event_sauce_memory::{InMemoryEventStore, InMemoryCheckpointStore};
use event_sauce_core::SnapshotConfig;
use std::sync::Arc;

// Configure with custom snapshot settings
let store = InMemoryEventStore::builder()
    .snapshot_config(SnapshotConfig::builder()
        .default_strategy(event_sauce_core::EveryNEvents(50))
        .build())
    .build();

// Or with checkpoint store for subscriptions
let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
let store = InMemoryEventStore::builder()
    .snapshot_config(SnapshotConfig::builder().build())
    .checkpoint_store(checkpoint_store)
    .build();
```

### Helper Functions

```rust
async fn save_aggregate<A, S>(
    store: &S,
    aggregate: &A,
) -> Result<(), S::Error>
where
    A: Aggregate,
    S: EventStore,
{
    let stream_id = format!("{}-{}", A::TYPE_NAME, aggregate.id());

    store.append(
        &stream_id,
        A::TYPE_NAME,
        aggregate.version(),
        aggregate.pending_events()
            .iter()
            .map(|e| e.to_envelope(&stream_id))
            .collect::<Result<Vec<_>, _>>()?,
    ).await?;

    Ok(())
}

async fn load_aggregate<A, S>(
    store: &S,
    id: A::Id,
) -> Result<A, S::Error>
where
    A: Aggregate,
    S: EventStore,
{
    let stream_id = format!("{}-{}", A::TYPE_NAME, id);
    let mut stream = store.load_stream(&stream_id, 0).await?;

    let mut aggregate = A::default();

    while let Some(envelope) = stream.next().await {
        let envelope = envelope?;
        let event = A::Event::from_envelope(&envelope)?;
        aggregate.apply(&event);
    }

    Ok(aggregate)
}
```

## Building Projections (Read Models)

Build type-safe read models with the `projection!` macro:

```rust
use event_sauce::projection;
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct CounterView {
    value: i32,
    total_operations: u64,
}

projection! {
    pub struct CounterListProjection {
        state: HashMap<CounterId, CounterView>,

        on "Counter.Incremented" => IncrementedEvent |proj, event| {
            proj.state.entry(event.aggregate_id)
                .and_modify(|v| {
                    v.value += event.amount;
                    v.total_operations += 1;
                })
                .or_insert(CounterView {
                    value: event.amount,
                    total_operations: 1,
                });
        },

        on "Counter.Decremented" => DecrementedEvent |proj, event| {
            if let Some(counter) = proj.state.get_mut(&event.aggregate_id) {
                counter.value -= event.amount;
                counter.total_operations += 1;
            }
        },
    }
}

// Use with subscriptions for automatic updates
let mut projection = CounterListProjection::new(HashMap::new());
let subscription = event_store
    .subscription_builder("counter-projection")
    .build()?;

let stream = subscription.into_stream().await?;
tokio::pin!(stream);

while let Some(result) = stream.next().await {
    let envelope = result?;
    projection.handle(&envelope).await?;
}
```

**Benefits:**
- ✅ Declarative event-to-handler mapping
- ✅ Type-safe with automatic deserialization
- ✅ Unknown events safely ignored

## Testing

Event sourcing makes testing straightforward:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_increment() {
        let mut counter = Counter::create();
        counter.increment(5).unwrap();

        assert_eq!(counter.value, 5);
        assert_eq!(counter.version(), 1);
    }

    #[test]
    fn test_invalid_amount_rejected() {
        let mut counter = Counter::create();
        let result = counter.increment(-5);

        assert!(result.is_err());
        assert_eq!(counter.value, 0); // State unchanged
    }

    #[test]
    fn test_event_replay() {
        let mut counter = Counter::create();
        counter.increment(10).unwrap();
        counter.decrement(3).unwrap();

        // Events can be replayed to reconstruct state
        let events = counter.pending_events().clone();
        let mut replayed = Counter::create();

        for event in events {
            replayed.apply_unchecked(&event);
        }

        assert_eq!(replayed.value, counter.value);
    }
}
```

## Next Steps

Now that you understand the basics, explore:

1. **[Architecture](architecture.md)** - Learn about the overall design
2. **[TDD Workflow](tdd-workflow.md)** - Follow test-driven development
3. **[PostgreSQL Backend](postgres.md)** - Use a production-ready store
4. **[Projections Guide](projections.md)** - Build read models
5. **Complete Examples** - See real-world applications:
   - **[postgres-quickstart.rs](../crates/event-sauce/examples/postgres-quickstart.rs)** - User and Order aggregates with projections using `define_events!` macro (recommended approach)
   - **[apply-event.rs](../crates/event-sauce/examples/apply-event.rs)** - Bank account with manual `ApplyEvent` trait implementation (for complex validation scenarios)

## Common Patterns

### Command Handler Pattern

```rust
async fn handle_increment_command(
    store: &impl EventStore,
    id: CounterId,
    amount: i32,
) -> Result<()> {
    let mut counter = load_aggregate(store, id).await?;
    counter.increment(amount)?;
    save_aggregate(store, &counter).await?;
    Ok(())
}
```

### Using the Repository Pattern

For cleaner code, use the built-in `Repository` type:

```rust
use event_sauce::Repository;

let repo = Repository::new(store);

// Save and load aggregates
let mut counter = Counter::create();
counter.increment(5)?;
repo.save(&counter).await?;

let loaded = repo.load::<Counter>(counter.id()).await?;
```

## Tips

1. **Keep aggregates small** - They should represent a single consistency boundary
2. **Commands can fail** - Return `Result` types and validate before creating events
3. **Events never fail** - They represent facts that have happened
4. **Test with events** - Given events, when command, then new events
5. **Use projections for queries** - Never query aggregates directly

## Need Help?

- **Run the examples** to see complete working applications:
  ```bash
  cargo run --example postgres-quickstart --all-features  # Full-featured example
  cargo run --example apply-event --all-features          # Manual approach
  ```
- Check the [examples directory](../crates/event-sauce/examples/) for more
- Read the [architecture docs](architecture.md)
- See the [API documentation](https://docs.rs/event-sauce)
- Open an issue on GitHub
