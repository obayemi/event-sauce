# Getting Started with event-sauce

This guide will walk you through creating your first event-sourced application with event-sauce.

## Table of Contents

1. [Installation](#installation)
2. [Your First Aggregate](#your-first-aggregate)
3. [Events and Commands](#events-and-commands)
4. [Storing and Loading](#storing-and-loading)
5. [Projections](#projections)
6. [Testing](#testing)
7. [Next Steps](#next-steps)

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

// Auto-implements AggregateId trait and Display
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CounterId(Uuid);

impl CounterId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for CounterId {
    fn default() -> Self {
        Self(Uuid::nil())
    }
}
```

### Step 2: Define Events

Events represent things that have happened in your domain:

```rust
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

// Individual event structs (recommended pattern)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incremented {
    amount: i32,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decremented {
    amount: i32,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reset {
    timestamp: DateTime<Utc>,
}

// Wrap in enum (auto-generates apply_event method)
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Counter", aggregate = "CounterAggregate")]
pub enum CounterEvent {
    Incremented(Incremented),
    Decremented(Decremented),
    Reset(Reset),
}
```

The `#[derive(Event)]` macro automatically implements the `DomainEvent` trait.

### Step 3: Define the Aggregate State

```rust
use event_sauce_macros::AggregateState;

// Define the STATE - contains only business data
#[derive(AggregateState, Debug, Clone, Default)]
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
pub struct CounterState {
    #[aggregate_id]
    id: CounterId,
    value: i32,
}

// CounterAggregate is auto-generated!
// It wraps CounterState and adds: version, pending_events
```

The `#[derive(AggregateState)]` macro generates a `CounterAggregate` wrapper that:
- Implements the `Aggregate` trait
- Manages version and pending_events automatically
- Provides transparent field access via Deref
- Reduces boilerplate by ~40%

### Step 4: Implement ApplyEvent for Each Event

```rust
use event_sauce_core::ApplyEvent;

// Implement event validation and application logic
impl ApplyEvent<CounterAggregate, CounterError> for Incremented {
    fn validate(&self, _counter: &CounterAggregate) -> Result<(), CounterError> {
        if self.amount <= 0 {
            return Err(CounterError::InvalidAmount(self.amount));
        }
        Ok(())
    }

    fn apply(&self, counter: &mut CounterAggregate) {
        counter.value += self.amount;  // Deref allows direct access
    }
}

impl ApplyEvent<CounterAggregate, CounterError> for Decremented {
    fn validate(&self, counter: &CounterAggregate) -> Result<(), CounterError> {
        if self.amount <= 0 {
            return Err(CounterError::InvalidAmount(self.amount));
        }
        if counter.value < self.amount {
            return Err(CounterError::WouldBeNegative {
                current: counter.value,
                requested: self.amount,
            });
        }
        Ok(())
    }

    fn apply(&self, counter: &mut CounterAggregate) {
        counter.value -= self.amount;
    }
}

impl ApplyEvent<CounterAggregate, CounterError> for Reset {
    fn apply(&self, counter: &mut CounterAggregate) {
        counter.value = 0;
    }
}
```

### Step 5: Implement Business Logic

Commands are implemented on the generated `CounterAggregate`:

```rust
impl CounterAggregate {
    /// Create a new counter
    pub fn create(id: CounterId) -> Self {
        Self::from_state(CounterState { id, value: 0 })
    }

    /// Increment the counter
    pub fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = Incremented {
            amount,
            timestamp: Utc::now(),
        };
        event.validate(self)?;  // Validate
        self.apply(event);      // Apply & record
        Ok(())
    }

    /// Decrement the counter
    pub fn decrement(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = Decremented {
            amount,
            timestamp: Utc::now(),
        };
        event.validate(self)?;
        self.apply(event);
        Ok(())
    }

    /// Reset the counter to zero
    pub fn reset(&mut self) {
        let event = Reset {
            timestamp: Utc::now(),
        };
        self.apply(event);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CounterError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i32),

    #[error("Would result in negative value: current={current}, requested={requested}")]
    WouldBeNegative { current: i32, requested: i32 },
}

impl AggregateError for CounterError {}
```

**Note**: The `apply_event` method is auto-generated by the `#[event(aggregate = "CounterAggregate")]` attribute. You don't need to implement it manually!

## Storing and Loading

### Using the In-Memory Store

Perfect for testing and development:

```rust
use event_sauce::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an in-memory event store
    let store = MemoryEventStore::new();

    // Create a counter
    let id = CounterId::new();
    let mut counter = CounterAggregate::create(id);

    // Execute commands
    counter.increment(5)?;
    counter.increment(3)?;
    counter.decrement(2)?;

    // Save to store
    save_aggregate(&store, &counter).await?;

    // Load from store
    let loaded: CounterAggregate = load_aggregate(&store, id).await?;
    println!("Counter value: {}", loaded.value);

    Ok(())
}
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

## Projections

Projections provide read models by subscribing to events:

```rust
use event_sauce_projections::Projection;

#[derive(Debug, Default)]
pub struct CounterStats {
    total_increments: u64,
    total_decrements: u64,
    current_value: i32,
}

#[async_trait::async_trait]
impl Projection for CounterStats {
    type Error = anyhow::Error;

    async fn handle_event(&mut self, envelope: &EventEnvelope) -> Result<(), Self::Error> {
        match envelope.event_type.as_str() {
            "Counter.Incremented" => {
                self.total_increments += 1;
                // Parse event data and update current_value
            }
            "Counter.Decremented" => {
                self.total_decrements += 1;
                // Parse event data and update current_value
            }
            "Counter.Reset" => {
                self.current_value = 0;
            }
            _ => {}
        }
        Ok(())
    }
}
```

## Testing

Event sourcing makes testing straightforward:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_increment() {
        let mut counter = CounterAggregate::create(CounterId::new());

        counter.increment(5).unwrap();
        assert_eq!(counter.value, 5);
        assert_eq!(counter.version(), 1);
        assert_eq!(counter.pending_events().len(), 1);
    }

    #[test]
    fn test_counter_prevents_negative() {
        let mut counter = CounterAggregate::create(CounterId::new());

        counter.increment(5).unwrap();
        let result = counter.decrement(10);

        assert!(result.is_err());
        assert_eq!(counter.value, 5); // State unchanged
    }

    #[test]
    fn test_event_replay() {
        let mut counter = CounterAggregate::create(CounterId::new());

        // Execute commands
        counter.increment(10).unwrap();
        counter.decrement(3).unwrap();
        counter.increment(5).unwrap();

        // Replay events to reconstruct state
        let events = counter.pending_events().clone();
        let id = counter.aggregate_id();
        let mut replayed = CounterAggregate::create(id);

        for event in events {
            replayed.apply_unchecked(&event);  // Fast replay without validation
        }

        assert_eq!(replayed.value, counter.value);
        assert_eq!(replayed.version(), counter.version());
    }
}
```

## Next Steps

Now that you understand the basics, explore:

1. **[Architecture](architecture.md)** - Learn about the overall design
2. **[TDD Workflow](tdd-workflow.md)** - Follow test-driven development
3. **[PostgreSQL Backend](postgres.md)** - Use a production-ready store
4. **[Projections Guide](projections.md)** - Build read models
5. **[Examples](../examples/)** - See complete applications

## Common Patterns

### Command Handler Pattern

```rust
pub async fn handle_increment_command(
    store: &impl EventStore,
    id: CounterId,
    amount: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load aggregate
    let mut counter = load_aggregate(store, id).await?;

    // Execute command
    counter.increment(amount)?;

    // Save events
    save_aggregate(store, &counter).await?;

    Ok(())
}
```

### Repository Pattern

```rust
pub struct CounterRepository<S> {
    store: S,
}

impl<S: EventStore> CounterRepository<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn save(&self, counter: &Counter) -> Result<(), S::Error> {
        save_aggregate(&self.store, counter).await
    }

    pub async fn load(&self, id: CounterId) -> Result<Counter, S::Error> {
        load_aggregate(&self.store, id).await
    }
}
```

## Tips

1. **Keep aggregates small** - They should represent a single consistency boundary
2. **Commands can fail** - Return `Result` types and validate before creating events
3. **Events never fail** - They represent facts that have happened
4. **Test with events** - Given events, when command, then new events
5. **Use projections for queries** - Never query aggregates directly

## Need Help?

- Check the [examples](../examples/) directory
- Read the [architecture docs](architecture.md)
- See the [API documentation](https://docs.rs/event-sauce)
- Open an issue on GitHub
