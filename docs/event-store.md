# Event Store

The Event Store is the persistence layer in event-sauce that stores and retrieves event streams for aggregates. This guide covers the event store API, available backends, and best practices.

## Table of Contents

1. [Overview](#overview)
2. [Core Concepts](#core-concepts)
3. [API Reference](#api-reference)
4. [Available Backends](#available-backends)
5. [Usage Patterns](#usage-patterns)
6. [Best Practices](#best-practices)
7. [Migration Guide](#migration-guide)
8. [Advanced Topics](#advanced-topics)

## Overview

Event stores in event-sauce provide:

- **Persistent event storage** - Events are the source of truth
- **Aggregate reconstruction** - Rebuild aggregate state from events
- **Optimistic concurrency control** - Version-based conflict detection
- **Stream processing** - Subscribe to and process event streams
- **Backend flexibility** - In-memory, PostgreSQL, and extensible

### Key Design Principles

1. **Simple API** - Two main operations: `commit()` and `load()`
2. **Type-safe** - Full Rust type safety with generics
3. **Async-first** - Built on `tokio` for concurrent operations
4. **Backend-agnostic** - Switch backends without changing code

## Core Concepts

### Events vs Event Envelopes

**Events** are your domain objects:

```rust
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
pub enum CounterEvent {
    Incremented { amount: i32 },
    Decremented { amount: i32 },
}
```

**Event Envelopes** wrap events with metadata for storage:

```rust
pub struct EventEnvelope {
    pub id: Uuid,              // Unique event ID
    pub aggregate_id: Uuid,    // Which aggregate
    pub aggregate_type: String,// Type of aggregate
    pub event_type: String,    // Type of event
    pub event_version: Version,// Aggregate version
    pub event_data: Value,     // JSON-serialized event
    pub occurred_at: DateTime<Utc>,  // Timestamp
}
```

### Stream IDs

Internally, the event store uses `StreamId` to identify event streams:

```rust
pub struct StreamId {
    aggregate_type: String,
    aggregate_id: Uuid,
}
```

However, you typically don't interact with `StreamId` directly. The `commit()` and `load()` methods handle this for you.

### Versioning

Each event has a version number that increments with each event:

- Version 0: Initial (no events)
- Version 1: After first event
- Version 2: After second event
- And so on...

Versions enable **optimistic concurrency control** - if two operations try to modify the same aggregate simultaneously, one will fail with a concurrency conflict.

## API Reference

### EventStore Trait

The core trait that all backends implement:

```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append events to a stream
    async fn append_events(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<()>;

    /// Load events from a stream
    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: Version,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    /// Commit pending events from an aggregate (convenience method)
    async fn commit<A>(&self, aggregate: &mut A) -> Result<()>
    where
        A: Aggregate,
        A::Event: serde::Serialize;
}
```

### commit() - Save Pending Events

The `commit()` method is the primary way to save events:

```rust
async fn commit<A>(&self, aggregate: &mut A) -> Result<()>
where
    A: Aggregate,
    A::Event: serde::Serialize;
```

**What it does:**

1. Takes pending events from the aggregate
2. Converts them to event envelopes
3. Appends them to the event store
4. Clears pending events on success

**Example:**

```rust
let store = InMemoryEventStore::new();
let mut counter = CounterAggregate::create(CounterId::new());

// Execute commands (generates events)
counter.increment(5)?;
counter.decrement(2)?;

// Save all pending events
store.commit(&mut counter).await?;

// Pending events are now cleared
assert_eq!(counter.pending_events().len(), 0);
```

**Error Handling:**

```rust
match store.commit(&mut counter).await {
    Ok(()) => println!("Events saved successfully"),
    Err(Error::ConcurrencyConflict { .. }) => {
        // Another operation modified this aggregate
        println!("Conflict detected - retry or merge");
    }
    Err(e) => println!("Storage error: {}", e),
}
```

### load() - Reconstruct Aggregate

The `load()` function reconstructs an aggregate from stored events:

```rust
pub async fn load<S, A>(store: &S, aggregate_id: A::Id) -> Result<A>
where
    S: EventStore,
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned;
```

> **Note:** Due to Rust's async trait limitations with generic return types, `load()` is a standalone function rather than a trait method.

**What it does:**

1. Creates a new aggregate with `Aggregate::new(id)`
2. Loads all events for that aggregate
3. Replays events using `apply_unchecked()` (no validation)
4. Returns the reconstructed aggregate

**Example:**

```rust
use event_sauce_core::load;

let store = InMemoryEventStore::new();
let counter_id = CounterId::new();

// Later, load the aggregate
let counter: CounterAggregate = load(&store, counter_id).await?;

println!("Loaded counter value: {}", counter.value());
println!("Loaded version: {}", counter.version());
```

**Why apply_unchecked()?**

During event replay, we use `apply_unchecked()` instead of `apply()`:

- Events already passed validation when first applied
- Replaying is about reconstruction, not validation
- Much faster (no validation overhead)
- Events are the source of truth

## Available Backends

### In-Memory Store

Perfect for testing, prototyping, and examples:

```rust
use event_sauce_memory::InMemoryEventStore;

let store = InMemoryEventStore::new();
```

**Features:**
- No external dependencies
- Fast (all in RAM)
- Events lost on restart
- No concurrency guarantees across processes

**When to use:**
- Unit tests
- Integration tests
- Local development
- Prototyping
- Examples and tutorials

### PostgreSQL Store

Production-ready with full ACID guarantees:

```rust
use event_sauce_postgres::PostgresEventStore;
use sqlx::PgPool;

let pool = PgPool::connect("postgresql://localhost/eventstore").await?;
let store = PostgresEventStore::new(pool);
```

**Features:**
- Full ACID transactions
- Optimistic locking via DB constraints
- Efficient stream queries
- Scalable for production

**Requirements:**
- PostgreSQL 12+
- Run migrations (see setup guide)

**When to use:**
- Production applications
- Multi-instance deployments
- Strong consistency requirements
- Complex querying needs

## Usage Patterns

### Basic Save/Load Cycle

```rust
use event_sauce_core::{load, EventStore};
use event_sauce_memory::InMemoryEventStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryEventStore::new();
    let counter_id = CounterId::new();

    // Create and modify aggregate
    let mut counter = CounterAggregate::create(counter_id);
    counter.increment(5)?;
    counter.increment(3)?;
    counter.decrement(2)?;

    // Save to store
    store.commit(&mut counter).await?;
    println!("Saved {} events", 3);

    // Load from store
    let loaded: CounterAggregate = load(&store, counter_id).await?;

    assert_eq!(loaded.value(), counter.value());
    assert_eq!(loaded.version(), counter.version());

    Ok(())
}
```

### Continuing Operations

After loading, you can continue operations:

```rust
// Load existing aggregate
let mut counter: CounterAggregate = load(&store, counter_id).await?;

// Continue operations
counter.increment(10)?;
counter.decrement(3)?;

// Save new events
store.commit(&mut counter).await?;
```

### Event Sourcing Loop

Typical command handling pattern:

```rust
async fn handle_counter_command(
    store: &InMemoryEventStore,
    counter_id: CounterId,
    command: CounterCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load aggregate
    let mut counter: CounterAggregate = load(store, counter_id).await?;

    // Execute command
    match command {
        CounterCommand::Increment(amount) => counter.increment(amount)?,
        CounterCommand::Decrement(amount) => counter.decrement(amount)?,
        CounterCommand::Reset => counter.reset(),
    }

    // Save events
    store.commit(&mut counter).await?;

    Ok(())
}
```

### Handling Concurrency Conflicts

```rust
use event_sauce_core::Error;

async fn safe_update(
    store: &InMemoryEventStore,
    counter_id: CounterId,
    operation: impl Fn(&mut CounterAggregate) -> Result<(), CounterError>,
) -> Result<(), Box<dyn std::error::Error>> {
    const MAX_RETRIES: usize = 3;

    for attempt in 1..=MAX_RETRIES {
        // Load current state
        let mut counter: CounterAggregate = load(store, counter_id).await?;

        // Execute operation
        operation(&mut counter)?;

        // Try to commit
        match store.commit(&mut counter).await {
            Ok(()) => return Ok(()),
            Err(Error::ConcurrencyConflict { .. }) if attempt < MAX_RETRIES => {
                println!("Conflict detected, retrying ({}/{})", attempt, MAX_RETRIES);
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err("Max retries exceeded".into())
}
```

### Batch Operations

Accumulate multiple changes before committing:

```rust
let mut account = BankAccountAggregate::open(
    account_id,
    "Alice".to_string(),
    1000,
)?;

// Multiple operations
account.deposit(500)?;
account.withdraw(200)?;
account.deposit(100)?;

// Single commit for all events
store.commit(&mut account).await?;

// All 4 events (open + 3 transactions) saved together
```

## Best Practices

### 1. One Aggregate, One Store Commit

Keep aggregate boundaries clear:

```rust
// ✅ Good: One aggregate per commit
let mut order = load(&store, order_id).await?;
order.add_item(item)?;
store.commit(&mut order).await?;

// ❌ Bad: Don't try to coordinate multiple aggregates in one commit
// Use subscriptions and event-driven workflows instead
```

### 2. Always Handle Conflicts

Never ignore concurrency conflicts:

```rust
// ✅ Good: Handle conflicts
match store.commit(&mut aggregate).await {
    Ok(()) => {},
    Err(Error::ConcurrencyConflict { .. }) => {
        // Reload and retry, or return error to user
    }
    Err(e) => return Err(e),
}

// ❌ Bad: Blindly unwrapping
store.commit(&mut aggregate).await.unwrap();
```

### 3. Use Idempotent Operations

Make commands safe to retry:

```rust
impl OrderAggregate {
    pub fn cancel(&mut self, reason: String) -> Result<(), OrderError> {
        // Idempotent: safe to call multiple times
        if self.status == OrderStatus::Cancelled {
            return Ok(()); // Already cancelled
        }

        // Apply cancellation event
        self.apply(OrderCancelledEvent { reason })?;
        Ok(())
    }
}
```

### 4. Keep Events Small

Don't store unnecessary data:

```rust
// ✅ Good: Only store what changed
#[derive(Event)]
pub struct ProductPriceChanged {
    new_price: Decimal,
    timestamp: DateTime<Utc>,
}

// ❌ Bad: Don't duplicate entire aggregate state
#[derive(Event)]
pub struct ProductPriceChanged {
    new_price: Decimal,
    old_price: Decimal,
    product_name: String,      // Already in product
    product_category: String,  // Already in product
    timestamp: DateTime<Utc>,
}
```

### 5. Event Schema Versioning

Plan for schema evolution:

```rust
// Version 1
#[derive(Event)]
#[event(version = 1)]
pub enum OrderEventV1 {
    Created { customer_id: String },
}

// Version 2 - Added email field
#[derive(Event)]
#[event(version = 2)]
pub enum OrderEventV2 {
    Created {
        customer_id: String,
        email: String,  // New field
    },
}
```

### 6. Test Event Replay

Always test that aggregates can be reconstructed:

```rust
#[tokio::test]
async fn test_counter_replay() {
    let store = InMemoryEventStore::new();
    let counter_id = CounterId::new();

    // Create and modify
    let mut counter = CounterAggregate::create(counter_id);
    counter.increment(10)?;
    counter.decrement(3)?;

    let expected_value = counter.value();
    let expected_version = counter.version();

    // Save
    store.commit(&mut counter).await?;

    // Load and verify
    let loaded: CounterAggregate = load(&store, counter_id).await?;
    assert_eq!(loaded.value(), expected_value);
    assert_eq!(loaded.version(), expected_version);
}
```

## Migration Guide

### From Manual Event Handling

**Old Pattern:**

```rust
// Old: Manual envelope creation
async fn save_counter(store: &InMemoryEventStore, counter: &mut Counter) {
    let envelopes: Vec<EventEnvelope> = counter
        .pending_events()
        .iter()
        .enumerate()
        .map(|(i, event)| {
            EventEnvelope::new(
                Uuid::new_v4(),
                counter.id().to_uuid(),
                "Counter".to_string(),
                event.event_type().to_string(),
                counter.version() + Version::new(i as i32 + 1),
                serde_json::to_value(event).unwrap(),
            )
        })
        .collect();

    let stream_id = StreamId::new("Counter", counter.id().to_uuid());
    store.append_events(stream_id, envelopes, counter.version()).await?;
    counter.clear_pending_events();
}
```

**New Pattern:**

```rust
// New: Simple commit
store.commit(&mut counter).await?;
```

### From Custom Constructors

**Old Pattern:**

```rust
impl Counter {
    fn from_events(id: CounterId, events: Vec<CounterEvent>) -> Self {
        let mut counter = Self {
            id,
            value: 0,
            version: Version::initial(),
            pending_events: Vec::new(),
        };

        for event in events {
            counter.apply_unchecked(&event);
        }

        counter
    }
}

// Loading
let events = load_events_from_store(&store, counter_id).await?;
let counter = Counter::from_events(counter_id, events);
```

**New Pattern:**

```rust
// New: Simple load
let counter: CounterAggregate = load(&store, counter_id).await?;
```

### From StreamId-based APIs

**Old Pattern:**

```rust
let stream_id = StreamId::new("Counter", counter_id.to_uuid());
let stream = store.load_stream(stream_id, Version::initial()).await?;
```

**New Pattern:**

```rust
// The new API handles StreamId internally
let counter: CounterAggregate = load(&store, counter_id).await?;
```

## Advanced Topics

### Custom Event Store Implementations

Implement `EventStore` trait for custom backends:

```rust
use async_trait::async_trait;
use event_sauce_core::{EventStore, EventEnvelope, StreamId, Version, Result};
use futures::Stream;

pub struct CustomEventStore {
    // Your storage backend
}

#[async_trait]
impl EventStore for CustomEventStore {
    async fn append_events(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<()> {
        // Your implementation
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: Version,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        // Your implementation
    }
}
```

### Event Store Decorators

Add cross-cutting concerns:

```rust
pub struct LoggingEventStore<S> {
    inner: S,
}

#[async_trait]
impl<S: EventStore> EventStore for LoggingEventStore<S> {
    async fn append_events(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<()> {
        println!("Appending {} events to {}", events.len(), stream_id);
        self.inner.append_events(stream_id, events, expected_version).await
    }

    // ... other methods
}
```

### Snapshotting

For aggregates with long event histories, consider snapshotting:

```rust
pub struct SnapshotStore<S> {
    event_store: S,
    snapshots: HashMap<Uuid, (Version, Vec<u8>)>,
}

impl<S: EventStore> SnapshotStore<S> {
    pub async fn load_with_snapshot<A: Aggregate>(
        &self,
        aggregate_id: A::Id,
    ) -> Result<A> {
        // Load snapshot if available
        if let Some((snapshot_version, snapshot_data)) =
            self.snapshots.get(&aggregate_id.to_uuid())
        {
            let mut aggregate: A = bincode::deserialize(snapshot_data)?;

            // Load only events after snapshot
            let stream_id = StreamId::new(
                A::aggregate_type(),
                aggregate_id.to_uuid(),
            );
            let events = self.event_store
                .load_stream(stream_id, *snapshot_version)
                .await?;

            // Replay remaining events
            futures::pin_mut!(events);
            while let Some(envelope) = events.next().await {
                let event: A::Event = serde_json::from_value(envelope?.event_data)?;
                aggregate.apply_unchecked(&event);
            }

            return Ok(aggregate);
        }

        // No snapshot, load from beginning
        load(&self.event_store, aggregate_id).await
    }
}
```

### Event Store Metrics

Track performance and usage:

```rust
pub struct MetricsEventStore<S> {
    inner: S,
    metrics: Arc<Metrics>,
}

#[async_trait]
impl<S: EventStore> EventStore for MetricsEventStore<S> {
    async fn append_events(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: Version,
    ) -> Result<()> {
        let start = Instant::now();
        let result = self.inner.append_events(stream_id, events, expected_version).await;

        self.metrics.record_append(start.elapsed(), result.is_ok());
        result
    }

    // ... other methods with metrics
}
```

## Summary

The event store is the foundation of event sourcing in event-sauce:

- **Simple API**: Just `commit()` and `load()`
- **Type-safe**: Full Rust type safety
- **Flexible**: Multiple backend options
- **Reliable**: ACID guarantees where needed
- **Testable**: Easy to test with in-memory store

Key takeaways:

1. Use `commit()` to save pending events
2. Use `load()` to reconstruct aggregates
3. Always handle concurrency conflicts
4. Test event replay thoroughly
5. Choose the right backend for your needs

For more information:
- [Aggregates Guide](aggregates.md)
- [Events Guide](events.md)
- [Getting Started](getting-started.md)
- [Architecture Overview](architecture.md)
