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
    async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
    where
        A: Aggregate,
        A::Event: serde::Serialize;
}
```

### commit() - Save Pending Events

The `commit()` method is the primary way to save events:

```rust
async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
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
let mut counter = AggregateRoot::<Counter>::new(EntityId::new());

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
pub async fn load<S, A>(store: &S, aggregate_id: EntityId) -> Result<AggregateRoot<A>>
where
    S: EventStore,
    A: Aggregate,
    A::Event: serde::de::DeserializeOwned;
```

> **Note:** Due to Rust's async trait limitations with generic return types, `load()` is a standalone function rather than a trait method.

**What it does:**

1. Loads all events for that aggregate
2. If there are no events (and no snapshot), returns `Error::NotFound` — an empty
   stream means the aggregate does not exist. This is uniform for both legacy
   (default-state) and `@init` aggregates.
3. Otherwise reconstructs the `AggregateRoot<A>` from the first event (init or
   legacy) and replays the remaining events using `apply_unchecked()` (no validation)
4. Returns the reconstructed `AggregateRoot<A>`

> **Note:** `load()` never returns a synthetic empty/default aggregate. To create
> a brand-new aggregate, use the create/init path (e.g. `repo.create()` or an
> `@init` command), not `load()`. Handle `Error::NotFound` (`err.is_not_found()`)
> when an ID may not exist yet.

> **Snapshots self-heal.** When snapshots are enabled, `load()` uses a stored
> snapshot as a fast-forward cache and then replays only the events after it.
> The snapshot is a cache, never the source of truth: if it is stale or
> incompatible — its `aggregate_type` or `snapshot_schema_version` no longer
> matches, or it can no longer be deserialized — it is discarded and the state
> is rebuilt from the full event stream. Bump `Aggregate::snapshot_version()`
> (e.g. `#[aggregate(snapshot_version = N)]`) when you change the serialized
> state shape so older snapshots are transparently discarded; the next
> `commit()` writes a fresh snapshot at the new version. See
> [architecture.md](architecture.md#snapshots-are-a-cache-never-the-source-of-truth).

**Example:**

```rust
use event_sauce_core::load;

let store = InMemoryEventStore::new();
let counter_id = EntityId::new();

// Later, load the aggregate
let counter: AggregateRoot<Counter> = load(&store, counter_id).await?;

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
use event_sauce_postgres::PostgresBackend;

// Simplest setup — creates pool, event store, checkpoint store, runs migrations
let backend = PostgresBackend::setup("postgresql://localhost/eventstore", "event_sauce").await?;
let store = backend.event_store();
```

Or create individual stores for more control:

```rust
use event_sauce_postgres::PostgresEventStore;
use sqlx::PgPool;

let pool = PgPool::connect("postgresql://localhost/eventstore").await?;
let store = PostgresEventStore::new(pool);
store.migrate().await?;
```

**Features:**
- Full ACID transactions
- Optimistic locking via DB constraints
- Efficient stream queries
- Scalable for production

**Requirements:**
- PostgreSQL 12+
- Run migrations (automatic with `PostgresBackend`, or call `store.migrate()`)

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
    let counter_id = EntityId::new();

    // Create and modify aggregate
    let mut counter = AggregateRoot::<Counter>::new(counter_id);
    counter.increment(5)?;
    counter.increment(3)?;
    counter.decrement(2)?;

    // Save to store
    store.commit(&mut counter).await?;
    println!("Saved {} events", 3);

    // Load from store
    let loaded: AggregateRoot<Counter> = load(&store, counter_id).await?;

    assert_eq!(loaded.value(), counter.value());
    assert_eq!(loaded.version(), counter.version());

    Ok(())
}
```

### Continuing Operations

After loading, you can continue operations:

```rust
// Load existing aggregate
let mut counter: AggregateRoot<Counter> = load(&store, counter_id).await?;

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
    counter_id: EntityId,
    command: CounterCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load aggregate
    let mut counter: AggregateRoot<Counter> = load(store, counter_id).await?;

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

Optimistic concurrency control means a `commit` can lose a race to another writer
and come back as a conflict. This happens consistently across backends: the
in-memory store detects it on its version check, and `PostgresEventStore` reports
it whether the conflict is caught by the `MAX(stream_version)` precheck or by the
`UNIQUE(aggregate_id, aggregate_type, stream_version)` index when two transactions
both pass the precheck and one loses the insert race. Either way you get
`Error::ConcurrencyConflict` / `Error::is_concurrency_conflict()`, so a single
retry loop works for every backend.

The retried closure **must be idempotent**: re-derive the events from freshly
loaded state on each attempt (re-`load` inside the loop) rather than replaying a
captured command result, otherwise a retry would re-apply stale decisions. Add
jittered exponential backoff so contending writers do not synchronise into a
thundering herd.

```text
use std::time::Duration;
use event_sauce_core::Error;
use event_sauce_postgres::PostgresEventStore;

async fn safe_update(
    store: &PostgresEventStore,
    counter_id: EntityId,
    operation: impl Fn(&mut AggregateRoot<Counter>) -> Result<(), CounterError>,
) -> Result<(), Box<dyn std::error::Error>> {
    const MAX_RETRIES: u32 = 5;
    const BASE_DELAY: Duration = Duration::from_millis(20);
    const MAX_DELAY: Duration = Duration::from_millis(500);

    for attempt in 1..=MAX_RETRIES {
        // Re-load current state on every attempt so the operation is derived
        // from the latest committed events (idempotent retry).
        let mut counter: AggregateRoot<Counter> = load(store, counter_id).await?;

        // Execute operation against fresh state.
        operation(&mut counter)?;

        // Try to commit.
        match store.commit(&mut counter).await {
            Ok(()) => return Ok(()),
            Err(e) if e.is_concurrency_conflict() && attempt < MAX_RETRIES => {
                // Jittered exponential backoff: base * 2^(attempt-1), capped,
                // then randomised in [0, delay) to de-synchronise writers.
                let exp = BASE_DELAY.saturating_mul(1u32 << (attempt - 1));
                let capped = exp.min(MAX_DELAY);
                let jitter = rand::random::<f64>() * capped.as_secs_f64();
                tokio::time::sleep(Duration::from_secs_f64(jitter)).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err("Max retries exceeded".into())
}
```

> The example is marked `text` (not a compiled doctest) because the jittered
> backoff uses `rand`, which the library does not depend on. Use any jitter
> source you already have; the important parts are looping on
> `is_concurrency_conflict()`, re-loading inside the loop, and backing off.

### Batch Operations

Accumulate multiple changes before committing:

```rust
let mut account = AggregateRoot::<BankAccount>::open(
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
impl AggregateRoot<Order> {
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
    let counter_id = EntityId::new();

    // Create and modify
    let mut counter = AggregateRoot::<Counter>::new(counter_id);
    counter.increment(10)?;
    counter.decrement(3)?;

    let expected_value = counter.value();
    let expected_version = counter.version();

    // Save
    store.commit(&mut counter).await?;

    // Load and verify
    let loaded: AggregateRoot<Counter> = load(&store, counter_id).await?;
    assert_eq!(loaded.value(), expected_value);
    assert_eq!(loaded.version(), expected_version);
}
```

## Migration Guide

### From Manual Event Handling

**Old Pattern:**

```rust
// Old: Manual envelope creation
async fn save_counter(store: &InMemoryEventStore, counter: &mut AggregateRoot<Counter>) {
    let envelopes: Vec<EventEnvelope> = counter
        .pending_events()
        .iter()
        .enumerate()
        .map(|(i, event)| {
            EventEnvelope::new(
                Uuid::new_v4(),
                counter.entity_id().to_uuid(),
                "Counter".to_string(),
                event.event_type().to_string(),
                counter.version() + Version::new(i as u64 + 1),
                serde_json::to_value(event).unwrap(),
            )
        })
        .collect();

    let stream_id = StreamId::new("Counter", counter.entity_id().to_uuid());
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
    fn from_events(id: EntityId, events: Vec<CounterEvent>) -> Self {
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
let counter: AggregateRoot<Counter> = load(&store, counter_id).await?;
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
let counter: AggregateRoot<Counter> = load(&store, counter_id).await?;
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
        aggregate_id: EntityId,
    ) -> Result<AggregateRoot<A>> {
        // Load snapshot if available
        if let Some((snapshot_version, snapshot_data)) =
            self.snapshots.get(&aggregate_id.to_uuid())
        {
            let entity: A = bincode::deserialize(snapshot_data)?;
            let mut aggregate = AggregateRoot::from_snapshot(entity, *snapshot_version);

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
