# Event Store

The Event Store is the persistence layer in event-sauce that stores and retrieves event streams for aggregates. This guide covers the event store API, available backends, and best practices.

## Table of Contents

1. [Overview](#overview)
2. [Core Concepts](#core-concepts)
3. [API Reference](#api-reference)
4. [Available Backends](#available-backends)
5. [Usage Patterns](#usage-patterns)
6. [Best Practices](#best-practices)
7. [Advanced Topics](#advanced-topics)

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
    pub id: Uuid,                        // Unique event ID
    pub aggregate_id: Uuid,              // Which aggregate
    pub aggregate_type: AggregateType,   // Type of aggregate
    pub event_type: String,              // Type of event
    pub event_version: EventVersion,     // Event SCHEMA version (not the
                                          // aggregate's stream version)
    pub event_data: serde_json::Value,   // JSON-serialized event
    pub created_by: Option<Uuid>,        // Actor, if any (from an @actor event)
    pub metadata: Option<EventMetadata>, // Causation/correlation, additional data
    pub created_at: DateTime<Utc>,       // Timestamp
}
```

### Stream IDs

Internally, the event store uses `StreamId` (an opaque `aggregate_type` +
`aggregate_id` pair, built with `StreamId::new(aggregate_type, aggregate_id)`)
to identify event streams.

However, you typically don't interact with `StreamId` directly — the
`Repository` trait and `EventStore::commit`/`commit_deleted` build it for you.

### Versioning

Two distinct version types exist, easy to confuse because both are called
"version":

- **`AggregateVersion`** — the stream's position: 0 (no events), 1 (after the
  first event), 2 (after the second), and so on. Used for optimistic
  concurrency control: `Repository::save`/`EventStore::commit` compare the
  aggregate's expected version against what is stored, and a mismatch is
  `Error::ConcurrencyConflict`.
- **`EventVersion`** — the *schema* version of one event type, set by
  `@version(n)` in `define_events!` (or `#[event(version = n)]` on a manual
  event). Unrelated to the aggregate's position in its stream; it exists so
  `DomainEvent::upcast`/`@upcast` can migrate an older payload shape on load.

## API Reference

Application code almost never calls the store directly — it goes through
`Repository<A>` (`store.repository::<A>()`), whose `load`/`save`/`modify`
delegate to the store's `commit`/`commit_deleted`. This section documents
both levels: `Repository` for everyday code, `EventStore` for what it's
built on.

### EventStore Trait

A condensed view of the real trait (see
[Custom Event Store Implementations](#custom-event-store-implementations)
below, and the full version in
[architecture.md](architecture.md#eventstore-trait)):

```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    // Primitives backends implement:
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        claims: Vec<AggregateClaim>,
        clear_claims: bool,
    ) -> Result<()>;

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    // ES-specific orchestration, generic over every backend:
    async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>
    where
        A: Aggregate,
        A::Event: serde::Serialize;

    // ... append_batch, snapshots, checkpoints, repository() — see architecture.md
}
```

### commit() - Save Pending Events

`Repository::save` is the everyday entry point; it delegates to
`EventStore::commit`, which is the primary way to save events:

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

### repo.load() - Reconstruct Aggregate

`Repository::load` reconstructs an aggregate from stored events:

```rust
async fn load<I: EntityIdFor<A> + Send>(&self, id: I) -> Result<AggregateRoot<A>>;
```

Obtain the repository from the store with `store.repository::<A>()`; it
accepts both a raw `EntityId` and a typed `AggregateId` newtype.

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
let store = Arc::new(InMemoryEventStore::new());
let repo = store.repository::<Counter>();
let counter_id = EntityId::new();

// Later, load the aggregate
let counter: AggregateRoot<Counter> = repo.load(counter_id).await?;

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
use event_sauce::memory::InMemoryEventStore;

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
use event_sauce::postgres::PostgresBackend;

// Simplest setup — creates pool, event store, checkpoint store, runs migrations
let backend = PostgresBackend::setup("postgresql://localhost/eventstore", "event_sauce").await?;
let store = backend.event_store();
```

Or create individual stores for more control:

```rust
use event_sauce::postgres::PostgresEventStore;
use sqlx::PgPool;

let pool = PgPool::connect("postgresql://localhost/eventstore").await?;
let store = PostgresEventStore::new(pool);
store.migrate().await?;
```

**Features:**
- Full ACID transactions
- Optimistic locking via DB constraints
- Efficient stream queries

**Requirements:**
- PostgreSQL 12+
- Run migrations (automatic with `PostgresBackend`, or call `store.migrate()`)

**When to use:**
- Production applications
- Strong consistency requirements
- Complex querying needs

**Multiple instances of your app** (commands, `repo.load`/`repo.save`,
`store.migrate()`) are safe to run side by side — see
[postgres-production.md](postgres-production.md#which-components-are-multi-node-safe)
for exactly which *background* components (projections, policies, the
outbox) need a lease or a dedicated worker to stay safe under more than one
instance.

## Usage Patterns

### Basic Save/Load Cycle

```rust
use std::sync::Arc;
use event_sauce::memory::InMemoryEventStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Counter>();
    let counter_id = EntityId::new();

    // Create and modify aggregate
    let mut counter = AggregateRoot::<Counter>::new(counter_id);
    counter.increment(5)?;
    counter.increment(3)?;
    counter.decrement(2)?;

    // Save to store
    repo.save(&mut counter).await?;
    println!("Saved {} events", 3);

    // Load from store
    let loaded: AggregateRoot<Counter> = repo.load(counter_id).await?;

    assert_eq!(loaded.value(), counter.value());
    assert_eq!(loaded.version(), counter.version());

    Ok(())
}
```

### Continuing Operations

After loading, you can continue operations:

```rust
// Load existing aggregate
let mut counter: AggregateRoot<Counter> = repo.load(counter_id).await?;

// Continue operations
counter.increment(10)?;
counter.decrement(3)?;

// Save new events
repo.save(&mut counter).await?;
```

### Event Sourcing Loop

Typical command handling pattern:

```rust
async fn handle_counter_command(
    repo: &EventSourcedRepository<InMemoryEventStore, Counter>,
    counter_id: EntityId,
    command: CounterCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load aggregate
    let mut counter: AggregateRoot<Counter> = repo.load(counter_id).await?;

    // Execute command
    match command {
        CounterCommand::Increment(amount) => counter.increment(amount)?,
        CounterCommand::Decrement(amount) => counter.decrement(amount)?,
        CounterCommand::Reset => counter.reset(),
    }

    // Save events
    repo.save(&mut counter).await?;

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
use event_sauce::{Error, EventSourcedRepository};
use event_sauce::postgres::PostgresEventStore;

async fn safe_update(
    repo: &EventSourcedRepository<PostgresEventStore, Counter>,
    counter_id: EntityId,
    operation: impl Fn(&mut AggregateRoot<Counter>) -> Result<(), CounterError>,
) -> Result<(), Box<dyn std::error::Error>> {
    const MAX_RETRIES: u32 = 5;
    const BASE_DELAY: Duration = Duration::from_millis(20);
    const MAX_DELAY: Duration = Duration::from_millis(500);

    for attempt in 1..=MAX_RETRIES {
        // Re-load current state on every attempt so the operation is derived
        // from the latest committed events (idempotent retry).
        let mut counter: AggregateRoot<Counter> = repo.load(counter_id).await?;

        // Execute operation against fresh state.
        operation(&mut counter)?;

        // Try to commit.
        match repo.save(&mut counter).await {
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

### Multi-Aggregate Atomic Writes

A single `commit()` covers one aggregate. When a use case must change **several
aggregates together** — the canonical "move funds between two accounts" — use
`Repository::save_all`, which persists every aggregate through a single
`EventStore::append_batch`:

```rust
let repo = store.repository::<Account>();

let mut from = repo.load(from_id).await?;
let mut to = repo.load(to_id).await?;

from.withdraw(amount)?;
to.deposit(amount)?;

// Both aggregates are persisted as one logical write.
repo.save_all(&mut [&mut from, &mut to]).await?;
```

`append_batch` is the consistency-boundary primitive: it takes a `Vec<StreamCommit>`
(one per stream) and writes them together.

- **PostgreSQL** runs the whole batch in **one transaction** under a single append
  serialization lock: all commits succeed or the whole batch rolls back. A
  `ConcurrencyConflict` (or claim conflict) on any stream aborts the entire write —
  no aggregate is left partially applied.
- **In-memory** keeps the per-stream default: `append_batch` simply loops `append`,
  so it is **not** transactional. A failure mid-batch leaves earlier streams
  written. Use it for the API shape and single-process happy path; rely on the
  PostgreSQL backend when atomic multi-aggregate writes matter.

Each aggregate's pending events are cleared as it is prepared, and aggregates with
no pending events are skipped.

## Best Practices

### 1. One Aggregate, One Store Commit

Keep aggregate boundaries clear:

```rust
// ✅ Good: One aggregate per commit
let mut order = repo.load(order_id).await?;
order.add_item(item)?;
repo.save(&mut order).await?;

// ✅ Also fine: an intentional, bounded multi-aggregate write via save_all
// (atomic on PostgreSQL — see "Multi-Aggregate Atomic Writes" above).
repo.save_all(&mut [&mut from, &mut to]).await?;

// ❌ Bad: hand-rolling multiple separate saves and hoping they all land
// repo.save(&mut from).await?; // if the next line fails, this is orphaned
// repo.save(&mut to).await?;
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
// AggregateRoot is defined in event-sauce-core, so reach it through an
// extension trait rather than a plain (orphan-rule-violating) inherent impl.
trait OrderCommands {
    fn cancel(&mut self, reason: String) -> Result<(), OrderError>;
}

impl OrderCommands for AggregateRoot<Order> {
    fn cancel(&mut self, reason: String) -> Result<(), OrderError> {
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
struct PriceChangedEvent {
    new_price: Decimal,
    timestamp: DateTime<Utc>,
}
impl ApplyEvent<Product> for PriceChangedEvent { /* ... */ }

// ❌ Bad: Don't duplicate entire aggregate state
struct PriceChangedEvent {
    new_price: Decimal,
    old_price: Decimal,
    product_name: String,      // Already in product
    product_category: String,  // Already in product
    timestamp: DateTime<Utc>,
}
```

(`#[derive(Event)]` generates `EventApplicator` for an enum whose variants each
wrap one such struct, e.g. `enum ProductEvent { PriceChanged(PriceChangedEvent) }`
— see [Events Guide](events.md#manual-event-definition) or
`examples/apply-event.rs` for the full pattern. `define_events!`'s inline
closures are the declarative alternative and don't need a separate struct.)

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
    let store = Arc::new(InMemoryEventStore::new());
    let repo = store.repository::<Counter>();
    let counter_id = EntityId::new();

    // Create and modify
    let mut counter = AggregateRoot::<Counter>::new(counter_id);
    counter.increment(10).unwrap();
    counter.decrement(3).unwrap();

    let expected_value = counter.value();
    let expected_version = counter.version();

    // Save
    repo.save(&mut counter).await.unwrap();

    // Load and verify
    let loaded: AggregateRoot<Counter> = repo.load(counter_id).await.unwrap();
    assert_eq!(loaded.value(), expected_value);
    assert_eq!(loaded.version(), expected_version);
}
```

## Advanced Topics

### Custom Event Store Implementations

Implement `EventStore` for a custom backend by providing its primitives —
`append`, `load_stream`, `stream_all`, and `get_version`. Everything else
(`commit`, `repository()`, `append_batch`, snapshots, ...) has a default
built on those four, so a minimal backend needs only them:

```rust
use async_trait::async_trait;
use event_sauce::{
    AggregateClaim, AggregateVersion, EventEnvelope, EventLogEntry, EventStore, Position,
    Result, StreamId,
};
use futures::Stream;

pub struct CustomEventStore {
    // Your storage backend
}

#[async_trait]
impl EventStore for CustomEventStore {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        claims: Vec<AggregateClaim>,
        clear_claims: bool,
    ) -> Result<()> {
        // Your implementation
    }

    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send> {
        // Your implementation
    }

    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventLogEntry>> + Send> {
        // Your implementation
    }

    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion> {
        // Your implementation
    }
}
```

### Event Store Decorators

Add cross-cutting concerns by wrapping the primitives and delegating the rest:

```rust
pub struct LoggingEventStore<S> {
    inner: S,
}

#[async_trait]
impl<S: EventStore> EventStore for LoggingEventStore<S> {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        claims: Vec<AggregateClaim>,
        clear_claims: bool,
    ) -> Result<()> {
        println!("Appending {} events to {stream_id:?}", events.len());
        self.inner
            .append(stream_id, events, expected_version, claims, clear_claims)
            .await
    }

    // ... load_stream, stream_all, get_version delegate the same way
}
```

### Snapshotting

Don't hand-roll this: snapshotting is built in. Configure a strategy on the
store —

```rust
use event_sauce::{EveryNEvents, SnapshotConfig};

let config = SnapshotConfig::builder()
    .default_strategy(EveryNEvents::try_new(100).expect("100 != 0"))
    .build();
```

— and `repo.load`/`EventStore::commit` use it transparently: a snapshot is a
fast-forward cache the store consults before replaying, refreshed on the
cadence the strategy sets, and treated as stale (rebuilt from the full
stream) whenever its `aggregate_type` or `snapshot_schema_version` doesn't
match. See [repo.load() - Reconstruct Aggregate](#repoload---reconstruct-aggregate)
above and
[architecture.md](architecture.md#snapshots-are-a-cache-never-the-source-of-truth).

### Event Store Metrics

Track performance and usage the same way a decorator does — wrap the
primitives:

```rust
pub struct MetricsEventStore<S> {
    inner: S,
    metrics: Arc<Metrics>,
}

#[async_trait]
impl<S: EventStore> EventStore for MetricsEventStore<S> {
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        claims: Vec<AggregateClaim>,
        clear_claims: bool,
    ) -> Result<()> {
        let start = Instant::now();
        let result = self
            .inner
            .append(stream_id, events, expected_version, claims, clear_claims)
            .await;

        self.metrics.record_append(start.elapsed(), result.is_ok());
        result
    }

    // ... other methods with metrics
}
```

## Summary

The event store is the foundation of event sourcing in event-sauce:

- **Simple API**: Just `repo.save()` and `repo.load()`
- **Type-safe**: Full Rust type safety
- **Flexible**: Multiple backend options
- **Reliable**: ACID guarantees where needed
- **Testable**: Easy to test with in-memory store

Key takeaways:

1. Use `repo.save()` (or `EventStore::commit`) to save pending events
2. Use `repo.load()` to reconstruct aggregates
3. Always handle concurrency conflicts
4. Test event replay thoroughly
5. Choose the right backend for your needs

For more information:
- [Aggregates Guide](aggregates.md)
- [Events Guide](events.md)
- [Getting Started](getting-started.md)
- [Architecture Overview](architecture.md)
