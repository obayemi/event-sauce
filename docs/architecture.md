# Architecture Overview

This document explains the architecture and design principles of event-sauce.

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Crate Structure](#crate-structure)
3. [Event Sourcing Flow](#event-sourcing-flow)
4. [Backend Implementations](#backend-implementations)
5. [Design Decisions](#design-decisions)
6. [Performance Considerations](#performance-considerations)

## Core Concepts

### Event Sourcing Fundamentals

Event sourcing is a pattern where state changes are stored as a sequence of events rather than just the current state. This provides:

- **Complete audit trail** - Every change is recorded
- **Time travel** - Reconstruct state at any point in time
- **Event replay** - Rebuild state from events
- **Event-driven architecture** - React to domain events

### Key Components

```
┌─────────────────────────────────────────────────────────────┐
│                       Application Layer                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Commands   │  │   Queries    │  │   Handlers   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└────────────┬────────────────┬──────────────────┬────────────┘
             │                │                   │
┌────────────┴────────────────┴───────────────────┴────────────┐
│                     event-sauce Core                          │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐   │
│  │Aggregate│  │  Event   │  │EventStore│  │Subscription│   │
│  │  Trait  │  │  Trait   │  │  Trait   │  │   System   │   │
│  └─────────┘  └──────────┘  └──────────┘  └────────────┘   │
└────────────┬────────────────┬──────────────────┬────────────┘
             │                │                   │
┌────────────┴────────────────┴───────────────────┴────────────┐
│                    Backend Implementations                    │
│           ┌──────────┐            ┌──────────┐               │
│           │PostgreSQL│            │ In-Memory│               │
│           └──────────┘            └──────────┘               │
└───────────────────────────────────────────────────────────────┘
```

## Crate Structure

event-sauce is organized as a workspace with focused crates:

### event-sauce-core

The foundation providing traits and types:

- **Entity** - Domain object with identity (`EntityId`)
- **Aggregate** - Entity with associated event and error types
- **AggregateRoot** - Infrastructure wrapper for version tracking, pending events, and event application
- **DomainEvent** - Something that happened in the domain
- **ApplyEvent** - Trait for event validation and state mutation
- **EventApplicator** - Dispatches event enum variants to individual `ApplyEvent` impls
- **EventStore** - Persistence abstraction
- **Subscription** - Durable event consumption with guaranteed delivery
- **Version** - Optimistic concurrency control

**Why separate?**
- No dependencies on specific backends
- Enables custom implementations
- Clear contracts via traits
- Testable without infrastructure

### event-sauce-memory

In-memory implementation for testing:

- **InMemoryEventStore** - HashMap-based storage with durable subscriptions
- Fast, no I/O, deterministic
- Perfect for unit tests

### event-sauce-postgres

Production-ready PostgreSQL backend:

- **PostgresEventStore** - Durable event storage with subscription support
- Optimistic concurrency via unique constraints
- Streaming support for memory efficiency
- Transaction support
- Checkpoint storage for resumable subscriptions
- **Commit-order == global-id-order** for the event log: an id-allocating
  `append` takes a transaction-scoped advisory lock (keyed by the qualified
  events table) before inserting, so `events.id` is assigned in commit order.
  This is what makes `Position`-based consumers safe to scan `id > checkpoint`
  without ever skipping a still-uncommitted lower id (see
  [projections.md](projections.md#why-where-id--checkpoint-is-safe-commit-order--id-order)).
  Reads stay concurrent; the wait bound is configurable via
  `append_lock_timeout` (default 5s). The in-memory backend has the same
  property because it publishes the global position under its append locks.

**Schema Design:**
```sql
CREATE TABLE events (
    event_id UUID PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    stream_type VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    data JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(stream_id, aggregate_version)
);

CREATE INDEX idx_events_stream ON events(stream_id, aggregate_version);
CREATE INDEX idx_events_stream_type ON events(stream_type, created_at);
CREATE INDEX idx_events_type ON events(event_type, created_at);
```

### event-sauce-macros

Derive macros for reducing boilerplate (~40% less code):

- **#[aggregate(...)]** - Generates `Entity` and `Aggregate` trait implementations
- **#[derive(Entity)]** - Auto-implements `Entity` trait using `#[id]` field attribute
- **#[derive(AggregateError)]** - Auto-implements `AggregateError` marker trait
- **#[derive(Event)]** - Implements `DomainEvent` trait + auto-generates `EventApplicator`
- **#[specification]** - Converts functions into `Specification` trait implementations
- Compile-time code generation
- Type-safe and zero-runtime cost
- Clean separation between business logic and infrastructure

### Subscription System (in event-sauce-core)

Read model building via durable subscriptions:

- **Subscription** - Durable event subscription with guaranteed delivery
- **EventFilter** - Filter events by type or aggregate
- **CheckpointStore** - Track progress for resumability, plus
  `try_acquire_lease` / `renew_lease` / `release_lease` so multiple
  workers can coordinate which one is the active processor
- **CheckpointStrategy** - Configure checkpoint frequency
- **PostgresProjection** trait (in `event-sauce-postgres`) - Transactional read models whose writes commit atomically with the subscription checkpoint
- **`PostgresEventStore::listen_for_events`** - `LISTEN`/`NOTIFY` stream that
  wakes subscribers within milliseconds of a commit, with the new max
  position as payload
- **`PostgresPolicyOutbox`** - queue-shaped sibling for side-effecting
  policies, drained via `FOR UPDATE SKIP LOCKED`. See
  [policies.md](policies.md#two-ways-to-dispatch-in-process-runner-vs-queue)

### Tiers for downstream consumers

event-sauce splits *downstream consumption* into three tiers, picked per
consumer based on what it needs:

| Tier | Storage | Best for | Mechanism |
|---|---|---|---|
| Event log | `events` (immutable) | Source of truth, replay | — |
| Checkpointed subscription | `events` + `checkpoints` (with lease) | Projections / read models — ordering and replay matter | `run_leased_projection`, `Subscription` |
| Outbox queue | `events` + `policy_outbox` | Side effects (email, webhooks, payments) — parallelism + per-event retry/DLQ matter | `dispatch_policies_to_outbox` + `claim_batch` (SKIP LOCKED) |

The log is the source of truth in all three; the outbox is *derived* from
it, so a new consumer can be added later by replaying history.

### event-sauce (facade)

Re-exports all crates with feature flags:

```toml
[dependencies]
event-sauce = { version = "0.1", features = ["postgres", "macros"] }
```

## Event Sourcing Flow

### 1. Command Execution

```
User Command
    ↓
Command Handler
    ↓
Load Aggregate (from EventStore)
    ↓
Execute Business Logic
    ↓
Generate Domain Events
    ↓
Save Events (to EventStore)
    ↓
Subscriptions Process Events
    ↓
Update Projections
```

### 2. State Reconstruction

```
Load Events from Stream
    ↓
Replay Events in Order
    ↓
Apply Each Event to Aggregate
    ↓
Result: Current State
```

### 3. Projection Building with Subscriptions

```
Load Checkpoint (resume from last position)
    ↓
Stream Events from EventStore
    ↓
Filter Events
    ↓
For each matched event: BEGIN TX
    ↓
        Update Read Model (through tx)
    ↓
        Save Checkpoint (through tx)
    ↓
    COMMIT
    ↓
Repeat
```

## Backend Implementations

### EventStore Trait

```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    type Error;

    // Append events with optimistic concurrency
    async fn append(
        &self,
        stream_id: &str,
        stream_type: &str,
        expected_version: Version,
        events: Vec<EventEnvelope>,
    ) -> Result<(), Self::Error>;

    // Load events from a specific stream
    async fn load_stream(
        &self,
        stream_id: &str,
        from_version: Version,
    ) -> Result<impl Stream<Item = Result<EventEnvelope, Self::Error>> + Send, Self::Error>;

    // Stream all events (for projections). Each item is an `EventLogEntry`
    // pairing the store-issued global `Position` with the `EventEnvelope`;
    // checkpoint `entry.position` of the last processed entry to resume.
    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventLogEntry, Self::Error>> + Send, Self::Error>;

    // Highest position in the log (or `Position::start()` if empty). New
    // consumers checkpoint this to skip existing history.
    async fn max_position(&self) -> Result<Position, Self::Error>;
}
```

### Subscription System

Subscriptions provide guaranteed delivery with checkpoint management:

```rust
/// Durable subscription with guaranteed delivery
pub struct Subscription<S> {
    name: String,
    store: Arc<S>,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    config: SubscriptionConfig,
}

impl<S: EventStore> Subscription<S> {
    /// Create a subscription with builder pattern
    pub fn builder(name: impl Into<String>, store: Arc<S>) -> SubscriptionBuilder<S>;

    /// Run the subscription, processing events through handler
    pub async fn run<F>(&mut self, handler: F) -> Result<()>
    where
        F: FnMut(EventEnvelope) -> Result<()>;

    /// Rebuild from scratch by deleting checkpoint
    pub async fn rebuild(&mut self) -> Result<()>;
}

/// Event filter for selective subscription
pub enum EventFilter {
    All,
    EventType(String),
    AggregateType(String),
    Both { event_type: String, aggregate_type: String },
}

/// Checkpoint strategy
pub enum CheckpointStrategy {
    EveryEvent,      // Save after each event (safest)
    EveryN(usize),   // Save every N events
    Manual,          // User controls checkpointing
}

/// Error handling policy
pub enum ErrorPolicy {
    Retry,  // Retry with backoff
    Skip,   // Skip and continue
    Fail,   // Fail subscription
}
```

Key differences from pub/sub:
- **Guaranteed delivery** - Events never lost, always processed
- **Checkpoint tracking** - Resume from last position after restart
- **Eventual consistency** - All subscribers eventually see all events
- **No broadcast** - Each subscription independently reads from event store

## Design Decisions

### Why Async?

- **Non-blocking I/O** - Better resource utilization
- **Scalability** - Handle many concurrent operations
- **Modern Rust** - Aligns with ecosystem (Tokio, SQLx)
- **Streaming** - Process large event streams efficiently

### Why Traits?

- **Abstraction** - Swap implementations easily
- **Testing** - Mock stores for unit tests
- **Extensibility** - Users can implement custom backends
- **Clear contracts** - Well-defined interfaces

### Why Optimistic Concurrency?

```rust
// Multiple users trying to modify same aggregate
User A: Load counter (version 5) → increment → save (expects 5)
User B: Load counter (version 5) → decrement → save (expects 5)

// First write wins
User A saves successfully → counter now at version 6
User B fails with ConcurrencyConflict

// User B must reload and retry
User B: Load counter (version 6) → decrement → save (expects 6) → Success
```

Benefits:
- No locks needed
- Better performance
- Natural fit for distributed systems
- Explicit conflict handling

### Why JSONB for Event Data?

- **Flexibility** - Schema evolution without migrations
- **Queryable** - PostgreSQL JSONB supports indexes and queries
- **Human-readable** - Easy debugging and inspection
- **Version-safe** - Old events remain readable

### Why Workspace?

- **Focused crates** - Single responsibility
- **Optional features** - Only include what you need
- **Independent versioning** - Core stable, backends evolve
- **Clear boundaries** - Explicit dependencies

## Performance Considerations

### Snapshots

For aggregates with many events:

```rust
// Instead of replaying 10,000 events every time
load_snapshot(id, latest_version) // Fast
    .then(load_events_after(latest_version)) // Only recent events
```

Snapshots trade:
- (+) Fast load times
- (-) Additional storage
- (-) Complexity

#### Snapshots are a cache, never the source of truth

The event log is always the authoritative source of state; a snapshot is only a
materialized cache of a point on the timeline. `load()` therefore *self-heals*:
when a stored snapshot can no longer be trusted it is silently discarded and the
aggregate is rebuilt from events instead. A snapshot is treated as a **cache
miss** (and the loader falls through to full event replay) when any of these
hold:

1. its `aggregate_type` no longer matches the type being loaded;
2. its `snapshot_schema_version` no longer matches `A::snapshot_version()`;
3. its data can no longer be deserialized into the current aggregate shape.

Each aggregate declares a snapshot schema version via the
`Aggregate::snapshot_version()` method (default `0`). Bump it whenever you make
an incompatible change to the aggregate's serialized state shape — for example
renaming or removing a field, or changing a field's meaning:

```rust
#[aggregate(event = "OrderEvent", error = "OrderError", snapshot_version = 1)]
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Order { /* new state shape */ }
```

After a bump, every snapshot written under the old version is automatically
ignored on load (a `tracing::warn!` records the miss) and the state is
reconstructed from events. The next `commit()` writes a fresh snapshot stamped
at the new version, so the cache repopulates without any manual migration or
backfill. This makes snapshots safe to evolve: a stale snapshot can never
corrupt state or take an aggregate offline.

> **Encryption interaction:** an encrypted snapshot whose key has been
> crypto-shredded is *not* a cache miss — it intentionally returns
> `KeyNotFound` (the data is unrecoverable by design), never a silent
> fall-through to replay.

### Event Streaming

Use async streams for memory efficiency:

```rust
let mut stream = store.load_stream(stream_id, 0).await?;

while let Some(event) = stream.next().await {
    // Process one event at a time
    // Memory usage stays constant
}
```

### Connection Pooling

PostgreSQL backend uses SQLx connection pools:

```rust
let pool = PgPoolOptions::new()
    .max_connections(20)
    .connect(&database_url)
    .await?;

let store = PostgresEventStore::new(pool);
```

### Projection Strategies

**Real-time Projections:**
- Subscribe to EventBus
- Update immediately
- Good for critical read models

**Batch Projections:**
- Poll for new events periodically
- Process in batches
- Better for analytics

## Testing Strategy

### Unit Tests

```rust
#[test]
fn test_business_logic() {
    let mut counter = AggregateRoot::<Counter>::new(EntityId::new());
    counter.increment(5).unwrap();
    assert_eq!(counter.value, 5);
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_with_store() {
    let store = InMemoryEventStore::new();
    // Test full flow
}
```

### Property-Based Tests

```rust
#[proptest]
fn test_invariants(operations: Vec<Operation>) {
    // Verify invariants hold for any sequence
}
```

## Extension Points

### Custom Event Store

```rust
pub struct MyCustomStore;

#[async_trait]
impl EventStore for MyCustomStore {
    // Implement trait methods
}
```

### Custom Serialization

```rust
impl DomainEvent for MyEvent {
    fn to_envelope(&self, stream_id: &str) -> Result<EventEnvelope> {
        // Custom serialization logic
    }

    fn from_envelope(envelope: &EventEnvelope) -> Result<Self> {
        // Custom deserialization logic
    }
}
```

### Custom Projections

```rust
struct MyProjection;

#[async_trait]
impl event_sauce_postgres::PostgresProjection for MyProjection {
    const NAME: &'static str = "MyProjection";

    fn handled_event_types() -> Option<Vec<&'static str>> {
        Some(vec!["MyEvent"])
    }

    async fn handle(
        &mut self,
        envelope: &EventEnvelope,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<()> {
        // Apply the event to your read-model table through `tx`.
        let _ = (envelope, tx);
        Ok(())
    }
}
```

## Best Practices

1. **Use derive macros** - `#[aggregate(...)]`, `#[derive(Entity)]`, `#[derive(AggregateError)]`, `#[derive(Event)]`
2. **Keep aggregates small** - One consistency boundary
3. **Events are immutable** - Never modify historical events
4. **Version events** - Plan for schema evolution
5. **Use projections for reads** - Never query aggregates
6. **Handle concurrency** - Retry on conflicts
7. **Test with events** - Given/When/Then with events
8. **Separate validation** - Use ApplyEvent trait for event-specific validation
9. **Monitor performance** - Track event counts, load times
10. **Plan for growth** - Consider snapshots early

## Resources

- [Getting Started Guide](getting-started.md)
- [TDD Workflow](tdd-workflow.md)
- [Examples](../examples/)
- [API Documentation](https://docs.rs/event-sauce)
