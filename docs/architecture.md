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

### CQRS

event-sauce is CQRS-oriented: commands and queries take separate paths.
Aggregates are the **write model** — consistency boundaries that validate
commands and emit events. They are deliberately poor read models: small,
normalized around invariants, and loaded one at a time. Queries never touch
aggregates; they go through **projections**, read models denormalized for
reading and kept up to date from the emitted events.

### Key Components

```
┌──────────────────────────────────────────────────────────────┐
│                      Application Layer                        │
│   aggregates · events · commands · define_events! ·           │
│   command_handler! · code written against R: Repository<A>    │
└──────────────────────────────┬───────────────────────────────┘
                               │  trait Repository<A>
             ┌─────────────────┴──────────────────┐
             │                                    │
┌────────────┴─────────────┐        ┌─────────────┴────────────┐
│ EventSourcedRepository   │        │ StateStoredRepository    │
│   <S: EventStore, A>     │        │   <S: StateStore, A>     │
│ (persists the pending    │        │ (persists current state  │
│  events as a stream)     │        │  as one versioned row)   │
└────────────┬─────────────┘        └─────────────┬────────────┘
             │                                    │
┌────────────┴─────────────┐        ┌─────────────┴────────────┐
│ InMemoryEventStore       │        │ InMemoryStateStore       │
│ PostgresEventStore       │        │ PostgresStateStore       │
│ (streams, snapshots,     │        │ (state rows, in-tx       │
│  policies, checkpoints,  │        │  projections, outbox     │
│  audit log, crypto)      │        │  dispatcher)             │
└──────────────────────────┘        └──────────────────────────┘
```

The application layer depends on the `Repository<A>` trait, never on a
concrete store. `EventSourcedRepository` (over the `EventStore` backends) is
the classic event-sourcing path; `StateStoredRepository` (over the
`StateStore` backends) persists only current state for applications that
don't need an event log. The two are interchangeable at the composition
root — see [state-storage.md](state-storage.md) for choosing between them
and for the state-store feature status.

### Domain / persistence separation

The domain layer is pure: `Entity`, `Aggregate`, `ApplyEvent`, the
`AggregateRoot` lifecycle (`UninitAggregateRoot` → `AggregateRoot` →
`DeletedAggregateRoot`), and `Specification` know nothing about storage.
`AggregateRoot::apply()` applies events eagerly and buffers them as
`pending_events`, so an aggregate always carries both its current state and
the changes that produced it. That single design choice is what makes the
persistence style pluggable:

- an **event-sourced** repository persists the buffered events (state is
  derived by replay);
- a **state-stored** repository persists the entity itself (`Entity` is
  already `Serialize + DeserializeOwned`), with `AggregateVersion` acting as
  an optimistic-lock row version.

Everything above the `Repository` trait — aggregates, events, commands, the
`define_events!` and `command_handler!` macros, validation — is identical in
both modes; everything below it is a persistence detail chosen once, at the
composition root.

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
- **Repository** - Persistence-style-agnostic aggregate persistence trait (`load`/`save`/`modify`/`create_*`); `EventSourcedRepository` is its event-store-backed implementation
- **EventStore** - Event-stream persistence abstraction
- **EventFilter** - Predicate for selecting events a consumer cares about
- **CheckpointStore** - Per-consumer position tracking and leasing for projections and policies
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

### Event Consumption (in event-sauce-core)

Read model building and reactions consume events durably from the store:

- **EventFilter** - Filter events by type or aggregate
- **CheckpointStore** - Track progress for resumability, plus
  `try_acquire_lease` / `renew_lease` / `release_lease` so multiple
  workers can coordinate which one is the active processor
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
| Checkpointed projection | `events` + `checkpoints` (with lease) | Projections / read models — ordering and replay matter | `run_postgres_projection`, `run_leased_projection` |
| Outbox queue | `events` + `policy_outbox` | Side effects (email, webhooks, payments) — parallelism + per-event retry/DLQ matter | `dispatch_policies_to_outbox` + `claim_batch` (SKIP LOCKED) |

The log is the source of truth in all three; the outbox is *derived* from
it, so a new consumer can be added later by replaying history.

### event-sauce (facade)

The single dependency downstream crates take. It re-exports the core surface
at its root, the backends as `event_sauce::{memory, postgres}`, and the
AES-256-GCM provider alongside the traits it implements in
`event_sauce::crypto`. The derive macros resolve their generated paths to
whichever of `event-sauce` or `event-sauce-core` a crate depends on, so
depending on the facade alone is enough:

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
Projection Runner Processes Events
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

### 3. Transactional Projection Building

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

A condensed view of the real trait in
`crates/event-sauce-core/src/event_store.rs` (doc comments trimmed; all
methods use the crate-wide `Result<T>` / `Error`):

```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    // ── Primitives (backends must implement) ─────────────────────────────

    // Append events with optimistic concurrency, enforcing uniqueness
    // `claims` in the same write (`clear_claims` drops all claims on
    // deletion). MUST assign global positions in commit order — the
    // guarantee that makes `position > checkpoint` scans safe.
    async fn append(
        &self,
        stream_id: StreamId,
        events: Vec<EventEnvelope>,
        expected_version: AggregateVersion,
        claims: Vec<AggregateClaim>,
        clear_claims: bool,
    ) -> Result<()>;

    // Load one stream's events from a version onward.
    async fn load_stream(
        &self,
        stream_id: StreamId,
        from_version: AggregateVersion,
    ) -> Result<impl Stream<Item = Result<EventEnvelope>> + Send>;

    // Stream the global log after `from_position`, ascending; each item is
    // an `EventLogEntry` pairing a `Position` with its `EventEnvelope`.
    async fn stream_all(
        &self,
        from_position: Position,
    ) -> Result<impl Stream<Item = Result<EventLogEntry>> + Send>;

    // Current version of a stream.
    async fn get_version(&self, stream_id: StreamId) -> Result<AggregateVersion>;

    // ── Default methods (generic; backends override for performance/atomicity)

    // Multi-stream write — the consistency-boundary primitive behind
    // `Repository::save_all` and atomic policy reactions. The default loops
    // `append` (NOT atomic); PostgreSQL overrides it with one transaction.
    async fn append_batch(&self, commits: Vec<StreamCommit>) -> Result<()>;

    // Highest position in the log (`Position::start()` if empty) — what a
    // brand-new consumer checkpoints to skip history. Default scans
    // `stream_all`; PostgreSQL answers with a cheap MAX query.
    async fn max_position(&self) -> Result<Position>;

    async fn stream_exists(&self, stream_id: StreamId) -> Result<bool>;

    // Snapshot cache hooks — no-ops by default, wired by backends that
    // support snapshotting.
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()>;
    async fn load_snapshot(&self, stream_id: StreamId) -> Result<Option<Snapshot>>;
    fn snapshot_config(&self) -> &SnapshotConfig;

    // Optional capabilities — `None` unless the backend provides them.
    fn checkpoint_store(&self) -> Option<CheckpointStoreRef>;
    fn crypto_key_store(&self) -> Option<&dyn CryptoKeyStore>;
    fn crypto_provider(&self) -> Option<&dyn CryptoProvider>;

    // Conveniences built on the above.
    fn policy_runner(self: &Arc<Self>) -> Result<PolicyRunner<Self>>;
    fn repository<A>(self: &Arc<Self>) -> EventSourcedRepository<Self, A>;

    // ES-specific commit orchestration: drain an aggregate's pending events
    // into envelopes, encrypt if needed, append with concurrency control,
    // snapshot per strategy, clear pending on success. `commit_deleted` is
    // the tombstone-writing sibling for `DeletedAggregateRoot`.
    async fn commit<A>(&self, aggregate: &mut AggregateRoot<A>) -> Result<()>;
    async fn commit_deleted<A>(&self, aggregate: &mut DeletedAggregateRoot<A>) -> Result<()>;
}
```

Application code normally doesn't call the store directly — it goes through
`Repository<A>` (usually obtained via `store.repository::<A>()`), which
delegates `save`/`save_deleted` to `commit`/`commit_deleted`.

### Event Consumption Primitives

Downstream consumers (projections and policies) read durably from the store
using two core primitives plus a backend-specific runner:

```rust
/// Predicate for selecting events a consumer cares about.
pub enum EventFilter {
    All,
    EventType(String),
    AggregateType(String),
    Both { event_type: String, aggregate_type: String },
    AnyOfEventTypes(Vec<String>),
}

/// Per-consumer position tracking and leasing.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn save_checkpoint(&self, name: &str, position: Position) -> Result<()>;
    async fn load_checkpoint(&self, name: &str) -> Result<Option<Position>>;
    async fn delete_checkpoint(&self, name: &str) -> Result<()>;
    // Plus try_acquire_lease / renew_lease / release_lease for multi-worker
    // coordination — exactly one worker is the active processor at a time.
}
```

The only built-in projection model is postgres-backed and transactional:
`run_postgres_projection` (and the multi-worker `run_leased_projection`)
applies each matched event and advances the checkpoint **inside the same
database transaction**, so a crash mid-batch never leaves the read model
ahead of or behind its checkpoint. See
[projections.md](projections.md) for the full guide.

Key properties:
- **Guaranteed delivery** - Events never lost; replayed from the log on restart
- **Atomic checkpointing** - Read-model write and checkpoint commit together
- **Resumable** - Resume from last checkpoint after restart
- **Lease-coordinated** - Multiple instances elect a single active processor

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
impl event_sauce::postgres::PostgresProjection for MyProjection {
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
