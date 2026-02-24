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

- **Aggregate** - Root entity with identity and lifecycle
- **DomainEvent** - Something that happened in the domain
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

- **#[derive(AggregateId)]** - Auto-implements AggregateId trait + Display
- **#[derive(AggregateError)]** - Auto-implements AggregateError marker trait
- **#[derive(AggregateState)]** - Generates aggregate wrapper with infrastructure
- **#[derive(Event)]** - Implements DomainEvent trait + auto-generates apply_event
- Compile-time code generation
- Type-safe and zero-runtime cost
- Clean separation between business logic and infrastructure

### Subscription System (in event-sauce-core)

Read model building via durable subscriptions:

- **Subscription** - Durable event subscription with guaranteed delivery
- **EventFilter** - Filter events by type or aggregate
- **CheckpointStore** - Track progress for resumability
- **CheckpointStrategy** - Configure checkpoint frequency
- No separate projection trait needed - use simple handler functions

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
Create Subscription
    ↓
Load Checkpoint (resume from last position)
    ↓
Stream Events from EventStore
    ↓
Filter Events
    ↓
Update Read Model
    ↓
Save Checkpoint
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

    // Stream all events (for projections)
    async fn stream_all(
        &self,
        from_position: Option<GlobalPosition>,
    ) -> Result<impl Stream<Item = Result<EventEnvelope, Self::Error>> + Send, Self::Error>;
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
    let mut counter = Counter::new(id);
    counter.increment(5).unwrap();
    assert_eq!(counter.value(), 5);
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_with_store() {
    let store = MemoryEventStore::new();
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
#[async_trait]
impl Projection for MyProjection {
    async fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        // Custom projection logic
    }
}
```

## Best Practices

1. **Use derive macros** - #[derive(AggregateId)], #[derive(AggregateError)], #[derive(AggregateState)], #[derive(Event)]
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
