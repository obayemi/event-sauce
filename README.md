# event-sauce

**Event-driven modeling and event sourcing for Rust**

> **Note**: event-sauce is **not production-ready**. The current architecture targets single-node deployments and has not been validated for horizontal scalability or high-throughput distributed workloads. Use it for prototyping, learning, and small-scale applications.

[![CI](https://github.com/obayemi/event-sauce/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/obayemi/event-sauce/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/obayemi/event-sauce/branch/master/graph/badge.svg)](https://codecov.io/gh/obayemi/event-sauce)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust](https://img.shields.io/badge/rust-1.98%2B-orange.svg)](https://www.rust-lang.org)

## The idea

You model your domain as **event-driven aggregates**: every state change is
an explicit event that is validated, applied, and observable. Modeling
stays light — `define_events!` and `command_handler!` generate the
boilerplate, so an aggregate is little more than its state, its events,
and the business rules that guard them.

Storage is just as hands-off: aggregates are loaded and saved through the
`Repository` trait, so command handlers read as plain domain code — load,
apply a command, save — while the store takes care of concurrency control,
uniqueness claims, and event propagation.

The library is **CQRS-oriented** throughout: aggregates are the write
model — consistency boundaries that guard commands, and deliberately poor
read models. Queries never touch aggregates; they go through projections,
read models kept up to date from the very events your aggregates emit.

## Features

### Event-driven core

- 🎯 **Type-Safe Events**: `ApplyEvent` trait for self-contained event logic — validate → apply → post-validate
- 📝 **Declarative Macros**: `define_events!` (events with inline validation), `command_handler!` (command methods), `#[aggregate]`, `#[derive(Event)]`, `#[derive(AggregateId)]`, `#[derive(AggregateError)]`, `#[specification]`
- 🏗️ **Repository Pattern**: persistence-agnostic `Repository` trait with `EventSourcedRepository` and `StateStoredRepository` implementations
- 🔀 **CQRS-Oriented**: aggregates are the write model; queries go through projections built from your events
- 🛡️ **Rich Validation**: Aggregate-specific errors with business rule enforcement
- 🧩 **Specification Pattern**: Composable, reusable business rules with AND/OR/NOT combinators
- 🚪 **Init Events**: Type-state aggregate construction — no more invalid uninitialized states
- 👤 **Actor Events**: Permission validation tied to actor identity
- 🔒 **Uniqueness Claims**: Cross-aggregate uniqueness constraints enforced transactionally (e.g., unique emails)
- 📦 **Optimistic Concurrency**: Version-checked writes with typed `ConcurrencyConflict` errors
- 🗄️ **Multiple Backends**: PostgreSQL, in-memory
- 🧪 **Testing First-Class**: Built-in test helpers and fixtures; 🚀 latest stable dependencies (Tokio 1.48, SQLx 0.8); strict TDD with property-based testing

### Event sourcing

- 📋 **Audit Log**: Paginated event log query with filters by actor, aggregate, event type, and time range
- 🔄 **Async Streaming & Replay**: Memory-efficient event processing with backpressure; fast replay without re-validation
- 🔗 **Policies**: Cross-aggregate event orchestration with causation tracking, checkpoint-based resumption, and configurable error handling
- 📊 **Checkpointed Projections**: Transactional postgres-backed read models, rebuildable from history (`PostgresProjection`)
- 🔐 **Encryption & Crypto-Shredding**: Full-aggregate or field-level encryption with pluggable providers
- 📸 **Snapshots**: Replay optimization with schema versioning and replay fallback
- ⏱️ **Battle-Tested Patterns**: Global ordering, distributed lease fencing, atomic multi-aggregate writes

### State storage

- 🗃️ **State Storage**: One versioned state row per aggregate — no log to operate
- ⚡ **In-Transaction Projections**: Read models updated inside the save transaction — exactly-once, read-your-writes, rollback on failure
- 📬 **Transactional Outbox**: Durable at-least-once side effects, enqueued atomically with the state write

## Quick Start

```rust
use event_sauce::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// 1. Define your aggregate ID — a typed wrapper around EntityId
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, AggregateId)]
#[aggregate_id(Counter)]
struct CounterId(EntityId);

// 2. Define domain errors
#[derive(AggregateError, Debug, Error)]
enum CounterError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i32),
}

// 3. Define your aggregate with business state
#[aggregate(event = "CounterEvent", error = "CounterError")]
#[derive(Default, Debug, Serialize, Deserialize)]
struct Counter {
    #[id]
    id: CounterId,
    value: i32,
}

// 4. Define events with validation and apply logic
use event_sauce::define_events;

define_events! {
    enum CounterEvent for Counter {
        Incremented {
            amount: i32,
        }
        @validate |_counter, event| {
            if event.amount <= 0 {
                return Err(CounterError::InvalidAmount(event.amount));
            }
            Ok(())
        }
        => |counter, event| {
            counter.value += event.amount;
        },

        Decremented {
            amount: i32,
        }
        => |counter, event| {
            counter.value -= event.amount;
        },
    }
}

// 5. Generate command methods — @clock stamps `timestamp` from the wall
// clock; without it a command takes its instant as a parameter instead
// (see docs/events.md).
use event_sauce::command_handler;

command_handler! {
    impl Counter {
        @clock fn increment(amount: i32) -> IncrementedEvent { amount };
        @clock fn decrement(amount: i32) -> DecrementedEvent { amount };
    }
}
```

That's it! Pair your aggregate with a store and start issuing commands:

```rust
# use event_sauce::prelude::*;
# use event_sauce::memory::InMemoryEventStore;
# use serde::{Deserialize, Serialize};
# use thiserror::Error;
# use std::sync::Arc;
# #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, AggregateId)]
# #[aggregate_id(Counter)]
# struct CounterId(EntityId);
# #[derive(AggregateError, Debug, Error)]
# enum CounterError {
#     #[error("Invalid amount: {0}")]
#     InvalidAmount(i32),
# }
# #[aggregate(event = "CounterEvent", error = "CounterError")]
# #[derive(Default, Debug, Serialize, Deserialize)]
# struct Counter {
#     #[id]
#     id: CounterId,
#     value: i32,
# }
# event_sauce::define_events! {
#     enum CounterEvent for Counter {
#         Incremented {
#             amount: i32,
#         }
#         @validate |_counter, event| {
#             if event.amount <= 0 {
#                 return Err(CounterError::InvalidAmount(event.amount));
#             }
#             Ok(())
#         }
#         => |counter, event| {
#             counter.value += event.amount;
#         },
#         Decremented {
#             amount: i32,
#         }
#         => |counter, event| {
#             counter.value -= event.amount;
#         },
#     }
# }
# event_sauce::command_handler! {
#     impl Counter {
#         @clock fn increment(amount: i32) -> IncrementedEvent { amount };
#         @clock fn decrement(amount: i32) -> DecrementedEvent { amount };
#     }
# }
# #[tokio::main]
# async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
let store = Arc::new(InMemoryEventStore::builder().build());
let repo = store.repository::<Counter>();

let mut counter = repo.create();
counter.increment(5)?;
repo.save(&mut counter).await?;
# Ok(())
# }
```

### Init Events (Type-State Construction)

Use `@init` events to enforce that aggregates are always constructed through a valid creation event — no more `Default` with invalid states:

```rust,ignore
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AggregateId)]
#[aggregate_id(Order)]
struct OrderId(EntityId);

#[aggregate(event = "OrderEvent", error = "OrderError", init)]
#[derive(Debug, Serialize, Deserialize)]
struct Order {
    #[id]
    id: OrderId,
    order_id: String,
    status: OrderStatus,
}

define_events! {
    enum OrderEvent for Order {
        Created {
            order_id: String,
        }
        @init
        @validate |evt| {
            if evt.order_id.is_empty() {
                return Err(OrderError::EmptyOrderId);
            }
            Ok(())
        }
        => |id, event| {
            Order { id: id.into(), order_id: event.order_id.clone(), status: OrderStatus::Pending }
        },

        Completed {} => |order, _event| {
            order.status = OrderStatus::Completed;
        },
    }
}

command_handler! {
    impl Order {
        @clock @init fn create_order(order_id: String) -> CreatedEvent { order_id };
        @clock fn complete() -> CompletedEvent {};
    }
}

// Creation returns AggregateRoot<Order> directly
let mut order = Order::create_order("ORD-001".into())?;
order.complete()?;
```

Multiple init variants are supported for different creation paths (e.g., `CreatedByAdmin` vs `CreatedByInvite`).

### Actor Events (Permission Validation)

Require an actor (e.g., a `User`) for specific commands, with automatic permission validation and audit trail:

```rust,ignore
define_events! {
    pub enum DocumentEvent for Document {
        ContentUpdated {
            content: String,
        }
        @actor(User)
        @validate |_doc, actor, _evt| {
            if actor.role == Role::Viewer {
                return Err(DocumentError::PermissionDenied);
            }
            Ok(())
        }
        => |doc, event| {
            doc.content = event.content.clone();
        },
    }
}

command_handler! {
    impl Document {
        @clock @actor(User) fn update_content(content: String) -> ContentUpdatedEvent { content };
    }
}

// Actor required at command time — validated, then stored in EventEnvelope::created_by
doc.update_content(&editor, "new content".into())?;
```

Actor validation is **skipped during replay** — events are historical facts, only fresh commands validate permissions.

### Policies (Cross-Aggregate Orchestration)

React to events on one aggregate to trigger commands on another, with full causation tracking and checkpoint-based resumption:

```rust,ignore
use event_sauce::policy;

// Declarative policy — reacts to UserKickedEvent, removes them from their group
policy! {
    KickUserPolicy {
        on KickedEvent |event, ctx| {
            let mut group: AggregateRoot<Group> = ctx.load(event.group_id).await?;
            group.remove_member(event.user_id)?;
            ctx.commit(&mut group).await?;
            // Committed events automatically carry causation metadata:
            //   causation_id, correlation_id, causation_chain
            Ok(())
        },
    }
}

// Register policies and process pending events
let runner = PolicyRunner::new(store, checkpoint_store)
    .with_max_cascade_depth(5)  // prevent infinite loops
    .on_error(OnError::Retry(RetryConfig::default()))  // retry transient failures
    .register(Arc::new(KickUserPolicy));
let processed = runner.process_pending().await?;
```

Cascading reactions are supported — a policy's output events can trigger further policies, with configurable depth limits. Checkpoint-based resumption prevents duplicate processing on restart.

See the **[Policies Guide](docs/policies.md)** for full details.

### Encryption & Crypto-Shredding

Protect sensitive data at rest with full-aggregate or field-level encryption:

**Full-aggregate encryption** — all event and snapshot data encrypted:

```rust,ignore
#[aggregate(event = "UserEvent", error = "UserError", encrypted)]
struct User {
    #[id]
    id: EntityId,
    name: String,
    email: String,
}
```

**Field-level encryption** — encrypt only sensitive fields, keep others queryable:

```rust,ignore
define_events! {
    pub enum PatientEvent for Patient {
        Registered {
            name: String,
            diagnosis: String,
            visit_count: i32,
        }
        @encrypted_fields(name, diagnosis)
        => |patient, event| {
            patient.name = event.name.clone();
            patient.diagnosis = event.diagnosis.clone();
            patient.visit_count = event.visit_count;
        },
    }
}
```

With the `crypto` feature enabled, every store builder installs **AES-256-GCM
encryption** automatically — no extra setup needed beyond turning the
feature on (`event-sauce = { version = "0.1", features = ["crypto"] }`).

**Crypto-shredding** (right to be forgotten) — delete the key to make data permanently unreadable:

```rust,ignore
store.crypto_key_store()
    .expect("crypto key store configured")
    .delete_key(user_id.as_uuid()).await?;
// Subsequent loads return Error::KeyNotFound
```

See the **[Privacy & Encryption Guide](docs/privacy.md)** for full details.

### Uniqueness Claims

Enforce cross-aggregate uniqueness constraints — like unique emails or usernames — transactionally during event commit:

```rust,ignore
#[aggregate(event = "ProfileEvent", error = "ProfileError", claims)]
struct Profile {
    #[id]
    id: EntityId,
    email: String,
    username: String,
}

impl Profile {
    fn aggregate_claims(&self) -> Vec<AggregateClaim> {
        vec![
            AggregateClaim::new("Profile.email", json!(&self.email)),
            AggregateClaim::new("Profile.username", json!(&self.username)),
        ]
    }
}
```

Claims are enforced atomically within the same transaction as events. If another aggregate already holds the claim, you get a `ClaimConflict` error:

```rust,ignore
let result = store.commit(&mut profile).await;
match result {
    Err(e) if e.is_claim_conflict() => println!("Email already taken!"),
    other => other?,
}
```

**Key features:**
- Transactional enforcement — claims and events committed atomically
- SHA-256 hashed storage — no plaintext leakage (crypto-shredding safe)
- Composite keys — use structured JSON values for multi-field uniqueness
- Automatic cleanup on delete — claims are released when aggregates are deleted

See the **[Claims Guide](docs/claims.md)** for full details.

### Audit Log

Every event automatically captures who did what and why. Query the full event history with paginated, filtered access:

```rust,ignore
use event_sauce::memory::InMemoryEventLogQuery;
use event_sauce::{EventLogQuery, EventLogParams};

// Wrap a store in its backend's EventLogQuery (InMemoryEventLogQuery here;
// PostgresEventLogQuery for the Postgres backend)
let log_query = InMemoryEventLogQuery::new(store);

// Find all events created by a specific user
let params = EventLogParams {
    created_by: Some(user_id.as_uuid()),
    ..Default::default()
};
let page = log_query.query_events(params).await?;
println!("Found {} events by user", page.total_count);

// Filter by aggregate type and time range
let params = EventLogParams {
    aggregate_type: Some("Order".to_string()),
    from_date: Some(start_of_day),
    to_date: Some(end_of_day),
    per_page: 25,
    ..Default::default()
};
let page = log_query.query_events(params).await?;
```

**Built-in traceability:**
- **Actor tracking** — `created_by` field on every event, populated automatically from actor events
- **Causation chain** — `causation_id`, `correlation_id`, and full `causation_chain` trace policy cascades back to the root cause
- **Extensible metadata** — attach application-specific data (IP address, tenant ID, etc.) via `EventMetadata::additional`
- **Filterable queries** — filter by aggregate type, event type, aggregate ID, actor, and time range

See the **[Audit Log Guide](docs/audit-log.md)** for full details.

### Building Projections (Read Models)

Read models are postgres-backed and transactional: the runner applies each
event and advances the subscription checkpoint inside the same database
transaction, so a crash mid-batch never leaves the projection ahead of (or
behind) its checkpoint. Implement [`PostgresProjection`] and run it with
[`PostgresBackend::run_postgres_projection`]:

```rust,ignore
use event_sauce::postgres::{PostgresBackend, PostgresProjection};
use event_sauce::{EventEnvelope, Result};

struct UserListProjection;

impl UserListProjection {
    async fn migrate(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS user_list (
                user_id UUID PRIMARY KEY,
                email TEXT NOT NULL,
                name TEXT NOT NULL
            )",
        )
        .execute(pool)
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PostgresProjection for UserListProjection {
    const NAME: &'static str = "UserListProjection";

    fn handled_event_types() -> Option<Vec<&'static str>> {
        Some(vec!["UserCreated", "UserNameChanged", "UserDeleted"])
    }

    async fn handle(
        &mut self,
        envelope: &EventEnvelope,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<()> {
        // Inspect envelope.event_type and run the appropriate UPDATE/INSERT
        // through `tx` — the runner commits after handle() returns Ok.
        let _ = (envelope, tx);
        Ok(())
    }
}

UserListProjection::migrate(backend.pool()).await?;
backend.run_postgres_projection(&mut UserListProjection).await?;
```

## Installation

`event-sauce` is the only **event-sauce** crate you need — the derive
macros expand to paths rooted at it, so the sibling crates
(`event-sauce-core`, `-macros`, `-memory`, `-postgres`, `-crypto`) never
appear in your `Cargo.toml`:

```toml
[dependencies]
event-sauce = "0.1"

# With specific features
event-sauce = { version = "0.1", features = ["postgres"] }

# The one companion dependency every caller of define_events!/command_handler!
# needs: the generated event structs derive Serialize/Deserialize themselves.
serde = { version = "1", features = ["derive"] }
```

`paste`, `uuid`, `serde_json` and `async-trait` (needed only by `policy!`)
are macro-only — a caller adds none of them. `chrono` is reachable as
`event_sauce::chrono` (the same one the macros use), so a command's
`DateTime<Utc>` fields need no separate `chrono` line either, even though
`chrono` isn't macro-only: it's part of the public API (every command
without `@clock` takes its instant as a parameter). `thiserror` isn't
required by the library, but every example pairs it with
`#[derive(AggregateError)]` for the `Error` bound the trait needs — add it
if you follow that pattern. Implementing one of the async traits by hand —
`PostgresProjection`, `EventStore`, `CheckpointStore`, `StateProjection` —
needs `async-trait` as a direct dependency, since each trait is itself
declared with `#[async_trait]`; a Postgres implementation also adds `sqlx`
for the pool and transaction types those APIs take. See
[event-sauce's crate docs](https://docs.rs/event-sauce) for the full
breakdown.

Backends live under their own module, so a swap is a one-line change:

```rust,ignore
use event_sauce::memory::InMemoryEventStore;
use event_sauce::postgres::PostgresBackend;
use event_sauce::crypto::Aes256GcmProvider;
```

### Feature Flags

- `macros` (default) - Derive macros for aggregates and events
- `memory` (default) - In-memory backend for testing
- `event-sourcing` (default) - The event-sourced persistence style: `EventStore`, snapshots, checkpoints, policies, audit log, encryption
- `state-store` (default) - The state-stored persistence style: `StateStore`, in-transaction projections
- `postgres` - PostgreSQL backend (implies `event-sourcing` and `state-store`)
- `crypto` - Encryption support (AES-256-GCM provider, key stores); turns on encryption in whichever backends are enabled
- `full` - All features enabled

The event-driven domain layer (aggregates, events, commands, the `Repository`
trait, and the macros) is always available; the two persistence styles are
independent, additive features.

## Architecture

```text
┌────────────────────────────────────────────────────────────┐
│                        Application                         │
│      aggregates · events · commands · R: Repository<A>     │
└─────────────────────────────┬──────────────────────────────┘
                              │
┌─────────────────────────────┴──────────────────────────────┐
│              event-sauce Core (event-driven domain)        │
│   Aggregate · ApplyEvent · Specification · Repository      │
└──────────────┬──────────────────────────────┬──────────────┘
               │                              │
   EventSourcedRepository          StateStoredRepository
   (EventStore trait)              (StateStore trait)
               │                              │
┌──────────────┴──────────────┐ ┌─────────────┴──────────────┐
│  Event-sourced backends     │ │  State-stored backends     │
│  PostgresEventStore         │ │  PostgresStateStore        │
│  InMemoryEventStore         │ │  InMemoryStateStore        │
│  + snapshots, checkpoints,  │ │  + in-transaction          │
│    policies, audit log,     │ │    projections,            │
│    crypto-shredding         │ │    transactional outbox    │
└─────────────────────────────┘ └────────────────────────────┘
```

### Design Principle: Generic Implementation First

**Key architectural principle**: New features for EventStore and other core traits should be implemented **generically in the trait** using default methods, not in individual backend implementations.

**Benefits:**

- ✅ **Single source of truth** - Write once, works everywhere
- ✅ **Consistent behavior** - All backends work identically
- ✅ **Easier testing** - Test once at trait level
- ✅ **Less maintenance** - Fixes apply to all backends
- ✅ **Faster development** - New backends get features for free

**When to implement in backends:**

- Only when feature **requires** backend-specific behavior (e.g., transactions, performance optimizations)
- Backend can optionally override generic implementation for performance
- Must maintain semantic equivalence with generic version

See [CLAUDE.md](CLAUDE.md#trait-design) for detailed guidelines and examples.

## Development

### Prerequisites

- Rust 1.98+ (latest stable)
- Jujutsu (`cargo install jj-cli` or `brew install jj`)
- Docker (for PostgreSQL testcontainers)

### Setup

```bash
# Clone repository
jj git clone https://github.com/obayemi/event-sauce
cd event-sauce

# Run all tests (COMPREHENSIVE - includes all features, examples, bins, benches, doc tests)
cargo test --workspace --all-features --all-targets
cargo test --workspace --all-features --doc

# Build all examples, binaries, and benchmarks
cargo build --workspace --all-features --examples --bins --benches

# Check coverage (requires cargo-llvm-cov)
cargo install cargo-llvm-cov
cargo llvm-cov --workspace --all-features --all-targets

# Run clippy
cargo clippy --workspace --all-features --all-targets -- -D warnings

# Format code
cargo fmt --all
```

### Running Tests

```bash
# COMPREHENSIVE - All tests with all features, examples, binaries, and benches
cargo test --workspace --all-features --all-targets

# Doc tests (important - tests all documentation examples)
cargo test --workspace --all-features --doc

# Build examples (ensures all examples compile)
cargo build --workspace --all-features --examples

# Build binaries (ensures all binaries compile)
cargo build --workspace --all-features --bins

# Build benchmarks (ensures all benches compile)
cargo build --workspace --all-features --benches

# Specific crate with all features
cargo test -p event-sauce-core --all-features --all-targets

# With output
cargo test -- --nocapture

# Integration tests only
cargo test --test '*' --all-features

# PostgreSQL tests (requires Docker for testcontainers)
cargo test -p event-sauce-postgres --all-features --all-targets

# Run examples
cargo run --example postgres-quickstart --all-features  # Full-featured with PostgreSQL
cargo run --example apply-event --all-features          # Manual ApplyEvent implementation
```

**Note**: PostgreSQL tests use testcontainers to automatically start PostgreSQL in Docker. Ensure Docker is running before executing these tests.

### Coverage Reporting

```bash
# Generate coverage report (COMPREHENSIVE - includes all features, examples, bins, benches)
cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path coverage.lcov

# View HTML report
cargo llvm-cov --workspace --all-features --all-targets --html
open target/llvm-cov/html/index.html

# Summary only
cargo llvm-cov --workspace --all-features --all-targets --summary-only

# Specific crate
cargo llvm-cov -p event-sauce-core --all-features --all-targets --summary-only
```

**Current test status: 1220 tests across 6 crates**

| Crate | Tests | Line coverage |
|---|---|---|
| `event-sauce-core` | 755 | 93.7% |
| `event-sauce-memory` | 186 | 98.2% |
| `event-sauce-postgres` | 129 | 93.1% |
| `event-sauce-macros` | 121 | 79.6% |
| `event-sauce` | 16 | — (re-exports) |
| `event-sauce-crypto` | 13 | 99.4% |

CI enforces a 90% workspace line-coverage floor.

The PostgreSQL tests use [testcontainers](https://github.com/testcontainers/testcontainers-rs) to automatically spin up isolated PostgreSQL instances. Tests run automatically in CI and locally with Docker installed.


### Contributing

See [CLAUDE.md](CLAUDE.md) for detailed development guidelines.

All contributions must:

1. Follow TDD workflow (write tests first)
2. Keep workspace line coverage above the 90% CI floor (on all features, examples, bins, benches)
3. Pass all tests and clippy checks:
   - `cargo test --workspace --all-features --all-targets`
   - `cargo test --workspace --all-features --doc`
   - `cargo build --workspace --all-features --examples --bins --benches`
   - `cargo clippy --workspace --all-features --all-targets -- -D warnings`
4. Include documentation
5. Use Jujutsu for commits

## PostgreSQL Production Setup

Event-sauce provides PostgreSQL support with **schema isolation** to avoid migration conflicts.

### Using Builder Pattern (Recommended)

The builder pattern provides full control over configuration:

```rust,ignore
use event_sauce::postgres::PostgresEventStore;
use event_sauce::{SnapshotConfig, EveryNEvents};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure connection pool
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    // Build event store with custom configuration
    let store = PostgresEventStore::builder()
        .pool(pool)
        .schema("event_sauce")                      // Custom schema name
        .snapshot_config(SnapshotConfig::builder()
            .default_strategy(EveryNEvents::try_new(100).expect("100 != 0"))    // Snapshot every 100 events
            .build())
        .build()?;

    // Run migrations to create schema and tables
    store.migrate().await?;

    // Creates:
    // - event_sauce.events table
    // - event_sauce.snapshots table
    // - event_sauce._event_sauce_migrations (separate from your app!)

    Ok(())
}
```

### Simple Setup with Defaults

For quick setup, use the convenience constructor:

```rust,ignore
use event_sauce::postgres::PostgresEventStore;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;

    // Simple setup - uses "event_sauce" schema and default snapshot config
    let store = PostgresEventStore::new(pool);
    store.migrate().await?;

    Ok(())
}
```

**Why schema isolation?**

- ✅ Your app's migrations stay in `public._sqlx_migrations`
- ✅ Event-sauce migrations go to `event_sauce._event_sauce_migrations`
- ✅ Zero conflicts, clean separation, easy cleanup

📖 **[Full PostgreSQL Production Guide](docs/postgres-production.md)** - Connection pooling, snapshots, HA setup, monitoring, and more.

## Documentation

### Guides

- **[Getting Started Guide](docs/getting-started.md)** - Your first event-driven application
- **[State Storage](docs/state-storage.md)** - Choosing between the two storage options: event sourcing vs log-free state storage
- **[Events Guide](docs/events.md)** - Event definitions, init events, actor events, and validation
- **[Aggregates Guide](docs/aggregates.md)** - Aggregate design, type-state construction
- **[Validation Guide](docs/validation.md)** - Business rule validation and specification pattern
- **[Privacy & Encryption](docs/privacy.md)** - Full-aggregate and field-level encryption, crypto-shredding
- **[Projections](docs/projections.md)** - Building read models with transactional, durable delivery
- **[PostgreSQL Production Setup](docs/postgres-production.md)** - Complete production deployment guide
- **[Policies Guide](docs/policies.md)** - Cross-aggregate event orchestration, causation tracking, and error handling
- **[Claims Guide](docs/claims.md)** - Cross-aggregate uniqueness constraints
- **[Audit Log Guide](docs/audit-log.md)** - Event log queries, actor tracking, and causation tracing
- **[Architecture Overview](docs/architecture.md)** - System design and patterns
- **[TDD Workflow](docs/tdd-workflow.md)** - Test-driven development for event sourcing

### API Documentation

- [event-sauce](https://docs.rs/event-sauce) - The entry point: everything below is re-exported from here
- [event-sauce-core](https://docs.rs/event-sauce-core) - Core traits and types (includes `EventFilter` and `CheckpointStore` for projections and policies)
- [event-sauce-postgres](https://docs.rs/event-sauce-postgres) - PostgreSQL backend, re-exported as `event_sauce::postgres`

### Learn More

- **Examples** - See `crates/event-sauce/examples/` for complete applications:
  - **[postgres-quickstart.rs](crates/event-sauce/examples/postgres-quickstart.rs)** - Full-featured example with User and Order aggregates, projections, and PostgreSQL backend (requires `postgres` + `crypto`; needs Docker)
  - **[apply-event.rs](crates/event-sauce/examples/apply-event.rs)** - Bank account example with manual `ApplyEvent` trait implementation
  - **[actor-events.rs](crates/event-sauce/examples/actor-events.rs)** - Role-based permission validation with actor events
  - **[crypto-shredding.rs](crates/event-sauce/examples/crypto-shredding.rs)** - Full-aggregate encryption and right-to-be-forgotten (requires `memory` + `crypto`)
  - **[field-encryption.rs](crates/event-sauce/examples/field-encryption.rs)** - Selective field-level encryption for sensitive data (requires `memory` + `crypto`)
  - **[delete-events.rs](crates/event-sauce/examples/delete-events.rs)** - Type-state delete lifecycle with terminal state
  - **[policy.rs](crates/event-sauce/examples/policy.rs)** - Cross-aggregate event orchestration with causation tracking
  - **[save-all.rs](crates/event-sauce/examples/save-all.rs)** - Multi-aggregate atomic writes with `Repository::save_all`
  - **[state-stored-order.rs](crates/event-sauce/examples/state-stored-order.rs)** - The same aggregate against both `EventStore` and `StateStore` (requires `memory` + `state-store` + `event-sourcing`)
  - **[upcasting.rs](crates/event-sauce/examples/upcasting.rs)** - On-load schema migration for historical event payloads
  - **[projection-worker.rs](crates/event-sauce/examples/projection-worker.rs)** - Running a postgres-backed projection as its own worker process (requires `postgres`; needs Docker)
- **CLAUDE.md** - Development guidelines and principles

## Roadmap

- [x] Phase 0: Repository setup with Jujutsu
- [x] Phase 0.5: Workspace and crate structure
- [x] **Phase 1: Core traits and types (event-sauce-core)** - ✅ 755 tests
- [x] **Phase 2: In-memory implementation (event-sauce-memory)** - ✅ 186 tests
- [x] **Phase 3: PostgreSQL backend (event-sauce-postgres)** - ✅ 129 tests
- [x] **Phase 4: Derive macros (event-sauce-macros)** - ✅ 121 tests
- [x] **Phase 5: Event consumption primitives (event-sauce-core)** - ✅ Integrated into core (`CheckpointStore`, `EventFilter`)
- [x] **Phase 6: Examples and documentation** - ✅ 11 examples, comprehensive guides
- [x] **Phase 7: Correctness hardening** - ✅ all 30 audit findings in [ISSUES.md](ISSUES.md) fixed with regression tests
- [x] **Phase 7.5: Domain/persistence split** - ✅ `Repository` trait, state-stored persistence (`StateStore`, in-transaction projections, transactional outbox), feature flags
- [ ] Phase 8: v0.1.0 release

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

## Acknowledgments

Built with inspiration from:

- [Axum](https://github.com/tokio-rs/axum) - Ergonomic API design
- [SQLx](https://github.com/launchbadge/sqlx) - Compile-time SQL verification
- [Eventide](http://docs.eventide-project.org/) - Event sourcing patterns
- [Marten](https://martendb.io/) - Event store implementation patterns

---

**Status**: 🚧 In Development - Comprehensive feature set with documentation and examples. Not yet validated for production scalability.

Built with ❤️ and strict TDD in Rust
