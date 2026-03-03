# event-sauce

**Event sourcing for Rust** - Simple by default, powerful when needed.

> **Note**: event-sauce is **not production-ready**. The current architecture targets single-node deployments and has not been validated for horizontal scalability or high-throughput distributed workloads. Use it for prototyping, learning, and small-scale applications.

[![CI](https://github.com/yourusername/event-sauce/workflows/CI/badge.svg)](https://github.com/yourusername/event-sauce/actions)
[![Coverage](https://codecov.io/gh/yourusername/event-sauce/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/event-sauce)
[![Crates.io](https://img.shields.io/crates/v/event-sauce.svg)](https://crates.io/crates/event-sauce)
[![Documentation](https://docs.rs/event-sauce/badge.svg)](https://docs.rs/event-sauce)

## Features

- 🚀 **Modern Rust**: Built with latest stable dependencies (Tokio 1.48, SQLx 0.8)
- ✅ **100% Test Coverage**: Strict TDD with property-based testing
- 🔄 **Async Streaming**: Memory-efficient event processing with backpressure
- 🗄️ **Multiple Backends**: PostgreSQL, in-memory
- 🎯 **Type-Safe**: Compile-time guarantees with derive macros
- 📦 **Battle-Tested Patterns**: Optimistic concurrency, snapshots, distributed locking
- 🧪 **Testing First-Class**: Built-in test helpers and fixtures
- 🛡️ **Rich Validation**: Aggregate-specific errors with business rule enforcement
- 🧩 **Specification Pattern**: Composable, reusable business rules with AND/OR/NOT combinators
- ⚡ **Optimized Replay**: Fast event replay without re-validation
- 🔐 **Encryption & Crypto-Shredding**: Full-aggregate or field-level encryption with pluggable providers
- 🚪 **Init Events**: Type-state aggregate construction — no more invalid uninitialized states
- 👤 **Actor Events**: Permission validation tied to actor identity, with automatic audit trails
- 🔗 **Policies**: Cross-aggregate event orchestration with causation tracking, checkpoint-based resumption, and configurable error handling

### Modern Event Sourcing Features

event-sauce provides a powerful and ergonomic event sourcing experience with minimal boilerplate:

- **📝 Declarative Macros**: Build event-sourced aggregates with minimal code
  - `#[derive(AggregateId)]` - ID types with Display
  - `#[derive(AggregateError)]` - Error marker trait
  - `#[aggregate_error(...)]` - Error type with spec support
  - `#[specification(...)]` - Specification struct generation
  - `#[aggregate(...)]` - Aggregate infrastructure management
  - `#[derive(Event)]` - Event dispatching
  - `define_events!` - Event definitions with validation
  - `command_handler!` - Command method generation
  - `projection!` - Read model definitions
- **🎯 Type-Safe Events**: `ApplyEvent` trait for self-contained event logic
- **✅ Validation & Replay**: Separate validation from application for fast replay
- **🛡️ Rich Errors**: Aggregate-specific error types with `AggregateError` trait
- **⚡ Zero-Cost Abstractions**: All macro-generated code optimizes away
- **🏗️ Repository Pattern**: High-level `Repository<S, A>` abstraction for clean aggregate operations

## Quick Start

```rust
use event_sauce::prelude::*;
use event_sauce_macros::{AggregateError, AggregateId};
use thiserror::Error;
use uuid::Uuid;

// 1. Define your aggregate ID
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
struct CounterId(Uuid);

// 2. Define domain errors
#[derive(AggregateError, Debug, Error)]
enum CounterError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i32),
}

// 3. Define your aggregate with business state
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
#[derive(Default)]
struct Counter {
    value: i32,
}

// 4. Define events with validation and apply logic
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

// 5. Implement business logic
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

That's it! Your aggregate is ready to use with full event sourcing capabilities.

### Init Events (Type-State Construction)

Use `@init` events to enforce that aggregates are always constructed through a valid creation event — no more `Default` with invalid states:

```rust
#[aggregate(id = "OrderId", event = "OrderEvent", error = "OrderError", init)]
struct Order {
    order_id: String,
    status: OrderStatus,
}

define_events! {
    pub enum OrderEvent for Order {
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
            Order { order_id: event.order_id.clone(), status: OrderStatus::Pending }
        },

        Completed {} => |order, _event| {
            order.status = OrderStatus::Completed;
        },
    }
}

command_handler! {
    impl Order {
        @init fn create_order(order_id: String) -> CreatedEvent { order_id };
        fn complete() -> CompletedEvent {};
    }
}

// Creation returns AggregateRoot<Order> directly
let mut order = Order::create_order("ORD-001".into())?;
order.complete()?;
```

Multiple init variants are supported for different creation paths (e.g., `CreatedByAdmin` vs `CreatedByInvite`).

### Actor Events (Permission Validation)

Require an actor (e.g., a `User`) for specific commands, with automatic permission validation and audit trail:

```rust
define_events! {
    pub enum DocumentEvent for Document {
        ContentUpdated {
            content: String,
        }
        @actor(User)
        @validate |_doc, actor, _evt| {
            if actor.state.role == Role::Viewer {
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
        @actor(User) fn update_content(content: String) -> ContentUpdatedEvent { content };
    }
}

// Actor required at command time — validated, then stored in EventEnvelope::created_by
doc.update_content(&editor, "new content".into())?;
```

Actor validation is **skipped during replay** — events are historical facts, only fresh commands validate permissions.

### Policies (Cross-Aggregate Orchestration)

React to events on one aggregate to trigger commands on another, with full causation tracking and checkpoint-based resumption:

```rust
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

```rust
#[aggregate(event = "UserEvent", error = "UserError", encrypted)]
struct User {
    name: String,
    email: String,
}
```

**Field-level encryption** — encrypt only sensitive fields, keep others queryable:

```rust
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

All store builders include **AES-256-GCM encryption** by default — no extra setup needed.

**Crypto-shredding** (right to be forgotten) — delete the key to make data permanently unreadable:

```rust
store.crypto_key_store()
    .expect("crypto key store configured")
    .delete_key(user_id.as_uuid()).await?;
// Subsequent loads return Error::KeyNotFound
```

See the **[Privacy & Encryption Guide](docs/privacy.md)** for full details.

### Building Projections (Read Models)

Build type-safe read models with the `projection!` macro:

```rust
use event_sauce::projection;
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct UserView {
    email: String,
    name: String,
    status: UserStatus,
}

projection! {
    pub struct UserListProjection {
        state: HashMap<UserId, UserView>,

        on "UserCreated" => UserCreatedEvent |proj, event| {
            proj.state.insert(event.user_id, UserView {
                email: event.email.clone(),
                name: event.name.clone(),
                status: UserStatus::Active,
            });
        },

        on "UserNameChanged" => UserNameChangedEvent |proj, event| {
            if let Some(user) = proj.state.get_mut(&event.user_id) {
                user.name = event.new_name.clone();
            }
        },

        on "UserDeleted" => UserDeletedEvent |proj, event| {
            proj.state.remove(&event.user_id);
        },
    }
}

// Use with subscriptions for automatic updates
let mut projection = UserListProjection::new(HashMap::new());
let subscription = event_store
    .subscription_builder("user-projection")
    .build()?;

let stream = subscription.into_stream().await?;
tokio::pin!(stream);

while let Some(result) = stream.next().await {
    let envelope = result?;
    projection.handle(&envelope).await?;
}
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
event-sauce = "0.1"

# With specific features
event-sauce = { version = "0.1", features = ["postgres"] }
```

### Feature Flags

- `macros` (default) - Derive macros for aggregates and events
- `memory` (default) - In-memory backend for testing
- `postgres` - PostgreSQL backend
- `crypto` - Encryption support (AES-256-GCM provider, key stores)
- `full` - All features enabled

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                        Application                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Services   │  │  Aggregates  │  │  Projections │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────┬────────────────┬───────────────────┬─────────┘
             │                │                   │
┌────────────┴────────────────┴───────────────────┴─────────┐
│                     event-sauce Core                      │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │Aggregate│  │  Event   │  │EventStore│  │Subscription│  │
│  │  Trait  │  │  Trait   │  │  Trait   │  │   System   │  │
│  └─────────┘  └──────────┘  └──────────┘  └────────────┘  │
└────────────┬────────────────┬───────────────────┬─────────┘
             │                │                   │
┌────────────┴────────────────┴───────────────────┴──────────┐
│                      Backends                              │
│           ┌──────────┐            ┌──────────┐             │
│           │PostgreSQL│            │ In-Memory│             │
│           └──────────┘            └──────────┘             │
└────────────────────────────────────────────────────────────┘
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

- Rust 1.75+ (latest stable recommended)
- Jujutsu (`cargo install jj-cli` or `brew install jj`)
- Docker (for PostgreSQL testcontainers)

### Setup

```bash
# Clone repository
jj git clone https://github.com/yourusername/event-sauce
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

**Current Test Status: across 4 crates**

- Core: 180 tests, 100% coverage
- Memory: 33 tests, 100% coverage
- PostgreSQL: 39 tests (uses testcontainers - requires Docker)
- Macros: 78 tests (Aggregate, Event, AggregateState, UI tests)

The PostgreSQL tests use [testcontainers](https://github.com/testcontainers/testcontainers-rs) to automatically spin up isolated PostgreSQL instances. Tests run automatically in CI and locally with Docker installed.

CI enforces minimum 95% coverage - all PRs must maintain this standard.

### Contributing

See [CLAUDE.md](CLAUDE.md) for detailed development guidelines.

All contributions must:

1. Follow TDD workflow (write tests first)
2. Maintain 100% code coverage (on all features, examples, bins, benches)
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

```rust
use event_sauce_postgres::PostgresEventStore;
use event_sauce_core::{SnapshotConfig, EveryNEvents};
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
            .default_strategy(EveryNEvents(100))    // Snapshot every 100 events
            .build())
        .build();

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

```rust
use event_sauce_postgres::PostgresEventStore;
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

- **[Getting Started Guide](docs/getting-started.md)** - Your first event-sourced application
- **[Events Guide](docs/events.md)** - Event definitions, init events, actor events, and validation
- **[Aggregates Guide](docs/aggregates.md)** - Aggregate design, type-state construction
- **[Validation Guide](docs/validation.md)** - Business rule validation and specification pattern
- **[Privacy & Encryption](docs/privacy.md)** - Full-aggregate and field-level encryption, crypto-shredding
- **[Projections & Subscriptions](docs/projections.md)** - Building read models with durable, guaranteed delivery
- **[PostgreSQL Production Setup](docs/postgres-production.md)** - Complete production deployment guide
- **[Policies Guide](docs/policies.md)** - Cross-aggregate event orchestration, causation tracking, and error handling
- **[Architecture Overview](docs/architecture.md)** - System design and patterns
- **[TDD Workflow](docs/tdd-workflow.md)** - Test-driven development for event sourcing

### API Documentation

- [event-sauce (facade)](https://docs.rs/event-sauce) - Main entry point
- [event-sauce-core](https://docs.rs/event-sauce-core) - Core traits and types (includes Subscription system for projections)
- [event-sauce-postgres](https://docs.rs/event-sauce-postgres) - PostgreSQL backend

### Learn More

- **Examples** - See `crates/event-sauce/examples/` for complete applications:
  - **[postgres-quickstart.rs](crates/event-sauce/examples/postgres-quickstart.rs)** - Full-featured example with User and Order aggregates, projections, and PostgreSQL backend
  - **[apply-event.rs](crates/event-sauce/examples/apply-event.rs)** - Bank account example with manual `ApplyEvent` trait implementation
  - **[actor-events.rs](crates/event-sauce/examples/actor-events.rs)** - Role-based permission validation with actor events
  - **[crypto-shredding.rs](crates/event-sauce/examples/crypto-shredding.rs)** - Full-aggregate encryption and right-to-be-forgotten
  - **[field-encryption.rs](crates/event-sauce/examples/field-encryption.rs)** - Selective field-level encryption for sensitive data
  - **[delete-events.rs](crates/event-sauce/examples/delete-events.rs)** - Type-state delete lifecycle with terminal state
  - **[policy.rs](crates/event-sauce/examples/policy.rs)** - Cross-aggregate event orchestration with causation tracking
- **CLAUDE.md** - Development guidelines and principles

## Roadmap

- [x] Phase 0: Repository setup with Jujutsu
- [x] Phase 0.5: Workspace and crate structure
- [x] **Phase 1: Core traits and types (event-sauce-core)** - ✅ 180 tests, 100% coverage
- [x] **Phase 2: In-memory implementation (event-sauce-memory)** - ✅ 33 tests, 100% coverage
- [x] **Phase 3: PostgreSQL backend (event-sauce-postgres)** - ✅ 39 tests
- [x] **Phase 4: Derive macros (event-sauce-macros)** - ✅ 78 tests
- [x] **Phase 5: Subscription system (event-sauce-core)** - ✅ Integrated into core (Subscription, CheckpointStore, EventFilter)
- [x] **Phase 6: Examples and documentation** - ✅ 5 examples, comprehensive guides
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
