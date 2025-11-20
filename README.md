# event-sauce

**Production-ready event sourcing for Rust** - Simple by default, powerful when needed.

[![CI](https://github.com/yourusername/event-sauce/workflows/CI/badge.svg)](https://github.com/yourusername/event-sauce/actions)
[![Coverage](https://codecov.io/gh/yourusername/event-sauce/branch/main/graph/badge.svg)](https://codecov.io/gh/yourusername/event-sauce)
[![Crates.io](https://img.shields.io/crates/v/event-sauce.svg)](https://crates.io/crates/event-sauce)
[![Documentation](https://docs.rs/event-sauce/badge.svg)](https://docs.rs/event-sauce)

## Features

- 🚀 **Modern Rust**: Built with latest stable dependencies (Tokio 1.48, SQLx 0.8, Clap 4.5)
- ✅ **100% Test Coverage**: Strict TDD with property-based testing
- 🔄 **Async Streaming**: Memory-efficient event processing with backpressure
- 🗄️ **Multiple Backends**: PostgreSQL, in-memory
- 🎯 **Type-Safe**: Compile-time guarantees with derive macros
- 📦 **Production Ready**: Optimistic concurrency, snapshots, distributed locking
- 🧪 **Testing First-Class**: Built-in test helpers and fixtures
- 🛡️ **Rich Validation**: Aggregate-specific errors with business rule enforcement
- ⚡ **Optimized Replay**: Fast event replay without re-validation

### Modern Event Sourcing Features

event-sauce provides a powerful and ergonomic event sourcing experience with minimal boilerplate:

- **📝 Auto-Generated Boilerplate**: Derive macros eliminate manual implementations
  - `#[derive(AggregateId)]` - Auto-implement ID types with Display
  - `#[derive(AggregateError)]` - Auto-implement error marker trait
  - `#[aggregate(...)]` - Auto-manage ID, separate business logic from infrastructure
  - `#[derive(Event)]` - Auto-generate apply_event dispatching
  - `command_handler!` - **NEW!** Auto-generate command methods (70% less code)
  - `projection!` - **NEW!** Declarative read model definitions
- **🎯 Type-Safe Events**: `ApplyEvent` trait for self-contained event logic
- **✅ Validation & Replay**: Separate validation from application for fast replay
- **🛡️ Rich Errors**: Aggregate-specific error types with `AggregateError` trait
- **⚡ Zero-Cost Abstractions**: All macro-generated code optimizes away
- **🏗️ Repository Pattern**: High-level `Repository<S, A>` abstraction for clean aggregate operations

## Quick Start

```rust
use event_sauce::prelude::*;
use event_sauce_macros::{AggregateError, AggregateId, AggregateState, Event};
use thiserror::Error;
use uuid::Uuid;
use chrono::Utc;

// 1. Define your aggregate ID (auto-implements AggregateId + Display)
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CounterId(Uuid);

impl CounterId {
    fn new() -> Self { Self(Uuid::new_v4()) }
}

impl Default for CounterId {
    fn default() -> Self { Self(Uuid::nil()) }
}

// 2. Define domain-specific errors (auto-implements AggregateError)
#[derive(AggregateError, Debug, Error)]
enum CounterError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i32),
}

// 3. Define individual events
#[derive(Debug, Clone)]
struct Incremented { amount: i32, timestamp: DateTime<Utc> }

#[derive(Debug, Clone)]
struct Decremented { amount: i32, timestamp: DateTime<Utc> }

// 4. Define event enum (auto-generates apply_event method)
#[derive(Event, Debug, Clone)]
#[event(version = 1, type_prefix = "Counter", aggregate = "Counter")]
enum CounterEvent {
    Incremented(Incremented),
    Decremented(Decremented),
}

// 5. Implement ApplyEvent for each event on the Aggregate
impl ApplyEvent<Counter, CounterError> for Incremented {
    fn validate(&self, _: &Counter) -> Result<(), CounterError> {
        if self.amount <= 0 {
            return Err(CounterError::InvalidAmount(self.amount));
        }
        Ok(())
    }

    fn apply(&self, counter: &mut Counter) {
        counter.value += self.amount;  // Access via Deref to state
    }
}

impl ApplyEvent<Counter, CounterError> for Decremented {
    fn apply(&self, counter: &mut Counter) {
        counter.value -= self.amount;
    }
}

// 6. Define your aggregate (business data only - ID is auto-managed!)
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
#[derive(Default)]
struct Counter {
    value: i32,  // Only business fields - no ID needed!
}

// 7. Implement business logic using command_handler! macro (recommended)
use event_sauce::command_handler;

impl Counter {
    fn create(id: CounterId) -> Self {
        Self::new(id)  // ID automatically stored in wrapper
    }
}

// Generate command methods automatically - reduces boilerplate by ~70%!
command_handler! {
    impl Counter {
        fn increment(amount: i32) -> Incremented { amount };
        fn decrement(amount: i32) -> Decremented { amount };
    }
}

// Without macro (manual approach):
// impl Counter {
//     fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
//         let event = Incremented { amount, timestamp: Utc::now() };
//         self.apply(event)?;  // Validation happens in ApplyEvent
//         Ok(())
//     }
// }
```

**Key Points:**
- ✅ No manual `apply_event` - auto-generated by `#[event(aggregate = "...")]`
- ✅ No manual ID field - automatically managed by `#[aggregate(...)]` macro
- ✅ No manual command methods - auto-generated by `command_handler!` macro
- ✅ Separation of concerns: Business state vs Infrastructure (ID, version, events)
- ✅ Type-safe event handling with `ApplyEvent` trait
- ✅ Validation separate from application (fast replay)
- ✅ **70% less boilerplate** with macro-driven development

### Building Projections (Read Models)

EventSauce provides the `projection!` macro for declarative read model creation:

```rust
use event_sauce::projection;
use std::collections::HashMap;

// Define your read model state
#[derive(Debug, Clone)]
struct UserView {
    email: String,
    name: String,
    status: UserStatus,
}

// Create a projection declaratively - type-safe and clean!
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

// Use with subscriptions for automatic updates:
let mut projection = UserListProjection::new(HashMap::new());
let subscription = event_store
    .subscription_builder("user-projection")
    .build()?;

let stream = subscription.into_stream().await?;
tokio::pin!(stream);

while let Some(result) = stream.next().await {
    let envelope = result?;
    projection.handle(&envelope).await?;  // Type-safe event handling
    // Checkpoints saved automatically!
}
```

**Benefits of `projection!` macro:**
- ✅ **Declarative syntax**: Clear event-to-handler mapping
- ✅ **Type-safe**: Automatic deserialization with compile-time checks
- ✅ **Clean code**: No boilerplate event matching
- ✅ **Automatic filtering**: Unknown events are safely ignored
- ✅ **Composable**: Works seamlessly with subscriptions

**Why subscriptions?**
- ✅ **Integrated setup**: Checkpoint store configured once with event store
- ✅ **Stream API**: Composable with futures, type-safe, familiar async patterns
- ✅ **Builder pattern**: Fluent configuration with automatic checkpoint injection
- ✅ **Guaranteed delivery**: Events never lost, resumable after failures

See the [task-projections example](crates/event-sauce/examples/task-projections.rs) for a complete working example.

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
- `full` - All features enabled

## Development Philosophy

### Test-Driven Development (TDD)

This library is built following **strict TDD principles**:

- ✅ **100% test coverage** - No exceptions, enforced by CI
- ✅ **Tests written first** - Every feature starts with a failing test
- ✅ **Living documentation** - Tests demonstrate API usage
- ✅ **Property-based testing** - Invariants proven with proptest 1.9
- ✅ **Integration tests** - Real database testing with PostgreSQL

#### TDD Workflow

```bash
# 1. Write failing test (RED)
cargo test test_new_feature --all-features -- --nocapture
# Should FAIL

# 2. Implement feature (GREEN)
# ... edit src/ ...
cargo test test_new_feature --all-features
# Should PASS

# 3. Check coverage (must be 100% - includes all features and targets)
cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path coverage.lcov

# 4. Refactor while keeping tests green
cargo test --workspace --all-features --all-targets
cargo test --workspace --all-features --doc
cargo build --workspace --all-features --examples --bins --benches
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

### Latest Dependencies

We maintain up-to-date dependencies for security, performance, and features:

| Dependency | Version | Purpose |
|------------|---------|---------|
| tokio | 1.48.0 | Async runtime |
| sqlx | 0.8.6 | Database access |
| clap | 4.5.51 | CLI framework |
| serde | 1.0.228 | Serialization |
| syn | 2.0.104 | Proc macros |
| thiserror | 2.0.16 | Error handling |
| uuid | 1.18.1 | Unique identifiers |
| proptest | 1.9.0 | Property testing |

Check for updates:
```bash
cargo update --workspace
cargo outdated
```

### Version Control with Jujutsu

This project uses [Jujutsu (jj)](https://github.com/martinvonz/jj) for version control:

```bash
# Clone with jj
jj git clone https://github.com/yourusername/event-sauce
cd event-sauce

# Create a new change
jj new -m "Add feature X"

# View status
jj status
jj diff

# Commit current change
jj commit -m "Implement feature X with tests"

# View history
jj log

# Push to remote
jj git push
```

#### Why Jujutsu?

- **Automatic tracking**: Changes are automatically tracked
- **Flexible history**: Easy to rewrite and reorganize commits
- **Git compatible**: Works with GitHub and other Git hosting
- **Better UX**: More intuitive than git rebase/cherry-pick

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Application                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Services   │  │  Aggregates  │  │  Projections │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────┬────────────────┬──────────────────┬───────────┘
             │                │                   │
┌────────────┴────────────────┴───────────────────┴───────────┐
│                     event-sauce Core                         │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │Aggregate│  │  Event   │  │EventStore│  │Subscription│  │
│  │  Trait  │  │  Trait   │  │  Trait   │  │   System   │  │
│  └─────────┘  └──────────┘  └──────────┘  └────────────┘  │
└────────────┬────────────────┬──────────────────┬───────────┘
             │                │                   │
┌────────────┴────────────────┴───────────────────┴───────────┐
│                      Backends                                │
│           ┌──────────┐            ┌──────────┐              │
│           │PostgreSQL│            │ In-Memory│              │
│           └──────────┘            └──────────┘              │
└──────────────────────────────────────────────────────────────┘
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

## Examples

### Available Examples

The [`crates/event-sauce/examples/`](crates/event-sauce/examples/) directory contains complete, runnable examples:

#### 1. Counter - Basic Event Sourcing
Simple counter demonstrating fundamental concepts:
- Aggregate with state
- Events and commands
- Event replay
- Business rules and validation

```bash
cargo run -p event-sauce --example counter --features "memory,macros"
```

#### 2. Shopping Cart - Complex Aggregate
E-commerce cart with multiple operations:
- Multiple item management
- Price calculations
- Business rules (checkout, inventory)
- Complex state (HashMap)

```bash
cargo run -p event-sauce --example shopping-cart --features "memory,macros"
```

#### 3. Bank Account - Complete Flow
Full event sourcing workflow:
- Using derive macros
- In-memory event store
- Deposits, withdrawals, transfers
- Overdraft protection

```bash
cargo run -p event-sauce --example bank-account --features "memory,macros"
```

#### 4. Task Projections - Read Models
Projection building demonstration:
- Multiple projections from same events
- Checkpointing for resumability
- Event filtering
- Real-time updates

```bash
cargo run -p event-sauce --example task-projections --features "memory"
```

### Complete Minimal Example

Here's a complete, working example showing modern best practices:

```rust
use event_sauce_macros::{AggregateError, AggregateId, AggregateState, Event};
use event_sauce_core::{Aggregate, ApplyEvent};
use chrono::{DateTime, Utc};
use uuid::Uuid;

// 1. Aggregate ID (auto-implements AggregateId + Display)
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CounterId(Uuid);

impl CounterId {
    fn new() -> Self { Self(Uuid::new_v4()) }
}

impl Default for CounterId {
    fn default() -> Self { Self(Uuid::nil()) }
}

// 2. Domain errors (auto-implements AggregateError)
#[derive(AggregateError, Debug, thiserror::Error)]
enum CounterError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i32),
}

// 3. Individual event types
#[derive(Debug, Clone)]
struct Incremented { amount: i32, timestamp: DateTime<Utc> }

// 4. Event enum (auto-generates apply_event)
#[derive(Event, Debug, Clone)]
#[event(version = 1, type_prefix = "Counter", aggregate = "Counter")]
enum CounterEvent {
    Incremented(Incremented),
}

// 5. Event application logic
impl ApplyEvent<Counter, CounterError> for Incremented {
    fn validate(&self, _: &Counter) -> Result<(), CounterError> {
        if self.amount <= 0 {
            return Err(CounterError::InvalidAmount(self.amount));
        }
        Ok(())
    }

    fn apply(&self, counter: &mut Counter) {
        counter.value += self.amount; // Access state via Deref
    }
}

// 6. Aggregate (business data only - ID auto-managed!)
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
#[derive(Default)]
struct Counter {
    value: i32,  // No ID field needed - macro handles it!
}

// 7. Business logic
impl Counter {
    fn create(id: CounterId) -> Self {
        Self::new(id)  // ID stored in infrastructure layer
    }

    fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = Incremented { amount, timestamp: Utc::now() };
        event.validate(self)?;
        self.apply(event); // Auto-records in pending_events
        Ok(())
    }
}

fn main() -> Result<(), CounterError> {
    let mut counter = Counter::create(CounterId::new());
    counter.increment(5)?;
    counter.increment(3)?;

    println!("Value: {}", counter.value);           // 8
    println!("Version: {}", counter.version());     // v2
    println!("Events: {}", counter.pending_events().len()); // 2

    Ok(())
}
```

**Why this pattern?**
- 🎯 **No boilerplate**: Macros generate all infrastructure code
- 🆔 **Auto-managed ID**: No manual ID field needed - macro handles it
- 🔄 **Separation**: Business state separate from infrastructure (ID, version, events)
- ✅ **Type-safe**: ApplyEvent ensures each event targets correct aggregate
- ⚡ **Fast replay**: Validation skipped during `apply_unchecked()`

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

# Run an example
cargo run --example counter --all-features
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

**Current Test Status: across 5 crates**
- Core: 180 tests, 100% coverage
- Memory: 33 tests, 100% coverage
- PostgreSQL: 39 tests (uses testcontainers - requires Docker)
- Macros: 78 tests (Aggregate, Event, AggregateState, UI tests)
- CLI: 23 tests (init, generate, db commands)

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

## CLI Tool

The `event-sauce` CLI provides project scaffolding and code generation:

### Installation

```bash
cargo install event-sauce-cli
```

### Commands

**Initialize a new project:**
```bash
# Create a new project with PostgreSQL backend
event-sauce init my-project --backend postgres

# Create with in-memory backend (for testing)
event-sauce init my-project --backend memory
```

**Generate code:**
```bash
# Generate an aggregate
event-sauce generate aggregate BankAccount

# Generate events for an aggregate
event-sauce generate event AccountOpened --aggregate BankAccount

# Generate a projection
event-sauce generate projection AccountBalance --events AccountOpened,FundsDeposited,FundsWithdrawn
```

**Database management:**
```bash
# Display PostgreSQL schema
event-sauce db init --backend postgres

# For memory backend (no-op)
event-sauce db init --backend memory
```

### Help

```bash
# Show all commands
event-sauce --help

# Show help for specific command
event-sauce generate --help
event-sauce init --help
```

## PostgreSQL Production Setup

Event-sauce provides production-ready PostgreSQL support with **schema isolation** to avoid migration conflicts:

```rust
use event_sauce_postgres::PostgresEventStore;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;

    // Simple setup with schema isolation (recommended)
    let store = PostgresEventStore::new(pool);
    store.migrate().await?;

    // Creates:
    // - event_sauce.events table
    // - event_sauce.snapshots table
    // - event_sauce._event_sauce_migrations (separate from your app!)

    Ok(())
}
```

### Builder Pattern for Custom Configuration

```rust
use event_sauce_postgres::PostgresEventStore;
use event_sauce_core::{SnapshotConfig, EveryNEvents};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

let pool = PgPoolOptions::new()
    .max_connections(50)
    .acquire_timeout(Duration::from_secs(30))
    .connect(&database_url)
    .await?;

let store = PostgresEventStore::builder()
    .pool(pool)
    .schema("event_sauce")                      // Custom schema name
    .snapshot_config(SnapshotConfig::builder()
        .default_strategy(EveryNEvents(100))    // Snapshot every 100 events
        .build())
    .build();

store.migrate().await?;
```

**Why schema isolation?**
- ✅ Your app's migrations stay in `public._sqlx_migrations`
- ✅ Event-sauce migrations go to `event_sauce._event_sauce_migrations`
- ✅ Zero conflicts, clean separation, easy cleanup

📖 **[Full PostgreSQL Production Guide](docs/postgres-production.md)** - Connection pooling, snapshots, HA setup, monitoring, and more.

## Documentation

### Guides

- **[Getting Started Guide](docs/getting-started.md)** - Your first event-sourced application
- **[Projections & Subscriptions](docs/projections.md)** - 🆕 Building read models with durable, guaranteed delivery
- **[PostgreSQL Production Setup](docs/postgres-production.md)** - Complete production deployment guide
- **[Architecture Overview](docs/architecture.md)** - System design and patterns
- **[TDD Workflow](docs/tdd-workflow.md)** - Test-driven development for event sourcing

### API Documentation

- [event-sauce (facade)](https://docs.rs/event-sauce) - Main entry point
- [event-sauce-core](https://docs.rs/event-sauce-core) - Core traits and types (includes Subscription system for projections)
- [event-sauce-postgres](https://docs.rs/event-sauce-postgres) - PostgreSQL backend

### Learn More

- **Examples** - See `crates/event-sauce/examples/` for complete applications
- **Tests** - 231 tests show how to use every feature
- **CLAUDE.md** - Development guidelines and principles

## Roadmap

- [x] Phase 0: Repository setup with Jujutsu
- [x] Phase 0.5: Workspace and crate structure
- [x] **Phase 1: Core traits and types (event-sauce-core)** - ✅ 180 tests, 100% coverage
- [x] **Phase 2: In-memory implementation (event-sauce-memory)** - ✅ 33 tests, 100% coverage
- [x] **Phase 3: PostgreSQL backend (event-sauce-postgres)** - ✅ 39 tests
- [x] **Phase 4: Derive macros (event-sauce-macros)** - ✅ 78 tests
- [x] **Phase 5: Subscription system (event-sauce-core)** - ✅ Integrated into core (Subscription, CheckpointStore, EventFilter)
- [x] **Phase 6: CLI tooling (event-sauce-cli)** - ✅ 23 tests (init, generate, db commands)
- [x] **Phase 7: Examples and documentation** - ✅ 5 examples, comprehensive guides
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

**Status**: 🚀 Phase 7 Complete - Production-ready with comprehensive documentation and examples!

Built with ❤️ and strict TDD in Rust
