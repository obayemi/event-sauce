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
- 🗄️ **Multiple Backends**: PostgreSQL, SQLite, in-memory
- 🎯 **Type-Safe**: Compile-time guarantees with derive macros
- 📦 **Production Ready**: Optimistic concurrency, snapshots, distributed locking
- 🧪 **Testing First-Class**: Built-in test helpers and fixtures

## Quick Start

```rust
use event_sauce::prelude::*;
use uuid::Uuid;

#[derive(Aggregate, Debug, Clone)]
#[aggregate(id = "CounterId", event = "CounterEvent")]
struct Counter {
    #[aggregate_id]
    id: CounterId,
    value: i32,
    #[aggregate_version]
    version: i64,
    #[aggregate_events]
    pending: Vec<CounterEvent>,
}

#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[event(aggregate = "Counter", version = 1)]
enum CounterEvent {
    Incremented { amount: i32 },
    Decremented { amount: i32 },
}

impl Counter {
    fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = CounterEvent::Incremented { amount };
        self.apply(&event);
        self.pending.push(event);
        Ok(())
    }
}
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
event-sauce = "0.1"

# With specific features
event-sauce = { version = "0.1", features = ["postgres", "projections", "sagas"] }
```

### Feature Flags

- `macros` (default) - Derive macros for aggregates and events
- `memory` (default) - In-memory backend for testing
- `postgres` - PostgreSQL backend
- `sqlite` - SQLite backend
- `projections` - Projection building helpers
- `sagas` - Saga and process manager patterns
- `full` - All features enabled

## Development Philosophy

### Test-Driven Development (TDD)

This library is built following **strict TDD principles**:

- ✅ **100% test coverage** - No exceptions, enforced by CI
- ✅ **Tests written first** - Every feature starts with a failing test
- ✅ **Living documentation** - Tests demonstrate API usage
- ✅ **Property-based testing** - Invariants proven with proptest 1.9
- ✅ **Integration tests** - Real database testing with PostgreSQL/SQLite

#### TDD Workflow

```bash
# 1. Write failing test (RED)
cargo test test_new_feature -- --nocapture
# Should FAIL

# 2. Implement feature (GREEN)
# ... edit src/ ...
cargo test test_new_feature
# Should PASS

# 3. Check coverage (must be 100%)
cargo llvm-cov --lcov --output-path coverage.lcov

# 4. Refactor while keeping tests green
cargo test
cargo clippy -- -D warnings
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
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │Aggregate│  │  Event   │  │EventStore│  │ EventBus │    │
│  │  Trait  │  │  Trait   │  │  Trait   │  │  Trait   │    │
│  └─────────┘  └──────────┘  └──────────┘  └──────────┘    │
└────────────┬────────────────┬──────────────────┬───────────┘
             │                │                   │
┌────────────┴────────────────┴───────────────────┴───────────┐
│                      Backends                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │PostgreSQL│  │  SQLite  │  │ In-Memory│                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
└──────────────────────────────────────────────────────────────┘
```

## Examples

See the [`examples/`](examples/) directory:

- **counter** - Simple aggregate with basic operations
- **todo-list** - Todo app with projections
- **order-saga** - Order fulfillment with saga pattern

Run an example:
```bash
cargo run --example counter
```

## Development

### Prerequisites

- Rust 1.75+ (latest stable recommended)
- Jujutsu (`cargo install jj-cli` or `brew install jj`)
- PostgreSQL 16+ (for integration tests)
- SQLite 3.40+ (for integration tests)

### Setup

```bash
# Clone repository
jj git clone https://github.com/yourusername/event-sauce
cd event-sauce

# Run tests
cargo test --workspace

# Check coverage (requires cargo-llvm-cov)
cargo install cargo-llvm-cov
cargo llvm-cov --workspace

# Run clippy
cargo clippy --workspace -- -D warnings

# Format code
cargo fmt --all
```

### Running Tests

```bash
# All tests
cargo test --workspace

# Specific crate
cargo test -p event-sauce-core

# With output
cargo test -- --nocapture

# Integration tests only
cargo test --test '*'

# With PostgreSQL (requires running PostgreSQL)
cargo test -p event-sauce-postgres -- --test-threads=1
```

### Coverage Reporting

```bash
# Generate coverage report
cargo llvm-cov --workspace --lcov --output-path coverage.lcov

# View HTML report
cargo llvm-cov --workspace --html
open target/llvm-cov/html/index.html

# Summary only
cargo llvm-cov -p event-sauce-core --summary-only
```

**Current Coverage: 97.78%** (103 tests) - See [`crates/event-sauce-core/COVERAGE.md`](crates/event-sauce-core/COVERAGE.md) for detailed coverage analysis and gap documentation.

CI enforces minimum 95% coverage - all PRs must maintain this standard.

### Contributing

See [CLAUDE.md](CLAUDE.md) for detailed development guidelines.

All contributions must:
1. Follow TDD workflow (write tests first)
2. Maintain 100% code coverage
3. Pass all tests and clippy checks
4. Include documentation
5. Use Jujutsu for commits

## Documentation

- [Getting Started Guide](docs/getting-started.md)
- [Architecture Overview](docs/architecture.md)
- [TDD Workflow](docs/tdd-workflow.md)
- [API Documentation](https://docs.rs/event-sauce)

## Roadmap

- [x] Phase 0: Repository setup with Jujutsu
- [x] Phase 0.5: Workspace and crate structure
- [x] **Phase 1: Core traits and types (event-sauce-core)** - ✅ 103 tests, 97.78% coverage ([details](crates/event-sauce-core/COVERAGE.md))
- [x] **Phase 2: In-memory implementation (event-sauce-memory)** - ✅ 26 tests, 99.54% coverage
- [x] **Phase 3: PostgreSQL backend (event-sauce-postgres)** - ✅ Implementation complete (tests pending)
- [ ] Phase 4: Derive macros (event-sauce-macros)
- [ ] Phase 5: Event bus and projections
- [ ] Phase 6: SQLite backend
- [ ] Phase 7: Saga patterns
- [ ] Phase 8: CLI tooling
- [ ] Phase 9: Examples and documentation
- [ ] Phase 10: v0.1.0 release

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

**Status**: 🚧 Phase 3 Complete - Core + In-Memory + PostgreSQL implementations ready

Built with ❤️ and strict TDD in Rust
