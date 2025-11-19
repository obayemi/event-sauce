# Claude Development Guide for event-sauce

This document provides comprehensive guidelines for developing the event-sauce library. Please read carefully and follow these standards strictly.

## Project Philosophy

This is a **professional-grade event sourcing library** built with three core principles:

1. **Latest stable dependencies** - Security, performance, and modern features
2. **Strict TDD** - 100% test coverage, no exceptions
3. **Jujutsu VCS** - Modern version control workflow

## Core Development Principles

### Principle 1: Test-Driven Development (TDD)

**CRITICAL**: This project follows TDD without compromise.

#### The TDD Workflow

```
RED → GREEN → REFACTOR → COMMIT
```

1. **RED** - Write a failing test first
2. **GREEN** - Write minimal code to make it pass
3. **REFACTOR** - Clean up while keeping tests green
4. **COMMIT** - Use jj to commit each step

#### TDD Rules

- **NEVER** write production code without a failing test
- **ALWAYS** run tests before committing
- **100% coverage** is mandatory - CI will fail below 100%
- Tests are documentation - make them readable
- Property-based tests for invariants (use proptest 1.9)
- Integration tests for cross-crate functionality

#### Testing Standards

**File Organization**:
```
src/
  module.rs
  module/
    tests.rs        # Unit tests alongside code
tests/
  integration/
    feature_test.rs # Integration tests
```

**Test Types**:

1. **Unit Tests**: Test individual functions/methods
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_applies_event() {
        let mut counter = Counter::new();
        counter.increment(5).unwrap();
        assert_eq!(counter.value(), 5);
    }
}
```

2. **Integration Tests**: Test cross-component functionality
```rust
// tests/integration/event_store_test.rs
#[tokio::test]
async fn test_store_and_load_events() {
    let store = PostgresEventStore::new(pool);
    // ... test full flow
}
```

3. **Property Tests**: Test invariants
```rust
#[proptest]
fn test_version_always_increases(operations: Vec<Op>) {
    // ... property test
}
```

4. **Documentation Tests**: All rustdoc examples must run
```rust
/// Increments the counter
///
/// ```
/// let mut counter = Counter::new();
/// counter.increment(5)?;
/// assert_eq!(counter.value(), 5);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn increment(&mut self, amount: i32) -> Result<()> { ... }
```

#### Coverage Checks

```bash
# Generate coverage report
cargo llvm-cov --workspace --lcov --output-path coverage.lcov

# View HTML report
cargo llvm-cov --workspace --html
open target/llvm-cov/html/index.html

# Check coverage percentage
cargo llvm-cov --workspace --summary-only
```

**CI enforces 100% coverage** - PRs failing this check will be rejected.

---

### Principle 2: Latest Dependencies

#### Current Versions (as of 2025)

| Dependency | Version | Update Frequency |
|------------|---------|------------------|
| tokio | 1.48.0 | Check monthly |
| sqlx | 0.8.6 | Check monthly |
| clap | 4.5.51 | Check monthly |
| serde | 1.0.228 | Check monthly |
| syn | 2.0.104 | Check monthly |
| thiserror | 2.0.16 | Check monthly |
| uuid | 1.18.1 | Check monthly |
| chrono | 0.4.42 | Check monthly |
| proptest | 1.9.0 | Check monthly |
| async-trait | 0.1.88 | Check monthly |
| indicatif | 0.17.11 | Check monthly |

#### Dependency Management

```bash
# Check for updates
cargo update --workspace
cargo outdated

# Security audit
cargo audit

# Before adding new dependency, verify:
# 1. It's actively maintained (recent commits)
# 2. No security advisories
# 3. Prefer widely-used, stable crates
# 4. Document why it's needed in PR
```

#### Before Adding Dependencies

1. Check maintenance status (recent commits on GitHub)
2. Run `cargo audit` for security issues
3. Prefer stable, widely-used crates
4. Consider MSRV (Minimum Supported Rust Version)
5. Document the reason in PR description

---

### Principle 3: Jujutsu Version Control

#### Why Jujutsu?

- **Automatic tracking**: No `git add`, changes are automatically tracked
- **Flexible history**: Easy to rewrite and reorganize
- **Git compatible**: Push/pull from GitHub
- **Better UX**: More intuitive than git rebase/cherry-pick

#### Basic Jujutsu Workflow

```bash
# Create new change (like git checkout -b)
jj new -m "Add feature: event filtering"

# View current state
jj status          # Like git status
jj diff            # Like git diff

# Commit current change
jj commit -m "Implement event filtering with tests"

# Amend current change
jj describe -m "Updated description"

# View log
jj log

# Squash changes
jj squash --into <change-id>

# Push to remote
jj git push
```

#### TDD with Jujutsu

```bash
# Step 1: RED - Write failing test
jj new -m "RED: Add test for streaming events"
# ... write test ...
cargo test  # Should FAIL
jj commit

# Step 2: GREEN - Implement feature
jj new -m "GREEN: Implement event streaming"
# ... implement feature ...
cargo test  # Should PASS
jj commit

# Step 3: REFACTOR - Clean up
jj new -m "REFACTOR: Simplify streaming implementation"
# ... refactor code ...
cargo test && cargo clippy
jj commit

# View your TDD history
jj log
```

#### Handling Mistakes

```bash
# Abandon current change
jj abandon

# Undo last operation
jj undo

# Edit specific change
jj edit <change-id>

# Rebase onto main
jj rebase -d main
```

#### Daily Workflow

```bash
# Morning: Start work
jj git fetch
jj rebase -d main

# During work: Create changes for features
jj new -m "Feature description"
# ... code ...
jj commit

# Before pushing: Organize history
jj log  # Review commits
jj squash --into <change-id>  # Combine related changes

# Push
jj git push
```

---

## Code Standards

### Rust Standards

```bash
# Format code (required before commit)
cargo fmt --all

# Lint with clippy (zero warnings required)
cargo clippy --workspace -- -D warnings

# Check compilation
cargo check --workspace --all-features
```

### Code Quality Checklist

Before committing, ensure:
- [ ] All tests pass: `cargo test --workspace`
- [ ] 100% coverage: `cargo llvm-cov --workspace`
- [ ] Zero clippy warnings: `cargo clippy -- -D warnings`
- [ ] Code formatted: `cargo fmt --all -- --check`
- [ ] Documentation complete: `cargo doc --no-deps`
- [ ] Security audit passes: `cargo audit`

### Documentation Standards

1. **All public items must have documentation**:
```rust
/// Short one-line summary
///
/// Longer description with details about behavior,
/// edge cases, and examples.
///
/// # Examples
///
/// ```
/// # use event_sauce::prelude::*;
/// let counter = Counter::new();
/// counter.increment(5)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns `CounterError::Overflow` if incrementing would overflow.
///
/// # Panics
///
/// Never panics (or document when it does).
pub fn increment(&mut self, amount: i32) -> Result<(), CounterError> { ... }
```

2. **Module documentation**:
```rust
//! # event-sauce-core
//!
//! Core traits and types for event-sauce.
//!
//! This module provides...
```

3. **Examples**:
- Simple examples in rustdoc
- Complex examples in `examples/` directory
- All examples must have tests

---

## Architecture Guidelines

### Crate Organization

```
event-sauce/
├── event-sauce-core/          # Core traits (no implementations) + Subscription system
├── event-sauce-memory/        # In-memory implementation
├── event-sauce-postgres/      # PostgreSQL implementation
├── event-sauce-macros/        # Derive macros
├── event-sauce-cli/           # CLI tool
└── event-sauce/               # Facade crate
```

### Dependency Rules

- `event-sauce-core` depends on: **Nothing** (only common utilities)
- Backend crates depend on: `event-sauce-core` only
- `event-sauce` depends on: All crates (re-exports)
- Use `workspace = true` for all dependencies

### Trait Design

**CRITICAL PRINCIPLE: Generic Implementation First**

When adding new features to EventStore or other core traits:

1. **ALWAYS implement new functionality in the generic trait** using existing trait methods
2. **ONLY add new trait methods** when absolutely necessary (when the feature cannot be built with existing primitives)
3. **Keep backend implementations minimal** - they should only provide the primitive operations

**Why?**
- ✅ Single implementation - write once, works for all backends
- ✅ Consistency - all backends behave identically
- ✅ Maintainability - fewer places to fix bugs
- ✅ Testability - test once at the trait level
- ✅ DRY - don't repeat implementation logic across backends

**Example: Adding a feature (CORRECT approach)**

```rust
// Core trait in event-sauce-core - Generic implementation using default methods
#[async_trait]
pub trait EventStore: Send + Sync {
    // Primitive operations (must be implemented)
    async fn append(&self, ...) -> Result<()>;
    async fn load_stream(&self, ...) -> Result<impl Stream<Item = Result<Event>> + Send>;

    // Generic feature implementation (works for ALL backends automatically)
    async fn count_events(&self, stream_id: &str) -> Result<usize> {
        let mut count = 0;
        let mut stream = self.load_stream(stream_id, None).await?;
        while let Some(_) = stream.next().await {
            count += 1;
        }
        Ok(count)
    }

    // Another generic feature
    async fn stream_exists(&self, stream_id: &str) -> Result<bool> {
        let mut stream = self.load_stream(stream_id, None).await?;
        Ok(stream.next().await.is_some())
    }
}

// Backend implementation - ONLY implements primitives
#[async_trait]
impl EventStore for PostgresEventStore {
    async fn append(&self, ...) -> Result<()> {
        // PostgreSQL-specific implementation
    }

    async fn load_stream(&self, ...) -> Result<impl Stream<Item = Result<Event>> + Send> {
        // PostgreSQL-specific implementation
    }

    // count_events() and stream_exists() work automatically via trait!
}
```

**Example: When to add a new trait method (RARE)**

Only add new trait methods when the feature **requires backend-specific implementation**:

```rust
#[async_trait]
pub trait EventStore: Send + Sync {
    // This MUST be backend-specific (e.g., database transaction semantics)
    async fn append_transactional(&self, events: Vec<Event>) -> Result<()>;

    // This could be optimized per-backend but has a generic fallback
    async fn count_events_fast(&self, stream_id: &str) -> Result<usize> {
        // Default: use the generic implementation
        self.count_events(stream_id).await
    }
}

// Postgres can override for performance using COUNT(*)
#[async_trait]
impl EventStore for PostgresEventStore {
    async fn count_events_fast(&self, stream_id: &str) -> Result<usize> {
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE stream_id = $1")
            .bind(stream_id)
            .fetch_one(&self.pool)
            .await
    }
}
```

**Decision Tree:**

```
Need new EventStore feature?
│
├─ Can it be built using load_stream/append?
│  └─ YES → Implement as default trait method (generic)
│
└─ NO → Requires backend-specific behavior?
   ├─ YES → Add new trait method
   │        Provide default implementation if possible
   │        Document why backends might override
   │
   └─ NO → Reconsider design
```

### Error Handling

Use `thiserror` for library errors:
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EventStoreError {
    #[error("Concurrency conflict: expected version {expected}, found {actual}")]
    ConcurrencyConflict {
        expected: Version,
        actual: Version,
    },

    #[error("Event not found: {stream_id}")]
    NotFound {
        stream_id: StreamId,
    },

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}
```

---

## Testing Strategies

### Testing Database Code

```rust
// Common test setup
pub struct TestDatabase {
    pool: PgPool,
    db_name: String,
}

impl TestDatabase {
    pub async fn new() -> Result<Self> {
        let db_name = format!("test_event_sauce_{}", Uuid::new_v4());
        // Create isolated database
        // Run migrations
        Ok(Self { pool, db_name })
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Cleanup database
    }
}

// Usage in tests
#[tokio::test]
#[serial]  // Run database tests serially
async fn test_postgres_store() {
    let db = TestDatabase::new().await.unwrap();
    let store = PostgresEventStore::new(db.pool());
    // ... test ...
}
```

### Testing Async Code

```rust
#[tokio::test]
async fn test_async_function() {
    let result = async_function().await;
    assert_eq!(result, expected);
}

// Testing streams
#[tokio::test]
async fn test_stream() {
    let mut stream = create_stream().await;

    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.event_type, "Created");

    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.event_type, "Updated");
}
```

### Property-Based Testing

```rust
use proptest::prelude::*;

#[proptest]
fn test_version_monotonic(operations: Vec<Operation>) {
    let mut aggregate = TestAggregate::new();
    let initial_version = aggregate.version();

    for op in operations {
        aggregate.apply(op);
    }

    prop_assert!(aggregate.version() >= initial_version);
}
```

---

## CI/CD Guidelines

### GitHub Actions Workflow

The CI pipeline runs:
1. Tests on all crates
2. Coverage check (must be 100%)
3. Clippy (zero warnings)
4. Format check
5. Security audit
6. Documentation build

### Before Pushing

```bash
# Run full CI locally
./scripts/ci-check.sh

# Or manually:
cargo test --workspace
cargo llvm-cov --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --all -- --check
cargo doc --no-deps --workspace
cargo audit
```

---

## Common Commands Reference

```bash
# === Testing ===
cargo test --workspace                    # All tests
cargo test -p event-sauce-core           # Single crate
cargo test -- --nocapture                # With output
cargo test --test '*'                    # Integration tests only

# === Coverage ===
cargo llvm-cov --workspace              # Generate coverage
cargo llvm-cov --workspace --html       # HTML report
cargo llvm-cov --workspace --summary-only  # Just the summary

# === Linting ===
cargo clippy --workspace -- -D warnings  # Clippy (zero warnings)
cargo fmt --all                          # Format code
cargo fmt --all -- --check              # Check formatting

# === Documentation ===
cargo doc --no-deps --workspace         # Build docs
cargo doc --no-deps --workspace --open  # Build and open

# === Dependencies ===
cargo update --workspace                # Update dependencies
cargo outdated                          # Check for outdated deps
cargo audit                            # Security audit
cargo tree --depth 1                   # View dependency tree

# === Jujutsu ===
jj new -m "description"                # Create new change
jj status                              # View status
jj diff                                # View changes
jj commit -m "message"                 # Commit change
jj log                                 # View history
jj git push                            # Push to remote
```

---

## Troubleshooting

### Tests Failing

```bash
# Run specific test with output
cargo test test_name -- --nocapture

# Run tests serially (for database tests)
cargo test -- --test-threads=1

# Show backtrace
RUST_BACKTRACE=1 cargo test
```

### Coverage Issues

```bash
# Identify untested code
cargo llvm-cov --workspace --html
open target/llvm-cov/html/index.html

# Check specific crate
cargo llvm-cov -p event-sauce-core
```

### Dependency Issues

```bash
# Clean and rebuild
cargo clean
cargo build

# Update Cargo.lock
cargo update

# Check for conflicts
cargo tree
```

---

## Release Process

1. Ensure all tests pass and coverage is 100%
2. Update version in Cargo.toml files
3. Update CHANGELOG.md
4. Create release commit:
   ```bash
   jj new -m "Release v0.1.0"
   # Update versions
   jj commit
   ```
5. Tag release:
   ```bash
   jj git tag v0.1.0
   ```
6. Push:
   ```bash
   jj git push
   jj git push --tag v0.1.0
   ```
7. Publish to crates.io:
   ```bash
   cd crates/event-sauce-core && cargo publish
   cd crates/event-sauce-memory && cargo publish
   # ... etc
   ```

---

## Getting Help

- Check existing tests for examples
- Read the rustdoc: `cargo doc --open`
- Review closed PRs for patterns
- Ask in discussions on GitHub

---

## Remember

1. **Always TDD**: RED → GREEN → REFACTOR → COMMIT
2. **Always 100% coverage**: No exceptions
3. **Always use Jujutsu**: Better workflow than Git
4. **Always latest deps**: Security and features
5. **Always document**: Code is read more than written

---

**This is a professional project - no shortcuts, no placeholders, no "TODO" comments in committed code.**

Every line must be tested, documented, and production-ready.
