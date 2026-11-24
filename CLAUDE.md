# Claude Development Guide for event-sauce

This document provides comprehensive guidelines for developing the event-sauce library. Please read carefully and follow these standards strictly.

## Project Philosophy

This is a **professional-grade event sourcing library** built with three core principles:

1. **Latest stable dependencies** - Security, performance, and modern features
2. **Strict TDD** - 90% coverage floor enforced by CI, 100% is the target
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
- **90% coverage floor** is enforced by CI - aim for 100%, never regress below 90%
- Tests are documentation - make them readable
- Property-based tests for invariants (use proptest 1.9)
- Integration tests for cross-crate functionality

#### Testing Standards

**File Organization**:

Unit tests are an inline `#[cfg(test)] mod tests { ... }` by default. When they
are split out, they live in a `tests/` folder next to the source file, named
`<module>_tests.rs` and wired with `#[path]`:

```rust
// src/aggregate_root.rs
#[cfg(test)]
#[path = "tests/aggregate_root_tests.rs"]
mod tests;
```

```
src/
  aggregate_root.rs
  tests/
    aggregate_root_tests.rs # Unit tests of aggregate_root.rs
  commands/
    mod.rs
    tests/
      commands_tests.rs     # mod.rs is named after its module, not mod_tests.rs
```

A test module with child modules puts them in `tests/<module>_tests/` and
declares each with an explicit `#[path = "<module>_tests/<name>.rs"]`.

The crate-root `tests/` directory remains for integration tests:

```
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
# Generate coverage report (includes all features, examples, binaries, and benches)
cargo llvm-cov --workspace --all-features --all-targets --lcov --output-path coverage.lcov

# View HTML report
cargo llvm-cov --workspace --all-features --all-targets --html
open target/llvm-cov/html/index.html

# Check coverage percentage
cargo llvm-cov --workspace --all-features --all-targets --summary-only
```

**CI enforces a 90% line-coverage floor** - PRs dropping below it will be rejected. Treat 100% as the target, not the gate.

---

### Principle 2: Latest Dependencies

#### Current Versions (as of 2025)

| Dependency | Version | Update Frequency |
|------------|---------|------------------|
| tokio | 1.48.0 | Check monthly |
| sqlx | 0.8.6 | Check monthly |
| serde | 1.0.228 | Check monthly |
| syn | 2.0.104 | Check monthly |
| thiserror | 2.0.16 | Check monthly |
| uuid | 1.18.1 | Check monthly |
| chrono | 0.4.42 | Check monthly |
| proptest | 1.9.0 | Check monthly |
| async-trait | 0.1.88 | Check monthly |

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
jj rebase -d master
```

#### Daily Workflow

```bash
# Morning: Start work
jj git fetch
jj rebase -d master

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

### Event Definition Standards

**RECOMMENDED**: Use the `define_events!` macro for declarative event definitions:

```rust
use event_sauce::define_events;

define_events! {
    pub enum OrderEvent for Order {
        Created {
            order_id: String,
        } => |order, event| {
            order.state.order_id = event.order_id.clone();
        },

        ItemAdded {
            item_id: String,
            quantity: u32,
            price: i64,
        }
        @validate {
            if aggregate.state.status == OrderStatus::Completed {
                return Err(OrderError::OrderAlreadyCompleted);
            }
            return Ok(());
        }
        => |order, event| {
            order.state.items.push(OrderItem { ... });
        },
    }
}
```

**Benefits:**
- ✅ **60% less boilerplate** - Automatic struct and trait generation
- ✅ **Inline validation** - Business rules co-located with events using `@validate` and `@post_validate`
- ✅ **Automatic versioning** - Schema evolution support with `@version(n)`
- ✅ **Type-safe** - Compile-time validation and apply logic
- ✅ **Self-documenting** - Clear event structure and behavior

**Alternative**: For complex scenarios requiring fine-grained control, use manual event definition with separated structs and `ApplyEvent` trait (see `docs/events.md`).

### Command Implementation Standards

**RECOMMENDED**: Use the `aggregate_command!` macro for command methods:

```rust
use event_sauce::aggregate_command;

// Auto-generate command methods
aggregate_command! {
    impl Order {
        @clock @init fn create(order_id: String) -> CreatedEvent { order_id };
        @clock fn add_item(item_id: String, quantity: u32, price: i64) -> ItemAddedEvent {
            item_id, quantity, price
        };
        @clock fn complete() -> CompletedEvent { };
    }
}
```

**Benefits:**
- ✅ **70% less boilerplate** - No manual event creation or timestamp handling
- ✅ **Consistent patterns** - All commands follow the same structure
- ✅ **Explicit time** - `@clock` stamps `timestamp` from `Utc::now()`; otherwise the instant is a parameter
- ✅ **Type-safe** - Compile-time parameter validation
- ✅ **Combined with `define_events!`** - Total 80%+ reduction in code

`command_handler!` is a deprecated alias of `aggregate_command!`; never use it.

A named, lifecycle-aware operation is a typed `Command` run with
`Repository::execute` and a `RetryPolicy` (see `docs/commands.md`). An atomic
operation over several aggregates is a `UnitOfWorkCommand` (see
`docs/unit-of-work.md`).

**Alternative**: For commands requiring complex logic, conditional validation, or custom error handling, implement methods manually (see `docs/getting-started.md`).

### User Aggregates and HTTP Standards

- A user that logs in is an aggregate marked `#[aggregate(..., user)]`,
  with one `#[user(identity)] identity: Identity<C>` field and an optional
  `#[user(active)]` field. `C` is a `Credentials` type: `Password<L>`,
  `Federated`, `PasswordOrFederated<L>`, or the app's own (see
  `docs/auth.md`)
- Logins resolve users through claims (`EventStore::resolve_claim`), never
  through a projection
- A login is a typestate: `Logins::begin` runs a `FirstFactor` (a
  `LoginMethod` and a `Route`) and yields a `Login<U, S>`; each state's
  `Step` is the only way out of it, and only `Login::finish` on a `Complete`
  state yields the `Authenticated<U>` a session is opened from. Model a new
  factor as a new state with its own `Step`, never as a flag checked by the
  handler; save a login between requests with `PendingLogins`
- A session opens only through `event-sauce-axum`'s `AuthSession<U>::login`,
  which takes only an `Authenticated<U>`. The `axum-login` backend and its
  `SessionUser` stay `pub(crate)`: never export, alias or return them from a
  public signature or name them in a public trait impl, never add a public
  path to `axum_login::AuthSession` or a backend value, and keep
  `#![deny(unnameable_types)]`. The `tests/ui` cases of the crate pin this.
  What stays outside the types: a session record written by hand through
  `tower_sessions::Session`, and an app's own `AuthnBackend`
- A hot aggregate declares its snapshot strategy on the aggregate itself:
  `#[aggregate(..., snapshots(every = N))]` (also `always`, `never`,
  `with = "path"`); a store `per_type_override` still wins over it
- HTTP apps use `event-sauce-axum`: `EventSauceApp` for the Postgres
  defaults, `AuthLayer` and the `AuthSession` extractor for login sessions
  (`AuthSession::finish` and `PendingLogin::hold`, answering a `LoginStep`,
  end and continue a login), the
  `Store`/`Repo`/`UnitOfWork`/`Found`/`Authorized` and `CurrentUser`/`MaybeUser`/`Registered`/`Acting`
  extractors, `StoreError` and `LoginError` for the error responses, and
  `CorrelationLayer`
  so events carry the request id (see `docs/axum.md` and the `axum-auth`
  and `axum-crud` examples)
- An HTTP handler never writes its own status table for a failure: it returns
  `event-sauce-axum`'s `StoreError` for a store failure (`404`, `409`, `500`,
  and `410` only through `deleted_as(DeletedStatus::Gone)`; split a
  `ModifyError`, `ExecuteError` or `UnitOfWorkError` with `SplitDomain::split`
  to keep the domain error) and `LoginError` for a login step, so a store
  outage is never answered as a bad login and no refusal tells an unknown
  user from a wrong password
- A handler never writes `Path`, `load` and an owner check for the aggregate its
  route names: it takes `Found<A, S>` (read only, loaded once per request;
  `impl PathAggregate for A` names the path parameter and `410` for a deleted
  one) or, behind a user, `Authorized<A, P, S>` with `P: Access<A>`, whose
  denial is `404` unless the policy sets `DENIED = Forbidden`. Writes keep
  `Repo::modify` or `execute`; there is no write path through either extractor
- A command context that holds the acting user implements `FromUser` and
  reaches the handler as `Acting<Ctx>`, never rebuilt from `CurrentUser` and
  never a second load of the user
- A login handler returns `Result<LoginStep, LoginError>`: `AuthSession::finish`
  for a `Complete` state and `PendingLogin::hold` for a state that waits, never
  a hand-built `204`/`202` or a `Set-Cookie` of the pending cookie. Resume a
  pending login in the handler body, after every extractor, never in an
  extractor: a malformed body would burn the single-use login
- An HTML login flow whose route asks for a TOTP code goes through
  `TotpChallenge` and `TotpProof` (feature `totp`), which keep the waiting
  login in the session record under a key the module owns, never in the
  pending login cookie of the JSON flow, and answer every step through the
  app's `TotpLoginPages`. An endpoint that checks a password or a code starts
  an `Attempt` of `LoginThrottles` before it verifies
- A reset by emailed link goes through `PasswordReset` (feature `reset`): the
  reset state stays in the app's aggregate behind `ResetAccounts`, the link is
  built from a configured `PublicOrigin` and never from `Host`, and the
  session after a redeem opens through the flow's own private first factor,
  never through a public voucher
- An OAuth sign-in, link or claim goes through `OAuthSignIn` (feature
  `oauth`): the state, the PKCE verifier, the target and the pending link live
  in the session record under keys the module owns, the callback takes the
  link and the claim, then the state, so any callback ends them and a refused
  state ends the flow, and a link carries
  its proof only until `LINK_PROOF_TTL` and works for its own flow's state
  once. Whose account a provider identity is stays the app's, behind
  `OAuthAccounts`; never set `LinkProof::Proved` for a code nobody checked
- An HTML app puts `LoginRedirect` on the router of its pages, so
  `CurrentUser`, `Registered`, `Authorized` and `Acting` redirect a browser
  that is not logged in to the login page with the page it asked for
  (`OriginalUri`), and
  the JSON API on the same `AuthLayer` keeps its `401`. A redirect target that
  comes from a request is a `ReturnTo`, never a `String`: it refuses `//host`,
  schemes, `\` and control characters
- New examples declare every facade feature they need in `required-features`,
  so they build with `--no-default-features`

### Code Quality Checklist

Before committing, ensure:
- [ ] All tests pass: `cargo test --workspace --all-features --all-targets`
- [ ] Doc tests pass: `cargo test --workspace --all-features --doc`
- [ ] Examples build: `cargo build --workspace --all-features --examples`
- [ ] Binaries build: `cargo build --workspace --all-features --bins`
- [ ] Benches build: `cargo build --workspace --all-features --benches`
- [ ] Coverage above 90%: `cargo llvm-cov --workspace --all-features --all-targets`
- [ ] Zero clippy warnings: `cargo clippy --workspace --all-features --all-targets -- -D warnings`
- [ ] Code formatted: `cargo fmt --all -- --check`
- [ ] Documentation complete: `cargo doc --no-deps --workspace --all-features`
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
crates/
├── event-sauce-core/                # Core traits and their generic default implementations
├── event-sauce-crypto/              # AES-256-GCM provider for crypto-shredding
├── event-sauce-memory/              # In-memory backend
├── event-sauce-postgres/            # PostgreSQL backend
├── event-sauce-macros/              # Proc macros (#[aggregate], #[derive(AggregateId)], ...)
├── event-sauce-auth/                # User aggregates, credentials, login, login stores, user cache
├── event-sauce-axum/                # axum integration: correlation, extractors, sealed axum-login backend, app builder
├── event-sauce-macro-surface-test/  # Checks the macros expand through the facade alone
└── event-sauce/                     # Facade crate, re-exports everything behind features
vendor/
└── testcontainers-shared/           # Test-only: one testcontainers server per test binary, a slice per test
```

### Dependency Rules

- `event-sauce-core` depends on: **Nothing** (only common utilities)
- `event-sauce-crypto` depends on: `event-sauce-core` only
- Backend crates (`event-sauce-memory`, `event-sauce-postgres`) depend on:
  `event-sauce-core` and `event-sauce-crypto` only
- `event-sauce-auth` depends on: `event-sauce-core` only. Its Postgres and
  Redis stores talk to `sqlx` and `redis` directly, never through a backend
  crate
- `event-sauce-axum` is the integration crate: `event-sauce-core` always,
  `event-sauce-auth` behind `login`, `event-sauce-postgres` behind `app`. It
  is the only non-facade crate allowed to depend on a backend
- `event-sauce` depends on: All crates (re-exports, each behind a feature)
- `vendor/` holds standalone, unpublished (`publish = false`) libraries that
  are not about event sourcing. They are workspace members, and crates may
  only take them as dev-dependencies (`testcontainers-shared` provides the
  test servers shared across a test binary)
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

Tests take their PostgreSQL and Redis from `testcontainers-shared`
(`vendor/testcontainers-shared`, a dev-dependency): one server per test
binary, started by the first test that asks for it and removed when the
process exits, and a slice of it per test. Tests run in parallel; never
start a container per test.

```rust
use testcontainers_shared::{postgres::fresh_database, redis::fresh_redis};

#[tokio::test]
async fn test_postgres_store() {
    // An empty database of its own, dropped with the guard.
    let (_database, url) = fresh_database().await;
    let pool = PgPool::connect(&url).await.unwrap();
    let store = PostgresEventStore::new(pool);
    // ... test ...
}

#[tokio::test]
async fn test_redis_store() {
    // A logical database of its own, flushed and released with the guard.
    let (_redis, url) = fresh_redis();
    let store = RedisLoginStore::connect(&url).await.unwrap();
    // ... test ...
}
```

A Redis logical database isolates keys and per-database commands only:
pub/sub channels, `FLUSHALL`, `CONFIG SET` (keyspace notifications
included), `SCRIPT`/`FUNCTION`, `SWAPDB` and `CLIENT KILL` are server-wide.

A test whose side effects reach beyond its slice (creating databases or
roles, any of those server-wide Redis commands, a custom container setup)
starts a dedicated container with `postgres::start_postgres_url` or
`redis::start_redis_url`, or with `start_with_retry` on a request built by
`postgres::test_postgres` or `redis::test_redis`.

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

The CI pipeline (`.github/workflows/ci.yml`) runs on pushes and PRs against
`master`:

1. **Test Suite** — `cargo test` via `cargo llvm-cov` across all features and
   targets, then enforces a 90% line-coverage floor and uploads to Codecov
2. **Doc Tests** — `cargo test --doc`
3. **Examples & Benchmarks** — both must build
4. **Clippy** — zero warnings, all targets
5. **Rustfmt** — format check
6. **Documentation** — `cargo doc` with `RUSTDOCFLAGS: -D warnings`
7. **Security Audit** — `cargo audit` (justified ignores live in
   `.cargo/audit.toml`)
8. **MSRV** — `cargo check` on the pinned `rust-version`
9. **Core Feature Combinations** — `event-sauce-core` with each combination of
   `event-sourcing` / `state-store`, plus `commands`, `commands,state-store`,
   `commands,event-sourcing` and `unit-of-work`
10. **Auth and axum Feature Combinations** — `event-sauce-auth`,
    `event-sauce-axum` and the `event-sauce` facade with each of their
    features alone and none by default

PostgreSQL and Redis tests share one testcontainers server of each per test
binary (`testcontainers-shared`), each test getting its own database on it,
so the pipeline needs Docker (present on `ubuntu-latest`) rather than a
database service.

### Before Pushing

```bash
# Run the CI checks locally (COMPREHENSIVE - includes all features, examples, bins, benches):
cargo test --workspace --all-features --all-targets    # All tests
cargo test --workspace --all-features --doc            # Doc tests
cargo build --workspace --all-features --examples      # Examples
cargo build --workspace --all-features --bins          # Binaries
cargo build --workspace --all-features --benches       # Benchmarks
cargo llvm-cov --workspace --all-features --all-targets  # Coverage
cargo clippy --workspace --all-features --all-targets -- -D warnings  # Lint
cargo fmt --all -- --check                             # Format check
cargo doc --no-deps --workspace --all-features         # Documentation
cargo audit                                            # Security
```

---

## Common Commands Reference

```bash
# === Testing (COMPREHENSIVE - includes all features, examples, bins, benches) ===
cargo test --workspace --all-features --all-targets           # All tests
cargo test --workspace --all-features --doc                   # Doc tests
cargo test -p event-sauce-core --all-features                # Single crate
cargo test -- --nocapture                                     # With output
cargo test --test '*' --all-features                         # Integration tests only
cargo build --workspace --all-features --examples            # Build examples
cargo build --workspace --all-features --bins                # Build binaries
cargo build --workspace --all-features --benches             # Build benchmarks
cargo run --example counter --all-features                   # Run an example

# === Coverage ===
cargo llvm-cov --workspace --all-features --all-targets      # Generate coverage
cargo llvm-cov --workspace --all-features --all-targets --html  # HTML report
cargo llvm-cov --workspace --all-features --all-targets --summary-only  # Summary

# === Linting ===
cargo clippy --workspace --all-features --all-targets -- -D warnings  # Clippy (zero warnings)
cargo fmt --all                                              # Format code
cargo fmt --all -- --check                                   # Check formatting

# === Documentation ===
cargo doc --no-deps --workspace --all-features              # Build docs
cargo doc --no-deps --workspace --all-features --open       # Build and open

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
# Identify untested code (with all features and targets)
cargo llvm-cov --workspace --all-features --all-targets --html
open target/llvm-cov/html/index.html

# Check specific crate
cargo llvm-cov -p event-sauce-core --all-features --all-targets
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

1. Ensure all tests pass and coverage is above the 90% floor
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
2. **Always keep coverage above 90%**: Aim for 100%
3. **Always use Jujutsu**: Better workflow than Git
4. **Always latest deps**: Security and features
5. **Always document**: Code is read more than written

---

**This is a professional project - no shortcuts, no placeholders, no "TODO" comments in committed code.**

Every line must be tested, documented, and production-ready.
