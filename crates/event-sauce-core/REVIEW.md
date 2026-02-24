# Event-Sauce-Core: Code Review

## Overall Impression

The crate is well-structured with thorough documentation and consistent formatting. The core abstractions (`Aggregate`, `DomainEvent`, `EventStore`) are sound for an event sourcing library. However, there are several design issues, significant test boilerplate problems, and some tests that don't add meaningful behavioral confidence.

---

## Design Issues

### 1. Massive `Aggregate` trait boilerplate — no default methods for the standard pattern

Every `Aggregate` implementation copies the exact same `apply` / `apply_internal` / `apply_unchecked` wiring:

```rust
fn apply<E: Into<Self::Event>>(&mut self, event: E) -> Result<(), Self::Error> {
    let event = event.into();
    self.apply_internal(&event)?;
    self.pending_events.push(event);
    Ok(())
}
```

This appears identically in every test module (6+ times) and will be the same for every user. `apply` should be a provided default method on the trait, with `apply_internal` as the only required override. This would require the trait to have a `pending_events_mut()` method or similar, but the boilerplate savings would be enormous.

**Files affected:** `aggregate.rs`, and every test module that creates an Aggregate impl.

### 2. `ApplyEvent` is disconnected from `Aggregate`

`ApplyEvent<A>` defines `validate`, `apply`, and `post_validate`, but `Aggregate::apply_internal` has no knowledge of `ApplyEvent`. Users must manually wire them:

```rust
fn apply_internal(&mut self, event: &Self::Event) -> Result<(), Self::Error> {
    match event {
        MyEvent::Foo(e) => e.apply(self),
        // ...
    }
}
```

This means the library has two competing patterns (match-block vs `ApplyEvent` dispatch) with no trait-level enforcement. The `define_events!` macro presumably bridges this gap, but the core design leaves it as a pit of confusion.

**Files affected:** `apply_event.rs`, `aggregate.rs`

### 3. `EventStore` is not object-safe

`load_stream` and `stream_all` return `impl Stream<...>`, making `EventStore` non-`dyn`-compatible. This prevents `Arc<dyn EventStore>`, which is common in DDD architectures where you inject stores at runtime. Consider using `Pin<Box<dyn Stream>>` return types.

**File:** `event_store.rs:236-240, 254-257`

### 4. `unsafe impl Send/Sync` for specification combinators is unnecessary

`And<L, R, T>`, `Or<L, R, T>`, `Not<S, T>` use `unsafe impl Send/Sync`. But the `Specification` trait already requires `Send + Sync`, and `PhantomData<fn() -> T>` is already `Send + Sync`. The compiler should auto-derive these without `unsafe`. The `unsafe` here is a code smell — review whether it's actually needed.

**File:** `specification.rs:232-233, 262-263, 289-290`

### 5. `ErrorPolicy::Retry` is a stub

```rust
ErrorPolicy::Retry => {
    // For now, just fail - full retry logic would need backoff
    return Err(e);
}
```

Shipping an enum variant that behaves identically to `Fail` is misleading API design. Either implement retry with backoff, or remove the variant until it's ready.

**File:** `subscription.rs:475-478`

### 6. `commit()` clones envelopes unnecessarily

```rust
self.append(
    StreamId::new(aggregate_type, aggregate_id),
    envelopes.clone(),  // <-- unnecessary
    expected_version,
).await?;
```

`envelopes` is not used after the `append` call (the snapshot logic serializes `aggregate.state()`, not the envelopes). This clone is wasteful.

**File:** `event_store.rs:457`

### 7. `apply_unchecked` panics on validation failure

```rust
fn apply_unchecked(&mut self, event: &Self::Event) {
    self.apply_internal(event)
        .expect("Event replay should not fail validation");
}
```

If any implementor puts validation logic in `apply_internal` (which the trait docs suggest), event replay will panic. The comment says "events are historical facts" but the code doesn't skip validation — it just panics instead of returning an error.

**File:** `aggregate.rs:400-406`

### 8. `Subscription` position tracking is fragile

Position is manually incremented with `Position::new(current_position.as_i64() + 1)`. This assumes events are numbered sequentially starting from 0 with no gaps. If the underlying store uses database sequence IDs (which often have gaps), this will be wrong.

**File:** `subscription.rs:442`

---

## Test Relevance Issues

### 1. ~600+ lines of duplicated test scaffolding

Every test module (`aggregate.rs`, `domain_event.rs`, `event_store.rs`, `apply_event.rs`, `repository.rs`, `subscription.rs`) re-creates the exact same pattern:
- Test error type (3-5 lines)
- Test state type (3-5 lines)
- Test event enum (10-20 lines)
- `DomainEvent` impl (15-20 lines)
- `Aggregate` impl (40-60 lines)

This is ~100 lines per module, 6 modules = ~600 lines of test boilerplate that adds zero behavioral coverage. This should be a shared `test_utils` module or a `test_aggregate!` macro.

### 2. Tests that verify derive macro behavior, not library logic

These tests don't test library code — they test the Rust compiler:

- `test_aggregate_is_send_sync` — compile-time check, always passes if it compiles
- `test_stream_id_clone` — tests that `#[derive(Clone)]` works
- `test_stream_id_debug` — tests that `#[derive(Debug)]` works
- `test_snapshot_clone`, `test_snapshot_debug` — same
- `test_event_filter_clone`, `test_event_filter_debug` — same
- `test_checkpoint_strategy_every_event` (just `assert_eq!(strategy, strategy)`)
- `test_error_policy_retry` (just `assert_eq!(policy, policy)`)

These add ~50 tests that provide zero behavioral confidence. They inflate test counts without catching bugs.

### 3. Flaky timestamp test

```rust
fn test_different_events_have_different_timestamps() {
    let time1 = Utc::now();
    std::thread::sleep(std::time::Duration::from_millis(1));
    let time2 = Utc::now();
    assert!(event2.occurred_at() > event1.occurred_at());
}
```

`sleep(1ms)` is unreliable on CI or fast systems where clock resolution might not distinguish 1ms differences. This test adds no meaningful coverage — timestamps being different is a property of `Utc::now()`, not of this library.

**File:** `domain_event.rs:517-529`

### 4. No property-based tests despite CLAUDE.md mandate

CLAUDE.md states: *"Property-based tests for invariants (use proptest 1.9)"* and `proptest` is in dev-dependencies, but there are zero `#[proptest]` attributes in the entire crate. Good candidates:
- Version monotonically increases after applying N events
- Serialization roundtrip preserves event equality
- Aggregate version equals event count after replay

### 5. No error-path tests for subscriptions

The subscription tests only cover happy paths. Missing:
- Checkpoint store fails during `save_checkpoint`
- Event stream errors mid-way through processing
- `ErrorPolicy::Skip` actually skipping and continuing
- Handler returning errors under different policies

### 6. Doc examples use `ignore` instead of being runnable

`aggregate.rs` has `/// ```ignore` on its main doc example. These should be compilable doc-tests. The amount of boilerplate needed to make them compile is a symptom of Issue #1 (too much `Aggregate` boilerplate).

### 7. Mock event stores are re-implemented per module

`MockEventStore` appears with different implementations in `event_store.rs`, `subscription.rs`, and `repository.rs`. These could be a shared crate-internal test utility, reducing duplication and ensuring consistent mock behavior.

---

## Code Quality Issues

### 1. `aggregate_type()` uses `type_name` which is not stable across compiler versions

```rust
fn aggregate_type() -> &'static str {
    std::any::type_name::<Self>().split("::").last().unwrap_or("Unknown")
}
```

`type_name` is documented as *"primarily for diagnostic use"* and its format is not guaranteed. Using it as a storage key in event streams means changing rustc versions or module paths could break event deserialization.

**File:** `aggregate.rs:423-427`

### 2. `DomainEvent::Aggregate` creates tight circular coupling

`DomainEvent` requires `type Aggregate: Aggregate<Event = Self>`, meaning each event type is permanently bound to exactly one aggregate. This makes it impossible to have shared events across aggregates — a common DDD pattern.

**File:** `domain_event.rs:73`

### 3. `Version` wraps `i32` which limits to ~2.1 billion events

For a production event sourcing library, `i32` might be insufficient for high-throughput systems. `i64` would be more future-proof and aligns with what most databases use for sequences.

**File:** `version.rs`

---

## Positives

- Consistent `#![deny(missing_docs)]` and `#![deny(clippy::all)]` with `#![warn(clippy::pedantic)]`
- Good use of `thiserror` for error types
- Clean builder patterns (`SnapshotConfig`, `SubscriptionBuilder`)
- Well-designed specification pattern with composable combinators
- Proper use of `PhantomData<fn() -> T>` for `Send + Sync` safety in `SpecificationError`
- Snapshot strategy is cleanly separated from snapshot storage
- `into_stream()` on subscriptions is a nice API alongside the callback-based `run()`
- `#[must_use]` on constructor methods is good practice
- Comprehensive documentation on all public items

---

## Summary of Recommendations (priority order)

| # | Recommendation | Impact |
|---|---------------|--------|
| 1 | Reduce `Aggregate` boilerplate: add default implementations for `apply`/`apply_unchecked` or provide a derive macro | High — affects every user |
| 2 | Extract shared test utilities: one `test_utils` module for mock stores, test aggregates | High — ~600 lines of duplication |
| 3 | Remove trivial derive-verification tests: they test the compiler, not your code | Medium — noise reduction |
| 4 | Add property-based tests: use the proptest dep that's already there | Medium — invariant coverage |
| 5 | Fix the `unsafe impl Send/Sync`: likely unnecessary | Medium — safety concern |
| 6 | Remove or implement `ErrorPolicy::Retry`: don't ship stub behavior | Medium — API honesty |
| 7 | Make doc examples compilable: replace `ignore` with actual runnable examples | Medium — doc quality |
| 8 | Consider `aggregate_type` stability: don't rely on `type_name` for storage keys | High — data integrity |
| 9 | Add subscription error-path tests: test checkpoint failures, mid-stream errors | Medium — coverage gap |
| 10 | Remove the unnecessary `envelopes.clone()` in `commit()` | Low — performance |
