//! Integration tests for the read-your-writes `wait_for_checkpoint` helper.
//!
//! `wait_for_checkpoint` blocks until a named subscription's checkpoint reaches
//! a target [`Position`], enabling a caller to read an eventually-consistent
//! read model only *after* the projection has caught up to the write it just
//! produced. These tests exercise the helper against the in-memory checkpoint
//! store, with no Docker / Postgres required.

use std::time::Duration;

use event_sauce_core::{wait_for_checkpoint, CheckpointStore, Position};
use event_sauce_memory::InMemoryCheckpointStore;

/// When the checkpoint is already at (or past) the target, the helper returns
/// `Ok(true)` essentially immediately, without consuming the timeout.
#[tokio::test]
async fn returns_true_when_checkpoint_already_at_target() {
    let store = InMemoryCheckpointStore::new();
    store
        .save_checkpoint("MyProjection", Position::new(42))
        .await
        .unwrap();

    let start = tokio::time::Instant::now();
    let reached = wait_for_checkpoint(
        &store,
        "MyProjection",
        Position::new(42),
        Duration::from_millis(5),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    assert!(reached, "checkpoint already >= target must return Ok(true)");
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "should return promptly without waiting out the timeout"
    );
}

/// When the checkpoint is past the target (strictly greater), the helper still
/// reports success — `>= target` is the contract, not `== target`.
#[tokio::test]
async fn returns_true_when_checkpoint_past_target() {
    let store = InMemoryCheckpointStore::new();
    store
        .save_checkpoint("MyProjection", Position::new(100))
        .await
        .unwrap();

    let reached = wait_for_checkpoint(
        &store,
        "MyProjection",
        Position::new(50),
        Duration::from_millis(5),
        Duration::from_millis(500),
    )
    .await
    .unwrap();

    assert!(reached, "checkpoint past target must return Ok(true)");
}

/// When the checkpoint stays below the target for the whole (short) timeout,
/// the helper returns `Ok(false)` after roughly the timeout elapses.
#[tokio::test]
async fn returns_false_on_timeout_when_checkpoint_stays_below_target() {
    let store = InMemoryCheckpointStore::new();
    store
        .save_checkpoint("MyProjection", Position::new(5))
        .await
        .unwrap();

    let start = tokio::time::Instant::now();
    let reached = wait_for_checkpoint(
        &store,
        "MyProjection",
        Position::new(10),
        Duration::from_millis(10),
        Duration::from_millis(150),
    )
    .await
    .unwrap();

    assert!(
        !reached,
        "checkpoint below target for the whole timeout must return Ok(false)"
    );
    assert!(
        start.elapsed() >= Duration::from_millis(150),
        "should wait out the full timeout before reporting failure"
    );
}

/// A missing checkpoint is treated as `Position::start` (below any positive
/// target), so the helper times out rather than succeeding or erroring.
#[tokio::test]
async fn returns_false_when_checkpoint_missing_and_target_positive() {
    let store = InMemoryCheckpointStore::new();

    let reached = wait_for_checkpoint(
        &store,
        "NeverSeen",
        Position::new(1),
        Duration::from_millis(10),
        Duration::from_millis(120),
    )
    .await
    .unwrap();

    assert!(
        !reached,
        "absent checkpoint (treated as start) below target must return Ok(false)"
    );
}

/// Read-your-writes path: a background task advances the checkpoint to the
/// target after a short delay (simulating a projection catching up). A
/// `wait_for_checkpoint` with a generous timeout observes the catch-up and
/// returns `Ok(true)` well before the timeout would have fired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returns_true_when_projection_catches_up_before_timeout() {
    let store = InMemoryCheckpointStore::new();
    let target = Position::new(7);

    // Background "projection" that catches up after a short delay.
    let writer = store.clone();
    let advancer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        writer
            .save_checkpoint("MyProjection", target)
            .await
            .unwrap();
    });

    let timeout = Duration::from_millis(1000);
    let start = tokio::time::Instant::now();
    let reached = wait_for_checkpoint(
        &store,
        "MyProjection",
        target,
        Duration::from_millis(5),
        timeout,
    )
    .await
    .unwrap();
    let elapsed = start.elapsed();

    advancer.await.unwrap();

    assert!(
        reached,
        "must observe the projection catching up and return Ok(true)"
    );
    assert!(
        elapsed < timeout,
        "must return as soon as the checkpoint reaches target, not after the timeout (elapsed: {elapsed:?})"
    );
}
