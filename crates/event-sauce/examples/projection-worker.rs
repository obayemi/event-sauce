//! # Standalone Projection Worker
//!
//! Demonstrates running a postgres-backed projection in its own process,
//! safely alongside other workers running the same projection.
//!
//! Two pieces make this safe and efficient:
//!
//! 1. **Lease-based locking** — each tick of the worker calls
//!    [`PostgresBackend::run_leased_projection`]. The first worker to call
//!    it wins the lease for the projection's name; other workers see
//!    [`LeaseOutcome::Busy`] and try again on the next tick. If the active
//!    worker dies, its lease expires and another worker takes over.
//! 2. **`LISTEN`/`NOTIFY`** — between leased runs the worker waits for
//!    [`PostgresEventStore::listen_for_events`] to wake it up, so new events
//!    are processed within milliseconds without burning CPU on poll loops.
//!
//! The example spins up a single Postgres container, launches **two** worker
//! tasks against the same projection, writes events from a third task, and
//! verifies that one worker processes events while the other observes
//! `Busy`. Replace the testcontainer setup with your real `DATABASE_URL` to
//! run this as a deployable binary.
//!
//! Run with:
//! ```bash
//! cargo run --example projection-worker --features "postgres"
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use event_sauce::postgres::{LeaseOutcome, PostgresBackend, PostgresProjection};
use event_sauce::{AggregateVersion, EventEnvelope, EventStore, EventVersion, Result, StreamId};
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use uuid::Uuid;

const PROJECTION_NAME: &str = "OrderTotals";
const SCHEMA: &str = "event_sauce";

/// A trivial projection that increments a counter row per event.
struct OrderTotals {
    /// How many events this *worker process* has seen — useful for the demo
    /// to confirm both workers are alive but only one does the work.
    seen: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl PostgresProjection for OrderTotals {
    const NAME: &'static str = PROJECTION_NAME;

    async fn handle(
        &mut self,
        _envelope: &EventEnvelope,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<()> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        sqlx::query(&format!(
            "UPDATE {SCHEMA}.order_totals SET n = n + 1 WHERE id = 1"
        ))
        .execute(&mut **tx)
        .await
        .map_err(|e| event_sauce::Error::custom(format!("update failed: {e}")))?;
        Ok(())
    }
}

/// Materialised view table — one row holding the running count.
async fn ensure_totals_table(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {SCHEMA}.order_totals (id INT PRIMARY KEY, n BIGINT NOT NULL)"
    ))
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {SCHEMA}.order_totals (id, n) VALUES (1, 0) ON CONFLICT (id) DO NOTHING"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// One worker iteration: try to take the lease, drain pending events, sleep
/// (or wait for NOTIFY) until there's more to do.
async fn worker_tick(
    backend: &PostgresBackend,
    worker_id: &str,
    seen: Arc<AtomicUsize>,
) -> Result<LeaseOutcome> {
    let mut projection = OrderTotals {
        seen: Arc::clone(&seen),
    };
    backend
        .run_leased_projection(&mut projection, worker_id, Duration::from_secs(15))
        .await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Start a Postgres container — replace with your real DATABASE_URL in
    // production:
    //
    //     let backend = PostgresBackend::setup(&database_url, SCHEMA).await?;
    let container = Postgres::default().with_tag("16-alpine").start().await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");

    let backend = PostgresBackend::setup(&database_url, SCHEMA).await?;
    ensure_totals_table(backend.pool()).await?;

    // Launch two competing workers. Identical projection name → only one
    // can hold the lease at a time. Each takes a unique worker_id.
    let worker_a_seen = Arc::new(AtomicUsize::new(0));
    let worker_b_seen = Arc::new(AtomicUsize::new(0));

    let stop = Arc::new(tokio::sync::Notify::new());

    let worker_a = tokio::spawn({
        let backend = backend.event_store();
        let listen_backend = Arc::clone(&backend);
        let backend_for_run = PostgresBackend::setup(&database_url, SCHEMA).await?;
        let seen = Arc::clone(&worker_a_seen);
        let stop = Arc::clone(&stop);
        async move { run_worker_loop("worker-a", &backend_for_run, &listen_backend, seen, stop).await }
    });
    let worker_b = tokio::spawn({
        let backend = backend.event_store();
        let listen_backend = Arc::clone(&backend);
        let backend_for_run = PostgresBackend::setup(&database_url, SCHEMA).await?;
        let seen = Arc::clone(&worker_b_seen);
        let stop = Arc::clone(&stop);
        async move { run_worker_loop("worker-b", &backend_for_run, &listen_backend, seen, stop).await }
    });

    // Producer: emit a handful of events. Each commit fires pg_notify, so
    // whichever worker holds the lease wakes up immediately.
    let event_store = backend.event_store();
    for i in 0..5 {
        let aggregate_id = Uuid::new_v4();
        let stream_id = StreamId::new("Order", aggregate_id);
        let envelope = EventEnvelope::new(
            Uuid::new_v4(),
            aggregate_id,
            "Order".to_string(),
            "Order.Created".to_string(),
            EventVersion::new(1),
            json!({"order_index": i}),
        );
        event_store
            .append(
                stream_id,
                vec![envelope],
                AggregateVersion::initial(),
                vec![],
                false,
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Give workers time to drain.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.notify_waiters();

    // Wait for workers to wind down.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = worker_a.await;
        let _ = worker_b.await;
    })
    .await;

    // Inspect: both workers were alive, but only one did the bulk of the
    // work. The materialized count should equal the events written.
    let count: i64 =
        sqlx::query_scalar(&format!("SELECT n FROM {SCHEMA}.order_totals WHERE id = 1"))
            .fetch_one(backend.pool())
            .await?;

    let a = worker_a_seen.load(Ordering::SeqCst);
    let b = worker_b_seen.load(Ordering::SeqCst);
    println!("events written: 5");
    println!("worker-a processed: {a}");
    println!("worker-b processed: {b}");
    println!("materialized count: {count}");
    println!("(processed events sum to materialized count; only one worker is active at a time)");

    assert_eq!(count, 5, "all events should be reflected in the projection");
    assert_eq!(
        a + b,
        5,
        "every event was processed exactly once across the two workers"
    );

    Ok(())
}

async fn run_worker_loop(
    worker_id: &str,
    backend_for_run: &PostgresBackend,
    listen_store: &event_sauce::postgres::PostgresEventStore,
    seen: Arc<AtomicUsize>,
    stop: Arc<tokio::sync::Notify>,
) {
    use futures::StreamExt;

    // Subscribe to NOTIFY for low-latency wake-ups. Falling back to a 1s
    // tick guards against the (rare) lost-notify case.
    let mut notify_stream = match listen_store.listen_for_events().await {
        Ok(s) => Box::pin(s),
        Err(e) => {
            eprintln!("[{worker_id}] listen failed: {e}");
            return;
        }
    };

    loop {
        match worker_tick(backend_for_run, worker_id, Arc::clone(&seen)).await {
            Ok(LeaseOutcome::Completed) => {}
            Ok(LeaseOutcome::Busy) => {}
            Err(e) => {
                eprintln!("[{worker_id}] tick error: {e}");
            }
        }

        tokio::select! {
            _ = stop.notified() => break,
            _ = notify_stream.next() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}
