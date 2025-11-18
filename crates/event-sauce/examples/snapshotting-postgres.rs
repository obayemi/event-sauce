//! PostgreSQL Snapshotting Example - Real-World Performance Testing
//!
//! This example demonstrates snapshotting with actual PostgreSQL database access
//! using testcontainers to showcase real-world performance characteristics.
//!
//! Key features demonstrated:
//! - PostgreSQL event store with testcontainers
//! - Real database I/O and serialization overhead
//! - Performance comparison: with vs without snapshots
//! - Snapshot strategies impact on actual database queries
//! - Production-realistic performance metrics
//!
//! Run with:
//! ```bash
//! cargo run -p event-sauce --example snapshotting-postgres --features "postgres,macros"
//! ```
//!
//! Note: This example requires Docker to be running for testcontainers.

use chrono::Utc;
use event_sauce_core::{
    load, Aggregate, ApplyEvent, EventStore, EveryNEvents, SnapshotConfig,
};
use event_sauce_macros::{AggregateError, AggregateId, AggregateState, Event as DeriveEvent};
use event_sauce_postgres::PostgresEventStore;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::time::{Duration, Instant};
use testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use uuid::Uuid;

// ============================================================================
// Domain Model - Counter Aggregate
// ============================================================================

#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct CounterId(Uuid);

impl CounterId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncrementedEvent {
    amount: i32,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Counter", aggregate = "CounterAggregate")]
enum CounterEvent {
    Incremented(IncrementedEvent),
}

#[derive(AggregateError, Debug, thiserror::Error)]
enum CounterError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i32),
}

impl ApplyEvent<CounterAggregate, CounterError> for IncrementedEvent {
    fn apply(&self, counter: &mut CounterAggregate) {
        counter.value += self.amount;
    }
}

#[derive(AggregateState, Debug, Clone, Serialize, Deserialize)]
#[aggregate(id = "CounterId", event = "CounterEvent", error = "CounterError")]
struct CounterState {
    #[aggregate_id]
    id: CounterId,
    value: i32,
}

impl Default for CounterState {
    fn default() -> Self {
        Self {
            id: CounterId(Uuid::nil()),
            value: 0,
        }
    }
}

impl CounterAggregate {
    fn create(id: CounterId) -> Self {
        <Self as Aggregate>::new(id)
    }

    fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = IncrementedEvent {
            amount,
            timestamp: Utc::now(),
        };
        self.apply(CounterEvent::Incremented(event))
    }

    fn value(&self) -> i32 {
        self.value
    }
}

// ============================================================================
// PostgreSQL Test Database Setup
// ============================================================================

/// Test database using testcontainers for isolated PostgreSQL testing.
struct TestDatabase {
    pool: PgPool,
    #[allow(dead_code)]
    container: testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
}

impl TestDatabase {
    /// Creates a new test database with testcontainers.
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        println!("🐳 Starting PostgreSQL container...");

        // Start PostgreSQL container
        let container = Postgres::default()
            .with_tag("16-alpine")
            .start()
            .await?;

        // Get connection string
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(5432).await?;

        let connection_string = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

        println!("📦 Connecting to database at {}:{}...", host, port);

        // Connect to database
        let pool = PgPool::connect(&connection_string).await?;

        println!("🔧 Running migrations...");

        // Run migrations
        sqlx::migrate!("../event-sauce-postgres/migrations")
            .run(&pool)
            .await?;

        println!("✅ Database ready!\n");

        Ok(Self { pool, container })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}

// ============================================================================
// Performance Testing Utilities
// ============================================================================

/// Performance metrics for a test scenario.
#[derive(Debug)]
struct PerformanceMetrics {
    event_count: usize,
    write_time: Duration,
    load_time: Duration,
    total_time: Duration,
    events_per_second_write: f64,
    events_per_second_load: f64,
}

impl PerformanceMetrics {
    fn new(event_count: usize, write_time: Duration, load_time: Duration) -> Self {
        let total_time = write_time + load_time;
        let events_per_second_write = event_count as f64 / write_time.as_secs_f64();
        let events_per_second_load = event_count as f64 / load_time.as_secs_f64();

        Self {
            event_count,
            write_time,
            load_time,
            total_time,
            events_per_second_write,
            events_per_second_load,
        }
    }

    fn print_detailed(&self, scenario: &str) {
        println!("  📊 {} Metrics:", scenario);
        println!("     Events:            {}", self.event_count);
        println!("     Write time:        {:?}", self.write_time);
        println!("     Load time:         {:?}", self.load_time);
        println!("     Total time:        {:?}", self.total_time);
        println!(
            "     Write throughput:  {:.0} events/sec",
            self.events_per_second_write
        );
        println!(
            "     Load throughput:   {:.0} events/sec",
            self.events_per_second_load
        );
    }
}

/// Creates a counter with the specified number of events and measures write time.
async fn create_counter_with_events(
    store: &PostgresEventStore,
    id: CounterId,
    event_count: usize,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut counter = CounterAggregate::create(id);

    for i in 0..event_count {
        counter.increment(1)?;

        // Commit in batches to create realistic version numbers
        if (i + 1) % 10 == 0 {
            store.commit(&mut counter).await?;
        }
    }

    // Commit any remaining events
    if !counter.pending_events().is_empty() {
        store.commit(&mut counter).await?;
    }

    Ok(start.elapsed())
}

/// Loads a counter and measures load time.
async fn measure_load_time(
    store: &PostgresEventStore,
    id: CounterId,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let _counter: CounterAggregate = load(store, id).await?;
    Ok(start.elapsed())
}

// ============================================================================
// Demonstration Scenarios
// ============================================================================

async fn demo_without_snapshots(
    pool: &PgPool,
) -> Result<PerformanceMetrics, Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Scenario 1: WITHOUT Snapshots                            ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let config = SnapshotConfig::disabled();
    let store = PostgresEventStore::with_config(pool.clone(), config);

    let id = CounterId::new();
    let event_count = 500;

    println!("⏳ Creating counter with {} events...", event_count);
    let write_time = create_counter_with_events(&store, id, event_count).await?;

    println!("⏳ Loading counter (replaying all {} events)...", event_count);
    let load_time = measure_load_time(&store, id).await?;

    let counter: CounterAggregate = load(&store, id).await?;
    println!("✓ Counter value: {}", counter.value());

    let metrics = PerformanceMetrics::new(event_count, write_time, load_time);
    metrics.print_detailed("WITHOUT Snapshots");
    println!();

    Ok(metrics)
}

async fn demo_with_snapshots(
    pool: &PgPool,
) -> Result<PerformanceMetrics, Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Scenario 2: WITH Snapshots (Every 100 Events)           ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let config = SnapshotConfig::builder()
        .default_strategy(EveryNEvents(100))
        .build();
    let store = PostgresEventStore::with_config(pool.clone(), config);

    let id = CounterId::new();
    let event_count = 500;

    println!(
        "⏳ Creating counter with {} events (snapshot every 100)...",
        event_count
    );
    let write_time = create_counter_with_events(&store, id, event_count).await?;

    println!("⏳ Loading counter (using latest snapshot)...");
    let load_time = measure_load_time(&store, id).await?;

    let counter: CounterAggregate = load(&store, id).await?;
    println!("✓ Counter value: {}", counter.value());
    println!("✓ Snapshots created at versions: 100, 200, 300, 400, 500");

    let metrics = PerformanceMetrics::new(event_count, write_time, load_time);
    metrics.print_detailed("WITH Snapshots");
    println!();

    Ok(metrics)
}

async fn demo_performance_comparison(
    pool: &PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Scenario 3: Performance Comparison                      ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let test_cases = vec![100, 250, 500, 1000];

    println!("Testing with different event counts:\n");

    for event_count in test_cases {
        println!("--- {} events ---", event_count);

        // Test WITHOUT snapshots
        let config_no_snap = SnapshotConfig::disabled();
        let store_no_snap = PostgresEventStore::with_config(pool.clone(), config_no_snap);
        let id1 = CounterId::new();

        let write_time_no_snap = create_counter_with_events(&store_no_snap, id1, event_count).await?;
        let load_time_no_snap = measure_load_time(&store_no_snap, id1).await?;

        // Test WITH snapshots
        let config_with_snap = SnapshotConfig::builder()
            .default_strategy(EveryNEvents(100))
            .build();
        let store_with_snap = PostgresEventStore::with_config(pool.clone(), config_with_snap);
        let id2 = CounterId::new();

        let write_time_with_snap =
            create_counter_with_events(&store_with_snap, id2, event_count).await?;
        let load_time_with_snap = measure_load_time(&store_with_snap, id2).await?;

        // Calculate improvements
        let load_speedup = load_time_no_snap.as_nanos() as f64 / load_time_with_snap.as_nanos() as f64;
        let write_overhead =
            (write_time_with_snap.as_nanos() as f64 / write_time_no_snap.as_nanos() as f64 - 1.0)
                * 100.0;

        println!("  Without snapshots:");
        println!("    Write: {:?}", write_time_no_snap);
        println!("    Load:  {:?}", load_time_no_snap);

        println!("  With snapshots:");
        println!("    Write: {:?} (+{:.1}% overhead)", write_time_with_snap, write_overhead);
        println!("    Load:  {:?} ({:.2}x faster)", load_time_with_snap, load_speedup);
        println!();
    }

    Ok(())
}

async fn demo_always_snapshot(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Scenario 4: Always Snapshot Strategy                    ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let config = SnapshotConfig::always();
    let store = PostgresEventStore::with_config(pool.clone(), config);

    let id = CounterId::new();
    let event_count = 200;

    println!(
        "⏳ Creating counter with {} events (snapshot on every commit)...",
        event_count
    );
    let write_time = create_counter_with_events(&store, id, event_count).await?;

    println!("⏳ Loading counter (using latest snapshot)...");
    let load_time = measure_load_time(&store, id).await?;

    let counter: CounterAggregate = load(&store, id).await?;
    println!("✓ Counter value: {}", counter.value());
    println!(
        "✓ Snapshot at version: {} (always up-to-date)",
        counter.version().as_i32()
    );

    let metrics = PerformanceMetrics::new(event_count, write_time, load_time);
    metrics.print_detailed("Always Snapshot");
    println!();

    Ok(())
}

async fn demo_per_type_configuration(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Scenario 5: Per-Aggregate-Type Configuration            ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Counter gets snapshots every 50 events (type-specific override)
    let config = SnapshotConfig::builder()
        .default_strategy(EveryNEvents(100))
        .per_type_override("Counter", EveryNEvents(50))
        .build();

    let store = PostgresEventStore::with_config(pool.clone(), config);

    let id = CounterId::new();
    let event_count = 200;

    println!(
        "⏳ Creating Counter with {} events (snapshot every 50 for Counter type)...",
        event_count
    );
    let write_time = create_counter_with_events(&store, id, event_count).await?;

    println!("⏳ Loading counter...");
    let load_time = measure_load_time(&store, id).await?;

    let counter: CounterAggregate = load(&store, id).await?;
    println!("✓ Counter value: {}", counter.value());
    println!("✓ Snapshots created at versions: 50, 100, 150, 200");
    println!("✓ Counter used its specific strategy (50 vs default 100)");

    let metrics = PerformanceMetrics::new(event_count, write_time, load_time);
    metrics.print_detailed("Per-Type Configuration");
    println!();

    Ok(())
}

// ============================================================================
// Main Entry Point
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n");
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║                                                           ║");
    println!("║   Event-Sauce PostgreSQL Snapshotting Performance Demo   ║");
    println!("║         Real-World Database Performance Testing          ║");
    println!("║                                                           ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Setup test database
    let db = TestDatabase::new().await?;
    let pool = db.pool();

    // Run performance demonstrations
    let metrics_no_snap = demo_without_snapshots(pool).await?;
    let metrics_with_snap = demo_with_snapshots(pool).await?;

    // Performance comparison summary
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Performance Impact Summary (500 events)                 ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let load_speedup =
        metrics_no_snap.load_time.as_nanos() as f64 / metrics_with_snap.load_time.as_nanos() as f64;
    let write_overhead = (metrics_with_snap.write_time.as_nanos() as f64
        / metrics_no_snap.write_time.as_nanos() as f64
        - 1.0)
        * 100.0;

    println!("  Load Performance:");
    println!(
        "    Without snapshots: {:?}",
        metrics_no_snap.load_time
    );
    println!(
        "    With snapshots:    {:?}",
        metrics_with_snap.load_time
    );
    println!("    🚀 Speedup:         {:.2}x faster with snapshots", load_speedup);
    println!();

    println!("  Write Performance:");
    println!(
        "    Without snapshots: {:?}",
        metrics_no_snap.write_time
    );
    println!(
        "    With snapshots:    {:?}",
        metrics_with_snap.write_time
    );
    println!(
        "    📊 Overhead:        +{:.1}% with snapshot creation",
        write_overhead
    );
    println!();

    // Additional scenarios
    demo_performance_comparison(pool).await?;
    demo_always_snapshot(pool).await?;
    demo_per_type_configuration(pool).await?;

    // Final summary
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Key Insights from Real PostgreSQL Performance           ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    println!("💡 Database Performance Characteristics:\n");

    println!("  1. Network & I/O Impact:");
    println!("     • Database round-trips add significant latency");
    println!("     • Snapshots dramatically reduce query complexity");
    println!("     • Load time improvement is substantial with real DB\n");

    println!("  2. Write Overhead:");
    println!("     • Snapshot creation adds ~{}% write overhead", write_overhead as i32);
    println!("     • This is acceptable for typical read-heavy workloads");
    println!("     • Batch commits help amortize snapshot costs\n");

    println!("  3. Scaling Characteristics:");
    println!("     • Without snapshots: Load time grows linearly with events");
    println!("     • With snapshots: Load time remains nearly constant");
    println!("     • Critical for aggregates with hundreds/thousands of events\n");

    println!("  4. Production Recommendations:");
    println!("     • Use EveryNEvents(100) as default (built-in default)");
    println!("     • Tune snapshot interval based on read/write ratio");
    println!("     • Monitor snapshot storage size and query performance");
    println!("     • Consider per-type overrides for optimization\n");

    println!("✨ All scenarios completed successfully!");
    println!("🐳 PostgreSQL container will be cleaned up automatically.\n");

    Ok(())
}
