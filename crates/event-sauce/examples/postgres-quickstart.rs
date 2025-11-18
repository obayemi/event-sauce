//! PostgreSQL Event Store - Quick Start Guide
//!
//! This example demonstrates the simplest way to get started with a PostgreSQL-backed
//! event store in Event-Sauce. Perfect for beginners and new projects.
//!
//! # Prerequisites
//!
//! 1. PostgreSQL running locally or via Docker:
//!    ```bash
//!    docker run -d -p 5432:5432 \
//!      -e POSTGRES_PASSWORD=postgres \
//!      -e POSTGRES_DB=eventsauce \
//!      postgres:16-alpine
//!    ```
//!
//! 2. Or use an existing PostgreSQL instance.
//!
//! # Run this example
//!
//! ```bash
//! cargo run -p event-sauce --example postgres-quickstart --features "postgres,macros"
//! ```
//!
//! # Environment Variables
//!
//! Set `DATABASE_URL` to override the default connection string:
//! ```bash
//! export DATABASE_URL="postgresql://user:pass@localhost/mydb"
//! cargo run -p event-sauce --example postgres-quickstart --features "postgres,macros"
//! ```

use chrono::Utc;
use event_sauce_core::{load, Aggregate, ApplyEvent, EventStore, Version};
use event_sauce_macros::{AggregateError, AggregateId, AggregateState, Event as DeriveEvent};
use event_sauce_postgres::PostgresEventStore;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

// ============================================================================
// Step 1: Define Your Domain Model
// ============================================================================

/// Unique identifier for a Counter aggregate
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct CounterId(Uuid);

impl CounterId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Event: Counter was incremented
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncrementedEvent {
    amount: i32,
    timestamp: chrono::DateTime<Utc>,
}

/// Event: Counter was decremented
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecrementedEvent {
    amount: i32,
    timestamp: chrono::DateTime<Utc>,
}

/// All events for the Counter aggregate
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Counter", aggregate = "CounterAggregate")]
enum CounterEvent {
    Incremented(IncrementedEvent),
    Decremented(DecrementedEvent),
}

/// Domain errors
#[derive(AggregateError, Debug, thiserror::Error)]
enum CounterError {
    #[error("Cannot decrement below zero (current: {current}, attempted: {amount})")]
    BelowZero { current: i32, amount: i32 },
}

/// Apply increment event to counter state
impl ApplyEvent<CounterAggregate, CounterError> for IncrementedEvent {
    fn apply(&self, counter: &mut CounterAggregate) {
        counter.value += self.amount;
    }
}

/// Apply decrement event to counter state
impl ApplyEvent<CounterAggregate, CounterError> for DecrementedEvent {
    fn apply(&self, counter: &mut CounterAggregate) {
        counter.value -= self.amount;
    }
}

/// Counter aggregate state
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

/// Counter aggregate business logic
impl CounterAggregate {
    /// Create a new counter
    fn create(id: CounterId) -> Self {
        <Self as Aggregate>::new(id)
    }

    /// Increment the counter
    fn increment(&mut self, amount: i32) -> Result<(), CounterError> {
        let event = IncrementedEvent {
            amount,
            timestamp: Utc::now(),
        };
        self.apply(CounterEvent::Incremented(event))
    }

    /// Decrement the counter (validates it won't go below zero)
    fn decrement(&mut self, amount: i32) -> Result<(), CounterError> {
        if self.value - amount < 0 {
            return Err(CounterError::BelowZero {
                current: self.value,
                amount,
            });
        }
        let event = DecrementedEvent {
            amount,
            timestamp: Utc::now(),
        };
        self.apply(CounterEvent::Decremented(event))
    }

    /// Get the current value
    fn value(&self) -> i32 {
        self.value
    }
}

// ============================================================================
// Step 2: Setup PostgreSQL Connection and Event Store
// ============================================================================

/// Sets up the PostgreSQL event store.
///
/// This function:
/// 1. Connects to PostgreSQL
/// 2. Runs database migrations (creates tables)
/// 3. Returns a ready-to-use event store
async fn setup_event_store() -> Result<PostgresEventStore, Box<dyn std::error::Error>> {
    // Get database URL from environment or use default
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost/eventsauce".to_string());

    println!("📡 Connecting to PostgreSQL...");
    println!("   URL: {}\n", database_url);

    // Create connection pool
    let pool = PgPool::connect(&database_url).await?;

    println!("🔧 Running database migrations...");

    // Run migrations to create the necessary tables
    // This is safe to run multiple times - it only applies new migrations
    sqlx::migrate!("../event-sauce-postgres/migrations")
        .run(&pool)
        .await?;

    println!("✅ Database ready!\n");

    // Create the event store with default configuration
    // Default: snapshots every 100 events for better performance
    let store = PostgresEventStore::new(pool);

    Ok(store)
}

// ============================================================================
// Step 3: Use the Event Store
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║                                                            ║");
    println!("║       Event-Sauce PostgreSQL Quick Start                  ║");
    println!("║                                                            ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Setup the event store
    let store = setup_event_store().await?;

    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Creating and Using an Aggregate                          ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Create a new counter
    let counter_id = CounterId::new();
    let mut counter = CounterAggregate::create(counter_id);

    println!("📝 Created counter with ID: {}", counter_id.0);
    println!("   Initial value: {}\n", counter.value());

    // Apply some events
    counter.increment(5)?;
    println!("➕ Incremented by 5");
    println!("   Current value: {}", counter.value());

    counter.increment(3)?;
    println!("➕ Incremented by 3");
    println!("   Current value: {}", counter.value());

    counter.decrement(2)?;
    println!("➖ Decremented by 2");
    println!("   Current value: {}\n", counter.value());

    // Save all changes to the database
    println!("💾 Saving changes to PostgreSQL...");
    store.commit(&mut counter).await?;
    println!("✅ Changes saved! (version: {})\n", counter.version());

    // Load the counter from the database
    println!("📂 Loading counter from PostgreSQL...");
    let loaded_counter: CounterAggregate = load(&store, counter_id).await?;
    println!("✅ Counter loaded!");
    println!("   Value: {}", loaded_counter.value());
    println!("   Version: {}\n", loaded_counter.version());

    // Verify the value is correct
    assert_eq!(loaded_counter.value(), 6);
    assert_eq!(loaded_counter.version(), Version::from(3));

    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Demonstrating Optimistic Concurrency Control            ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Make more changes
    let mut counter = loaded_counter;
    counter.increment(10)?;
    println!("➕ Incremented by 10");
    println!("   Current value: {}", counter.value());

    store.commit(&mut counter).await?;
    println!("✅ Changes committed (version: {})\n", counter.version());

    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Error Handling Example                                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Try to decrement below zero (will fail)
    let mut counter = CounterAggregate::create(CounterId::new());
    counter.increment(5)?;

    match counter.decrement(10) {
        Ok(_) => println!("❌ This shouldn't happen!"),
        Err(e) => println!("✅ Error caught correctly: {}\n", e),
    }

    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Summary                                                  ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    println!("🎉 Success! You've learned:");
    println!("   ✓ How to connect to PostgreSQL");
    println!("   ✓ How to run migrations");
    println!("   ✓ How to create an event store");
    println!("   ✓ How to create and use aggregates");
    println!("   ✓ How to save and load from the database");
    println!("   ✓ How optimistic concurrency works");
    println!("   ✓ How to handle domain errors\n");

    println!("📚 Next steps:");
    println!("   → Check out the snapshotting-postgres example");
    println!("   → Explore projections for read models");
    println!("   → Learn about sagas for complex workflows\n");

    Ok(())
}
