//! # Postgres Quickstart Example
//!
//! This example demonstrates a complete event-sourced system using PostgreSQL:
//!
//! 1. **Event Store**: Uses `PostgresEventStore` to persist events
//! 2. **Checkpoints**: Uses `PostgresCheckpointStore` for subscription progress
//! 3. **Projections**: Maintains a custom `product_summary` table as a read model
//! 4. **Subscriptions**: Uses the Stream API (recommended pattern) for processing events
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │ Product Aggregate (Write Model)             │
//! │ - Create product                            │
//! │ - Add/Remove stock                          │
//! │ - Change price                              │
//! └────┬────────────────────────────────────────┘
//!      │ Emits events
//!      ▼
//! ┌──────────────────────┐
//! │ PostgresEventStore   │
//! │ - events table       │
//! │ - snapshots table    │
//! │ (event_sauce schema) │
//! └──────┬───────────────┘
//!        │
//!        │ Subscription reads events
//!        ▼
//! ┌──────────────────────────────────┐
//! │ ProductSummaryProjection         │
//! │ (Read Model)                     │
//! │                                  │
//! │ Maintains: product_summary table │
//! │ - product_id                     │
//! │ - name                           │
//! │ - current_stock                  │
//! │ - current_price                  │
//! │ - last_updated                   │
//! │ (public schema)                  │
//! └──────────────────────────────────┘
//!        ▲
//!        │ Progress tracked by
//!        │
//! ┌──────────────────────────────────┐
//! │ PostgresCheckpointStore          │
//! │ - checkpoints table              │
//! │ (event_sauce schema)             │
//! └──────────────────────────────────┘
//! ```
//!
//! ## Domain: Product Inventory
//!
//! Simple product catalog with stock management:
//! - Create products with initial stock and price
//! - Add stock when receiving inventory
//! - Remove stock when selling
//! - Update product prices
//!
//! ## Validation Pattern
//!
//! This example demonstrates **validation in ApplyEvent** (recommended pattern):
//!
//! - **Commands**: Create events WITHOUT validation
//! - **ApplyEvent::validate()**: Pre-condition validation before applying
//! - **ApplyEvent::apply()**: State transformation (deterministic, no errors)
//! - **ApplyEvent::post_validate()**: Post-condition invariant checks
//!
//! ### Benefits:
//! 1. ✅ Validation runs whenever events are applied (commands, replays, projections)
//! 2. ✅ Business rules are centralized in one place
//! 3. ✅ Event sourcing replay is safe - invalid historical events are caught
//! 4. ✅ Commands stay simple - just event creation
//! 5. ✅ Separation of concerns - validation logic separate from commands
//!
//! ## Event Design Pattern
//!
//! Events contain **only decision data**, not derived or redundant information:
//!
//! - ❌ No aggregate_id (stored in EventEnvelope)
//! - ❌ No calculated values (computed in apply())
//! - ❌ No previous state (available from aggregate)
//! - ✅ Only the decision made and necessary context
//!
//! Example: `StockAdded` only contains `quantity`, not `new_total` or `product_id`
//!
//! ## Running the Example
//!
//! ```bash
//! cargo run --example postgres-quickstart
//! ```
//!
//! This example uses testcontainers to automatically start a PostgreSQL container.
//! No manual setup required!

use chrono::{DateTime, Utc};
use event_sauce_core::{load, Aggregate, ApplyEvent, CheckpointStore, CheckpointStrategy, DomainEvent, ErrorPolicy, EventEnvelope, EventStore};
use event_sauce_macros::{AggregateError, AggregateId, AggregateState, Event as DeriveEvent};
use event_sauce_postgres::{PostgresCheckpointStore, PostgresEventStore};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use thiserror::Error;
use uuid::Uuid;

// ============================================================================
// Domain Types
// ============================================================================

/// Product ID type
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct ProductId(Uuid);

impl ProductId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(AggregateError, Debug, Error)]
enum ProductError {
    #[error("Product name cannot be empty")]
    InvalidName,

    #[error("Stock cannot be negative")]
    NegativeStock,

    #[error("Price cannot be negative")]
    NegativePrice,

    #[error("Quantity must be positive")]
    InvalidQuantity,

    #[error("Insufficient stock: available {available}, requested {requested}")]
    InsufficientStock { available: i32, requested: i32 },
}

// ============================================================================
// Events
// ============================================================================
//
// EVENT DESIGN PRINCIPLES:
//
// Events contain ONLY the decision data (what was decided), not derived data.
//
// ❌ DON'T include:
// - Aggregate ID (already in EventEnvelope.aggregate_id)
// - Calculated values (e.g., new_total = current + quantity)
// - Previous state (e.g., old_price - available from aggregate)
// - Derived data that can be computed
//
// ✅ DO include:
// - The decision made (e.g., quantity added/removed, new price)
// - Context for the decision (e.g., timestamp, reason if needed)
// - Data needed to apply the event
//
// Benefits:
// 1. Smaller events = less storage, faster serialization
// 2. Single source of truth for calculations (in apply())
// 3. No risk of inconsistent derived data
// 4. Easier event evolution and versioning
//
// Examples in this code:
// - StockAdded: Only quantity (not new_total, not product_id)
// - StockRemoved: Only quantity (not new_total, not product_id)
// - PriceChanged: Only new_price_cents (not old_price_cents, not product_id)
// - ProductCreated: Only name, initial_stock, price_cents (not product_id)

/// Product was created
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProductCreated {
    name: String,
    initial_stock: i32,
    price_cents: i64,
    timestamp: DateTime<Utc>,
}

/// Stock was added to inventory
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StockAdded {
    quantity: i32,
    timestamp: DateTime<Utc>,
}

/// Stock was removed from inventory
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StockRemoved {
    quantity: i32,
    timestamp: DateTime<Utc>,
}

/// Product price was changed
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PriceChanged {
    new_price_cents: i64,
    timestamp: DateTime<Utc>,
}

/// All product events
#[derive(DeriveEvent, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Product", aggregate = "ProductAggregate")]
enum ProductEvent {
    Created(ProductCreated),
    StockAdded(StockAdded),
    StockRemoved(StockRemoved),
    PriceChanged(PriceChanged),
}

// ============================================================================
// Event Application (how events modify aggregate state)
// ============================================================================
//
// VALIDATION STRATEGY:
//
// This example demonstrates the recommended pattern for validation in event-sourced systems:
// validation happens in ApplyEvent implementations, NOT in commands.
//
// ## When to use validate() vs post_validate():
//
// ### validate() - Pre-condition checks
// Use for validating:
// - Event data itself (e.g., quantity > 0, name not empty)
// - Current aggregate state BEFORE changes (e.g., sufficient stock exists)
// - External constraints that don't depend on the new state
//
// Examples in this code:
// - ProductCreated: Check name not empty, stock/price non-negative
// - StockAdded: Check quantity is positive
// - StockRemoved: Check quantity is positive AND sufficient stock exists
// - PriceChanged: Check new price is non-negative
//
// ### post_validate() - Post-condition invariant checks
// Use for validating:
// - Business invariants AFTER state changes
// - Aggregate state consistency after the event is applied
// - Constraints that depend on the new state
//
// Examples in this code:
// - StockRemoved: Ensure stock is not negative after removal (defensive check)
//
// ### Why this pattern?
// 1. Validation runs EVERY TIME events are applied (not just commands)
// 2. Event replay from the event store is validated
// 3. Events loaded by projections are validated
// 4. Business rules are in ONE place (not scattered across commands)
// 5. Commands stay simple - just create events

impl ApplyEvent<ProductAggregate, ProductError> for ProductCreated {
    /// Validate product creation business rules
    fn validate(&self, _product: &ProductAggregate) -> Result<(), ProductError> {
        // Validate product name
        if self.name.is_empty() {
            return Err(ProductError::InvalidName);
        }

        // Validate initial stock is non-negative
        if self.initial_stock < 0 {
            return Err(ProductError::NegativeStock);
        }

        // Validate price is non-negative
        if self.price_cents < 0 {
            return Err(ProductError::NegativePrice);
        }

        Ok(())
    }

    fn apply(&self, product: &mut ProductAggregate) {
        // Note: product.id is already set by the Aggregate framework from the envelope
        product.name = self.name.clone();
        product.stock = self.initial_stock;
        product.price_cents = self.price_cents;
        product.created_at = self.timestamp;
        product.updated_at = self.timestamp;
    }
}

impl ApplyEvent<ProductAggregate, ProductError> for StockAdded {
    /// Validate stock addition
    fn validate(&self, _product: &ProductAggregate) -> Result<(), ProductError> {
        // Validate quantity is positive
        if self.quantity <= 0 {
            return Err(ProductError::InvalidQuantity);
        }

        Ok(())
    }

    fn apply(&self, product: &mut ProductAggregate) {
        // Calculate new total from current stock + quantity
        product.stock += self.quantity;
        product.updated_at = self.timestamp;
    }
}

impl ApplyEvent<ProductAggregate, ProductError> for StockRemoved {
    /// Validate stock removal pre-conditions
    fn validate(&self, product: &ProductAggregate) -> Result<(), ProductError> {
        // Validate quantity is positive
        if self.quantity <= 0 {
            return Err(ProductError::InvalidQuantity);
        }

        // Check if we have enough stock BEFORE removing
        // This is a pre-condition check
        if product.stock < self.quantity {
            return Err(ProductError::InsufficientStock {
                available: product.stock,
                requested: self.quantity,
            });
        }

        Ok(())
    }

    fn apply(&self, product: &mut ProductAggregate) {
        // Calculate new total by subtracting quantity from current stock
        product.stock -= self.quantity;
        product.updated_at = self.timestamp;
    }

    /// Validate stock is not negative after removal (invariant check)
    fn post_validate(&self, product: &ProductAggregate) -> Result<(), ProductError> {
        // Ensure the resulting stock is not negative (business invariant)
        // This is a defensive check - should never happen if validate() worked correctly
        if product.stock < 0 {
            return Err(ProductError::NegativeStock);
        }

        Ok(())
    }
}

impl ApplyEvent<ProductAggregate, ProductError> for PriceChanged {
    /// Validate price change
    fn validate(&self, _product: &ProductAggregate) -> Result<(), ProductError> {
        // Validate new price is non-negative
        if self.new_price_cents < 0 {
            return Err(ProductError::NegativePrice);
        }

        Ok(())
    }

    fn apply(&self, product: &mut ProductAggregate) {
        // Simply set the new price (old price is available in aggregate state if needed)
        product.price_cents = self.new_price_cents;
        product.updated_at = self.timestamp;
    }
}

// ============================================================================
// Aggregate State
// ============================================================================

/// Product aggregate state - manages product lifecycle and inventory
#[derive(AggregateState, Debug, Clone, Serialize, Deserialize)]
#[aggregate(id = "ProductId", event = "ProductEvent", error = "ProductError")]
struct ProductState {
    /// Product ID
    #[aggregate_id]
    id: ProductId,
    /// Product name
    name: String,
    /// Current stock quantity
    stock: i32,
    /// Current price in cents (to avoid floating point issues)
    price_cents: i64,
    /// When the product was created
    created_at: DateTime<Utc>,
    /// Last modification time
    updated_at: DateTime<Utc>,
}

impl Default for ProductState {
    fn default() -> Self {
        Self {
            id: ProductId(Uuid::nil()),
            name: String::new(),
            stock: 0,
            price_cents: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}

// ============================================================================
// Commands (business logic that generates events)
// ============================================================================
//
// Note: Commands now just create events without validation.
// Validation happens in the ApplyEvent implementations (validate/post_validate),
// ensuring all business rules are enforced when events are applied.

impl ProductAggregate {
    /// Create a new product - returns the creation event
    ///
    /// Note: product_id is not in the event - it's stored in the event envelope
    /// Validation happens when the event is applied via ApplyEvent::validate()
    pub fn create_product(
        _id: ProductId,  // Not used - aggregate_id comes from envelope
        name: String,
        initial_stock: i32,
        price_cents: i64,
    ) -> ProductEvent {
        ProductEvent::Created(ProductCreated {
            name,
            initial_stock,
            price_cents,
            timestamp: Utc::now(),
        })
    }

    /// Add stock command
    ///
    /// Note: new_total is calculated in apply(), not stored in event
    /// Validation happens when the event is applied via ApplyEvent::validate()
    pub fn cmd_add_stock(&self, quantity: i32) -> ProductEvent {
        ProductEvent::StockAdded(StockAdded {
            quantity,
            timestamp: Utc::now(),
        })
    }

    /// Remove stock command
    ///
    /// Note: new_total is calculated in apply(), not stored in event
    /// Validation happens when the event is applied via ApplyEvent::validate()
    /// and ApplyEvent::post_validate()
    pub fn cmd_remove_stock(&self, quantity: i32) -> ProductEvent {
        ProductEvent::StockRemoved(StockRemoved {
            quantity,
            timestamp: Utc::now(),
        })
    }

    /// Change price command
    ///
    /// Note: old_price is available from aggregate state, not stored in event
    /// Validation happens when the event is applied via ApplyEvent::validate()
    pub fn cmd_change_price(&self, new_price_cents: i64) -> ProductEvent {
        ProductEvent::PriceChanged(PriceChanged {
            new_price_cents,
            timestamp: Utc::now(),
        })
    }
}

// ============================================================================
// Projection: Product Summary Table
// ============================================================================

/// Maintains a denormalized read model of product summaries
///
/// This projection listens to all product events and maintains an up-to-date
/// view of each product's current state in a separate database table.
///
/// This is the recommended pattern for building read models:
/// - Events are the source of truth (write model)
/// - Projections are denormalized views (read model)
/// - Projections can be rebuilt by replaying events
struct ProductSummaryProjection {
    pool: PgPool,
}

impl ProductSummaryProjection {
    /// Create a new projection
    fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize the projection table
    ///
    /// Creates the `product_summary` table in the public schema.
    /// This is separate from the event_sauce schema used by the event store.
    async fn migrate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS product_summary (
                product_id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                current_stock INTEGER NOT NULL,
                current_price_cents BIGINT NOT NULL,
                last_updated TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Handle a product event and update the projection
    async fn handle_event(&self, event: &EventEnvelope) -> std::result::Result<(), Box<dyn std::error::Error>> {
        // Get product_id from the event envelope (aggregate_id)
        let product_id = event.aggregate_id;

        // Deserialize the event
        let product_event: ProductEvent = event.try_into_event()?;

        // Handle different event types
        match product_event {
            ProductEvent::Created(e) => {
                // Insert new product
                sqlx::query(
                    r#"
                    INSERT INTO product_summary
                        (product_id, name, current_stock, current_price_cents, last_updated)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (product_id) DO UPDATE
                    SET name = EXCLUDED.name,
                        current_stock = EXCLUDED.current_stock,
                        current_price_cents = EXCLUDED.current_price_cents,
                        last_updated = EXCLUDED.last_updated
                    "#,
                )
                .bind(product_id)  // From envelope, not event
                .bind(&e.name)
                .bind(e.initial_stock)
                .bind(e.price_cents)
                .bind(e.timestamp)
                .execute(&self.pool)
                .await?;
            }
            ProductEvent::StockAdded(e) => {
                // Update stock by adding quantity to current value
                sqlx::query(
                    r#"
                    UPDATE product_summary
                    SET current_stock = current_stock + $1,
                        last_updated = $2
                    WHERE product_id = $3
                    "#,
                )
                .bind(e.quantity)  // Add quantity, not set to new_total
                .bind(e.timestamp)
                .bind(product_id)  // From envelope
                .execute(&self.pool)
                .await?;
            }
            ProductEvent::StockRemoved(e) => {
                // Update stock by subtracting quantity from current value
                sqlx::query(
                    r#"
                    UPDATE product_summary
                    SET current_stock = current_stock - $1,
                        last_updated = $2
                    WHERE product_id = $3
                    "#,
                )
                .bind(e.quantity)  // Subtract quantity, not set to new_total
                .bind(e.timestamp)
                .bind(product_id)  // From envelope
                .execute(&self.pool)
                .await?;
            }
            ProductEvent::PriceChanged(e) => {
                // Update price
                sqlx::query(
                    r#"
                    UPDATE product_summary
                    SET current_price_cents = $1,
                        last_updated = $2
                    WHERE product_id = $3
                    "#,
                )
                .bind(e.new_price_cents)
                .bind(e.timestamp)
                .bind(product_id)  // From envelope
                .execute(&self.pool)
                .await?;
            }
        }

        Ok(())
    }

    /// Query the projection - get product summary
    async fn get_product_summary(&self, product_id: ProductId) -> Result<Option<ProductSummary>, sqlx::Error> {
        let result = sqlx::query_as::<_, ProductSummary>(
            r#"
            SELECT product_id, name, current_stock, current_price_cents, last_updated
            FROM product_summary
            WHERE product_id = $1
            "#,
        )
        .bind(product_id.0)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    /// Query all products
    async fn get_all_products(&self) -> Result<Vec<ProductSummary>, sqlx::Error> {
        let results = sqlx::query_as::<_, ProductSummary>(
            r#"
            SELECT product_id, name, current_stock, current_price_cents, last_updated
            FROM product_summary
            ORDER BY name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }
}

/// Product summary from the read model
#[derive(Debug, Clone, sqlx::FromRow)]
struct ProductSummary {
    product_id: Uuid,
    name: String,
    current_stock: i32,
    current_price_cents: i64,
    last_updated: DateTime<Utc>,
}

// ============================================================================
// Main Example
// ============================================================================

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!("🚀 EventSauce Postgres Quickstart\n");
    println!("This example demonstrates:");
    println!("  ✓ PostgresEventStore for event persistence");
    println!("  ✓ PostgresCheckpointStore for subscription progress");
    println!("  ✓ Custom projection with product_summary table");
    println!("  ✓ Stream API for processing events\n");

    // ========================================================================
    // 1. Setup: Start PostgreSQL container
    // ========================================================================
    println!("📦 Starting PostgreSQL container...");
    let postgres_container = Postgres::default().with_tag("17-alpine").start().await?;

    let host = postgres_container.get_host().await?;
    let port = postgres_container.get_host_port_ipv4(5432).await?;
    let connection_string = format!(
        "postgresql://postgres:postgres@{}:{}/postgres",
        host, port
    );

    println!("✓ PostgreSQL running on port {}\n", port);

    // ========================================================================
    // 2. Setup: Create database connection pool
    // ========================================================================
    println!("🔌 Connecting to database...");
    let pool = PgPool::connect(&connection_string).await?;
    println!("✓ Connected\n");

    // ========================================================================
    // 3. Setup: Create and migrate checkpoint store
    // ========================================================================
    println!("🗄️  Setting up checkpoint store...");
    let checkpoint_store = Arc::new(PostgresCheckpointStore::new(pool.clone()));
    checkpoint_store.migrate().await?;
    println!("✓ Checkpoint store ready (event_sauce.checkpoints table)\n");

    // ========================================================================
    // 4. Setup: Create and migrate event store with integrated checkpoint store
    // ========================================================================
    println!("🗄️  Setting up event store...");
    let event_store = Arc::new(
        PostgresEventStore::builder()
            .pool(pool.clone())
            .checkpoint_store(checkpoint_store.clone())
            .build(),
    );
    event_store.migrate().await?;
    println!("✓ Event store ready (event_sauce.events table)\n");

    // ========================================================================
    // 5. Setup: Create and migrate projection
    // ========================================================================
    println!("📊 Setting up product summary projection...");
    let projection = Arc::new(ProductSummaryProjection::new(pool.clone()));
    projection.migrate().await?;
    println!("✓ Projection ready (public.product_summary table)\n");

    // ========================================================================
    // 6. Write Side: Create products and execute commands
    // ========================================================================
    println!("📝 Write Side: Creating products and executing commands...\n");

    // Create first product: Laptop
    let laptop_id = ProductId::new();
    println!("Creating product: Laptop");
    // Command creates event (no validation yet)
    let event = ProductAggregate::create_product(
        laptop_id,
        "MacBook Pro 16\"".to_string(),
        10,
        299_900, // $2,999.00
    );
    let mut laptop = <ProductAggregate as Aggregate>::new(laptop_id);
    // Validation happens here in apply() via ApplyEvent::validate()
    laptop.apply(event)?;
    event_store.commit(&mut laptop).await?;
    println!("  ✓ Created: {} (${:.2})", "MacBook Pro 16\"", 2999.00);

    // Create second product: Mouse
    let mouse_id = ProductId::new();
    println!("Creating product: Mouse");
    let event = ProductAggregate::create_product(
        mouse_id,
        "Magic Mouse".to_string(),
        50,
        9_900, // $99.00
    );
    let mut mouse = <ProductAggregate as Aggregate>::new(mouse_id);
    mouse.apply(event)?;
    event_store.commit(&mut mouse).await?;
    println!("  ✓ Created: {} (${:.2})", "Magic Mouse", 99.00);

    // Add stock to laptop
    println!("\nAdding stock to laptop...");
    let mut laptop: ProductAggregate = load(event_store.as_ref(), laptop_id).await?;
    // Command creates event, validation happens in apply()
    let event = laptop.cmd_add_stock(5);
    laptop.apply(event)?;
    event_store.commit(&mut laptop).await?;
    println!("  ✓ Added 5 units (new total: 15)");

    // Remove stock from mouse
    println!("Removing stock from mouse...");
    let mut mouse: ProductAggregate = load(event_store.as_ref(), mouse_id).await?;
    // Command creates event, validation (including stock check) happens in apply()
    let event = mouse.cmd_remove_stock(3);
    mouse.apply(event)?;
    event_store.commit(&mut mouse).await?;
    println!("  ✓ Removed 3 units (new total: 47)");

    // Change laptop price
    println!("Changing laptop price...");
    let mut laptop: ProductAggregate = load(event_store.as_ref(), laptop_id).await?;
    let event = laptop.cmd_change_price(279_900); // $2,799.00
    laptop.apply(event)?;
    event_store.commit(&mut laptop).await?;
    println!("  ✓ Price changed to ${:.2}", 2799.00);

    println!("\n✓ Wrote 7 events to event store\n");

    // ========================================================================
    // 7. Read Side: Setup subscription and process events
    // ========================================================================
    println!("📖 Read Side: Processing events with subscription...\n");

    // Create subscription with checkpoint tracking
    let subscription = event_store
        .subscription_builder("product-summary-projection")
        .checkpoint_strategy(CheckpointStrategy::EveryEvent)
        .error_policy(ErrorPolicy::Fail)
        .build()?;

    println!("Subscription: product-summary-projection");
    println!("Checkpoint strategy: Save after every event");
    println!("Processing events...\n");

    // Convert subscription to stream (recommended pattern)
    let mut stream = Box::pin(subscription.into_stream().await?);

    let mut event_count = 0;

    // Process events using Stream API
    while let Some(result) = stream.next().await {
        let event = result?;
        event_count += 1;

        // Show event being processed
        let event_type = event.event_type.split("::").last().unwrap_or(&event.event_type);
        println!(
            "  [{}/7] Processing: {}",
            event_count, event_type
        );

        // Update projection
        projection.handle_event(&event).await?;
    }

    println!("\n✓ Processed {} events", event_count);
    println!("✓ Projection is up-to-date\n");

    // ========================================================================
    // 8. Query Side: Read from projection
    // ========================================================================
    println!("🔍 Query Side: Reading from projection...\n");

    // Query specific product
    if let Some(summary) = projection.get_product_summary(laptop_id).await? {
        println!("Product: {}", summary.name);
        println!("  ID: {}", summary.product_id);
        println!("  Stock: {} units", summary.current_stock);
        println!(
            "  Price: ${:.2}",
            summary.current_price_cents as f64 / 100.0
        );
        println!("  Last Updated: {}", summary.last_updated.format("%Y-%m-%d %H:%M:%S UTC"));
    }

    println!();

    // Query all products
    println!("All Products:");
    let all_products = projection.get_all_products().await?;
    for (i, summary) in all_products.iter().enumerate() {
        println!(
            "  {}. {} - {} units @ ${:.2}",
            i + 1,
            summary.name,
            summary.current_stock,
            summary.current_price_cents as f64 / 100.0
        );
    }

    println!();

    // ========================================================================
    // 9. Demonstrate checkpoint persistence
    // ========================================================================
    println!("💾 Checkpoint Status:\n");

    if let Some(_position) = checkpoint_store
        .as_ref()
        .load_checkpoint("product-summary-projection")
        .await?
    {
        println!("✓ Checkpoint saved for product-summary-projection");
        println!("✓ Projection can resume from this position on restart");
    }

    println!();

    // ========================================================================
    // 10. Summary
    // ========================================================================
    println!("✅ Example Complete!\n");
    println!("What happened:");
    println!("  1. Started PostgreSQL with testcontainers (no manual setup!)");
    println!("  2. Created event store (event_sauce.events)");
    println!("  3. Created checkpoint store (event_sauce.checkpoints)");
    println!("  4. Created projection table (public.product_summary)");
    println!("  5. Executed commands → generated events → stored in event store");
    println!("  6. Subscription read events → updated projection");
    println!("  7. Queried denormalized read model from projection");
    println!("  8. Checkpoint saved for resumable processing\n");

    println!("Key Concepts:");
    println!("  • Event Store = Source of truth (write model)");
    println!("  • Projections = Denormalized views (read models)");
    println!("  • Subscriptions = Event processors with checkpoints");
    println!("  • Stream API = Composable event processing (recommended)");
    println!("  • Schema Isolation = event_sauce schema ≠ public schema\n");

    println!("Next Steps:");
    println!("  • Add more projections (e.g., low stock alerts)");
    println!("  • Implement rebuilding (subscription.rebuild())");
    println!("  • Add event filters for selective processing");
    println!("  • Explore snapshotting for large aggregates");
    println!("  • Add tests following TDD principles\n");

    Ok(())
}
