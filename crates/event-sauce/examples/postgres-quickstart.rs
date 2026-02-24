//! # PostgreSQL Quick Start Example
//!
//! This example demonstrates:
//! - Two aggregates (User and Order) with events
//! - Repository pattern for type-safe aggregate persistence
//! - A projection that combines data from both aggregates
//! - PostgreSQL backend for events, snapshots, and projections
//! - Subscription system for real-time projection updates
//!
//! Run with:
//! ```bash
//! cargo run --example postgres-quickstart --features postgres
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use event_sauce_core::{
    command_handler, define_events, Aggregate, CheckpointStrategy, ErrorPolicy, EventFilter,
    EventStore, Repository,
};
use event_sauce_macros::{aggregate, AggregateError, AggregateId};
use event_sauce_postgres::{PostgresCheckpointStore, PostgresEventStore};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ============================================================================
// User Aggregate
// ============================================================================

/// User aggregate ID - auto-implements AggregateId trait and Display
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
struct UserId(Uuid);

impl UserId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// User domain errors - auto-implements AggregateError trait
#[derive(AggregateError, Debug, thiserror::Error)]
enum UserError {
    #[error("Invalid email: {0}")]
    InvalidEmail(String),
}

/// User status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
enum UserStatus {
    #[default]
    Active,
}

// User events with define_events! macro (defined BEFORE aggregate)
define_events! {
    enum UserEvent for User {
        Created {
            email: String,
            name: String,
        }
        @validate |_agg, evt| {
            if !evt.email.contains('@') {
                return Err(UserError::InvalidEmail(evt.email.clone()));
            }
            Ok(())
        }
        => |user, event| {
            user.email = event.email.clone();
            user.name = event.name.clone();
            user.status = UserStatus::Active;
        },

        EmailChanged {
            new_email: String,
        }
        @validate |_agg, evt| {
            if !evt.new_email.contains('@') {
                return Err(UserError::InvalidEmail(evt.new_email.clone()));
            }
            Ok(())
        }
        => |user, event| {
            user.email = event.new_email.clone();
        },
    }
}

/// User aggregate with #[aggregate] macro
#[aggregate(id = "UserId", event = "UserEvent", error = "UserError")]
#[derive(Default)]
struct User {
    email: String,
    name: String,
    status: UserStatus,
}

impl User {
    /// Create a new user
    fn create(email: String, name: String) -> Result<Self, UserError> {
        let mut user = Self::new(UserId::new());
        let event = CreatedEvent {
            email,
            name,
            timestamp: chrono::Utc::now(),
        };
        user.apply(event)?;
        Ok(user)
    }
}

// Use command_handler! macro for command methods
command_handler! {
    impl User {
        /// Change user's email address
        fn change_email(new_email: String) -> EmailChangedEvent { new_email };
    }
}

// ============================================================================
// Order Aggregate
// ============================================================================

/// Order aggregate ID - auto-implements AggregateId trait and Display
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
struct OrderId(Uuid);

impl OrderId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Order domain errors - auto-implements AggregateError trait
#[derive(AggregateError, Debug, thiserror::Error)]
enum OrderError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),
    #[error("Order is already {0:?}")]
    InvalidStatus(OrderStatus),
}

/// Order status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
enum OrderStatus {
    #[default]
    Pending,
    Completed,
    Cancelled,
}

/// Order item
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderItem {
    product_id: String,
    quantity: u32,
    price: i64,
}

// Order events with define_events! macro (defined BEFORE aggregate)
define_events! {
    enum OrderEvent for Order {
        OrderCreated {
            user_id: UserId,
        }
        => |order, event| {
            order.user_id = Some(event.user_id);
            order.status = OrderStatus::Pending;
        },

        ItemAdded {
            product_id: String,
            quantity: u32,
            price: i64,
        }
        @validate |agg, evt| {
            if evt.price <= 0 {
                return Err(OrderError::InvalidAmount(evt.price));
            }
            if agg.status != OrderStatus::Pending {
                return Err(OrderError::InvalidStatus(agg.status));
            }
            Ok(())
        }
        => |order, event| {
            let item_total = event.price * event.quantity as i64;
            order.items.push(OrderItem {
                product_id: event.product_id.clone(),
                quantity: event.quantity,
                price: event.price,
            });
            order.total += item_total;
        },

        OrderCompleted {}
        @validate |agg, _evt| {
            if agg.status != OrderStatus::Pending {
                return Err(OrderError::InvalidStatus(agg.status));
            }
            Ok(())
        }
        => |order, _event| {
            order.status = OrderStatus::Completed;
        },
    }
}

/// Order aggregate with #[aggregate] macro
#[aggregate(id = "OrderId", event = "OrderEvent", error = "OrderError")]
#[derive(Default)]
struct Order {
    user_id: Option<UserId>,
    items: Vec<OrderItem>,
    total: i64,
    status: OrderStatus,
}

impl Order {
    /// Create a new order for a user
    fn create(user_id: UserId) -> Result<Self, OrderError> {
        let mut order = Self::new(OrderId::new());
        let event = OrderCreatedEvent {
            user_id,
            timestamp: chrono::Utc::now(),
        };
        order.apply(event)?;
        Ok(order)
    }
}

// Use command_handler! macro for command methods
command_handler! {
    impl Order {
        /// Add an item to the order
        fn add_item(product_id: String, quantity: u32, price: i64) -> ItemAddedEvent {
            product_id, quantity, price
        };

        /// Complete the order
        fn complete() -> OrderCompletedEvent { };
    }
}

// ============================================================================
// Order Summary Projection
// ============================================================================

/// Order summary view
#[derive(Debug, Clone)]
struct OrderSummaryView {
    order_id: OrderId,
    user_id: UserId,
    user_email: String,
    user_name: String,
    item_count: usize,
    total_amount: i64,
    status: OrderStatus,
}

/// Projection state
#[derive(Debug, Default)]
struct ProjectionState {
    users: HashMap<UserId, (String, String)>,
    orders: HashMap<OrderId, OrderSummaryView>,
}

// Order summary projection using the projection! macro with aggregate_id
event_sauce_core::projection! {
    pub struct OrderSummaryProjection {
        state: ProjectionState,

        on UserEvent::Created |proj, event, aggregate_id| {
            let user_id = UserId(aggregate_id);
            proj.state.users.insert(user_id, (event.email.clone(), event.name.clone()));
        },

        on UserEvent::EmailChanged |proj, event, aggregate_id| {
            let user_id = UserId(aggregate_id);
            if let Some((email, _)) = proj.state.users.get_mut(&user_id) {
                *email = event.new_email.clone();
            }
            // Update orders
            for order in proj.state.orders.values_mut() {
                if order.user_id == user_id {
                    order.user_email = event.new_email.clone();
                }
            }
        },

        on OrderEvent::OrderCreated |proj, event, aggregate_id| {
            let order_id = OrderId(aggregate_id);
            let (email, name) = proj.state.users.get(&event.user_id).cloned().unwrap_or_else(|| {
                ("unknown@example.com".to_string(), "Unknown".to_string())
            });
            proj.state.orders.insert(
                order_id,
                OrderSummaryView {
                    order_id,
                    user_id: event.user_id,
                    user_email: email,
                    user_name: name,
                    item_count: 0,
                    total_amount: 0,
                    status: OrderStatus::Pending,
                },
            );
        },

        on OrderEvent::ItemAdded |proj, event, aggregate_id| {
            let order_id = OrderId(aggregate_id);
            if let Some(order) = proj.state.orders.get_mut(&order_id) {
                order.item_count += 1;
                order.total_amount += event.price * event.quantity as i64;
            }
        },

        on OrderEvent::OrderCompleted |proj, _event, aggregate_id| {
            let order_id = OrderId(aggregate_id);
            if let Some(order) = proj.state.orders.get_mut(&order_id) {
                order.status = OrderStatus::Completed;
            }
        },
    }
}

impl OrderSummaryProjection {
    fn get_all_orders(&self) -> Vec<OrderSummaryView> {
        self.state.orders.values().cloned().collect()
    }
}

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Event Sauce - PostgreSQL Quick Start\n");
    println!("This example demonstrates:");
    println!("  ✓ Two aggregates: User and Order");
    println!("  ✓ Events for both aggregates");
    println!("  ✓ Repository pattern for type-safe persistence");
    println!("  ✓ Projection combining data from both");
    println!("  ✓ PostgreSQL backend with testcontainers\n");

    // Setup PostgreSQL
    println!("📦 Starting PostgreSQL container...");
    let postgres = Postgres::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;

    let database_url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    let pool = sqlx::postgres::PgPool::connect(&database_url).await?;

    // Setup stores using builder pattern
    println!("🗄️  Initializing event store...");
    let event_store = PostgresEventStore::builder()
        .pool(pool.clone())
        .schema("event_sauce") // Use custom schema for isolation
        .build();
    event_store.migrate().await?;

    println!("📊 Initializing checkpoint store...");
    let checkpoint_store = PostgresCheckpointStore::builder()
        .pool(pool.clone())
        .schema("event_sauce") // Use same schema as event store
        .build();
    checkpoint_store.migrate().await?;

    let store = Arc::new(event_store);
    let checkpoint_ref = Arc::new(checkpoint_store);

    // Create repositories for type-safe aggregate persistence
    println!("🔧 Creating repositories...");
    let user_repo = Repository::<PostgresEventStore, User>::new(Arc::clone(&store));
    let order_repo = Repository::<PostgresEventStore, Order>::new(Arc::clone(&store));

    println!("\n=== Creating Users ===\n");

    // Create users
    let mut alice = User::create("alice@example.com".to_string(), "Alice Smith".to_string())?;
    println!("👤 Created user: {} ({})", alice.name, alice.email);
    user_repo.save(&mut alice).await?;

    let mut bob = User::create("bob@example.com".to_string(), "Bob Jones".to_string())?;
    println!("👤 Created user: {} ({})", bob.name, bob.email);
    user_repo.save(&mut bob).await?;

    println!("\n=== Creating Orders ===\n");

    // Create orders
    let mut order1 = Order::create(alice.id)?;
    order1.add_item("laptop".to_string(), 1, 120000)?;
    order1.add_item("mouse".to_string(), 2, 2500)?;
    println!(
        "🛒 Order {} created for Alice (${:.2})",
        order1.id,
        order1.total as f64 / 100.0
    );
    order_repo.save(&mut order1).await?;

    let mut order2 = Order::create(bob.id)?;
    order2.add_item("keyboard".to_string(), 1, 8500)?;
    println!(
        "🛒 Order {} created for Bob (${:.2})",
        order2.id,
        order2.total as f64 / 100.0
    );
    order_repo.save(&mut order2).await?;

    println!("\n=== Completing Orders ===\n");

    order1.complete()?;
    order_repo.save(&mut order1).await?;
    println!("✅ Order {} completed", order1.id);

    println!("\n=== Updating User ===\n");

    alice.change_email("alice.smith@newdomain.com".to_string())?;
    user_repo.save(&mut alice).await?;
    println!("📧 Alice's email changed to {}", alice.email);

    println!("\n=== Building Projection ===\n");

    let mut projection = OrderSummaryProjection::new(ProjectionState::default());

    let subscription = store
        .subscription_builder("order-summary")
        .checkpoint_store(checkpoint_ref)
        .checkpoint_strategy(CheckpointStrategy::EveryN(10))
        .error_policy(ErrorPolicy::Fail)
        .filter(EventFilter::all())
        .build()?;

    let stream = subscription.into_stream().await?;
    tokio::pin!(stream);

    let mut event_count = 0;
    while let Some(result) = stream.next().await {
        let envelope = result?;
        projection.handle(&envelope).await?;
        event_count += 1;
    }

    println!("📊 Processed {event_count} events");
    println!("\n=== Projection Results ===\n");

    for order_view in projection.get_all_orders() {
        println!(
            "Order {}: {} ({}) - {} items, ${:.2}, Status: {:?}",
            order_view.order_id,
            order_view.user_name,
            order_view.user_email,
            order_view.item_count,
            order_view.total_amount as f64 / 100.0,
            order_view.status
        );
    }

    println!("\n=== Repository Features ===\n");

    // Demonstrate repository features
    println!("Repository API examples:");

    // Check existence
    let alice_exists = user_repo.exists(alice.id).await?;
    println!("  • Alice exists: {alice_exists}");

    // Get version
    let alice_version = user_repo.get_version(alice.id).await?;
    println!("  • Alice version: {}", alice_version.as_i32());

    // Count events
    let alice_event_count = user_repo.count_events(alice.id).await?;
    println!("  • Alice event count: {alice_event_count}");

    // Load aggregate from repository
    let loaded_alice = user_repo.load(alice.id).await?;
    println!(
        "  • Loaded Alice: {} ({})",
        loaded_alice.name, loaded_alice.email
    );

    println!("\n✨ Demo complete!");

    Ok(())
}
