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
    command_handler, define_events, spec, CheckpointStrategy, EntityId, ErrorPolicy, EventFilter,
    EventStore, Specification,
};
use event_sauce_macros::{aggregate, aggregate_error, AggregateError};
use event_sauce_postgres::PostgresBackend;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ============================================================================
// User Aggregate
// ============================================================================

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
#[aggregate(event = "UserEvent", error = "UserError")]
#[derive(Default, Serialize, Deserialize)]
struct User {
    #[id]
    id: EntityId,
    email: String,
    name: String,
    status: UserStatus,
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

/// Order domain errors - auto-implements AggregateError trait with spec support.
/// Uses `#[aggregate_error]` to auto-inject `SpecificationFailed` variant
/// for seamless spec-based validation via the `?` operator.
#[aggregate_error(aggregate = "Order")]
#[derive(Debug, thiserror::Error)]
enum OrderError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),
}

/// Order status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
enum OrderStatus {
    #[default]
    Pending,
    Completed,
    #[allow(dead_code)]
    Cancelled,
}

/// Order item
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderItem {
    product_id: String,
    quantity: u32,
    price: i64,
}

// Specification: Order must be in Pending status
spec!(OrderIsPending for Order, "Order must be pending", |o| {
    o.status == OrderStatus::Pending
});

// Order events with define_events! macro (defined BEFORE aggregate)
define_events! {
    enum OrderEvent for Order {
        OrderCreated {
            user_id: EntityId,
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
            OrderIsPending.check(agg)?; // Uses SpecificationError -> OrderError via From
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
        @validate_spec(OrderIsPending)
        => |order, _event| {
            order.status = OrderStatus::Completed;
        },
    }
}

/// Order aggregate with #[aggregate] macro
#[aggregate(event = "OrderEvent", error = "OrderError")]
#[derive(Default, Serialize, Deserialize)]
struct Order {
    #[id]
    id: EntityId,
    user_id: Option<EntityId>,
    items: Vec<OrderItem>,
    total: i64,
    status: OrderStatus,
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
    order_id: EntityId,
    user_id: EntityId,
    user_email: String,
    user_name: String,
    item_count: usize,
    total_amount: i64,
    status: OrderStatus,
}

/// Projection state
#[derive(Debug, Default)]
struct ProjectionState {
    users: HashMap<EntityId, (String, String)>,
    orders: HashMap<EntityId, OrderSummaryView>,
}

// Order summary projection using the projection! macro with aggregate_id
event_sauce_core::projection! {
    pub struct OrderSummaryProjection {
        state: ProjectionState,

        on UserEvent::Created |proj, event, aggregate_id| {
            proj.state.users.insert(EntityId::from(aggregate_id), (event.email.clone(), event.name.clone()));
        },

        on UserEvent::EmailChanged |proj, event, aggregate_id| {
            let user_id = EntityId::from(aggregate_id);
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
            let order_id = EntityId::from(aggregate_id);
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
            let order_id = EntityId::from(aggregate_id);
            if let Some(order) = proj.state.orders.get_mut(&order_id) {
                order.item_count += 1;
                order.total_amount += event.price * event.quantity as i64;
            }
        },

        on OrderEvent::OrderCompleted |proj, _event, aggregate_id| {
            let order_id = EntityId::from(aggregate_id);
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
    println!("Event Sauce - PostgreSQL Quick Start\n");
    println!("This example demonstrates:");
    println!("  - Two aggregates: User and Order");
    println!("  - Events for both aggregates");
    println!("  - Repository pattern for type-safe persistence");
    println!("  - Projection combining data from both");
    println!("  - PostgreSQL backend with testcontainers\n");

    // Setup PostgreSQL using PostgresBackend (handles pool, stores, and migrations)
    println!("  Starting PostgreSQL container...");
    let postgres = Postgres::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let database_url = format!("postgres://postgres:postgres@localhost:{port}/postgres");

    println!("  Initializing backend (event store + checkpoint store + migrations)...");
    let backend = PostgresBackend::setup(&database_url, "event_sauce").await?;

    let store = Arc::new(backend.event_store().clone());
    let checkpoint_ref: Arc<dyn event_sauce_core::CheckpointStore> =
        Arc::new(backend.checkpoint_store().clone());

    // Create repositories for type-safe aggregate persistence
    println!("  Creating repositories...");
    let user_repo = store.repository::<User>();
    let order_repo = store.repository::<Order>();

    println!("\n=== Creating Users ===\n");

    // Create users
    let mut alice = user_repo.create();
    alice.apply(CreatedEvent {
        email: "alice@example.com".to_string(),
        name: "Alice Smith".to_string(),
        timestamp: chrono::Utc::now(),
    })?;
    println!("  Created user: {} ({})", alice.name, alice.email);
    user_repo.save(&mut alice).await?;

    let mut bob = user_repo.create();
    bob.apply(CreatedEvent {
        email: "bob@example.com".to_string(),
        name: "Bob Jones".to_string(),
        timestamp: chrono::Utc::now(),
    })?;
    println!("  Created user: {} ({})", bob.name, bob.email);
    user_repo.save(&mut bob).await?;

    println!("\n=== Creating Orders ===\n");

    // Create orders
    let mut order1 = order_repo.create();
    order1.apply(OrderCreatedEvent {
        user_id: alice.entity_id(),
        timestamp: chrono::Utc::now(),
    })?;
    order1.add_item("laptop".to_string(), 1, 120_000)?;
    order1.add_item("mouse".to_string(), 2, 2500)?;
    println!(
        "  Order {} created for Alice (${:.2})",
        order1.entity_id(),
        order1.total as f64 / 100.0
    );
    order_repo.save(&mut order1).await?;

    let mut order2 = order_repo.create();
    order2.apply(OrderCreatedEvent {
        user_id: bob.entity_id(),
        timestamp: chrono::Utc::now(),
    })?;
    order2.add_item("keyboard".to_string(), 1, 8500)?;
    println!(
        "  Order {} created for Bob (${:.2})",
        order2.entity_id(),
        order2.total as f64 / 100.0
    );
    order_repo.save(&mut order2).await?;

    println!("\n=== Completing Orders ===\n");

    order1.complete()?;
    order_repo.save(&mut order1).await?;
    println!("  Order {} completed", order1.entity_id());

    println!("\n=== Updating User ===\n");

    alice.change_email("alice.smith@newdomain.com".to_string())?;
    user_repo.save(&mut alice).await?;
    println!("  Alice's email changed to {}", alice.email);

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

    println!("  Processed {event_count} events");
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
    let alice_exists = user_repo.exists(alice.entity_id()).await?;
    println!("  Alice exists: {alice_exists}");

    // Get version
    let alice_version = user_repo.get_version(alice.entity_id()).await?;
    println!("  Alice version: {}", alice_version.as_u64());

    // Count events
    let alice_event_count = user_repo.count_events(alice.entity_id()).await?;
    println!("  Alice event count: {alice_event_count}");

    // Load aggregate from repository
    let loaded_alice = user_repo.load(alice.entity_id()).await?;
    println!(
        "  Loaded Alice: {} ({})",
        loaded_alice.name, loaded_alice.email
    );

    println!("\n  Demo complete!");

    Ok(())
}
