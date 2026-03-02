//! # PostgreSQL Quick Start Example
//!
//! This example demonstrates:
//! - **Private User aggregate** with crypto-shredding for GDPR compliance
//! - **Actor-validated Order commands** requiring an authenticated User
//! - **Delete events** for order cancellation (`@delete @actor(User)`)
//! - **Function-based specifications** with `#[specification]` for validation
//! - Init creation functions for ergonomic aggregate construction
//! - Repository pattern for type-safe aggregate persistence
//! - A projection that combines Order data (User PII stays encrypted)
//! - PostgreSQL backend with testcontainers
//! - Subscription system for real-time projection updates
//!
//! Run with:
//! ```bash
//! cargo run --example postgres-quickstart --features "postgres,crypto"
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use event_sauce_core::{
    command_handler, crypto, define_events, Aggregate, AggregateVersion, CryptoKeyStore, Entity,
    EntityId, EventStore, Loaded, Specification, StreamId,
};
use event_sauce_crypto::Aes256GcmProvider;
use event_sauce_macros::{aggregate, aggregate_error, specification, AggregateError};
use event_sauce_postgres::{PostgresBackend, PostgresCryptoKeyStore};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ============================================================================
// User Aggregate (Private — encrypted at rest for GDPR compliance)
// ============================================================================

/// User domain errors - auto-implements AggregateError trait
#[derive(AggregateError, Debug, thiserror::Error)]
enum UserError {
    #[error("Invalid email: {0}")]
    InvalidEmail(String),
    #[error("Empty name")]
    EmptyName,
}

/// User role — used for actor-based permission checks on Order commands
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum UserRole {
    Customer,
    Admin,
}

// Specification: Created event email must contain @
#[specification("Email must contain @")]
fn valid_created_email(event: &CreatedEvent) -> bool {
    event.email.contains('@')
}

// Specification: Created event name must not be empty
#[specification("Name must not be empty")]
fn valid_created_name(event: &CreatedEvent) -> bool {
    !event.name.is_empty()
}

// Specification: EmailChanged event email must contain @
#[specification("Email must contain @")]
fn valid_email_change(event: &EmailChangedEvent) -> bool {
    event.new_email.contains('@')
}

// User events with define_events! macro — Created is an @init event
define_events! {
    enum UserEvent for User {
        Created {
            email: String,
            name: String,
            role: UserRole,
        }
        @init
        @validate |evt| {
            ValidCreatedEmail.validate_or(evt, |_| UserError::InvalidEmail(evt.email.clone()))?;
            ValidCreatedName.validate_or(evt, |_| UserError::EmptyName)?;
            Ok(())
        }
        => |id, event| {
            User {
                id,
                email: event.email.clone(),
                name: event.name.clone(),
                role: event.role,
            }
        },

        EmailChanged {
            new_email: String,
        }
        @validate |_agg, evt| {
            ValidEmailChange.validate_or(evt, |_| UserError::InvalidEmail(evt.new_email.clone()))?;
            Ok(())
        }
        => |user, event| {
            user.email = event.new_email.clone();
        },
    }
}

/// User aggregate — **encrypted** for GDPR: all event data is encrypted at rest.
/// The `init` flag uses the type-state pattern; `encrypted` enables crypto-shredding.
#[aggregate(event = "UserEvent", error = "UserError", init, encrypted)]
#[derive(Debug, Serialize, Deserialize)]
struct User {
    #[id]
    id: EntityId,
    email: String,
    name: String,
    role: UserRole,
}

// Use command_handler! macro for command methods
command_handler! {
    impl User {
        /// Create a new user (init command)
        @init fn create_user(email: String, name: String, role: UserRole) -> CreatedEvent {
            email, name, role
        };

        /// Change user's email address
        fn change_email(new_email: String) -> EmailChangedEvent { new_email };
    }
}

// ============================================================================
// Order Aggregate (Actor-validated — requires authenticated User)
// ============================================================================

/// Order domain errors - auto-implements AggregateError trait with spec support.
/// Uses `#[aggregate_error]` to auto-inject `SpecificationFailed` variant
/// for seamless spec-based validation via the `?` operator.
#[aggregate_error(aggregate = "Order")]
#[derive(Debug, thiserror::Error)]
enum OrderError {
    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
}

/// Order status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum OrderStatus {
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
#[specification("Order must be pending")]
fn order_is_pending(order: &Order) -> bool {
    order.status == OrderStatus::Pending
}

// Specification: ItemAdded event price must be positive
#[specification("Price must be positive")]
fn valid_item_price(event: &ItemAddedEvent) -> bool {
    event.price > 0
}

// Specification: Actor must be the order owner
#[specification("Only the order owner can perform this action", actor = actor_id)]
fn is_order_owner(order: &Order, actor_id: EntityId) -> bool {
    order.user_id == *actor_id
}

// Order events with @actor — require an authenticated User for permission checks
define_events! {
    enum OrderEvent for Order {
        Placed {
            user_id: EntityId,
        }
        @init
        @actor(User)
        => |id, event| {
            Order {
                id,
                user_id: event.user_id,
                items: Vec::new(),
                total: 0,
                status: OrderStatus::Pending,
            }
        },

        ItemAdded {
            product_id: String,
            quantity: u32,
            price: i64,
        }
        @actor(User)
        @validate |agg, actor, evt| {
            IsOrderOwner { actor_id: actor.entity_id() }.validate_or(agg, |msg| {
                OrderError::PermissionDenied(msg)
            })?;
            ValidItemPrice.validate_or(evt, |_| OrderError::InvalidAmount(evt.price))?;
            OrderIsPending.check(agg)?;
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

        Completed {}
        @actor(User)
        @validate |order, actor, _evt| {
            IsOrderOwner { actor_id: actor.entity_id() }.validate_or(order, |msg| {
                OrderError::PermissionDenied(msg)
            })?;
            Ok(())
        }
        @validate_spec(OrderIsPending)
        => |order, _event| {
            order.status = OrderStatus::Completed;
        },

        Cancelled {
            reason: String,
        }
        @delete
        @actor(User)
        @validate |order, actor, _evt| {
            IsOrderOwner { actor_id: actor.entity_id() }.validate_or(order, |msg| {
                OrderError::PermissionDenied(msg)
            })?;
            OrderIsPending.check(order)?;
            Ok(())
        }
        => |mut order, _event| {
            order.status = OrderStatus::Cancelled;
            order
        },
    }
}

/// Order aggregate with actor-validated events
/// The `init` flag uses the type-state pattern; no Option fields needed.
#[aggregate(event = "OrderEvent", error = "OrderError", init)]
#[derive(Debug, Serialize, Deserialize)]
struct Order {
    #[id]
    id: EntityId,
    user_id: EntityId, // Always valid — set by init event, no Option needed
    items: Vec<OrderItem>,
    total: i64,
    status: OrderStatus,
}

// Command handler with @actor for permission-validated commands
command_handler! {
    impl Order {
        /// Create a new order (requires authenticated user)
        @init @actor(User)
        fn create_order(user_id: EntityId) -> PlacedEvent { user_id };

        /// Add an item to the order (requires order owner)
        @actor(User)
        fn add_item(product_id: String, quantity: u32, price: i64) -> ItemAddedEvent {
            product_id, quantity, price
        };

        /// Complete the order (requires order owner)
        @actor(User)
        fn complete() -> CompletedEvent { };

        /// Cancel the order (requires order owner, delete transition)
        @delete @actor(User)
        fn cancel_order(reason: String) -> CancelledEvent { reason };
    }
}

// ============================================================================
// Order Summary Projection
// ============================================================================

/// Order summary view — references user_id, not user PII.
/// User data is encrypted at rest and should not be denormalized into projections.
#[derive(Debug, Clone)]
struct OrderSummaryView {
    order_id: EntityId,
    user_id: EntityId,
    item_count: usize,
    total_amount: i64,
    status: OrderStatus,
}

/// Projection state
#[derive(Debug, Default)]
struct ProjectionState {
    orders: HashMap<EntityId, OrderSummaryView>,
}

// Order summary projection — subscribes only to OrderEvent
// (User events are encrypted and should not be denormalized)
event_sauce_core::projection! {
    struct OrderSummaryProjection {
        state: ProjectionState,

        on OrderEvent::Placed |proj, event, aggregate_id| {
            let order_id = EntityId::from(aggregate_id);
            proj.state.orders.insert(
                order_id,
                OrderSummaryView {
                    order_id,
                    user_id: event.user_id,
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

        on OrderEvent::Completed |proj, _event, aggregate_id| {
            let order_id = EntityId::from(aggregate_id);
            if let Some(order) = proj.state.orders.get_mut(&order_id) {
                order.status = OrderStatus::Completed;
            }
        },

        on OrderEvent::Cancelled |proj, _event, aggregate_id| {
            let order_id = EntityId::from(aggregate_id);
            if let Some(order) = proj.state.orders.get_mut(&order_id) {
                order.status = OrderStatus::Cancelled;
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
    println!("  - Private User aggregate with crypto-shredding (GDPR)");
    println!("  - Actor-validated Order commands (permission checks)");
    println!("  - Delete events for order cancellation (@delete @actor)");
    println!("  - Init creation functions: User::create_user(), Order::create_order()");
    println!("  - Repository pattern for type-safe persistence");
    println!("  - Projection over Order data (User PII stays encrypted)");
    println!("  - PostgreSQL backend with testcontainers\n");

    // Setup PostgreSQL using testcontainers
    println!("  Starting PostgreSQL container...");
    let postgres = Postgres::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let database_url = format!("postgres://postgres:postgres@localhost:{port}/postgres");

    // Create and migrate the PostgreSQL crypto key store
    let pool = sqlx::PgPool::connect(&database_url).await?;
    let crypto_key_store = PostgresCryptoKeyStore::builder()
        .pool(pool)
        .schema("event_sauce")
        .build()?;
    crypto_key_store.migrate().await?;
    let crypto_key_store = Arc::new(crypto_key_store);

    // Build backend with AES-256-GCM encryption for encrypted aggregates
    println!("  Initializing backend with AES-256-GCM encryption...");
    let backend = PostgresBackend::builder()
        .database_url(&database_url)
        .schema("event_sauce")
        .crypto_key_store(crypto_key_store.clone() as Arc<dyn CryptoKeyStore>)
        .crypto_provider(Arc::new(Aes256GcmProvider))
        .build()
        .await?;

    let store = backend.event_store();

    // Create repositories for type-safe aggregate persistence
    println!("  Creating repositories...");
    let user_repo = store.repository::<User>();
    let order_repo = store.repository::<Order>();

    // ========================================================================
    // Step 1: Create Users (Private — encrypted at rest)
    // ========================================================================

    println!("\n=== Creating Users (Private — Encrypted at Rest) ===\n");

    let mut alice = User::create_user(
        "alice@example.com".to_string(),
        "Alice Smith".to_string(),
        UserRole::Customer,
    )?;
    println!(
        "  Created user: {} ({}) [role: {:?}]",
        alice.name, alice.email, alice.role
    );
    user_repo.save(&mut alice).await?;

    let mut bob = User::create_user(
        "bob@example.com".to_string(),
        "Bob Jones".to_string(),
        UserRole::Customer,
    )?;
    println!(
        "  Created user: {} ({}) [role: {:?}]",
        bob.name, bob.email, bob.role
    );
    user_repo.save(&mut bob).await?;

    // ========================================================================
    // Step 2: Verify User data is encrypted at rest
    // ========================================================================

    println!("\n=== Verifying User Data is Encrypted at Rest ===\n");

    let alice_stream_id = StreamId::new(User::aggregate_type(), alice.entity_id().as_uuid());
    let mut alice_stream = store
        .load_stream(alice_stream_id, AggregateVersion::initial())
        .await?;
    while let Some(Ok(envelope)) = alice_stream.next().await {
        let encrypted = crypto::is_encrypted(&envelope.event_data);
        println!(
            "  User event '{}': encrypted={}",
            envelope.event_type, encrypted
        );
    }

    // ========================================================================
    // Step 3: Create Orders with actor validation
    // ========================================================================

    println!("\n=== Creating Orders (Actor-Validated) ===\n");

    // Alice creates an order — she's the actor providing permission
    let mut order1 = Order::create_order(&alice, alice.entity_id())?;
    order1.add_item(&alice, "laptop".to_string(), 1, 120_000)?;
    order1.add_item(&alice, "mouse".to_string(), 2, 2500)?;
    println!(
        "  Order {} created by Alice (${:.2})",
        order1.entity_id(),
        order1.total as f64 / 100.0
    );
    order_repo.save(&mut order1).await?;

    // Bob tries to add items to Alice's order — should fail!
    println!("\n  Bob tries to add item to Alice's order...");
    match order1.add_item(&bob, "stolen-item".to_string(), 1, 100) {
        Ok(()) => println!("    ERROR: Should have been denied!"),
        Err(e) => println!("    Denied: {e}"),
    }

    // Bob creates his own order
    let order2_id = EntityId::new();
    let mut order2 = Order::create_order_with_id(order2_id, &bob, bob.entity_id())?;
    order2.add_item(&bob, "keyboard".to_string(), 1, 8500)?;
    println!(
        "\n  Order {} created by Bob (${:.2})",
        order2.entity_id(),
        order2.total as f64 / 100.0
    );
    order_repo.save(&mut order2).await?;

    // ========================================================================
    // Step 4: Complete Orders with owner validation
    // ========================================================================

    println!("\n=== Completing Orders ===\n");

    // Alice tries to complete Bob's order — should fail!
    println!("  Alice tries to complete Bob's order...");
    match order2.complete(&alice) {
        Ok(()) => println!("    ERROR: Should have been denied!"),
        Err(e) => println!("    Denied: {e}"),
    }

    // Alice completes her own order
    order1.complete(&alice)?;
    order_repo.save(&mut order1).await?;
    println!("  Order {} completed by Alice", order1.entity_id());

    // ========================================================================
    // Step 4b: Cancel an Order (Delete Event)
    // ========================================================================

    println!("\n=== Cancelling Order (Delete Event) ===\n");

    {
        use OrderDeleteCommands;

        // Bob cancels his order — type-state transition to DeletedAggregateRoot
        let order2_id = order2.entity_id();
        println!("  Bob cancels his order...");
        let mut deleted_order = order2.cancel_order(&bob, "changed my mind".to_string())?;
        println!(
            "    Cancelled: status={:?}, type=DeletedAggregateRoot",
            deleted_order.status
        );
        order_repo.save_deleted(&mut deleted_order).await?;

        // Verify: load() errors on cancelled order
        println!("\n  Verifying load() errors on cancelled order...");
        match order_repo.load(order2_id).await {
            Ok(_) => println!("    ERROR: Should have failed!"),
            Err(e) => println!("    Error (expected): {e}"),
        }

        // load_any() returns Loaded::Deleted
        let loaded = order_repo.load_any(order2_id).await?;
        match loaded {
            Loaded::Deleted(d) => println!("    load_any() → Deleted (status={:?})", d.status),
            Loaded::Active(_) => println!("    ERROR: Should be Deleted!"),
        }
    }

    // ========================================================================
    // Step 5: Update User (transparent decrypt + re-encrypt)
    // ========================================================================

    println!("\n=== Updating User (Transparent Decrypt + Re-encrypt) ===\n");

    alice.change_email("alice.smith@newdomain.com".to_string())?;
    user_repo.save(&mut alice).await?;
    println!(
        "  Alice's email changed to {} (encrypted at rest)",
        alice.email
    );

    // ========================================================================
    // Step 6: Build Projection
    // ========================================================================

    println!("\n=== Building Projection ===\n");

    let mut projection = OrderSummaryProjection::new(ProjectionState::default());

    // projection_subscription auto-wires name, filter, and checkpoint store
    let mut subscription = backend
        .projection_subscription::<OrderSummaryProjection>()
        .build()?;
    subscription.run_projection(&mut projection).await?;

    println!("  Processed Order events via run_projection");
    println!("  (User events are encrypted — projection uses user_id references only)\n");

    for order_view in projection.get_all_orders() {
        println!(
            "  Order {}: user={}, {} items, ${:.2}, {:?}",
            order_view.order_id,
            order_view.user_id,
            order_view.item_count,
            order_view.total_amount as f64 / 100.0,
            order_view.status
        );
    }

    // ========================================================================
    // Step 7: Repository Features
    // ========================================================================

    println!("\n=== Repository Features ===\n");

    // Load user (transparent decryption)
    let loaded_alice = user_repo.load(alice.entity_id()).await?;
    println!(
        "  Loaded Alice (decrypted): {} ({})",
        loaded_alice.name, loaded_alice.email
    );

    // Check existence and version
    let alice_exists = user_repo.exists(alice.entity_id()).await?;
    println!("  Alice exists: {alice_exists}");

    let alice_version = user_repo.get_version(alice.entity_id()).await?;
    println!("  Alice version: {}", alice_version.as_i64());

    // Load Order
    let loaded_order = order_repo.load(order1.entity_id()).await?;
    println!(
        "  Loaded Order: user_id={}, total=${:.2}",
        loaded_order.user_id,
        loaded_order.total as f64 / 100.0
    );

    // ========================================================================
    // Step 8: Crypto-Shredding (GDPR Right to Be Forgotten)
    // ========================================================================

    println!("\n=== Crypto-Shredding (GDPR Right to Be Forgotten) ===\n");

    let bob_uuid = bob.entity_id().as_uuid();
    println!("  Deleting encryption key for Bob ({bob_uuid})...");
    crypto_key_store.delete_key(bob_uuid).await?;
    println!("  Key deleted — Bob's data is now permanently unreadable.\n");

    // Attempt to load Bob — should fail
    println!("  Attempting to load Bob after key deletion...");
    match user_repo.load(bob.entity_id()).await {
        Ok(_) => println!("    ERROR: Should have failed!"),
        Err(e) if e.is_key_not_found() => {
            println!("    KeyNotFound (expected): {e}");
            println!("    Bob's PII is permanently erased (GDPR Article 17).");
        }
        Err(e) => println!("    Unexpected error: {e}"),
    }

    // Bob's order is still accessible via load_any() (cancelled, not encrypted)
    println!("\n  Bob's order data is still accessible via load_any():");
    let loaded_order2 = order_repo.load_any(order2_id).await?;
    match loaded_order2 {
        Loaded::Deleted(d) => {
            println!(
                "    Order {}: user_id={}, total=${:.2}, status={:?} (cancelled)",
                d.entity_id(),
                d.user_id,
                d.total as f64 / 100.0,
                d.status
            );
        }
        Loaded::Active(a) => {
            println!(
                "    Order {}: user_id={}, total=${:.2} (active)",
                a.entity_id(),
                a.user_id,
                a.total as f64 / 100.0
            );
        }
    }

    println!("\n  Demo complete!");

    Ok(())
}
