//! # PostgreSQL Quick Start Example
//!
//! This example demonstrates:
//! - **Typed aggregate IDs** with `#[derive(AggregateId)]` for compile-time safety
//! - **Private User aggregate** with crypto-shredding for GDPR compliance
//! - **Actor-validated Order commands** requiring an authenticated User
//! - **Delete events** for order cancellation (`@delete @actor(User)`)
//! - **Policy system** for cross-aggregate event orchestration
//! - **Function-based specifications** with `#[specification]` for validation
//! - Init creation functions for ergonomic aggregate construction
//! - Repository pattern for type-safe aggregate persistence
//! - A projection that combines Order data (User PII stays encrypted)
//! - PostgreSQL backend with testcontainers
//! - Transactional projections via `run_postgres_projection` for read models
//! - **Causation chain tracking** across policy-produced events
//!
//! Run with:
//! ```bash
//! cargo run --example postgres-quickstart --features "postgres"
//! ```

use std::sync::Arc;

use event_sauce_core::{
    command_handler, crypto, define_events, policy, Aggregate, AggregateRoot, AggregateVersion,
    EntityId, EventStore, Loaded, Position, Specification, StreamId,
};
use event_sauce_macros::{aggregate, aggregate_error, specification, AggregateError, AggregateId};
use event_sauce_postgres::PostgresBackend;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// ============================================================================
// Typed Aggregate IDs
// ============================================================================

/// Type-safe User ID — prevents accidentally loading a Group with a User's ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AggregateId)]
#[aggregate_id(User)]
struct UserId(EntityId);

/// Type-safe Order ID — enables `order_repo.load(order_id)` with no type annotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, AggregateId)]
#[aggregate_id(Order)]
struct OrderId(EntityId);

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
                id: id.into(),
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
/// The `#[id]` field uses a typed `UserId` — compile-time safety with no separate ID definition.
#[aggregate(event = "UserEvent", error = "UserError", init, encrypted)]
#[derive(Debug, Serialize, Deserialize)]
struct User {
    #[id]
    id: UserId,
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
fn is_order_owner(order: &Order, actor_id: UserId) -> bool {
    order.user_id == *actor_id
}

// Order events with @actor — require an authenticated User for permission checks
define_events! {
    enum OrderEvent for Order {
        Placed {
            user_id: UserId,
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
            IsOrderOwner { actor_id: actor.id() }.validate_or(agg, |msg| {
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
            IsOrderOwner { actor_id: actor.id() }.validate_or(order, |msg| {
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
            IsOrderOwner { actor_id: actor.id() }.validate_or(order, |msg| {
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
    user_id: UserId, // Always valid — set by init event, no Option needed
    items: Vec<OrderItem>,
    total: i64,
    status: OrderStatus,
}

// Command handler with @actor for permission-validated commands
command_handler! {
    impl Order {
        /// Create a new order (requires authenticated user)
        @init @actor(User)
        fn create_order(user_id: UserId) -> PlacedEvent { user_id };

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
// Notification Aggregate (created by policy on order completion)
// ============================================================================

#[derive(Debug, thiserror::Error, AggregateError)]
#[error("notification error")]
struct NotificationError;

#[aggregate(event = "NotificationEvent", error = "NotificationError")]
#[derive(Debug, Serialize, Deserialize)]
struct Notification {
    #[id]
    id: EntityId,
    message: String,
}

define_events! {
    enum NotificationEvent for Notification {
        Sent {
            message: String,
        } => |n, event| {
            n.message = event.message.clone();
        },
    }
}

command_handler! {
    impl Notification {
        fn send_notification(message: String) -> SentEvent { message };
    }
}

// ============================================================================
// Policy: Send notification when an order is completed
// ============================================================================

policy! {
    /// Sends a notification when an order is completed.
    OrderCompletedPolicy {
        on CompletedEvent |_event, ctx| {
            let order_id = EntityId::from(ctx.source_event().aggregate_id);
            let id = EntityId::new();
            let mut notification = AggregateRoot::<Notification>::new(id);
            let msg = format!("Order {order_id} has been completed!");
            notification.send_notification(msg)?;
            ctx.commit(&mut notification).await?;
            Ok(())
        },
    }
}

// ============================================================================
// Order Summary Projection (transactional, postgres-backed)
// ============================================================================

/// Postgres-backed projection: writes go through the runner-provided
/// `&mut sqlx::Transaction`; the checkpoint advances atomically with each row.
struct OrderSummaryProjection;

impl OrderSummaryProjection {
    /// User-owned migration: create the read model table.
    async fn migrate(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS event_sauce.order_summary (
                order_id UUID PRIMARY KEY,
                user_id UUID NOT NULL,
                item_count INTEGER NOT NULL,
                total_amount BIGINT NOT NULL,
                status TEXT NOT NULL
            )",
        )
        .execute(pool)
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl event_sauce_postgres::PostgresProjection for OrderSummaryProjection {
    const NAME: &'static str = "OrderSummaryProjection";

    fn handled_event_types() -> Option<Vec<&'static str>> {
        Some(vec![
            <PlacedEvent as event_sauce_core::EventType>::EVENT_TYPE,
            <ItemAddedEvent as event_sauce_core::EventType>::EVENT_TYPE,
            <CompletedEvent as event_sauce_core::EventType>::EVENT_TYPE,
            <CancelledEvent as event_sauce_core::EventType>::EVENT_TYPE,
        ])
    }

    async fn handle(
        &mut self,
        envelope: &event_sauce_core::EventEnvelope,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> event_sauce_core::Result<()> {
        let order_id = envelope.aggregate_id;

        if envelope.event_type == <PlacedEvent as event_sauce_core::EventType>::EVENT_TYPE {
            let event: PlacedEvent = serde_json::from_value(envelope.event_data.clone())
                .map_err(|e| event_sauce_core::Error::custom(e.to_string()))?;
            sqlx::query(
                "INSERT INTO event_sauce.order_summary
                    (order_id, user_id, item_count, total_amount, status)
                 VALUES ($1, $2, 0, 0, 'pending')
                 ON CONFLICT (order_id) DO NOTHING",
            )
            .bind(order_id)
            .bind(event.user_id.as_uuid())
            .execute(&mut **tx)
            .await
            .map_err(|e| event_sauce_core::Error::custom(e.to_string()))?;
        } else if envelope.event_type == <ItemAddedEvent as event_sauce_core::EventType>::EVENT_TYPE
        {
            let event: ItemAddedEvent = serde_json::from_value(envelope.event_data.clone())
                .map_err(|e| event_sauce_core::Error::custom(e.to_string()))?;
            let delta = event.price * i64::from(event.quantity);
            sqlx::query(
                "UPDATE event_sauce.order_summary
                    SET item_count = item_count + 1,
                        total_amount = total_amount + $2
                  WHERE order_id = $1",
            )
            .bind(order_id)
            .bind(delta)
            .execute(&mut **tx)
            .await
            .map_err(|e| event_sauce_core::Error::custom(e.to_string()))?;
        } else if envelope.event_type == <CompletedEvent as event_sauce_core::EventType>::EVENT_TYPE
        {
            sqlx::query(
                "UPDATE event_sauce.order_summary SET status = 'completed' WHERE order_id = $1",
            )
            .bind(order_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| event_sauce_core::Error::custom(e.to_string()))?;
        } else if envelope.event_type == <CancelledEvent as event_sauce_core::EventType>::EVENT_TYPE
        {
            sqlx::query(
                "UPDATE event_sauce.order_summary SET status = 'cancelled' WHERE order_id = $1",
            )
            .bind(order_id)
            .execute(&mut **tx)
            .await
            .map_err(|e| event_sauce_core::Error::custom(e.to_string()))?;
        }

        Ok(())
    }
}

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Event Sauce - PostgreSQL Quick Start\n");
    println!("This example demonstrates:");
    println!("  - Typed aggregate IDs (#[derive(AggregateId)]) for compile-time safety");
    println!("  - Private User aggregate with crypto-shredding (GDPR)");
    println!("  - Actor-validated Order commands (permission checks)");
    println!("  - Delete events for order cancellation (@delete @actor)");
    println!("  - Policy system: auto-notify on order completion");
    println!("  - Init creation functions: User::create_user(), Order::create_order()");
    println!("  - Repository pattern for type-safe persistence");
    println!("  - Projection over Order data (User PII stays encrypted)");
    println!("  - PostgreSQL backend with testcontainers\n");

    // Setup PostgreSQL using testcontainers
    println!("  Starting PostgreSQL container...");
    let postgres = Postgres::default().start().await?;
    let port = postgres.get_host_port_ipv4(5432).await?;
    let database_url = format!("postgres://postgres:postgres@localhost:{port}/postgres");

    // Build backend — crypto key store (PostgresCryptoKeyStore) and provider
    // (AES-256-GCM) are included and migrated automatically.
    println!("  Initializing backend with AES-256-GCM encryption...");
    let backend = PostgresBackend::builder()
        .database_url(&database_url)
        .schema("event_sauce")
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
    // Typed ID: .id() returns UserId directly (no manual conversion needed)
    let alice_id = alice.id();
    println!(
        "  Created user: {} ({}) [role: {:?}] [id: {alice_id}]",
        alice.name, alice.email, alice.role
    );
    user_repo.save(&mut alice).await?;

    let mut bob = User::create_user(
        "bob@example.com".to_string(),
        "Bob Jones".to_string(),
        UserRole::Customer,
    )?;
    let bob_id = bob.id();
    println!(
        "  Created user: {} ({}) [role: {:?}] [id: {bob_id}]",
        bob.name, bob.email, bob.role
    );
    user_repo.save(&mut bob).await?;

    // ========================================================================
    // Step 2: Verify User data is encrypted at rest
    // ========================================================================

    println!("\n=== Verifying User Data is Encrypted at Rest ===\n");

    let alice_stream_id = StreamId::new(User::aggregate_type(), alice.id().as_uuid());
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
    let mut order1 = Order::create_order(&alice, alice.id())?;
    let order1_id = OrderId(order1.id());
    order1.add_item(&alice, "laptop".to_string(), 1, 120_000)?;
    order1.add_item(&alice, "mouse".to_string(), 2, 2500)?;
    println!(
        "  Order {order1_id} created by Alice (${:.2})",
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
    let order2_eid = EntityId::new();
    let order2_id = OrderId(order2_eid);
    let mut order2 = Order::create_order_with_id(order2_eid, &bob, bob.id())?;
    order2.add_item(&bob, "keyboard".to_string(), 1, 8500)?;
    println!(
        "\n  Order {order2_id} created by Bob (${:.2})",
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
    println!("  Order {order1_id} completed by Alice");

    // ========================================================================
    // Step 4b: Cancel an Order (Delete Event)
    // ========================================================================

    println!("\n=== Cancelling Order (Delete Event) ===\n");

    {
        use OrderDeleteCommands;

        // Bob cancels his order — type-state transition to DeletedAggregateRoot
        println!("  Bob cancels his order...");
        let mut deleted_order = order2.cancel_order(&bob, "changed my mind".to_string())?;
        println!(
            "    Cancelled: status={:?}, type=DeletedAggregateRoot",
            deleted_order.status
        );
        order_repo.save_deleted(&mut deleted_order).await?;

        // Verify: load() errors on cancelled order — typed ID for type-safe loading
        println!("\n  Verifying load() errors on cancelled order...");
        match order_repo.load(order2_id).await {
            Ok(_) => println!("    ERROR: Should have failed!"),
            Err(e) => println!("    Error (expected): {e}"),
        }

        // load_any() returns Loaded::Deleted — typed ID works here too
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
    // Step 6: Run Policy (Cross-Aggregate Event Orchestration)
    // ========================================================================

    println!("\n=== Running Policy (Order Completion → Notification) ===\n");

    let runner = store
        .policy_runner()?
        .register(Arc::new(OrderCompletedPolicy));

    let processed = runner.process_pending().await?;
    println!("  Processed {processed} event-policy matches");

    // ========================================================================
    // Step 7: Show Causation Chain
    // ========================================================================

    println!("\n=== Event Causation Chain ===\n");

    let stream = store.stream_all(Position::start()).await?;
    futures::pin_mut!(stream);

    while let Some(Ok(entry)) = stream.next().await {
        let envelope = entry.envelope;
        // Only show events with causation metadata (i.e., policy-produced)
        if let Some(ref meta) = envelope.metadata {
            if meta.causation_id.is_some() {
                println!(
                    "  Event: {} (aggregate: {}, id: {})",
                    envelope.event_type, envelope.aggregate_type, envelope.id
                );
                if let Some(causation_id) = meta.causation_id {
                    println!("    causation_id: {causation_id}");
                }
                if let Some(correlation_id) = meta.correlation_id {
                    println!("    correlation_id: {correlation_id}");
                }
                if !meta.causation_chain.is_empty() {
                    println!(
                        "    causation_chain: {} event(s) deep",
                        meta.causation_chain.len()
                    );
                }
            }
        }
    }

    // ========================================================================
    // Step 8: Build Projection (transactional, postgres-backed)
    // ========================================================================

    println!("\n=== Building Projection (transactional) ===\n");

    OrderSummaryProjection::migrate(backend.pool()).await?;

    let mut projection = OrderSummaryProjection;
    backend.run_postgres_projection(&mut projection).await?;

    println!("  Processed Order events via run_postgres_projection");
    println!("  (Each event applied + checkpoint advanced in the same transaction)\n");

    let rows: Vec<(uuid::Uuid, uuid::Uuid, i32, i64, String)> = sqlx::query_as(
        "SELECT order_id, user_id, item_count, total_amount, status
           FROM event_sauce.order_summary
           ORDER BY order_id",
    )
    .fetch_all(backend.pool())
    .await?;

    for (order_id, user_id, item_count, total_amount, status) in rows {
        println!(
            "  Order {}: user={}, {} items, ${:.2}, {}",
            order_id,
            user_id,
            item_count,
            total_amount as f64 / 100.0,
            status,
        );
    }

    // ========================================================================
    // Step 9: Repository Features with Typed IDs
    // ========================================================================

    println!("\n=== Repository Features (Typed IDs) ===\n");

    // Load user with typed ID — no type annotation needed
    let loaded_alice = user_repo.load(alice_id).await?;
    println!(
        "  Loaded Alice (decrypted): {} ({})",
        loaded_alice.name, loaded_alice.email
    );

    // Check existence with typed ID
    let alice_exists = user_repo.exists(alice_id).await?;
    println!("  Alice exists: {alice_exists}");

    let alice_version = user_repo.get_version(alice_id).await?;
    println!("  Alice version: {}", alice_version.as_i64());

    // Load Order with typed ID
    let loaded_order = order_repo.load(order1_id).await?;
    println!(
        "  Loaded Order: user_id={}, total=${:.2}",
        loaded_order.user_id,
        loaded_order.total as f64 / 100.0
    );

    // ========================================================================
    // Step 10: Crypto-Shredding (GDPR Right to Be Forgotten)
    // ========================================================================

    println!("\n=== Crypto-Shredding (GDPR Right to Be Forgotten) ===\n");

    let bob_uuid = bob.id().as_uuid();
    println!("  Deleting encryption key for Bob ({bob_uuid})...");
    store
        .crypto_key_store()
        .expect("crypto key store configured")
        .delete_key(bob_uuid)
        .await?;
    println!("  Key deleted — Bob's data is now permanently unreadable.\n");

    // Attempt to load Bob — should fail
    println!("  Attempting to load Bob after key deletion...");
    match user_repo.load(bob_id).await {
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
                a.id(),
                a.user_id,
                a.total as f64 / 100.0
            );
        }
    }

    println!("\n  Demo complete!");

    Ok(())
}
