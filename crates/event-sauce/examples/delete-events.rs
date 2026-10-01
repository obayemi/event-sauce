//! # Delete Events Example - Type-State Delete Transitions
//!
//! This example demonstrates delete events that provide a compile-time safe
//! type-state transition: `AggregateRoot<A>` → `DeletedAggregateRoot<A>`.
//!
//! Once deleted, no further events can be applied — this is enforced at
//! compile time by the type system.
//!
//! ## Key Concepts
//!
//! - **`@delete`** in `define_events!`: marks events as delete transitions
//! - **`@delete @actor(Type)`**: delete events with actor permission validation
//! - **`@delete fn`** in `command_handler!`: generates delete command methods
//! - **`DeletedAggregateRoot<A>`**: terminal state with `Deref<Target=A::DeletedState>`
//! - **`load_any()`**: returns `Loaded::Active` or `Loaded::Deleted`
//! - **`load_deleted()`**: returns `DeletedAggregateRoot` or errors
//! - **`save_deleted()`**: persists a deleted aggregate's events
//!
//! Run with:
//! ```bash
//! cargo run --example delete-events
//! ```

use std::sync::Arc;

use chrono::{DateTime, Utc};
use event_sauce::memory::InMemoryEventStore;
use event_sauce::{
    command_handler, define_events, Aggregate, AggregateError, AggregateRoot, DefaultEntity,
    DomainEvent, Entity, EntityId, EventApplicator, EventStore, Repository, StoredVersion,
};
use serde::{Deserialize, Serialize};

// ============================================================================
// Admin actor (for actor-validated deletes)
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
struct Admin {
    id: EntityId,
    role: String,
}

impl Entity for Admin {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            role: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Admin {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum AdminEvent {
    Noop,
}

impl DomainEvent for AdminEvent {
    type Aggregate = Admin;
    fn event_type(&self) -> &'static str {
        "Admin.Noop"
    }
    fn event_version(&self) -> event_sauce::EventVersion {
        event_sauce::EventVersion::new(1)
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

impl EventApplicator<Admin> for AdminEvent {
    fn dispatch(&self, _: &mut Admin) -> Result<(), SubscriptionError> {
        Ok(())
    }
    fn dispatch_unchecked(&self, _: &mut Admin) {}
}

impl Aggregate for Admin {
    type Event = AdminEvent;
    type Error = SubscriptionError;
    type DeletedState = Self;
}

// ============================================================================
// Subscription aggregate with delete events
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum SubscriptionError {
    #[error("Already cancelled")]
    AlreadyCancelled,
    #[error("Not authorized: {0}")]
    NotAuthorized(String),
}

impl AggregateError for SubscriptionError {}

#[derive(Debug, Serialize, Deserialize)]
struct Subscription {
    id: EntityId,
    plan: String,
    active: bool,
}

impl Entity for Subscription {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Subscription {
    type Event = SubscriptionEvent;
    type Error = SubscriptionError;
    type DeletedState = Self;
}

define_events! {
    enum SubscriptionEvent for Subscription {
        Started {
            plan: String,
        }
        @init
        => |id, event| {
            Subscription {
                id,
                plan: event.plan.clone(),
                active: true,
            }
        },

        PlanChanged {
            new_plan: String,
        }
        @validate |sub, _event| {
            if !sub.active {
                return Err(SubscriptionError::AlreadyCancelled);
            }
            return Ok(());
        }
        => |sub, event| {
            sub.plan = event.new_plan.clone();
        },

        // User-initiated cancellation
        Cancelled {
            reason: String,
        }
        @delete
        @validate |sub, _event| {
            if !sub.active {
                return Err(SubscriptionError::AlreadyCancelled);
            }
            return Ok(());
        }
        => |mut sub, _event| {
            sub.active = false;
            sub
        },

        // Admin-initiated forced cancellation
        ForceRevoked {
            reason: String,
        }
        @delete
        @actor(Admin)
        @validate |sub, admin, _event| {
            if admin.role != "billing_admin" {
                return Err(SubscriptionError::NotAuthorized(
                    "Only billing admins can force-revoke".to_string()
                ));
            }
            if !sub.active {
                return Err(SubscriptionError::AlreadyCancelled);
            }
            return Ok(());
        }
        => |mut sub, _event| {
            sub.active = false;
            sub.plan = "revoked".to_string();
            sub
        },
    }
}

command_handler! {
    impl Subscription {
        @clock @init fn start_subscription(plan: String) -> StartedEvent { plan };
        @clock fn change_plan(new_plan: String) -> PlanChangedEvent { new_plan };
        @clock @delete fn cancel_subscription(reason: String)
            -> CancelledEvent { reason };
        @clock @delete @actor(Admin) fn force_revoke(reason: String)
            -> ForceRevokedEvent { reason };
    }
}

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use SubscriptionDeleteCommands;

    println!("Event Sauce - Delete Events Example\n");
    println!("This example demonstrates:");
    println!("  - @delete events for type-safe aggregate deletion");
    println!("  - @delete @actor(Type) for permission-validated deletion");
    println!("  - DeletedAggregateRoot<A> terminal state (no further events)");
    println!("  - load_any() returning Loaded::Active or Loaded::Deleted");
    println!("  - load_deleted() for explicitly loading deleted aggregates\n");

    let store = Arc::new(InMemoryEventStore::builder().build());
    let repo = store.repository::<Subscription>();

    // === Create a subscription ===
    println!("=== Creating Subscription ===\n");

    let mut sub = Subscription::start_subscription("pro".to_string())?;
    sub.change_plan("enterprise".to_string())?;
    println!(
        "  Subscription created: plan='{}', active={}",
        sub.plan, sub.active
    );
    repo.save(&mut sub).await?;

    let sub_id = sub.entity_id();

    // === User cancels their subscription ===
    println!("\n=== User Cancels Subscription ===\n");

    // apply_delete() consumes the AggregateRoot and returns DeletedAggregateRoot
    let mut deleted = sub.cancel_subscription("switching providers".to_string())?;
    println!(
        "  Deleted: active={}, plan='{}'",
        deleted.active, deleted.plan
    );
    println!(
        "  Pending events: {} (init + plan change + cancel)",
        deleted.pending_events().len()
    );
    repo.save_deleted(&mut deleted).await?;

    // === Loading a deleted aggregate ===
    println!("\n=== Loading Deleted Aggregate ===\n");

    // load() returns an error for deleted aggregates
    println!("  Attempting load() on deleted aggregate...");
    match repo.load(sub_id).await {
        Ok(_) => println!("    ERROR: Should have failed!"),
        Err(e) => println!("    Error (expected): {e}"),
    }

    // load_any() returns the lifecycle state
    println!("  Using load_any()...");
    let loaded = repo.load_any(sub_id).await?;
    match loaded {
        event_sauce::Loaded::Active(_) => println!("    ERROR: Should be Deleted!"),
        event_sauce::Loaded::Deleted(d) => {
            println!(
                "    Loaded::Deleted: plan='{}', active={}",
                d.plan, d.active
            );
        }
    }

    // load_deleted() explicitly loads a deleted aggregate
    println!("  Using load_deleted()...");
    let loaded_deleted = repo.load_deleted(sub_id).await?;
    println!(
        "    DeletedAggregateRoot: plan='{}', active={}",
        loaded_deleted.plan, loaded_deleted.active
    );

    // === Admin force-revoke (actor delete) ===
    println!("\n=== Admin Force-Revoke (Actor Delete) ===\n");

    // Create a new subscription for the force-revoke demo
    let mut sub2 = Subscription::start_subscription("basic".to_string())?;
    repo.save(&mut sub2).await?;
    let sub2_id = sub2.entity_id();
    println!(
        "  New subscription: plan='{}', active={}",
        sub2.plan, sub2.active
    );

    // Non-admin tries to revoke — should fail
    // We need a separate subscription since force_revoke consumes self
    let sub2_for_test = {
        let entity = Subscription {
            id: sub2.entity_id(),
            plan: sub2.plan.clone(),
            active: sub2.active,
        };
        AggregateRoot::restore(StoredVersion::try_from(sub2.version())?, entity)
    };

    let regular_admin = {
        let entity = Admin {
            id: EntityId::new(),
            role: "support".to_string(),
        };
        AggregateRoot::restore(StoredVersion::FIRST, entity)
    };

    println!("  Support agent tries to force-revoke...");
    match sub2_for_test.force_revoke(&regular_admin, "policy".to_string()) {
        Ok(_) => println!("    ERROR: Should have been denied!"),
        Err(e) => println!("    Denied: {e}"),
    }

    // Billing admin revokes the real subscription
    let billing_admin = {
        let entity = Admin {
            id: EntityId::new(),
            role: "billing_admin".to_string(),
        };
        AggregateRoot::restore(StoredVersion::FIRST, entity)
    };

    println!("  Billing admin force-revokes...");
    let mut deleted2 = sub2.force_revoke(&billing_admin, "fraud detected".to_string())?;
    println!(
        "    Revoked: plan='{}', active={}",
        deleted2.plan, deleted2.active
    );
    repo.save_deleted(&mut deleted2).await?;

    // Verify it's deleted
    let loaded2 = repo.load_any(sub2_id).await?;
    assert!(loaded2.is_deleted());
    println!("    Verified: load_any() returns Deleted");

    // === Compile-time safety demo ===
    println!("\n=== Compile-Time Safety ===\n");
    println!("  Once an AggregateRoot is deleted, the type system prevents further events:");
    println!("  - `apply_delete()` consumes `self` → returns `DeletedAggregateRoot<A>`");
    println!("  - `DeletedAggregateRoot` has NO `apply()` method");
    println!("  - Attempting to apply events to a deleted aggregate is a compile error");
    println!("  - This is enforced at compile time, not runtime!");

    // === Snapshot behavior for deleted aggregates ===
    println!("\n=== Deleted Aggregate Snapshots ===\n");

    let snapshot_store = Arc::new(
        InMemoryEventStore::builder()
            .snapshot_config(event_sauce::SnapshotConfig::always())
            .build(),
    );
    let snapshot_repo = snapshot_store.repository::<Subscription>();

    let mut sub3 = Subscription::start_subscription("team".to_string())?;
    snapshot_repo.save(&mut sub3).await?;
    let sub3_id = sub3.entity_id();
    println!(
        "  Created subscription with snapshots: plan='{}'",
        sub3.plan
    );

    let mut deleted3 = sub3.cancel_subscription("migrating".to_string())?;
    snapshot_repo.save_deleted(&mut deleted3).await?;
    println!("  Deleted and snapshotted (is_deleted=true)");

    let loaded3 = snapshot_repo.load_any(sub3_id).await?;
    match loaded3 {
        event_sauce::Loaded::Active(_) => println!("    ERROR: Should be Deleted!"),
        event_sauce::Loaded::Deleted(d) => {
            println!(
                "  Loaded from deleted snapshot: plan='{}', active={}",
                d.plan, d.active
            );
        }
    }
    println!("  Deleted aggregates are snapshotted efficiently — no event replay needed!");

    println!("\n  Demo complete!");
    println!("\n  Key Takeaways:");
    println!("   - @delete marks events as delete transitions");
    println!("   - @delete @actor(Type) adds permission validation");
    println!("   - DeletedAggregateRoot is a terminal type-state");
    println!("   - load_any() detects deleted aggregates during replay");
    println!("   - Deleted aggregates are snapshotted with is_deleted flag");
    println!("   - No runtime checks needed — the type system enforces correctness");

    Ok(())
}
