//! # Policy Example - Cross-Aggregate Event Orchestration
//!
//! This example demonstrates the policy system for handling events that trigger
//! effects on other aggregates. A user getting kicked from a chat produces an event
//! that a policy picks up to remove them from their group, which in turn produces
//! an event that another policy picks up to send a notification.
//!
//! ## Key Concepts
//!
//! - **`policy!` macro**: Declaratively define event handlers that react to events
//! - **`PolicyRunner`**: Registers and routes events to matching policies with checkpoint-based resumption
//! - **Causation tracking**: Every event produced by a policy carries full provenance
//!   (`causation_id`, `correlation_id`, `causation_chain`)
//! - **Cascading reactions**: Reactions can trigger further reactions with configurable depth limits
//! - **Error handling**: Configurable `OnError::Fail`, `OnError::Skip`, or `OnError::Retry` with exponential backoff
//!
//! ## Architecture
//!
//! ```text
//!   User.Kicked event
//!       │
//!       ▼
//!   KickUserPolicy ─────► Group.MemberRemoved event
//!                               │
//!                               ▼
//!                       NotifyOnRemovalPolicy ─────► Notification.Sent event
//! ```
//!
//! Run with:
//! ```bash
//! cargo run --example policy
//! ```

use std::sync::Arc;

use event_sauce::memory::InMemoryEventStore;
use event_sauce::{
    command_handler, define_events, policy, Aggregate, AggregateError, AggregateRoot,
    DefaultEntity, Entity, EntityId, EventSourcedRepository, EventStore, Position, Repository,
};
use serde::{Deserialize, Serialize};

// ============================================================================
// User aggregate
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum UserError {
    #[error("User already inactive")]
    AlreadyInactive,
}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: EntityId,
    name: String,
    active: bool,
}

impl Entity for User {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            active: true,
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for User {}

impl Aggregate for User {
    type Event = UserEvent;
    type Error = UserError;
    type DeletedState = Self;
}

define_events! {
    enum UserEvent for User {
        Registered {
            name: String,
        } => |user, event| {
            user.name = event.name.clone();
        },

        Kicked {
            group_id: EntityId,
            reason: String,
        }
        @validate |user, _event| {
            if !user.active {
                return Err(UserError::AlreadyInactive);
            }
            return Ok(());
        }
        => |user, _event| {
            user.active = false;
        },
    }
}

command_handler! {
    impl User {
        @clock fn register(name: String) -> RegisteredEvent { name };
        @clock fn kick(group_id: EntityId, reason: String) -> KickedEvent { group_id, reason };
    }
}

// ============================================================================
// Group aggregate
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum GroupError {
    #[error("Member not found")]
    MemberNotFound,
}

impl AggregateError for GroupError {}

#[derive(Debug, Serialize, Deserialize)]
struct Group {
    id: EntityId,
    name: String,
    members: Vec<EntityId>,
}

impl Entity for Group {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            members: Vec::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Group {}

impl Aggregate for Group {
    type Event = GroupEvent;
    type Error = GroupError;
    type DeletedState = Self;
}

define_events! {
    enum GroupEvent for Group {
        Created {
            name: String,
        } => |group, event| {
            group.name = event.name.clone();
        },

        MemberAdded {
            member_id: EntityId,
        } => |group, event| {
            group.members.push(event.member_id);
        },

        MemberRemoved {
            member_id: EntityId,
            reason: String,
        }
        @validate |group, event| {
            if !group.members.contains(&event.member_id) {
                return Err(GroupError::MemberNotFound);
            }
            return Ok(());
        }
        => |group, event| {
            group.members.retain(|m| *m != event.member_id);
        },
    }
}

command_handler! {
    impl Group {
        @clock fn create(name: String) -> CreatedEvent { name };
        @clock fn add_member(member_id: EntityId) -> MemberAddedEvent { member_id };
        @clock fn remove_member(member_id: EntityId, reason: String)
            -> MemberRemovedEvent { member_id, reason };
    }
}

// ============================================================================
// Notification aggregate
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("notification error")]
struct NotificationError;

impl AggregateError for NotificationError {}

#[derive(Debug, Serialize, Deserialize)]
struct Notification {
    id: EntityId,
    message: String,
}

impl Entity for Notification {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            message: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Notification {}

impl Aggregate for Notification {
    type Event = NotificationEvent;
    type Error = NotificationError;
    type DeletedState = Self;
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
        @clock fn send_notification(message: String) -> SentEvent { message };
    }
}

// ============================================================================
// Policies
// ============================================================================

// When a user is kicked, remove them from the group they were kicked from.
policy! {
    /// Removes kicked users from their groups.
    KickUserPolicy {
        on KickedEvent |event, ctx| {
            let user_id = EntityId::from(ctx.source_event().aggregate_id);
            let mut group = ctx.load_as::<Group>(event.group_id).await?;
            group.remove_member(user_id, event.reason.clone())?;
            ctx.commit(&mut group).await?;
            Ok(())
        },
    }
}

// When a member is removed from a group, send a notification.
policy! {
    /// Sends a notification when a member is removed from a group.
    NotifyOnRemovalPolicy {
        on MemberRemovedEvent |event, ctx| {
            let id = EntityId::new();
            let mut notification = AggregateRoot::<Notification>::new(id);
            let msg = format!("Member {} was removed: {}", event.member_id, event.reason);
            notification.send_notification(msg)?;
            ctx.commit(&mut notification).await?;
            Ok(())
        },
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = InMemoryEventStore::for_testing();

    // --- Step 1: Set up the world ---

    let group_id = EntityId::new();
    let alice_id = EntityId::new();

    // Create a group and add Alice as a member
    let mut group = AggregateRoot::<Group>::new(group_id);
    group.create("Rust Enthusiasts".to_string())?;
    group.add_member(alice_id)?;
    store.commit(&mut group).await?;

    // Register Alice
    let mut alice = AggregateRoot::<User>::new(alice_id);
    alice.register("Alice".to_string())?;
    store.commit(&mut alice).await?;

    println!("=== Initial State ===");
    let group_repo: EventSourcedRepository<InMemoryEventStore, Group> =
        EventSourcedRepository::new(Arc::clone(&store));
    let loaded_group = group_repo.load(group_id).await?;
    println!(
        "Group '{}' members: {:?}",
        loaded_group.name, loaded_group.members
    );

    // --- Step 2: Kick Alice ---

    println!("\n=== Kicking Alice ===");
    let user_repo: EventSourcedRepository<InMemoryEventStore, User> =
        EventSourcedRepository::new(Arc::clone(&store));
    let mut alice = user_repo.load(alice_id).await?;
    alice.kick(group_id, "spamming".to_string())?;
    store.commit(&mut alice).await?;
    println!("Alice kicked from group (reason: spamming)");

    // --- Step 3: Run policies ---

    println!("\n=== Processing Reactions ===");
    let runner = store
        .policy_runner()?
        .with_max_cascade_depth(5)
        .register(Arc::new(KickUserPolicy))
        .register(Arc::new(NotifyOnRemovalPolicy));

    let processed = runner.process_pending().await?;
    println!("Processed {processed} event-policy matches");

    // --- Step 4: Verify results ---

    println!("\n=== Final State ===");
    let loaded_group = group_repo.load(group_id).await?;
    println!(
        "Group '{}' members: {:?} (Alice removed by policy)",
        loaded_group.name, loaded_group.members
    );

    // --- Step 5: Show causation chain ---

    println!("\n=== Event Causation Chain ===");
    use futures::StreamExt;
    let stream = store.stream_all(Position::start()).await?;
    futures::pin_mut!(stream);

    while let Some(Ok(entry)) = stream.next().await {
        let envelope = entry.envelope;
        println!("  Event: {} (id: {})", envelope.event_type, envelope.id);
        if let Some(ref meta) = envelope.metadata {
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

    println!("\n=== Done ===");
    println!("The policy system automatically:");
    println!("  1. Detected User.Kicked event");
    println!("  2. Removed Alice from the group (KickUserPolicy)");
    println!("  3. Sent a notification about the removal (NotifyOnRemovalPolicy)");
    println!("  4. All with full causation tracking through the chain!");

    Ok(())
}
