//! # Actor Events Example - Permission-Validated Events with Type-Safe Actors
//!
//! This example demonstrates actor-required events where certain operations
//! require an authorized actor (the entity performing the action). The actor's
//! permissions are validated at command time, and their identity is tracked
//! through to the event envelope's `created_by` field.
//!
//! ## Key Concepts
//!
//! - **`@actor(Type)`** in `define_events!`: marks events as requiring an actor
//! - **`@validate |agg, actor, evt|`**: 3-arg arity validates actor permissions at command time
//! - **`@actor(Type)`** in `command_handler!`: generates methods requiring an actor parameter
//! - Actor validation is **skipped during replay** (events are historical facts)
//! - Actor ID flows into `EventEnvelope::created_by` at commit time
//!
//! Run with:
//! ```bash
//! cargo run --example actor-events
//! ```

use std::sync::Arc;

use chrono::{DateTime, Utc};
use event_sauce::memory::InMemoryEventStore;
use event_sauce::{
    command_handler, define_events, Aggregate, AggregateError, AggregateRoot, DefaultEntity,
    DomainEvent, Entity, EntityId, EventApplicator, EventStore, Repository,
};
use serde::{Deserialize, Serialize};

// ============================================================================
// Actor: User (the entity performing actions)
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: EntityId,
    name: String,
    role: UserRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum UserRole {
    Viewer,
    Editor,
    Admin,
}

impl Entity for User {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            role: UserRole::Viewer,
        }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for User {}

// User needs its own event/aggregate setup to be used as an AggregateRoot
#[derive(Debug, Clone, Serialize, Deserialize)]
enum UserEvent {
    Registered {
        name: String,
        role: UserRole,
        timestamp: DateTime<Utc>,
    },
}

impl DomainEvent for UserEvent {
    type Aggregate = User;
    fn event_type(&self) -> &'static str {
        match self {
            UserEvent::Registered { .. } => "User.Registered",
        }
    }
    fn event_version(&self) -> event_sauce::EventVersion {
        event_sauce::EventVersion::new(1)
    }
    fn occurred_at(&self) -> DateTime<Utc> {
        match self {
            UserEvent::Registered { timestamp, .. } => *timestamp,
        }
    }
}

impl EventApplicator<User> for UserEvent {
    fn dispatch(&self, user: &mut User) -> Result<(), UserError> {
        match self {
            UserEvent::Registered { name, role, .. } => {
                user.name = name.clone();
                user.role = *role;
            }
        }
        Ok(())
    }
    fn dispatch_unchecked(&self, user: &mut User) {
        match self {
            UserEvent::Registered { name, role, .. } => {
                user.name = name.clone();
                user.role = *role;
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("User error")]
struct UserError;

impl AggregateError for UserError {}

impl Aggregate for User {
    type Event = UserEvent;
    type Error = UserError;
    type DeletedState = Self;
}

// ============================================================================
// Aggregate: Document (protected by actor permissions)
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum DocumentError {
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    #[error("Empty title")]
    EmptyTitle,
    #[error("Already published")]
    AlreadyPublished,
}

impl AggregateError for DocumentError {}

#[derive(Debug, Serialize, Deserialize)]
struct Document {
    id: EntityId,
    title: String,
    content: String,
    published: bool,
}

impl Entity for Document {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Document {
    type Event = DocumentEvent;
    type Error = DocumentError;
    type DeletedState = Self;
}

// ============================================================================
// Events with @actor annotations
// ============================================================================

define_events! {
    enum DocumentEvent for Document {
        // Only admins can create documents
        DocumentCreated {
            title: String,
            content: String,
        }
        @init
        @actor(User)
        @validate |actor, evt| {
            if actor.role != UserRole::Admin && actor.role != UserRole::Editor {
                return Err(DocumentError::PermissionDenied(
                    "Only editors and admins can create documents".to_string()
                ));
            }
            if evt.title.is_empty() {
                return Err(DocumentError::EmptyTitle);
            }
            return Ok(());
        }
        => |id, event| {
            Document {
                id,
                title: event.title.clone(),
                content: event.content.clone(),
                published: false,
            }
        },

        // Only editors/admins can edit content
        ContentUpdated {
            new_content: String,
        }
        @actor(User)
        @validate |_doc, actor, _evt| {
            if actor.role == UserRole::Viewer {
                return Err(DocumentError::PermissionDenied(
                    "Viewers cannot edit documents".to_string()
                ));
            }
            return Ok(());
        }
        => |doc, event| {
            doc.content = event.new_content.clone();
        },

        // Only admins can publish
        DocumentPublished {
            publisher_note: String,
        }
        @actor(User)
        @validate |doc, actor, _evt| {
            if actor.role != UserRole::Admin {
                return Err(DocumentError::PermissionDenied(
                    "Only admins can publish documents".to_string()
                ));
            }
            if doc.published {
                return Err(DocumentError::AlreadyPublished);
            }
            return Ok(());
        }
        => |doc, _event| {
            doc.published = true;
        },

        // Anyone can add a comment (no actor required)
        CommentAdded {
            comment: String,
        }
        => |_doc, _event| {
            // Comments stored elsewhere; this event exists for audit trail
        },
    }
}

// ============================================================================
// Command handlers with @actor
// ============================================================================

command_handler! {
    impl Document {
        @clock @init @actor(User)
        fn create_document(title: String, content: String) -> DocumentCreatedEvent { title, content };
        @clock @actor(User)
        fn update_content(new_content: String) -> ContentUpdatedEvent { new_content };
        @clock @actor(User)
        fn publish(publisher_note: String) -> DocumentPublishedEvent { publisher_note };
        @clock fn add_comment(comment: String) -> CommentAddedEvent { comment };
    }
}

// ============================================================================
// Helper to create a user AggregateRoot
// ============================================================================

fn register_user(name: &str, role: UserRole) -> AggregateRoot<User> {
    let mut user = AggregateRoot::<User>::new(EntityId::new());
    user.apply(UserEvent::Registered {
        name: name.to_string(),
        role,
        timestamp: Utc::now(),
    })
    .unwrap();
    user
}

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Event Sauce - Actor Events Example\n");
    println!("This example demonstrates:");
    println!("  - @actor(Type) annotations for permission-validated events");
    println!("  - @validate with arity-based dispatch for permission checks");
    println!("  - Actor ID tracking through to EventEnvelope::created_by");
    println!("  - Mixed actor and non-actor events in the same aggregate");
    println!("  - Replay skips actor validation (events are historical facts)\n");

    let store = Arc::new(InMemoryEventStore::builder().build());
    let repo = store.repository::<Document>();

    // Create users with different roles
    let admin = register_user("Alice (Admin)", UserRole::Admin);
    let editor = register_user("Bob (Editor)", UserRole::Editor);
    let viewer = register_user("Charlie (Viewer)", UserRole::Viewer);

    println!("=== Creating Users ===\n");
    println!("  Admin: {} (id: {})", admin.name, admin.entity_id());
    println!("  Editor: {} (id: {})", editor.name, editor.entity_id());
    println!("  Viewer: {} (id: {})", viewer.name, viewer.entity_id());

    // === Create a document (requires Editor or Admin) ===
    println!("\n=== Creating Document ===\n");

    // Viewer tries to create - should fail
    println!("  Viewer attempts to create document...");
    match Document::create_document(&viewer, "Draft".to_string(), "Content".to_string()) {
        Ok(_) => println!("   ERROR: Should have been denied!"),
        Err(e) => println!("   Denied: {e}"),
    }

    // Editor creates successfully
    println!("  Editor creates document...");
    let mut doc = Document::create_document(
        &editor,
        "Design Doc".to_string(),
        "Initial draft".to_string(),
    )?;
    println!("   Created: '{}' (v{})", doc.title, doc.version());
    repo.save(&mut doc).await?;

    // === Update content (requires Editor or Admin) ===
    println!("\n=== Updating Content ===\n");

    // Viewer tries to edit - should fail
    println!("  Viewer attempts to edit...");
    match doc.update_content(&viewer, "Hacked content".to_string()) {
        Ok(()) => println!("   ERROR: Should have been denied!"),
        Err(e) => println!("   Denied: {e}"),
    }

    // Editor updates successfully
    println!("  Editor updates content...");
    doc.update_content(&editor, "Revised draft with improvements".to_string())?;
    println!("   Content updated (v{})", doc.version());
    repo.save(&mut doc).await?;

    // === Publish (requires Admin only) ===
    println!("\n=== Publishing Document ===\n");

    // Editor tries to publish - should fail
    println!("  Editor attempts to publish...");
    match doc.publish(&editor, "Ready for review".to_string()) {
        Ok(()) => println!("   ERROR: Should have been denied!"),
        Err(e) => println!("   Denied: {e}"),
    }

    // Admin publishes successfully
    println!("  Admin publishes document...");
    doc.publish(&admin, "Approved for release".to_string())?;
    println!("   Published! (v{})", doc.version());
    repo.save(&mut doc).await?;

    // === Add comment (no actor required) ===
    println!("\n=== Adding Comments ===\n");

    // Anyone can comment - no actor needed
    doc.add_comment("Great document!".to_string())?;
    println!(
        "  Comment added without actor requirement (v{})",
        doc.version()
    );
    repo.save(&mut doc).await?;

    // === Verify actor tracking via envelopes ===
    println!("\n=== Actor Tracking (via stored envelopes) ===\n");

    use event_sauce::{AggregateVersion, StreamId};
    use futures::StreamExt;
    let stream_id = StreamId::new(Document::aggregate_type(), doc.entity_id().as_uuid());
    let mut event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;
    let mut i = 0;
    let labels = [
        "Editor (init)",
        "Editor (edit)",
        "Admin (publish)",
        "None (comment)",
    ];
    while let Some(envelope) = event_stream.next().await {
        let envelope = envelope?;
        let actor_str = match envelope.created_by {
            Some(id) => format!("{id}"),
            None => "none".to_string(),
        };
        let label = labels.get(i).unwrap_or(&"?");
        println!("  Event {}: created_by={} ({})", i + 1, actor_str, label);
        i += 1;
    }

    // === Replay verification ===
    println!("\n=== Event Replay ===\n");

    let doc_id = doc.entity_id();
    let replayed = repo.load(doc_id).await?;
    println!("  Replayed document from store:");
    println!("   Title: {}", replayed.title);
    println!("   Content: {}", replayed.content);
    println!("   Published: {}", replayed.published);
    println!("   Version: {}", replayed.version());
    println!("  (Actor validation was skipped during replay)");

    println!("\n  Demo complete!");
    println!("\n  Key Takeaways:");
    println!("   - @actor(Type) enforces type-safe actor requirements");
    println!("   - @validate arity dispatch checks permissions at command time");
    println!("   - Actor ID is stored in pending events for commit");
    println!("   - Non-actor events work alongside actor events");
    println!("   - Replay skips actor validation (historical facts)");

    Ok(())
}
