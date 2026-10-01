//! Integration tests for delete events with the in-memory event store.
//!
//! Tests the full delete event lifecycle including:
//! - `@delete` events via `define_events!` macro
//! - `@delete @actor(Type)` events with permission validation
//! - `command_handler!` `@delete fn` support
//! - Event store `load_any()`, `load_deleted()`, `commit_deleted()`
//! - Repository `save_deleted()`, `load_any()`, `load_deleted()`

use event_sauce_core::{
    command_handler, define_events, Aggregate, AggregateError, AggregateRoot, AggregateVersion,
    DomainEvent, Entity, EntityId, EventApplicator, EventStore, Repository, SnapshotConfig,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ============================================================================
// Admin actor
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
struct Admin {
    id: EntityId,
    is_super: bool,
}

impl Entity for Admin {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            is_super: false,
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Admin {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum AdminEvent {
    Noop,
}

impl DomainEvent for AdminEvent {
    type Aggregate = Admin;
    fn event_type(&self) -> &'static str {
        "Admin.Noop"
    }
    fn event_version(&self) -> event_sauce_core::EventVersion {
        event_sauce_core::EventVersion::new(1)
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl EventApplicator<Admin> for AdminEvent {
    fn dispatch(&self, _: &mut Admin) -> Result<(), ProjectError> {
        Ok(())
    }
    fn dispatch_unchecked(&self, _: &mut Admin) {}
}

impl Aggregate for Admin {
    type Event = AdminEvent;
    type Error = ProjectError;
    type DeletedState = Self;
}

// ============================================================================
// Project aggregate with init, regular, delete, and actor delete events
// ============================================================================

#[derive(Debug, thiserror::Error)]
enum ProjectError {
    #[error("Already archived")]
    AlreadyArchived,
    #[error("Not authorized")]
    NotAuthorized,
}

impl AggregateError for ProjectError {}

#[derive(Debug, Serialize, Deserialize)]
struct Project {
    id: EntityId,
    name: String,
    archived: bool,
}

impl Entity for Project {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Project {
    type Event = ProjectEvent;
    type Error = ProjectError;
    type DeletedState = Self;
}

define_events! {
    enum ProjectEvent for Project {
        Created {
            name: String,
        }
        @init
        => |id, event| {
            Project {
                id,
                name: event.name.clone(),
                archived: false,
            }
        },

        Renamed {
            new_name: String,
        }
        @validate |project, _event| {
            if project.archived {
                return Err(ProjectError::AlreadyArchived);
            }
            return Ok(());
        }
        => |project, event| {
            project.name = event.new_name.clone();
        },

        Archived {
            reason: String,
        }
        @delete
        @validate |project, _event| {
            if project.archived {
                return Err(ProjectError::AlreadyArchived);
            }
            return Ok(());
        }
        => |mut project, _event| {
            project.archived = true;
            project
        },

        ForceDeleted {
            reason: String,
        }
        @delete
        @actor(Admin)
        @validate |project, admin, _event| {
            if !admin.is_super {
                return Err(ProjectError::NotAuthorized);
            }
            if project.archived {
                return Err(ProjectError::AlreadyArchived);
            }
            return Ok(());
        }
        => |mut project, _event| {
            project.archived = true;
            project.name = "[deleted]".to_string();
            project
        },
    }
}

command_handler! {
    impl Project {
        @clock @init fn create_project(name: String) -> CreatedEvent { name };
        @clock fn rename_project(new_name: String) -> RenamedEvent { new_name };
        @clock @delete fn archive_project(reason: String) -> ArchivedEvent { reason };
        @clock @delete @actor(Admin)
        fn force_delete_project(reason: String) -> ForceDeletedEvent { reason };
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn create_store() -> Arc<InMemoryEventStore> {
    Arc::new(InMemoryEventStore::builder().build())
}

fn create_store_with_snapshots() -> Arc<InMemoryEventStore> {
    Arc::new(
        InMemoryEventStore::builder()
            .snapshot_config(SnapshotConfig::always())
            .build(),
    )
}

fn make_super_admin() -> AggregateRoot<Admin> {
    let entity = Admin {
        id: EntityId::new(),
        is_super: true,
    };
    AggregateRoot::restore(AggregateVersion::new(1), entity)
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_delete_event_full_lifecycle() {
    use ProjectDeleteCommands;

    let store = create_store();
    let repo = store.repository::<Project>();

    // Create and save
    let mut project = Project::create_project("My Project".to_string()).unwrap();
    project
        .rename_project("Renamed Project".to_string())
        .unwrap();
    repo.save(&mut project).await.unwrap();

    // Delete
    let project_id = project.entity_id();
    let mut deleted = project
        .archive_project("no longer needed".to_string())
        .unwrap();
    assert!(deleted.archived);
    repo.save_deleted(&mut deleted).await.unwrap();

    // load() should error
    let result = repo.load(project_id).await;
    assert!(result.is_err());

    // load_any() should return Deleted
    let loaded = repo.load_any(project_id).await.unwrap();
    assert!(loaded.is_deleted());

    // load_deleted() should succeed
    let loaded_deleted = repo.load_deleted(project_id).await.unwrap();
    assert!(loaded_deleted.archived);
    assert_eq!(loaded_deleted.name, "Renamed Project");
}

#[tokio::test]
async fn test_actor_delete_event_lifecycle() {
    use ProjectDeleteCommands;

    let store = create_store();
    let repo = store.repository::<Project>();

    let mut project = Project::create_project("Secret Project".to_string()).unwrap();
    repo.save(&mut project).await.unwrap();

    let admin = make_super_admin();
    let project_id = project.entity_id();
    let mut deleted = project
        .force_delete_project(&admin, "policy violation".to_string())
        .unwrap();
    assert!(deleted.archived);
    assert_eq!(deleted.name, "[deleted]");
    repo.save_deleted(&mut deleted).await.unwrap();

    let loaded = repo.load_any(project_id).await.unwrap();
    assert!(loaded.is_deleted());
    let loaded_deleted = loaded.into_deleted().unwrap();
    assert_eq!(loaded_deleted.name, "[deleted]");
}

#[tokio::test]
async fn test_load_any_returns_active_for_non_deleted() {
    let store = create_store();
    let repo = store.repository::<Project>();

    let mut project = Project::create_project("Active Project".to_string()).unwrap();
    repo.save(&mut project).await.unwrap();

    let loaded = repo.load_any(project.entity_id()).await.unwrap();
    assert!(loaded.is_active());
    let active = loaded.into_active().unwrap();
    assert_eq!(active.name, "Active Project");
}

#[tokio::test]
async fn test_load_deleted_errors_for_active() {
    let store = create_store();
    let repo = store.repository::<Project>();

    let mut project = Project::create_project("Active Project".to_string()).unwrap();
    repo.save(&mut project).await.unwrap();

    let result = repo.load_deleted(project.entity_id()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_validation_prevents_double_delete() {
    use ProjectDeleteCommands;

    // Build an already-archived project via snapshot
    let entity = Project {
        id: EntityId::new(),
        name: "Archived".to_string(),
        archived: true,
    };
    let agg = AggregateRoot::restore(AggregateVersion::new(1), entity);
    let result = agg.archive_project("again".to_string());
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().to_string(), "Already archived");
}

#[tokio::test]
async fn test_actor_delete_validation_rejects_unauthorized() {
    use ProjectDeleteCommands;

    let project = Project::create_project("Secret".to_string()).unwrap();
    let non_admin = AggregateRoot::<Admin>::new(EntityId::new());
    let result = project.force_delete_project(&non_admin, "hack".to_string());
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().to_string(), "Not authorized");
}

#[tokio::test]
async fn test_delete_with_snapshots() {
    use ProjectDeleteCommands;

    let store = create_store_with_snapshots();
    let repo = store.repository::<Project>();

    // Create project with enough events to trigger snapshot
    let mut project = Project::create_project("Snap Project".to_string()).unwrap();
    project.rename_project("Renamed 1".to_string()).unwrap();
    repo.save(&mut project).await.unwrap(); // 2 events → snapshot

    project.rename_project("Renamed 2".to_string()).unwrap();
    repo.save(&mut project).await.unwrap();

    // Verify we can load it back (with snapshot)
    let loaded = repo.load(project.entity_id()).await.unwrap();
    assert_eq!(loaded.name, "Renamed 2");

    // Now delete
    let project_id = project.entity_id();
    let mut deleted = project.archive_project("done".to_string()).unwrap();
    repo.save_deleted(&mut deleted).await.unwrap();

    // load_any should return Deleted (from deleted snapshot)
    let loaded = repo.load_any(project_id).await.unwrap();
    assert!(loaded.is_deleted());
    let loaded_deleted = loaded.into_deleted().unwrap();
    assert!(loaded_deleted.archived);
    assert_eq!(loaded_deleted.name, "Renamed 2");
}

#[tokio::test]
async fn test_delete_snapshot_round_trip() {
    use ProjectDeleteCommands;

    let store = create_store_with_snapshots();
    let repo = store.repository::<Project>();

    // Full lifecycle: create → rename → delete → load
    let mut project = Project::create_project("Snapshot Test".to_string()).unwrap();
    repo.save(&mut project).await.unwrap();

    project
        .rename_project("Snapshot Renamed".to_string())
        .unwrap();
    repo.save(&mut project).await.unwrap();

    let project_id = project.entity_id();
    let mut deleted = project.archive_project("done forever".to_string()).unwrap();
    repo.save_deleted(&mut deleted).await.unwrap();

    // load_any() should return the deleted state efficiently (from snapshot)
    let loaded = repo.load_any(project_id).await.unwrap();
    assert!(loaded.is_deleted());
    let loaded_deleted = loaded.into_deleted().unwrap();
    assert!(loaded_deleted.archived);

    // load_deleted() should also work
    let loaded_deleted2 = repo.load_deleted(project_id).await.unwrap();
    assert!(loaded_deleted2.archived);

    // load() should still error
    let result = repo.load(project_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_event_applicator_is_delete_flag() {
    let init = ProjectEvent::Created {
        name: "Test".to_string(),
        timestamp: chrono::Utc::now(),
    };
    let regular = ProjectEvent::Renamed {
        new_name: "Test2".to_string(),
        timestamp: chrono::Utc::now(),
    };
    let delete = ProjectEvent::Archived {
        reason: "bye".to_string(),
        timestamp: chrono::Utc::now(),
    };
    let actor_delete = ProjectEvent::ForceDeleted {
        reason: "admin".to_string(),
        timestamp: chrono::Utc::now(),
    };

    assert!(!init.is_delete());
    assert!(!regular.is_delete());
    assert!(delete.is_delete());
    assert!(actor_delete.is_delete());

    assert!(init.is_init());
    assert!(!regular.is_init());
    assert!(!delete.is_init());
    assert!(!actor_delete.is_init());
}

#[tokio::test]
async fn test_loaded_enum_methods() {
    let store = create_store();
    let repo = store.repository::<Project>();

    // Active case
    let mut project = Project::create_project("Test".to_string()).unwrap();
    repo.save(&mut project).await.unwrap();

    let loaded = repo.load_any(project.entity_id()).await.unwrap();
    assert!(loaded.is_active());
    assert!(!loaded.is_deleted());
    let entity_id = loaded.entity_id();
    assert_eq!(entity_id, project.entity_id());
}
