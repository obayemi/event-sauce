# Projections and Subscriptions

Projections are read models built from event streams. They enable efficient queries by maintaining denormalized views optimized for specific read patterns. The subscription system provides durable, guaranteed delivery of events to projections.

## Table of Contents

- [What are Projections?](#what-are-projections)
- [Subscription System](#subscription-system)
- [Building Projections](#building-projections)
- [Using Subscriptions](#using-subscriptions)
- [Checkpoint Management](#checkpoint-management)
- [Real-World Example](#real-world-example)
- [Production Patterns](#production-patterns)
- [Testing Projections](#testing-projections)
- [Best Practices](#best-practices)

## What are Projections?

In event sourcing, **aggregates** are optimized for writes and enforce business rules. **Projections** are optimized for reads and provide efficient query capabilities.

### Why Projections?

```rust
// ❌ DON'T: Query aggregates directly
// This requires loading all events for every query - slow!
let user = load_user(user_id).await?;
if user.is_active() {
    // ...
}

// ✅ DO: Query projections
// This reads from a pre-built index - fast!
let is_active = user_status_projection.is_user_active(user_id).await?;
```

### Key Benefits

1. **Performance** - Queries are instant (no event replay)
2. **Flexibility** - Build multiple views from same events
3. **Scalability** - Read models can scale independently
4. **Guaranteed Delivery** - Subscriptions ensure eventual consistency

## Subscription System

The subscription system provides **durable, guaranteed delivery** of events. Unlike pub/sub systems, subscriptions always read from the durable event store, ensuring:

- **No message loss** - Events are never lost
- **Eventual consistency** - Every subscription eventually processes all events
- **Resumability** - Checkpoints track progress for restarts
- **Filtering** - Process only relevant events

### Key Differences from Pub/Sub

| Aspect | Pub/Sub (EventBus) | Subscriptions |
|--------|-------------------|---------------|
| Delivery | Best-effort broadcast | Guaranteed delivery |
| Storage | In-memory channels | Durable event store |
| Resumption | Lost on restart | Checkpoint-based resume |
| Consistency | Eventually consistent (may lose events) | Eventual consistency guaranteed |
| Use Case | Real-time notifications | Durable projections |

### Core Components

```rust
use event_sauce_core::{Subscription, EventFilter, CheckpointStrategy, ErrorPolicy};

/// Create a durable subscription
let subscription = Subscription::builder("user-projection", store)
    .checkpoint_store(checkpoint_store)
    .filter(EventFilter::by_aggregate_type("User"))
    .error_policy(ErrorPolicy::Skip)
    .build()?;
    // Default checkpoint_strategy is EveryEvent (safest option)
```

### Event Filters

Filter events to reduce processing load:

```rust
use event_sauce_core::EventFilter;

// Match all events
let filter = EventFilter::all();

// Match specific event type
let filter = EventFilter::by_event_type("UserRegistered");

// Match specific aggregate type
let filter = EventFilter::by_aggregate_type("User");

// Match both event and aggregate type
let filter = EventFilter::both("UserRegistered", "User");
```

## Building Projections

event-sauce provides two approaches for building projections:

1. **Using the `projection!` macro** (recommended) - Declarative, less boilerplate
2. **Manual implementation** - Full control, useful for complex scenarios

### Using the `projection!` Macro (Recommended)

The `projection!` macro provides a declarative way to define projections with minimal boilerplate. Event types are automatically inferred from the `EventType` trait, eliminating string duplication.

```rust
use event_sauce_core::projection;
use std::collections::HashMap;

// Define your events (using define_events! or manually)
define_events! {
    pub enum UserEvent for User {
        Registered { email: String } => |user, event| {
            user.state.email = event.email.clone();
        },
        Activated { } => |user, _event| {
            user.state.status = UserStatus::Active;
        },
        Suspended { } => |user, _event| {
            user.state.status = UserStatus::Suspended;
        },
    }
}

// Define projection state
#[derive(Debug, Clone, Default)]
struct UserCountState {
    counts: HashMap<UserStatus, u64>,
}

// Create projection with declarative event handlers
projection! {
    pub struct UserCountProjection {
        state: UserCountState,

        // No string literals needed - event types inferred from EventType trait!
        on RegisteredEvent |proj, _event| {
            *proj.state.counts.entry(UserStatus::Inactive).or_insert(0) += 1;
        },

        on ActivatedEvent |proj, _event| {
            if let Some(count) = proj.state.counts.get_mut(&UserStatus::Inactive) {
                *count = count.saturating_sub(1);
            }
            *proj.state.counts.entry(UserStatus::Active).or_insert(0) += 1;
        },

        on SuspendedEvent |proj, _event| {
            if let Some(count) = proj.state.counts.get_mut(&UserStatus::Active) {
                *count = count.saturating_sub(1);
            }
            *proj.state.counts.entry(UserStatus::Suspended).or_insert(0) += 1;
        },
    }
}

// Usage
let mut projection = UserCountProjection::new(UserCountState::default());
projection.handle(&event_envelope).await?;
let active_users = projection.state().counts.get(&UserStatus::Active);
```

**Benefits:**
- ✅ **No string duplication** - Event types inferred via `EventType` trait
- ✅ **Type-safe** - Compile-time verification of event types
- ✅ **Less boilerplate** - ~60% less code than manual implementation
- ✅ **Declarative** - Clear per-event handlers
- ✅ **Auto-generated methods** - `new()`, `state()`, `state_mut()`, `handle()`

**When to use manual implementation instead:**
- Complex conditional logic across multiple events
- Need to maintain additional internal state
- Custom error handling beyond the default
- Performance-critical code paths requiring fine-grained control

### Projection Pattern

Projections in event-sauce are simple structs with handler methods. No trait implementation required - just define your state and a method to process events.

### Manual Implementation Example: User Count

```rust
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use event_sauce_core::{EventEnvelope, Result};

/// Maintains a count of users by status
#[derive(Debug, Clone, Default)]
struct UserCountProjection {
    counts: HashMap<UserStatus, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum UserStatus {
    Active,
    Inactive,
    Suspended,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum UserEvent {
    Registered { user_id: Uuid, email: String },
    Activated { user_id: Uuid },
    Deactivated { user_id: Uuid },
    Suspended { user_id: Uuid },
}

impl UserCountProjection {
    fn new() -> Self {
        Self::default()
    }

    fn get_count(&self, status: &UserStatus) -> u64 {
        *self.counts.get(status).unwrap_or(&0)
    }

    fn total_users(&self) -> u64 {
        self.counts.values().sum()
    }

    // Simple event handler method - no trait required
    fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        // Deserialize using from_envelope
        let event_data = UserEvent::from_envelope(event)?;

        match event_data {
            UserEvent::Registered { .. } => {
                *self.counts.entry(UserStatus::Inactive).or_insert(0) += 1;
            }
            UserEvent::Activated { .. } => {
                if let Some(count) = self.counts.get_mut(&UserStatus::Inactive) {
                    *count = count.saturating_sub(1);
                }
                *self.counts.entry(UserStatus::Active).or_insert(0) += 1;
            }
            UserEvent::Deactivated { .. } => {
                if let Some(count) = self.counts.get_mut(&UserStatus::Active) {
                    *count = count.saturating_sub(1);
                }
                *self.counts.entry(UserStatus::Inactive).or_insert(0) += 1;
            }
            UserEvent::Suspended { .. } => {
                if let Some(count) = self.counts.get_mut(&UserStatus::Active) {
                    *count = count.saturating_sub(1);
                }
                *self.counts.entry(UserStatus::Suspended).or_insert(0) += 1;
            }
        }

        Ok(())
    }
}
```

### Complex Example: User Details

```rust
use std::collections::HashMap;
use chrono::{DateTime, Utc};

/// Maintains detailed user information for efficient lookups
#[derive(Debug, Clone, Default)]
struct UserDetailsProjection {
    users: HashMap<Uuid, UserDetails>,
}

#[derive(Debug, Clone)]
struct UserDetails {
    user_id: Uuid,
    email: String,
    status: UserStatus,
    created_at: DateTime<Utc>,
    last_login: Option<DateTime<Utc>>,
}

impl UserDetailsProjection {
    fn new() -> Self {
        Self::default()
    }

    fn get_user(&self, user_id: &Uuid) -> Option<&UserDetails> {
        self.users.get(user_id)
    }

    fn find_by_email(&self, email: &str) -> Option<&UserDetails> {
        self.users.values().find(|u| u.email == email)
    }

    fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = UserEvent::from_envelope(event)?;

        match event_data {
            UserEvent::Registered { user_id, email } => {
                self.users.insert(user_id, UserDetails {
                    user_id,
                    email,
                    status: UserStatus::Inactive,
                    created_at: Utc::now(),
                    last_login: None,
                });
            }
            UserEvent::Activated { user_id } => {
                if let Some(user) = self.users.get_mut(&user_id) {
                    user.status = UserStatus::Active;
                }
            }
            UserEvent::Deactivated { user_id } => {
                if let Some(user) = self.users.get_mut(&user_id) {
                    user.status = UserStatus::Inactive;
                }
            }
            UserEvent::Suspended { user_id } => {
                if let Some(user) = self.users.get_mut(&user_id) {
                    user.status = UserStatus::Suspended;
                }
            }
        }

        Ok(())
    }
}
```

## Using Subscriptions

### Recommended Pattern: Stream API + Integrated Checkpoint Store

The **recommended** way to use subscriptions is with the Stream API and integrated checkpoint store configuration:

```rust
use event_sauce_core::{EventFilter, EventStore};
use futures::StreamExt;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Configure checkpoint store
    let checkpoint_store = Arc::new(PostgresCheckpointStore::new(pool.clone()));

    // 2. Create event store WITH integrated checkpoint store (recommended!)
    let store = Arc::new(PostgresEventStore::builder()
        .pool(pool)
        .checkpoint_store(checkpoint_store)  // Configure once
        .build());

    // 3. Create projection state
    let projection = Arc::new(Mutex::new(UserCountProjection::new()));

    // 4. Create subscription using trait method (checkpoint store included automatically!)
    let subscription = store
        .subscription_builder("user-count")
        .filter(EventFilter::by_aggregate_type("User"))
        .build()?;

    // 5. Process events using Stream API (recommended!)
    let stream = subscription.into_stream().await?;
    tokio::pin!(stream);

    let proj = projection.clone();
    while let Some(result) = stream.next().await {
        let event = result?;
        let mut p = proj.lock().unwrap();
        p.handle_event(&event)?;
    }

    Ok(())
}
```

**Why this pattern?**
- ✅ **Single configuration**: Checkpoint store set up once with event store
- ✅ **Stream API**: Composable with futures, type-safe error handling
- ✅ **Automatic injection**: Checkpoint store automatically included in subscriptions
- ✅ **Familiar patterns**: Standard async iterator with `tokio::pin!` and `.next().await`

### Alternative: Callback API

For simpler use cases, the callback API is still available:

```rust
let mut subscription = store
    .subscription_builder("user-count")
    .filter(EventFilter::by_aggregate_type("User"))
    .build()?;

subscription.run(move |event| {
    let mut p = projection.lock().unwrap();
    p.handle_event(&event)
}).await?;
```

### Running Multiple Projections

```rust
use std::sync::{Arc, Mutex};
use tokio::task;

#[tokio::main]
async fn main() -> Result<()> {
    let store = Arc::new(InMemoryEventStore::new());
    let checkpoint_store = Arc::new(SimpleCheckpointStore::default());

    // Create multiple projection states
    let count_state = Arc::new(Mutex::new(UserCountProjection::new()));
    let details_state = Arc::new(Mutex::new(UserDetailsProjection::new()));

    // Run them concurrently
    let count_handle = task::spawn({
        let store = store.clone();
        let checkpoint_store = checkpoint_store.clone();
        let state = count_state.clone();

        async move {
            let mut subscription = Subscription::builder("user-count", store)
                .checkpoint_store(checkpoint_store)
                .filter(EventFilter::all())
                .build()?;

            subscription.run(move |event| {
                let mut s = state.lock().unwrap();
                s.handle_event(&event)
            }).await
        }
    });

    let details_handle = task::spawn({
        let store = store.clone();
        let checkpoint_store = checkpoint_store.clone();
        let state = details_state.clone();

        async move {
            let mut subscription = Subscription::builder("user-details", store)
                .checkpoint_store(checkpoint_store)
                .filter(EventFilter::all())
                .build()?;

            subscription.run(move |event| {
                let mut s = state.lock().unwrap();
                s.handle_event(&event)
            }).await
        }
    });

    // Wait for both to complete (or run indefinitely)
    tokio::try_join!(count_handle, details_handle)?;

    Ok(())
}
```

## Checkpoint Management

Checkpoints track projection progress, enabling resumption after restarts or failures.

### What are Checkpoints?

```rust
pub struct Checkpoint {
    projection_name: String,  // e.g., "user_count_by_status"
    position: Position,       // Global position in event stream
    timestamp: DateTime<Utc>, // When checkpoint was saved
}
```

### Checkpoint Strategies

```rust
use event_sauce_core::CheckpointStrategy;

// Default: Save after every event (safest, recommended for production)
let strategy = CheckpointStrategy::EveryEvent;

// Alternative: Save every N events (reduced overhead, risk of data loss)
let strategy = CheckpointStrategy::EveryN(100);

// Advanced: Manual checkpointing (full control, use with caution)
let strategy = CheckpointStrategy::Manual;
```

**Note:** The default strategy is `EveryEvent`, which provides the strongest durability guarantees. Only use `EveryN` or `Manual` if you have specific performance requirements and can tolerate potential event loss on crashes.

### CheckpointStore Trait

```rust
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Save a checkpoint
    async fn save_checkpoint(&self, subscription_name: &str, position: Position) -> Result<()>;

    /// Load a checkpoint
    async fn load_checkpoint(&self, subscription_name: &str) -> Result<Option<Position>>;

    /// Delete a checkpoint (for rebuilding)
    async fn delete_checkpoint(&self, subscription_name: &str) -> Result<()>;
}
```

### Production Checkpoint Storage

For production, use PostgreSQL-backed checkpoint storage:

```rust
use sqlx::PgPool;

#[derive(Clone)]
struct PostgresCheckpointStore {
    pool: PgPool,
}

impl PostgresCheckpointStore {
    async fn save(&self, subscription: &str, position: Position) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO subscription_checkpoints (
                subscription_name,
                position,
                updated_at
            )
            VALUES ($1, $2, NOW())
            ON CONFLICT (subscription_name)
            DO UPDATE SET
                position = $2,
                updated_at = NOW()
            "#,
            subscription,
            position.as_i64(),
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn load(&self, subscription: &str) -> Result<Option<Position>> {
        let row = sqlx::query!(
            r#"
            SELECT position
            FROM subscription_checkpoints
            WHERE subscription_name = $1
            "#,
            subscription,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Position::new(r.position)))
    }
}
```

## Real-World Example

Complete example with task management domain:

```rust
use event_sauce_core::{Subscription, EventFilter, CheckpointStrategy};
use std::sync::Arc;

// Domain events
#[derive(Debug, Clone, Serialize, Deserialize)]
enum TaskEvent {
    Created {
        task_id: Uuid,
        title: String,
        assignee: String
    },
    StatusChanged {
        task_id: Uuid,
        old_status: TaskStatus,
        new_status: TaskStatus
    },
    Assigned {
        task_id: Uuid,
        from: String,
        to: String
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum TaskStatus {
    Todo,
    InProgress,
    Completed,
}

// Projection 1: Task counts by status
#[derive(Debug, Clone, Default)]
struct TaskCountProjection {
    counts: HashMap<TaskStatus, u64>,
}

impl TaskCountProjection {
    fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = TaskEvent::from_envelope(event)?;

        match event_data {
            TaskEvent::Created { .. } => {
                *self.counts.entry(TaskStatus::Todo).or_insert(0) += 1;
            }
            TaskEvent::StatusChanged { old_status, new_status, .. } => {
                if let Some(count) = self.counts.get_mut(&old_status) {
                    *count = count.saturating_sub(1);
                }
                *self.counts.entry(new_status).or_insert(0) += 1;
            }
            _ => {}
        }

        Ok(())
    }
}

// Projection 2: Tasks by assignee
#[derive(Debug, Clone, Default)]
struct TasksByAssigneeProjection {
    assignments: HashMap<String, Vec<Uuid>>,
}

impl TasksByAssigneeProjection {
    fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = TaskEvent::from_envelope(event)?;

        match event_data {
            TaskEvent::Created { task_id, assignee, .. } => {
                self.assignments
                    .entry(assignee)
                    .or_insert_with(Vec::new)
                    .push(task_id);
            }
            TaskEvent::Assigned { task_id, from, to } => {
                // Remove from old assignee
                if let Some(tasks) = self.assignments.get_mut(&from) {
                    tasks.retain(|&id| id != task_id);
                }
                // Add to new assignee
                self.assignments
                    .entry(to)
                    .or_insert_with(Vec::new)
                    .push(task_id);
            }
            _ => {}
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let store = Arc::new(InMemoryEventStore::new());
    let checkpoint_store = Arc::new(SimpleCheckpointStore::default());

    // Create projection states
    let count_state = Arc::new(Mutex::new(TaskCountProjection::default()));
    let assignee_state = Arc::new(Mutex::new(TasksByAssigneeProjection::default()));

    // Create and run projections concurrently
    let count_handle = tokio::spawn({
        let store = store.clone();
        let checkpoint_store = checkpoint_store.clone();
        let state = count_state.clone();

        async move {
            let mut subscription = Subscription::builder("task-count", store)
                .checkpoint_store(checkpoint_store)
                .build()?;

            subscription.run(move |event| {
                let mut s = state.lock().unwrap();
                s.handle_event(&event)
            }).await
        }
    });

    let assignee_handle = tokio::spawn({
        let store = store.clone();
        let checkpoint_store = checkpoint_store.clone();
        let state = assignee_state.clone();

        async move {
            let mut subscription = Subscription::builder("task-assignee", store)
                .checkpoint_store(checkpoint_store)
                .build()?;

            subscription.run(move |event| {
                let mut s = state.lock().unwrap();
                s.handle_event(&event)
            }).await
        }
    });

    // Run subscriptions
    tokio::try_join!(count_handle, assignee_handle)?;

    Ok(())
}
```

**See the complete example with recommended patterns**: `cargo run --example task-projections --features "memory,macros"`

> **Note**: The example above uses the older pattern for illustration. The task-projections example demonstrates the **recommended Stream API pattern** with integrated checkpoint stores.

## Production Patterns

### Pattern 1: Separate Query Service

```rust
/// Query service that exposes projection data via API
pub struct TaskQueryService {
    count_projection: Arc<RwLock<TaskCountProjection>>,
    assignee_projection: Arc<RwLock<TasksByAssigneeProjection>>,
}

impl TaskQueryService {
    /// Get task counts by status
    pub async fn get_task_counts(&self) -> HashMap<TaskStatus, u64> {
        let projection = self.count_projection.read().await;
        projection.counts.clone()
    }

    /// Get tasks for specific assignee
    pub async fn get_tasks_for_assignee(&self, assignee: &str) -> Vec<Uuid> {
        let projection = self.assignee_projection.read().await;
        projection.assignments
            .get(assignee)
            .cloned()
            .unwrap_or_default()
    }

    /// Check task count for status
    pub async fn count_tasks_with_status(&self, status: TaskStatus) -> u64 {
        let projection = self.count_projection.read().await;
        *projection.counts.get(&status).unwrap_or(&0)
    }
}
```

### Pattern 2: Materialized Views

```rust
use sqlx::PgPool;

/// Projection that writes to database table
#[derive(Clone)]
struct MaterializedTaskListProjection {
    pool: PgPool,
}

impl MaterializedTaskListProjection {
    fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = TaskEvent::from_envelope(event)?;

        match event_data {
            TaskEvent::Created { task_id, title, assignee } => {
                sqlx::query!(
                    "INSERT INTO task_list (id, title, assignee, status)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (id) DO NOTHING",
                    task_id,
                    title,
                    assignee,
                    "Todo",
                )
                .execute(&self.pool)
                .await?;
            }
            TaskEvent::StatusChanged { task_id, new_status, .. } => {
                sqlx::query!(
                    "UPDATE task_list SET status = $1 WHERE id = $2",
                    format!("{:?}", new_status),
                    task_id,
                )
                .execute(&self.pool)
                .await?;
            }
            TaskEvent::Assigned { task_id, to, .. } => {
                sqlx::query!(
                    "UPDATE task_list SET assignee = $1 WHERE id = $2",
                    to,
                    task_id,
                )
                .execute(&self.pool)
                .await?;
            }
        }

        Ok(())
    }
}
```

### Pattern 3: Event Filtering for Performance

```rust
// Only subscribe to relevant events
async fn run_specialized_projection(store: Arc<impl EventStore>) -> Result<()> {
    let state = Arc::new(Mutex::new(CompletedTasksProjection::new()));
    let checkpoint_store = Arc::new(SimpleCheckpointStore::default());

    // Filter: Only subscribe to "TaskCompleted" events
    let mut subscription = Subscription::builder("completed-tasks", store)
        .checkpoint_store(checkpoint_store)
        .filter(EventFilter::by_event_type("Task.Completed"))
        .build()?;

    subscription.run(move |event| {
        let mut s = state.lock().unwrap();
        s.handle_event(&event)
    }).await?;

    Ok(())
}
```

### Pattern 4: Rebuilding Projections

```rust
/// Rebuild projection from scratch
async fn rebuild_projection(
    subscription_name: &str,
    store: Arc<impl EventStore>,
    checkpoint_store: Arc<dyn CheckpointStore>,
) -> Result<()> {
    println!("Rebuilding projection: {subscription_name}");

    // Delete checkpoint to start from beginning
    checkpoint_store.delete_checkpoint(subscription_name).await?;

    // Create fresh subscription
    let state = Arc::new(Mutex::new(TaskCountProjection::default()));
    let mut subscription = Subscription::builder(subscription_name, store)
        .checkpoint_store(checkpoint_store)
        .build()?;

    // Run subscription (will process all events from start)
    subscription.run(move |event| {
        let mut s = state.lock().unwrap();
        s.handle_event(&event)
    }).await?;

    println!("Rebuild complete for {subscription_name}");
    Ok(())
}
```

## Testing Projections

### Unit Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_event(event: TaskEvent) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "Task".to_string(),
            "TaskEvent".to_string(),
            Version::new(1),
            serde_json::to_value(&event).unwrap(),
        )
    }

    #[tokio::test]
    async fn test_count_projection_increments_on_create() {
        let mut projection = TaskCountProjection::default();

        let event = create_test_event(TaskEvent::Created {
            task_id: Uuid::new_v4(),
            title: "Test".to_string(),
            assignee: "alice".to_string(),
        });

        projection.handle_event(&event).unwrap();

        assert_eq!(projection.counts.get(&TaskStatus::Todo), Some(&1));
    }

    #[tokio::test]
    async fn test_count_projection_handles_status_change() {
        let mut projection = TaskCountProjection::default();
        projection.counts.insert(TaskStatus::Todo, 1);

        let event = create_test_event(TaskEvent::StatusChanged {
            task_id: Uuid::new_v4(),
            old_status: TaskStatus::Todo,
            new_status: TaskStatus::InProgress,
        });

        projection.handle_event(&event).unwrap();

        assert_eq!(projection.counts.get(&TaskStatus::Todo), Some(&0));
        assert_eq!(projection.counts.get(&TaskStatus::InProgress), Some(&1));
    }
}
```

### Integration Testing with Subscriptions

```rust
#[tokio::test]
async fn test_projection_with_subscription() {
    let store = Arc::new(InMemoryEventStore::new());
    let checkpoint_store = Arc::new(SimpleCheckpointStore::default());

    // Add events to store
    let task_id = Uuid::new_v4();
    store.append(
        StreamId::new("Task", task_id),
        vec![
            create_test_event(TaskEvent::Created {
                task_id,
                title: "Test Task".to_string(),
                assignee: "alice".to_string(),
            }),
            create_test_event(TaskEvent::StatusChanged {
                task_id,
                old_status: TaskStatus::Todo,
                new_status: TaskStatus::InProgress,
            }),
        ],
        Version::initial(),
    ).await.unwrap();

    // Create projection and subscription
    let state = Arc::new(Mutex::new(TaskCountProjection::default()));

    let mut subscription = Subscription::builder("test-projection", store)
        .checkpoint_store(checkpoint_store)
        .build()
        .unwrap();

    // Run subscription
    let state_clone = state.clone();
    subscription.run(move |event| {
        let mut s = state_clone.lock().unwrap();
        s.handle_event(&event)
    }).await.unwrap();

    // Verify projection state
    let s = state.lock().unwrap();
    assert_eq!(s.counts.get(&TaskStatus::Todo), Some(&0));
    assert_eq!(s.counts.get(&TaskStatus::InProgress), Some(&1));
}
```

## Best Practices

### 1. Keep Projections Simple

```rust
// ✅ GOOD: Simple, focused projection
struct ActiveUserCountProjection {
    count: u64,
}

// ❌ BAD: Too complex, doing too much
struct GodProjection {
    user_counts: HashMap<Status, u64>,
    user_details: HashMap<Uuid, User>,
    user_logins: Vec<LoginEvent>,
    analytics: UserAnalytics,
    reports: MonthlyReports,
    // ... too many responsibilities
}
```

### 2. Make Projections Idempotent

```rust
// ✅ GOOD: Idempotent - can replay events safely
fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
    let event_data = UserEvent::from_envelope(event)?;

    match event_data {
        UserEvent::Registered { user_id, email } => {
            // Use upsert - safe to replay
            self.users.insert(user_id, UserDetails {
                user_id,
                email,
                status: UserStatus::Inactive,
                created_at: Utc::now(),
                last_login: None,
            });
        }
        _ => {}
    }
    Ok(())
}

// ❌ BAD: Not idempotent - replaying breaks state
fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
    // Increment is NOT idempotent - replaying adds extra counts!
    if event.event_type == "UserRegistered" {
        self.count += 1;
    }
    Ok(())
}
```

### 3. Use Event Filters

```rust
// ✅ GOOD: Filter at subscription
let subscription = Subscription::builder("orders", store)
    .filter(EventFilter::by_event_type("Order.Completed"))
    .build()?;

// ❌ BAD: Receive all events, filter in handler
let subscription = Subscription::builder("orders", store)
    .filter(EventFilter::all())
    .build()?;

fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
    if event.event_type != "Order.Completed" {
        return Ok(()); // Wasteful - still receives all events
    }
    // ...
}
```

### 4. Always Use Checkpoints in Production

```rust
// ✅ GOOD: Checkpoint store configured
let subscription = Subscription::builder("user-projection", store)
    .checkpoint_store(checkpoint_store)
    .build()?;
    // Default: saves checkpoint after every event (safest)

// ❌ BAD: No checkpoints - must rebuild from scratch on restart
let subscription = Subscription::builder("user-projection", store)
    .build()?;
```

### 5. Handle Deserialization Errors Gracefully

```rust
// ✅ GOOD: Graceful error handling
fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
    let event_data = match UserEvent::from_envelope(event) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Failed to deserialize event {}: {}", event.id, e);
            return Ok(()); // Skip this event, continue processing
        }
    };

    // Process event...
    Ok(())
}

// ❌ BAD: Panic on deserialization error
fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
    let event_data = UserEvent::from_envelope(event).unwrap(); // Will panic and crash projection!

    // ...
}
```

### 6. Separate Projections by Concern

```rust
// ✅ GOOD: Focused projections
struct UserCountProjection { count: u64 }
struct UserSearchProjection { index: SearchIndex }
struct UserAnalyticsProjection { metrics: Metrics }

// ❌ BAD: One giant projection
struct UserProjection {
    count: u64,
    search_index: SearchIndex,
    metrics: Metrics,
    // ... everything
}
```

### 7. Version Your Projections

```rust
#[derive(Default)]
struct UserProjectionV2 {
    // ... new fields with enhanced schema
}

impl UserProjectionV2 {
    fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        // ... handle with new logic
        Ok(())
    }
}

// Use different subscription names for different versions
// "user_projection_v1" vs "user_projection_v2"
// Can run V1 and V2 simultaneously during migration
```

### 8. Monitor Subscription Lag

```rust
// Track how far behind subscriptions are
struct SubscriptionMetrics {
    last_processed_position: Position,
    current_position: Position,
}

impl SubscriptionMetrics {
    fn lag(&self) -> i64 {
        self.current_position.as_i64() - self.last_processed_position.as_i64()
    }

    fn is_lagging(&self) -> bool {
        self.lag() > 1000  // Alert if >1000 events behind
    }
}
```

## Summary

Projections and subscriptions work together to provide durable, guaranteed-delivery read models:

1. **Subscriptions** provide guaranteed delivery from the durable event store
2. **Projections** maintain optimized read models from event streams
3. **Checkpoints** enable resumable processing after restarts
4. **Filters** reduce processing overhead
5. **Multiple projections** provide different views of the same data

**Key Takeaways:**

- Use projections for all queries (never query aggregates)
- Subscribe with durable subscriptions for guaranteed delivery
- Always use checkpoints in production
- Keep projections simple and focused
- Make projections idempotent for safe replay
- Test projections thoroughly

**Next Steps:**

- Run the complete example: `cargo run --example task-projections --features "memory,projections"`
- Read [PostgreSQL Production Setup](postgres-production.md) for production patterns
- Explore [Architecture Overview](architecture.md) for system design

---

**Need help?** Check the [examples](../crates/event-sauce/examples/) or open an issue on GitHub.
