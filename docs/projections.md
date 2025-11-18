# Projections and Event Bus

Projections are read models built from event streams. They enable efficient queries by maintaining denormalized views optimized for specific read patterns. The event bus provides real-time event distribution to keep projections up-to-date.

## Table of Contents

- [What are Projections?](#what-are-projections)
- [Event Bus Basics](#event-bus-basics)
- [Building Projections](#building-projections)
- [Using the Event Bus](#using-the-event-bus)
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
4. **Real-time** - Update as events occur via event bus

## Event Bus Basics

The event bus is a publish-subscribe system that distributes events to interested subscribers in real-time.

### Core Trait

```rust
#[async_trait]
pub trait EventBus: Send + Sync {
    /// Publish a single event to all subscribers
    async fn publish(&self, event: EventEnvelope) -> Result<()>;

    /// Publish multiple events efficiently
    async fn publish_batch(&self, events: Vec<EventEnvelope>) -> Result<()>;

    /// Subscribe to events matching a filter
    async fn subscribe(
        &self,
        filter: EventFilter,
    ) -> Result<impl Stream<Item = EventEnvelope> + Send>;
}
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

### Available Implementations

| Implementation | Use Case | Features |
|----------------|----------|----------|
| `InMemoryEventBus` | Development, testing | Fast, simple, in-process |
| `PostgresEventBus` | Production | LISTEN/NOTIFY, persistent |

## Building Projections

### The Projection Trait

```rust
use event_sauce_projections::Projection;
use event_sauce_core::{EventEnvelope, Result};
use async_trait::async_trait;

#[async_trait]
pub trait Projection: Send + Sync {
    /// Unique name for this projection
    fn name(&self) -> &str;

    /// Handle an event and update the projection state
    async fn handle(&mut self, event: &EventEnvelope) -> Result<()>;
}
```

### Simple Example: User Count

```rust
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Maintains a count of users by status
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
        let mut counts = HashMap::new();
        counts.insert(UserStatus::Active, 0);
        counts.insert(UserStatus::Inactive, 0);
        counts.insert(UserStatus::Suspended, 0);
        Self { counts }
    }

    fn get_count(&self, status: &UserStatus) -> u64 {
        *self.counts.get(status).unwrap_or(&0)
    }

    fn total_users(&self) -> u64 {
        self.counts.values().sum()
    }
}

#[async_trait]
impl Projection for UserCountProjection {
    fn name(&self) -> &str {
        "user_count_by_status"
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = serde_json::from_value::<UserEvent>(
            event.event_data.clone()
        ).map_err(|e| {
            event_sauce_core::Error::custom(format!("Deserialization failed: {}", e))
        })?;

        match event_data {
            UserEvent::Registered { .. } => {
                *self.counts.entry(UserStatus::Inactive).or_insert(0) += 1;
            }
            UserEvent::Activated { .. } => {
                *self.counts.entry(UserStatus::Inactive).or_insert(0) -= 1;
                *self.counts.entry(UserStatus::Active).or_insert(0) += 1;
            }
            UserEvent::Deactivated { .. } => {
                *self.counts.entry(UserStatus::Active).or_insert(0) -= 1;
                *self.counts.entry(UserStatus::Inactive).or_insert(0) += 1;
            }
            UserEvent::Suspended { .. } => {
                *self.counts.entry(UserStatus::Active).or_insert(0) -= 1;
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

/// Maintains detailed user information for efficient lookups
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
        Self {
            users: HashMap::new(),
        }
    }

    fn get_user(&self, user_id: &Uuid) -> Option<&UserDetails> {
        self.users.get(user_id)
    }

    fn find_by_email(&self, email: &str) -> Option<&UserDetails> {
        self.users.values().find(|u| u.email == email)
    }
}

#[async_trait]
impl Projection for UserDetailsProjection {
    fn name(&self) -> &str {
        "user_details"
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = serde_json::from_value::<UserEvent>(
            event.event_data.clone()
        )?;

        match event_data {
            UserEvent::Registered { user_id, email } => {
                self.users.insert(user_id, UserDetails {
                    user_id,
                    email,
                    status: UserStatus::Inactive,
                    created_at: event.occurred_at,
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

## Using the Event Bus

### Setup: Connecting Projections to Event Bus

```rust
use event_sauce::prelude::*;
use event_sauce_memory::InMemoryEventBus;
use event_sauce_projections::ProjectionRunner;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create event bus
    let bus = InMemoryEventBus::new();

    // 2. Create projections
    let count_projection = UserCountProjection::new();
    let details_projection = UserDetailsProjection::new();

    // 3. Create projection runners
    let mut count_runner = ProjectionRunner::new(count_projection);
    let mut details_runner = ProjectionRunner::new(details_projection);

    // 4. Subscribe to event streams
    let count_stream = bus.subscribe(EventFilter::all()).await?;
    let details_stream = bus.subscribe(EventFilter::all()).await?;

    // 5. Run projections in background tasks
    tokio::spawn(async move {
        count_runner.run(count_stream).await
    });

    tokio::spawn(async move {
        details_runner.run(details_stream).await
    });

    // 6. Publish events
    let user_id = Uuid::new_v4();
    bus.publish(create_event(UserEvent::Registered {
        user_id,
        email: "alice@example.com".to_string(),
    })).await?;

    bus.publish(create_event(UserEvent::Activated {
        user_id,
    })).await?;

    Ok(())
}
```

### Publishing Events

```rust
// Single event
bus.publish(event).await?;

// Batch publishing (more efficient)
bus.publish_batch(vec![event1, event2, event3]).await?;
```

### Subscribing with Filters

```rust
// Subscribe to all user events
let stream = bus
    .subscribe(EventFilter::by_aggregate_type("User"))
    .await?;

// Subscribe to specific event
let stream = bus
    .subscribe(EventFilter::by_event_type("UserRegistered"))
    .await?;

// Process events
while let Some(event) = stream.next().await {
    projection.handle(&event).await?;
}
```

## Checkpoint Management

Checkpoints track projection progress, enabling resumption after restarts or failures.

### What are Checkpoints?

```rust
pub struct Checkpoint {
    projection_name: String,  // e.g., "user_count_by_status"
    last_event_id: Uuid,      // Last processed event ID
    sequence: i64,            // Sequence number in stream
    timestamp: DateTime<Utc>, // When checkpoint was saved
}
```

### Using Checkpoints

```rust
use event_sauce_projections::{
    ProjectionRunner,
    Checkpoint,
    CheckpointStore,
    InMemoryCheckpointStore
};

async fn run_with_checkpoints(
    bus: &impl EventBus,
    projection: impl Projection,
) -> Result<()> {
    // 1. Create checkpoint store
    let checkpoint_store = InMemoryCheckpointStore::new();

    // 2. Load last checkpoint
    let last_checkpoint = checkpoint_store
        .load(projection.name())
        .await?;

    println!("Resuming from checkpoint: {:?}", last_checkpoint);

    // 3. Create runner
    let mut runner = ProjectionRunner::new(projection);

    // 4. Subscribe to events
    let stream = bus.subscribe(EventFilter::all()).await?;

    // 5. Run with checkpoint callbacks
    runner.run_with_callback(stream, {
        let checkpoint_store = checkpoint_store.clone();
        let projection_name = projection.name().to_string();

        move |event| {
            let checkpoint_store = checkpoint_store.clone();
            let projection_name = projection_name.clone();
            let event_id = event.id;
            let sequence = i64::from(event.event_version.as_i32());

            async move {
                // Save checkpoint after processing each event
                let checkpoint = Checkpoint::new(
                    &projection_name,
                    event_id,
                    sequence,
                );
                checkpoint_store.save(&projection_name, checkpoint).await?;
                Ok(())
            }
        }
    }).await?;

    Ok(())
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
    async fn save(&self, projection: &str, checkpoint: Checkpoint) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO projection_checkpoints (
                projection_name,
                last_event_id,
                sequence,
                timestamp
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (projection_name)
            DO UPDATE SET
                last_event_id = $2,
                sequence = $3,
                timestamp = $4
            "#,
            projection,
            checkpoint.last_event_id(),
            checkpoint.sequence(),
            checkpoint.timestamp(),
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn load(&self, projection: &str) -> Result<Option<Checkpoint>> {
        let row = sqlx::query!(
            r#"
            SELECT last_event_id, sequence, timestamp
            FROM projection_checkpoints
            WHERE projection_name = $1
            "#,
            projection,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Checkpoint::new(projection, r.last_event_id, r.sequence)))
    }
}
```

## Real-World Example

Complete example with task management domain:

```rust
use event_sauce::prelude::*;
use event_sauce_memory::InMemoryEventBus;
use event_sauce_projections::{Projection, ProjectionRunner};
use futures::StreamExt;

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
struct TaskCountProjection {
    counts: HashMap<TaskStatus, u64>,
}

#[async_trait]
impl Projection for TaskCountProjection {
    fn name(&self) -> &str {
        "task_count_by_status"
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = serde_json::from_value::<TaskEvent>(
            event.event_data.clone()
        )?;

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
struct TasksByAssigneeProjection {
    assignments: HashMap<String, Vec<Uuid>>,
}

#[async_trait]
impl Projection for TasksByAssigneeProjection {
    fn name(&self) -> &str {
        "tasks_by_assignee"
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = serde_json::from_value::<TaskEvent>(
            event.event_data.clone()
        )?;

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
    let bus = InMemoryEventBus::new();

    // Create and run projections
    let count_projection = TaskCountProjection {
        counts: HashMap::new()
    };
    let assignee_projection = TasksByAssigneeProjection {
        assignments: HashMap::new()
    };

    let mut count_runner = ProjectionRunner::new(count_projection);
    let mut assignee_runner = ProjectionRunner::new(assignee_projection);

    let count_stream = bus.subscribe(EventFilter::all()).await?;
    let assignee_stream = bus.subscribe(EventFilter::all()).await?;

    // Run projections in background
    tokio::spawn(async move {
        count_runner.run(count_stream).await
    });

    tokio::spawn(async move {
        assignee_runner.run(assignee_stream).await
    });

    // Publish events
    let task_id = Uuid::new_v4();
    bus.publish(create_event(TaskEvent::Created {
        task_id,
        title: "Implement auth".to_string(),
        assignee: "alice".to_string(),
    })).await?;

    bus.publish(create_event(TaskEvent::StatusChanged {
        task_id,
        old_status: TaskStatus::Todo,
        new_status: TaskStatus::InProgress,
    })).await?;

    Ok(())
}
```

**See the complete example**: `cargo run --example task-projections --features "memory,projections"`

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
/// Projection that writes to database table
struct MaterializedTaskListProjection {
    pool: PgPool,
}

#[async_trait]
impl Projection for MaterializedTaskListProjection {
    fn name(&self) -> &str {
        "materialized_task_list"
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = serde_json::from_value::<TaskEvent>(
            event.event_data.clone()
        )?;

        match event_data {
            TaskEvent::Created { task_id, title, assignee } => {
                sqlx::query!(
                    "INSERT INTO task_list (id, title, assignee, status)
                     VALUES ($1, $2, $3, $4)",
                    task_id,
                    title,
                    assignee,
                    TaskStatus::Todo as _,
                )
                .execute(&self.pool)
                .await?;
            }
            TaskEvent::StatusChanged { task_id, new_status, .. } => {
                sqlx::query!(
                    "UPDATE task_list SET status = $1 WHERE id = $2",
                    new_status as _,
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
async fn run_specialized_projection(bus: &impl EventBus) -> Result<()> {
    let projection = CompletedTasksProjection::new();
    let mut runner = ProjectionRunner::new(projection);

    // Filter: Only subscribe to "TaskCompleted" events
    let stream = bus
        .subscribe(EventFilter::by_event_type("TaskCompleted"))
        .await?;

    runner.run(stream).await?;
    Ok(())
}
```

### Pattern 4: Rebuilding Projections

```rust
use event_sauce_core::EventStore;

/// Rebuild projection from scratch by replaying all events
async fn rebuild_projection(
    store: &impl EventStore,
    projection: &mut impl Projection,
) -> Result<()> {
    println!("Rebuilding projection: {}", projection.name());

    // Load all events from event store
    let mut stream = store
        .load_all_events()
        .await?;

    let mut count = 0;
    while let Some(event) = stream.next().await {
        let event = event?;
        projection.handle(&event).await?;
        count += 1;

        if count % 1000 == 0 {
            println!("Processed {} events...", count);
        }
    }

    println!("Rebuild complete. Processed {} events", count);
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
        let mut projection = TaskCountProjection {
            counts: HashMap::new(),
        };

        let event = create_test_event(TaskEvent::Created {
            task_id: Uuid::new_v4(),
            title: "Test".to_string(),
            assignee: "alice".to_string(),
        });

        projection.handle(&event).await.unwrap();

        assert_eq!(projection.counts.get(&TaskStatus::Todo), Some(&1));
    }

    #[tokio::test]
    async fn test_count_projection_handles_status_change() {
        let mut projection = TaskCountProjection {
            counts: HashMap::from([
                (TaskStatus::Todo, 1),
                (TaskStatus::InProgress, 0),
            ]),
        };

        let event = create_test_event(TaskEvent::StatusChanged {
            task_id: Uuid::new_v4(),
            old_status: TaskStatus::Todo,
            new_status: TaskStatus::InProgress,
        });

        projection.handle(&event).await.unwrap();

        assert_eq!(projection.counts.get(&TaskStatus::Todo), Some(&0));
        assert_eq!(projection.counts.get(&TaskStatus::InProgress), Some(&1));
    }
}
```

### Integration Testing

```rust
#[tokio::test]
async fn test_projection_with_event_bus() {
    let bus = InMemoryEventBus::new();
    let projection = TaskCountProjection {
        counts: HashMap::new(),
    };
    let mut runner = ProjectionRunner::new(projection);

    let stream = bus.subscribe(EventFilter::all()).await.unwrap();

    // Run projection in background
    let handle = tokio::spawn(async move {
        runner.run(stream.take(3)).await
    });

    // Publish events
    bus.publish(create_test_event(TaskEvent::Created {
        task_id: Uuid::new_v4(),
        title: "Task 1".to_string(),
        assignee: "alice".to_string(),
    })).await.unwrap();

    bus.publish(create_test_event(TaskEvent::Created {
        task_id: Uuid::new_v4(),
        title: "Task 2".to_string(),
        assignee: "bob".to_string(),
    })).await.unwrap();

    // Wait for processing
    handle.await.unwrap().unwrap();

    // Verify projection state
    let projection = runner.projection();
    assert_eq!(projection.counts.get(&TaskStatus::Todo), Some(&2));
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
async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
    match event_data {
        UserEvent::Registered { user_id, email } => {
            // Use upsert - safe to replay
            self.users.insert(user_id, UserDetails {
                user_id,
                email
            });
        }
    }
    Ok(())
}

// ❌ BAD: Not idempotent - replaying breaks state
async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
    match event_data {
        UserEvent::Registered { .. } => {
            // Increment is NOT idempotent
            self.count += 1;
        }
    }
    Ok(())
}
```

### 3. Use Event Filters

```rust
// ✅ GOOD: Filter at subscription
let stream = bus
    .subscribe(EventFilter::by_event_type("OrderCompleted"))
    .await?;

// ❌ BAD: Receive all events, filter in handler
let stream = bus.subscribe(EventFilter::all()).await?;
async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
    if event.event_type != "OrderCompleted" {
        return Ok(()); // Wasteful processing
    }
    // ...
}
```

### 4. Always Use Checkpoints in Production

```rust
// ✅ GOOD: Checkpoint after each event
runner.run_with_callback(stream, |event| {
    async move {
        checkpoint_store.save(projection_name, checkpoint).await?;
        Ok(())
    }
}).await?;

// ❌ BAD: No checkpoints - must rebuild from scratch on restart
runner.run(stream).await?;
```

### 5. Handle Deserialization Errors Gracefully

```rust
// ✅ GOOD: Graceful error handling
async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
    let event_data = match serde_json::from_value::<UserEvent>(
        event.event_data.clone()
    ) {
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
async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
    let event_data = serde_json::from_value::<UserEvent>(
        event.event_data.clone()
    ).unwrap(); // Will panic and crash projection!

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
struct UserProjectionV2 {
    // ... new fields
}

impl Projection for UserProjectionV2 {
    fn name(&self) -> &str {
        "user_projection_v2"  // Different name = new projection
    }

    // ... handle with new logic
}

// Can run V1 and V2 simultaneously during migration
```

### 8. Monitor Projection Lag

```rust
// Track how far behind projections are
struct ProjectionMetrics {
    last_processed_sequence: i64,
    current_sequence: i64,
}

impl ProjectionMetrics {
    fn lag(&self) -> i64 {
        self.current_sequence - self.last_processed_sequence
    }

    fn is_lagging(&self) -> bool {
        self.lag() > 1000  // Alert if >1000 events behind
    }
}
```

## Summary

Projections and event bus work together to provide real-time, efficient read models:

1. **Event Bus** distributes events to subscribers in real-time
2. **Projections** maintain optimized read models from event streams
3. **Checkpoints** enable resumable processing
4. **Filters** reduce processing overhead
5. **Multiple projections** provide different views of the same data

**Key Takeaways:**

- Use projections for all queries (never query aggregates)
- Subscribe to event bus for real-time updates
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
