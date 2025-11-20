//! Task Management with Subscriptions
//!
//! This example demonstrates the **recommended patterns** for using subscriptions:
//!
//! **Key Patterns:**
//! 1. Configure event store with checkpoint store during setup
//! 2. Use the tokio Stream API (into_stream) for composable event processing
//! 3. Use builder pattern for subscription configuration
//! 4. Apply event filters for targeted subscriptions
//!
//! **Why Stream API?**
//! - Composable with futures ecosystem
//! - Type-safe error handling
//! - Natural async/await integration
//! - Better control flow with standard iterators
//!
//! Run with: `cargo run --example task-projections --features "memory,macros"`

use chrono::{DateTime, Utc};
use event_sauce::event_sauce_memory::{InMemoryCheckpointStore, InMemoryEventStore};
use event_sauce::{
    ApplyEvent, CheckpointStore, DomainEvent, EventEnvelope, EventFilter, EventStore, Result,
    StreamId, Version,
};
use event_sauce_macros::{aggregate, AggregateError, AggregateId, Event};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Task status enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
enum TaskStatus {
    #[default]
    Todo,
    InProgress,
    Completed,
}

// Individual event structs
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Created {
    task_id: Uuid,
    title: String,
    assignee: String,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusChanged {
    task_id: Uuid,
    old_status: TaskStatus,
    new_status: TaskStatus,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Assigned {
    task_id: Uuid,
    from_assignee: String,
    to_assignee: String,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Completed {
    task_id: Uuid,
    timestamp: DateTime<Utc>,
}

/// Task aggregate ID
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(transparent)]
struct TaskId(Uuid);

/// Error type for task operations
#[derive(AggregateError, Debug, Clone, thiserror::Error)]
#[allow(dead_code)]
enum TaskError {
    #[error("Task not found")]
    NotFound,
}

/// Task state
#[aggregate(id = "TaskId", event = "TaskEvent", error = "TaskError")]
#[derive(Default)]
struct Task {
    title: String,
    assignee: String,
    status: TaskStatus,
}

/// Task events
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Task", aggregate = "Task")]
enum TaskEvent {
    Created(Created),
    StatusChanged(StatusChanged),
    Assigned(Assigned),
    Completed(Completed),
}

// ApplyEvent implementations
impl ApplyEvent<Task> for Created {
    fn apply(&self, task: &mut Task) {
        task.title = self.title.clone();
        task.assignee = self.assignee.clone();
        task.status = TaskStatus::Todo;
    }
}

impl ApplyEvent<Task> for StatusChanged {
    fn apply(&self, task: &mut Task) {
        task.status = self.new_status.clone();
    }
}

impl ApplyEvent<Task> for Assigned {
    fn apply(&self, task: &mut Task) {
        task.assignee = self.to_assignee.clone();
    }
}

impl ApplyEvent<Task> for Completed {
    fn apply(&self, task: &mut Task) {
        task.status = TaskStatus::Completed;
    }
}

/// Task count projection state
#[derive(Debug, Clone, Default)]
struct TaskCountState {
    counts: HashMap<TaskStatus, u64>,
}

impl TaskCountState {
    fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data: TaskEvent = event.try_into_event()?;

        match event_data {
            TaskEvent::Created(_) => {
                *self.counts.entry(TaskStatus::Todo).or_insert(0) += 1;
            }
            TaskEvent::StatusChanged(StatusChanged {
                old_status,
                new_status,
                ..
            }) => {
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

/// Tasks by assignee projection state
#[derive(Debug, Clone, Default)]
struct TasksByAssigneeState {
    assignments: HashMap<String, Vec<Uuid>>,
}

impl TasksByAssigneeState {
    fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data: TaskEvent = event.try_into_event()?;

        match event_data {
            TaskEvent::Created(Created {
                task_id, assignee, ..
            }) => {
                self.assignments.entry(assignee).or_default().push(task_id);
            }
            TaskEvent::Assigned(Assigned {
                task_id,
                from_assignee,
                to_assignee,
                ..
            }) => {
                if let Some(tasks) = self.assignments.get_mut(&from_assignee) {
                    tasks.retain(|&id| id != task_id);
                }
                self.assignments
                    .entry(to_assignee)
                    .or_default()
                    .push(task_id);
            }
            _ => {}
        }

        Ok(())
    }
}

/// Completed tasks projection state
#[derive(Debug, Clone, Default)]
struct CompletedTasksState {
    count: u64,
}

impl CompletedTasksState {
    fn handle_event(&mut self, event: &EventEnvelope) -> Result<()> {
        if event.event_type == "Task.Completed" {
            self.count += 1;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("\n{}", "=".repeat(70));
    println!("🛒 Task Management - Subscriptions Example");
    println!("{}\n", "=".repeat(70));

    // Create checkpoint store
    let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());

    // Create event store WITH integrated checkpoint store (recommended pattern)
    let store = Arc::new(InMemoryEventStore::with_checkpoint_store(
        event_sauce::SnapshotConfig::builder().build(),
        checkpoint_store.clone(),
    ));
    println!("✓ Created event store with integrated checkpoint store\n");

    // Generate task IDs
    let task1_id = Uuid::new_v4();
    let task2_id = Uuid::new_v4();
    let task3_id = Uuid::new_v4();

    println!("--- Publishing Task Events ---");

    // Create events
    let events = vec![
        TaskEvent::Created(Created {
            task_id: task1_id,
            title: "Implement authentication".to_string(),
            assignee: "alice".to_string(),
            timestamp: Utc::now(),
        })
        .to_envelope(task1_id)?,
        TaskEvent::Created(Created {
            task_id: task2_id,
            title: "Write documentation".to_string(),
            assignee: "bob".to_string(),
            timestamp: Utc::now(),
        })
        .to_envelope(task2_id)?,
        TaskEvent::Created(Created {
            task_id: task3_id,
            title: "Fix bug #123".to_string(),
            assignee: "alice".to_string(),
            timestamp: Utc::now(),
        })
        .to_envelope(task3_id)?,
        TaskEvent::StatusChanged(StatusChanged {
            task_id: task1_id,
            old_status: TaskStatus::Todo,
            new_status: TaskStatus::InProgress,
            timestamp: Utc::now(),
        })
        .to_envelope(task1_id)?,
        TaskEvent::Assigned(Assigned {
            task_id: task2_id,
            from_assignee: "bob".to_string(),
            to_assignee: "charlie".to_string(),
            timestamp: Utc::now(),
        })
        .to_envelope(task2_id)?,
        TaskEvent::StatusChanged(StatusChanged {
            task_id: task1_id,
            old_status: TaskStatus::InProgress,
            new_status: TaskStatus::Completed,
            timestamp: Utc::now(),
        })
        .to_envelope(task1_id)?,
        TaskEvent::Completed(Completed {
            task_id: task1_id,
            timestamp: Utc::now(),
        })
        .to_envelope(task1_id)?,
        TaskEvent::StatusChanged(StatusChanged {
            task_id: task3_id,
            old_status: TaskStatus::Todo,
            new_status: TaskStatus::InProgress,
            timestamp: Utc::now(),
        })
        .to_envelope(task3_id)?,
    ];

    // Append all events to the store
    let dummy_id = Uuid::new_v4();
    store
        .append(
            StreamId::new("Task", dummy_id),
            events.clone(),
            Version::initial(),
        )
        .await?;

    println!("✓ Appended {} events to event store\n", events.len());

    println!("--- Processing Events with Stream API (Recommended Pattern) ---");

    // Create projection states
    let status_state = Arc::new(Mutex::new(TaskCountState::default()));
    let assignee_state = Arc::new(Mutex::new(TasksByAssigneeState::default()));
    let completed_state = Arc::new(Mutex::new(CompletedTasksState::default()));

    use futures::StreamExt;

    // Run status projection subscription using Stream API
    {
        let state = status_state.clone();
        // Note: subscription_builder() is a trait method that automatically includes checkpoint store!
        let subscription = store.subscription_builder("task_count_by_status").build()?;

        let stream = subscription.into_stream().await?;
        tokio::pin!(stream);

        while let Some(result) = stream.next().await {
            let event = result?;
            let mut s = state.lock().unwrap();
            s.handle_event(&event)?;
        }
    }

    // Run assignee projection subscription using Stream API
    {
        let state = assignee_state.clone();
        let subscription = store.subscription_builder("tasks_by_assignee").build()?;

        let stream = subscription.into_stream().await?;
        tokio::pin!(stream);

        while let Some(result) = stream.next().await {
            let event = result?;
            let mut s = state.lock().unwrap();
            s.handle_event(&event)?;
        }
    }

    // Run completed tasks projection subscription with event filter
    {
        let state = completed_state.clone();
        let subscription = store
            .subscription_builder("completed_tasks_counter")
            .filter(EventFilter::by_event_type("Task.Completed"))
            .build()?;

        let stream = subscription.into_stream().await?;
        tokio::pin!(stream);

        while let Some(result) = stream.next().await {
            let event = result?;
            let mut s = state.lock().unwrap();
            s.handle_event(&event)?;
        }
    }

    println!("✓ All subscriptions processed events using Stream API\n");

    // Display results
    println!("=== Projection Results ===\n");

    {
        let state = status_state.lock().unwrap();
        println!("📊 Task Count by Status:");
        println!(
            "  - Todo: {}",
            state.counts.get(&TaskStatus::Todo).unwrap_or(&0)
        );
        println!(
            "  - In Progress: {}",
            state.counts.get(&TaskStatus::InProgress).unwrap_or(&0)
        );
        println!(
            "  - Completed: {}",
            state.counts.get(&TaskStatus::Completed).unwrap_or(&0)
        );
        println!("  - Total: {}\n", state.counts.values().sum::<u64>());
    }

    {
        let state = assignee_state.lock().unwrap();
        println!("👥 Tasks by Assignee:");
        let alice_tasks = state.assignments.get("alice").map_or(0, |v| v.len());
        let bob_tasks = state.assignments.get("bob").map_or(0, |v| v.len());
        let charlie_tasks = state.assignments.get("charlie").map_or(0, |v| v.len());

        println!("  - Alice: {alice_tasks} task(s)");
        println!("  - Bob: {bob_tasks} task(s)");
        println!("  - Charlie: {charlie_tasks} task(s)");
        println!("  - Total assignees: {}\n", state.assignments.len());
    }

    {
        let state = completed_state.lock().unwrap();
        println!("✅ Completed Tasks: {}\n", state.count);
    }

    // Show checkpoint status
    println!("=== Checkpoint Status ===\n");

    if let Some(checkpoint) = checkpoint_store
        .load_checkpoint("task_count_by_status")
        .await?
    {
        println!(
            "✓ Status projection checkpoint: Position {}",
            checkpoint.as_i64()
        );
    }

    if let Some(checkpoint) = checkpoint_store
        .load_checkpoint("tasks_by_assignee")
        .await?
    {
        println!(
            "✓ Assignee projection checkpoint: Position {}",
            checkpoint.as_i64()
        );
    }

    if let Some(checkpoint) = checkpoint_store
        .load_checkpoint("completed_tasks_counter")
        .await?
    {
        println!(
            "✓ Completed tasks projection checkpoint: Position {}",
            checkpoint.as_i64()
        );
    }

    println!("{}", "=".repeat(70));
    println!("✅ Example completed!");
    println!("\n📚 Key Patterns Demonstrated:");
    println!();
    println!("1. **Integrated Setup**: Event store configured with checkpoint store");
    println!("   - Single configuration point eliminates boilerplate");
    println!("   - Checkpoint store automatically available to subscriptions");
    println!();
    println!("2. **Stream API (Recommended)**: Using into_stream() for event processing");
    println!("   - Composable with futures/tokio ecosystem");
    println!("   - Type-safe error handling with Result<T>");
    println!("   - Natural async/await patterns");
    println!();
    println!("3. **Builder Pattern**: Fluent API for subscription configuration");
    println!("   - subscription_builder() from EventStore trait");
    println!("   - Checkpoint store included automatically");
    println!("   - Filter, strategy, and error policy configuration");
    println!();
    println!("4. **Event Filtering**: Targeted subscriptions for efficiency");
    println!("   - Filter by event type, aggregate type, or both");
    println!("   - Reduces processing overhead");
    println!("   - Enables specialized projections");
    println!();
    println!("5. **Multiple Subscriptions**: Independent event processing");
    println!("   - Each builds its own read model");
    println!("   - Process same events differently");
    println!("   - Guaranteed delivery with checkpoints");
    println!();
    println!("{}", "=".repeat(70));
    println!();

    Ok(())
}
