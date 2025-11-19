//! Task Management with Subscriptions - Simplified
//!
//! This example demonstrates:
//! - Creating events from a task management domain
//! - Using the Subscription system for guaranteed delivery
//! - Building projections with subscriptions (without Projection trait)
//! - Using checkpoints to track progress
//! - Running multiple subscriptions
//!
//! Run with: `cargo run --example task-projections --features "memory,macros"`

use chrono::{DateTime, Utc};
use event_sauce::event_sauce_memory::{InMemoryCheckpointStore, InMemoryEventStore};
use event_sauce::{
    ApplyEvent, CheckpointStore, CheckpointStrategy, DomainEvent, EventEnvelope, EventFilter,
    EventStore, Result, StreamId, Subscription, Version,
};
use event_sauce_macros::{AggregateError, AggregateId, AggregateState, Event};
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
#[derive(AggregateId, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
struct TaskId(Uuid);

/// Error type for task operations
#[derive(AggregateError, Debug, Clone, thiserror::Error)]
#[allow(dead_code)]
enum TaskError {
    #[error("Task not found")]
    NotFound,
}

/// Task state
#[derive(AggregateState, Debug, Clone, Default, Serialize, Deserialize)]
#[aggregate(id = "TaskId", event = "TaskEvent", error = "TaskError")]
struct TaskState {
    #[aggregate_id]
    id: TaskId,
    title: String,
    assignee: String,
    status: TaskStatus,
}

/// Task events
#[derive(Event, Debug, Clone, Serialize, Deserialize)]
#[event(version = 1, type_prefix = "Task", aggregate = "TaskAggregate")]
enum TaskEvent {
    Created(Created),
    StatusChanged(StatusChanged),
    Assigned(Assigned),
    Completed(Completed),
}

// ApplyEvent implementations
impl ApplyEvent<TaskAggregate, TaskError> for Created {
    fn apply(&self, task: &mut TaskAggregate) {
        task.title = self.title.clone();
        task.assignee = self.assignee.clone();
        task.status = TaskStatus::Todo;
    }
}

impl ApplyEvent<TaskAggregate, TaskError> for StatusChanged {
    fn apply(&self, task: &mut TaskAggregate) {
        task.status = self.new_status.clone();
    }
}

impl ApplyEvent<TaskAggregate, TaskError> for Assigned {
    fn apply(&self, task: &mut TaskAggregate) {
        task.assignee = self.to_assignee.clone();
    }
}

impl ApplyEvent<TaskAggregate, TaskError> for Completed {
    fn apply(&self, task: &mut TaskAggregate) {
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

    // Create event store
    let store = Arc::new(InMemoryEventStore::new());
    println!("✓ Created event store");

    // Create checkpoint store
    let checkpoint_store = Arc::new(InMemoryCheckpointStore::new());
    println!("✓ Created checkpoint store\n");

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

    println!("--- Processing Events with Subscriptions ---");

    // Create projection states
    let status_state = Arc::new(Mutex::new(TaskCountState::default()));
    let assignee_state = Arc::new(Mutex::new(TasksByAssigneeState::default()));
    let completed_state = Arc::new(Mutex::new(CompletedTasksState::default()));

    // Run status projection subscription
    {
        let state = status_state.clone();
        let mut subscription = Subscription::builder("task_count_by_status", store.clone())
            .checkpoint_store(checkpoint_store.clone())
            .filter(EventFilter::all())
            .checkpoint_strategy(CheckpointStrategy::EveryEvent)
            .build()?;

        subscription
            .run(move |event| {
                let mut s = state.lock().unwrap();
                s.handle_event(&event)
            })
            .await?;
    }

    // Run assignee projection subscription
    {
        let state = assignee_state.clone();
        let mut subscription = Subscription::builder("tasks_by_assignee", store.clone())
            .checkpoint_store(checkpoint_store.clone())
            .filter(EventFilter::all())
            .checkpoint_strategy(CheckpointStrategy::EveryEvent)
            .build()?;

        subscription
            .run(move |event| {
                let mut s = state.lock().unwrap();
                s.handle_event(&event)
            })
            .await?;
    }

    // Run completed tasks projection subscription
    {
        let state = completed_state.clone();
        let mut subscription = Subscription::builder("completed_tasks_counter", store.clone())
            .checkpoint_store(checkpoint_store.clone())
            .filter(EventFilter::by_event_type("Task.Completed"))
            .checkpoint_strategy(CheckpointStrategy::EveryEvent)
            .build()?;

        subscription
            .run(move |event| {
                let mut s = state.lock().unwrap();
                s.handle_event(&event)
            })
            .await?;
    }

    println!("✓ All subscriptions processed events\n");

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

    println!("\n{}", "=".repeat(70));
    println!("✅ Example completed!");
    println!("\nKey Takeaways:");
    println!("1. Subscriptions provide guaranteed delivery from event store");
    println!("2. Multiple subscriptions can process the same events independently");
    println!("3. Each subscription builds its own optimized read model");
    println!("4. Checkpoints enable resuming subscriptions after restart");
    println!("5. Event filtering reduces processing for specialized subscriptions");
    println!("{}", "=".repeat(70));
    println!();

    Ok(())
}
