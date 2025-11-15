//! Task Management with Projections
//!
//! This example demonstrates:
//! - Creating events from a task management domain
//! - Building multiple projections (read models) from the same events
//! - Using checkpoints to track projection progress
//! - Running projections concurrently
//!
//! Run with: `cargo run --example task-projections --features "memory,projections"`

use async_trait::async_trait;
use event_sauce::{EventBus, EventEnvelope, EventFilter, Result, Version};
use event_sauce::event_sauce_memory::InMemoryEventBus;
use event_sauce::event_sauce_projections;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

// Re-export projection types for convenience
use event_sauce_projections::{
    Checkpoint, CheckpointStore, InMemoryCheckpointStore, Projection, ProjectionRunner,
};

/// Task status enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
enum TaskStatus {
    Todo,
    InProgress,
    Completed,
}

/// Task events
#[derive(Debug, Clone, Serialize, Deserialize)]
enum TaskEvent {
    Created {
        task_id: Uuid,
        title: String,
        assignee: String,
    },
    StatusChanged {
        task_id: Uuid,
        old_status: TaskStatus,
        new_status: TaskStatus,
    },
    Assigned {
        task_id: Uuid,
        from_assignee: String,
        to_assignee: String,
    },
    Completed {
        task_id: Uuid,
    },
}

/// Helper function to create event envelopes
fn create_event(event: TaskEvent, aggregate_id: Uuid, sequence: i32) -> EventEnvelope {
    let event_type = match &event {
        TaskEvent::Created { .. } => "TaskCreated",
        TaskEvent::StatusChanged { .. } => "TaskStatusChanged",
        TaskEvent::Assigned { .. } => "TaskAssigned",
        TaskEvent::Completed { .. } => "TaskCompleted",
    };

    EventEnvelope::new(
        Uuid::new_v4(),
        aggregate_id,
        "Task".to_string(),
        event_type.to_string(),
        Version::new(sequence),
        serde_json::to_value(&event).unwrap(),
    )
}

/// Projection 1: Task Count by Status
///
/// Maintains a count of tasks in each status.
#[derive(Debug)]
struct TaskCountByStatusProjection {
    counts: HashMap<TaskStatus, u64>,
}

impl TaskCountByStatusProjection {
    fn new() -> Self {
        let mut counts = HashMap::new();
        counts.insert(TaskStatus::Todo, 0);
        counts.insert(TaskStatus::InProgress, 0);
        counts.insert(TaskStatus::Completed, 0);
        Self { counts }
    }

    fn get_count(&self, status: &TaskStatus) -> u64 {
        *self.counts.get(status).unwrap_or(&0)
    }

    fn total_tasks(&self) -> u64 {
        self.counts.values().sum()
    }
}

#[async_trait]
impl Projection for TaskCountByStatusProjection {
    fn name(&self) -> &str {
        "task_count_by_status"
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = serde_json::from_value::<TaskEvent>(event.event_data.clone())
            .map_err(|e| event_sauce_core::Error::custom(format!("Failed to deserialize: {}", e)))?;

        match event_data {
            TaskEvent::Created { .. } => {
                *self.counts.entry(TaskStatus::Todo).or_insert(0) += 1;
            }
            TaskEvent::StatusChanged {
                old_status,
                new_status,
                ..
            } => {
                // Decrement old status count
                if let Some(count) = self.counts.get_mut(&old_status) {
                    *count = count.saturating_sub(1);
                }
                // Increment new status count
                *self.counts.entry(new_status).or_insert(0) += 1;
            }
            TaskEvent::Completed { .. } => {
                // Completed event might be separate from status change
                // This is idempotent if status was already changed to Completed
            }
            TaskEvent::Assigned { .. } => {
                // Assignment doesn't affect status counts
            }
        }

        Ok(())
    }
}

/// Projection 2: Tasks by Assignee
///
/// Maintains a list of task IDs for each assignee.
#[derive(Debug)]
struct TasksByAssigneeProjection {
    assignments: HashMap<String, Vec<Uuid>>,
}

impl TasksByAssigneeProjection {
    fn new() -> Self {
        Self {
            assignments: HashMap::new(),
        }
    }

    fn get_tasks_for_assignee(&self, assignee: &str) -> Vec<Uuid> {
        self.assignments
            .get(assignee)
            .cloned()
            .unwrap_or_default()
    }

    fn assignee_count(&self) -> usize {
        self.assignments.len()
    }
}

#[async_trait]
impl Projection for TasksByAssigneeProjection {
    fn name(&self) -> &str {
        "tasks_by_assignee"
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        let event_data = serde_json::from_value::<TaskEvent>(event.event_data.clone())
            .map_err(|e| event_sauce_core::Error::custom(format!("Failed to deserialize: {}", e)))?;

        match event_data {
            TaskEvent::Created {
                task_id, assignee, ..
            } => {
                self.assignments
                    .entry(assignee)
                    .or_insert_with(Vec::new)
                    .push(task_id);
            }
            TaskEvent::Assigned {
                task_id,
                from_assignee,
                to_assignee,
            } => {
                // Remove from old assignee
                if let Some(tasks) = self.assignments.get_mut(&from_assignee) {
                    tasks.retain(|&id| id != task_id);
                }
                // Add to new assignee
                self.assignments
                    .entry(to_assignee)
                    .or_insert_with(Vec::new)
                    .push(task_id);
            }
            _ => {
                // Other events don't affect assignments
            }
        }

        Ok(())
    }
}

/// Projection 3: Completed Tasks Counter
///
/// Simple projection that counts completed tasks.
#[derive(Debug)]
struct CompletedTasksProjection {
    count: u64,
    last_completed_task_id: Option<Uuid>,
}

impl CompletedTasksProjection {
    fn new() -> Self {
        Self {
            count: 0,
            last_completed_task_id: None,
        }
    }

    fn get_count(&self) -> u64 {
        self.count
    }
}

#[async_trait]
impl Projection for CompletedTasksProjection {
    fn name(&self) -> &str {
        "completed_tasks_counter"
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        // Only process TaskCompleted events
        if event.event_type == "TaskCompleted" {
            let event_data = serde_json::from_value::<TaskEvent>(event.event_data.clone())
                .map_err(|e| event_sauce_core::Error::custom(format!("Failed to deserialize: {}", e)))?;

            if let TaskEvent::Completed { task_id } = event_data {
                self.count += 1;
                self.last_completed_task_id = Some(task_id);
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Task Management with Projections ===\n");

    // Create event bus
    let bus = InMemoryEventBus::new();
    println!("✓ Created event bus");

    // Create checkpoint store
    let checkpoint_store = InMemoryCheckpointStore::new();
    println!("✓ Created checkpoint store\n");

    // Create projections
    println!("--- Building Projections ---");

    let status_projection = TaskCountByStatusProjection::new();
    let assignee_projection = TasksByAssigneeProjection::new();
    let completed_projection = CompletedTasksProjection::new();

    println!("✓ Created 3 projections");

    // Create runners
    let mut status_runner = ProjectionRunner::new(status_projection);
    let mut assignee_runner = ProjectionRunner::new(assignee_projection);
    let mut completed_runner = ProjectionRunner::new(completed_projection);

    // Subscribe to events (all projections get all events)
    let status_stream = bus.subscribe(EventFilter::all()).await?;
    let assignee_stream = bus.subscribe(EventFilter::all()).await?;
    let completed_stream = bus
        .subscribe(EventFilter::by_event_type("TaskCompleted"))
        .await?;

    println!("✓ Subscribed projections to event streams\n");

    // Publish a series of task events
    println!("--- Publishing Task Events ---");

    let task1_id = Uuid::new_v4();
    let task2_id = Uuid::new_v4();
    let task3_id = Uuid::new_v4();

    // Task 1: Created by Alice
    let event = create_event(
        TaskEvent::Created {
            task_id: task1_id,
            title: "Implement authentication".to_string(),
            assignee: "alice".to_string(),
        },
        task1_id,
        1,
    );
    bus.publish(event).await?;
    println!("✓ Task 1 created (Alice): 'Implement authentication'");

    // Task 2: Created by Bob
    let event = create_event(
        TaskEvent::Created {
            task_id: task2_id,
            title: "Write documentation".to_string(),
            assignee: "bob".to_string(),
        },
        task2_id,
        1,
    );
    bus.publish(event).await?;
    println!("✓ Task 2 created (Bob): 'Write documentation'");

    // Task 3: Created by Alice
    let event = create_event(
        TaskEvent::Created {
            task_id: task3_id,
            title: "Fix bug #123".to_string(),
            assignee: "alice".to_string(),
        },
        task3_id,
        1,
    );
    bus.publish(event).await?;
    println!("✓ Task 3 created (Alice): 'Fix bug #123'");

    // Task 1: Status changed to InProgress
    let event = create_event(
        TaskEvent::StatusChanged {
            task_id: task1_id,
            old_status: TaskStatus::Todo,
            new_status: TaskStatus::InProgress,
        },
        task1_id,
        2,
    );
    bus.publish(event).await?;
    println!("✓ Task 1 moved to In Progress");

    // Task 2: Assigned from Bob to Charlie
    let event = create_event(
        TaskEvent::Assigned {
            task_id: task2_id,
            from_assignee: "bob".to_string(),
            to_assignee: "charlie".to_string(),
        },
        task2_id,
        2,
    );
    bus.publish(event).await?;
    println!("✓ Task 2 reassigned from Bob to Charlie");

    // Task 1: Completed
    let event = create_event(
        TaskEvent::StatusChanged {
            task_id: task1_id,
            old_status: TaskStatus::InProgress,
            new_status: TaskStatus::Completed,
        },
        task1_id,
        3,
    );
    bus.publish(event).await?;
    println!("✓ Task 1 moved to Completed");

    let event = create_event(TaskEvent::Completed { task_id: task1_id }, task1_id, 4);
    bus.publish(event).await?;
    println!("✓ Task 1 completed");

    // Task 3: Status changed to InProgress
    let event = create_event(
        TaskEvent::StatusChanged {
            task_id: task3_id,
            old_status: TaskStatus::Todo,
            new_status: TaskStatus::InProgress,
        },
        task3_id,
        2,
    );
    bus.publish(event).await?;
    println!("✓ Task 3 moved to In Progress\n");

    println!("--- Processing Events (with Checkpointing) ---");

    // Process events with checkpoint tracking
    let mut event_count = 0;
    status_runner
        .run_with_callback(status_stream.take(8), {
            let checkpoint_store = checkpoint_store.clone();
            move |event| {
                let checkpoint_store = checkpoint_store.clone();
                let event_id = event.id;
                event_count += 1;
                let seq = event_count;
                async move {
                    let checkpoint = Checkpoint::new("task_count_by_status", event_id, seq);
                    checkpoint_store
                        .save("task_count_by_status", checkpoint)
                        .await?;
                    Ok(())
                }
            }
        })
        .await?;
    println!("✓ Status projection processed (with checkpoints)");

    assignee_runner
        .run_with_callback(assignee_stream.take(8), {
            let checkpoint_store = checkpoint_store.clone();
            move |event| {
                let checkpoint_store = checkpoint_store.clone();
                let event_id = event.id;
                let seq = i64::from(event.event_version.as_i32());
                async move {
                    let checkpoint = Checkpoint::new("tasks_by_assignee", event_id, seq);
                    checkpoint_store.save("tasks_by_assignee", checkpoint).await?;
                    Ok(())
                }
            }
        })
        .await?;
    println!("✓ Assignee projection processed (with checkpoints)");

    // Only 1 TaskCompleted event was published
    completed_runner.run(completed_stream.take(1)).await?;
    println!("✓ Completed tasks projection processed\n");

    // Display results
    println!("=== Projection Results ===\n");

    // Status counts
    println!("📊 Task Count by Status:");
    println!(
        "  - Todo: {}",
        status_runner.projection().get_count(&TaskStatus::Todo)
    );
    println!(
        "  - In Progress: {}",
        status_runner
            .projection()
            .get_count(&TaskStatus::InProgress)
    );
    println!(
        "  - Completed: {}",
        status_runner
            .projection()
            .get_count(&TaskStatus::Completed)
    );
    println!(
        "  - Total: {}\n",
        status_runner.projection().total_tasks()
    );

    // Tasks by assignee
    println!("👥 Tasks by Assignee:");
    let alice_tasks = assignee_runner
        .projection()
        .get_tasks_for_assignee("alice");
    let bob_tasks = assignee_runner
        .projection()
        .get_tasks_for_assignee("bob");
    let charlie_tasks = assignee_runner
        .projection()
        .get_tasks_for_assignee("charlie");

    println!("  - Alice: {} task(s)", alice_tasks.len());
    println!("  - Bob: {} task(s)", bob_tasks.len());
    println!("  - Charlie: {} task(s)", charlie_tasks.len());
    println!(
        "  - Total assignees: {}\n",
        assignee_runner.projection().assignee_count()
    );

    // Completed tasks
    println!(
        "✅ Completed Tasks: {}\n",
        completed_runner.projection().get_count()
    );

    // Show checkpoint status
    println!("=== Checkpoint Status ===\n");

    if let Some(checkpoint) = checkpoint_store
        .load("task_count_by_status")
        .await?
    {
        println!("✓ Status projection checkpoint:");
        println!("  - Sequence: {}", checkpoint.sequence());
        println!("  - Timestamp: {}", checkpoint.timestamp());
    }

    if let Some(checkpoint) = checkpoint_store.load("tasks_by_assignee").await? {
        println!("✓ Assignee projection checkpoint:");
        println!("  - Sequence: {}", checkpoint.sequence());
        println!("  - Timestamp: {}", checkpoint.timestamp());
    }

    println!("\n=== Example Complete ===");
    println!("\nKey Takeaways:");
    println!("1. Multiple projections can process the same events");
    println!("2. Each projection builds its own optimized read model");
    println!("3. Checkpoints enable resuming projections after restart");
    println!("4. Event filtering reduces processing for specialized projections");

    Ok(())
}
