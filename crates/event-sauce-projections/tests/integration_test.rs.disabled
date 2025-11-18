//! Integration tests for projections with event bus.

use async_trait::async_trait;
use event_sauce_core::{EventBus, EventEnvelope, EventFilter, Result, Version};
use event_sauce_memory::InMemoryEventBus;
use event_sauce_projections::{
    Checkpoint, CheckpointStore, InMemoryCheckpointStore, Projection, ProjectionRunner,
};
use futures::StreamExt;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

fn create_test_envelope(event_type: &str, aggregate_type: &str) -> EventEnvelope {
    EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        aggregate_type.to_string(),
        event_type.to_string(),
        Version::new(1),
        json!({"test": "data"}),
    )
}

/// Test projection that counts specific event types
struct EventCountProjection {
    name: String,
    event_type_filter: String,
    count: Arc<Mutex<u64>>,
}

impl EventCountProjection {
    fn new(
        name: impl Into<String>,
        event_type_filter: impl Into<String>,
        count: Arc<Mutex<u64>>,
    ) -> Self {
        Self {
            name: name.into(),
            event_type_filter: event_type_filter.into(),
            count,
        }
    }
}

#[async_trait]
impl Projection for EventCountProjection {
    fn name(&self) -> &str {
        &self.name
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        if event.event_type == self.event_type_filter {
            let mut count = self.count.lock().await;
            *count += 1;
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_projection_with_event_bus() {
    // Create event bus
    let bus = InMemoryEventBus::new();

    // Create projection
    let count = Arc::new(Mutex::new(0));
    let projection = EventCountProjection::new("user_counter", "UserCreated", Arc::clone(&count));
    let mut runner = ProjectionRunner::new(projection);

    // Subscribe to events
    let subscription = bus.subscribe(EventFilter::all()).await.unwrap();

    // Publish events
    bus.publish(create_test_envelope("UserCreated", "User"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("UserUpdated", "User"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("UserCreated", "User"))
        .await
        .unwrap();

    // Process events (take first 3 then stop)
    let limited_stream = subscription.take(3);
    runner.run(limited_stream).await.unwrap();

    // Verify count
    assert_eq!(*count.lock().await, 2); // Only counted "UserCreated" events
}

#[tokio::test]
async fn test_projection_with_filtering() {
    let bus = InMemoryEventBus::new();

    let count = Arc::new(Mutex::new(0));
    let projection = EventCountProjection::new("order_counter", "OrderPlaced", Arc::clone(&count));
    let mut runner = ProjectionRunner::new(projection);

    // Subscribe with filter
    let subscription = bus
        .subscribe(EventFilter::by_aggregate_type("Order"))
        .await
        .unwrap();

    // Publish mixed events
    bus.publish(create_test_envelope("UserCreated", "User"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("OrderPlaced", "Order"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("OrderShipped", "Order"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("OrderPlaced", "Order"))
        .await
        .unwrap();

    // Process only Order events (should receive 3)
    let limited_stream = subscription.take(3);
    runner.run(limited_stream).await.unwrap();

    // Should only count "OrderPlaced" events
    assert_eq!(*count.lock().await, 2);
}

#[tokio::test]
async fn test_projection_with_checkpointing() {
    let bus = InMemoryEventBus::new();
    let checkpoint_store = InMemoryCheckpointStore::new();

    // First run: process some events
    {
        let count = Arc::new(Mutex::new(0));
        let projection =
            EventCountProjection::new("checkpointed", "UserCreated", Arc::clone(&count));
        let mut runner = ProjectionRunner::new(projection);

        let subscription = bus.subscribe(EventFilter::all()).await.unwrap();

        // Publish and process events
        bus.publish(create_test_envelope("UserCreated", "User"))
            .await
            .unwrap();
        bus.publish(create_test_envelope("UserCreated", "User"))
            .await
            .unwrap();

        let event_id = Uuid::new_v4();
        let limited_stream = subscription.take(2);
        runner
            .run_with_callback(limited_stream, {
                let checkpoint_store = checkpoint_store.clone();
                move |_event| {
                    let checkpoint_store = checkpoint_store.clone();
                    let event_id = event_id;
                    async move {
                        let checkpoint = Checkpoint::new("checkpointed", event_id, 2);
                        checkpoint_store.save("checkpointed", checkpoint).await
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(*count.lock().await, 2);
    }

    // Verify checkpoint was saved
    let checkpoint = checkpoint_store.load("checkpointed").await.unwrap();
    assert!(checkpoint.is_some());
    assert_eq!(checkpoint.unwrap().sequence(), 2);
}

#[tokio::test]
async fn test_multiple_projections_same_bus() {
    let bus = InMemoryEventBus::new();

    // Create two different projections
    let user_count = Arc::new(Mutex::new(0));
    let order_count = Arc::new(Mutex::new(0));

    let user_projection =
        EventCountProjection::new("user_counter", "UserCreated", Arc::clone(&user_count));
    let order_projection =
        EventCountProjection::new("order_counter", "OrderPlaced", Arc::clone(&order_count));

    let mut user_runner = ProjectionRunner::new(user_projection);
    let mut order_runner = ProjectionRunner::new(order_projection);

    // Subscribe both projections first
    let user_sub = bus.subscribe(EventFilter::all()).await.unwrap();
    let order_sub = bus.subscribe(EventFilter::all()).await.unwrap();

    // Publish events
    bus.publish(create_test_envelope("UserCreated", "User"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("OrderPlaced", "Order"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("UserCreated", "User"))
        .await
        .unwrap();

    // Process events sequentially to avoid lifetime issues
    let limited_stream = user_sub.take(3);
    user_runner.run(limited_stream).await.unwrap();

    let limited_stream = order_sub.take(3);
    order_runner.run(limited_stream).await.unwrap();

    // Verify both projections counted their events
    assert_eq!(*user_count.lock().await, 2);
    assert_eq!(*order_count.lock().await, 1);
}

#[tokio::test]
async fn test_projection_resume_from_checkpoint() {
    let checkpoint_store = InMemoryCheckpointStore::new();

    // Simulate existing checkpoint
    let last_event_id = Uuid::new_v4();
    let checkpoint = Checkpoint::new("resumable", last_event_id, 5);
    checkpoint_store
        .save("resumable", checkpoint)
        .await
        .unwrap();

    // Load checkpoint
    let loaded = checkpoint_store.load("resumable").await.unwrap();
    assert!(loaded.is_some());

    let checkpoint = loaded.unwrap();
    assert_eq!(checkpoint.projection_name(), "resumable");
    assert_eq!(checkpoint.sequence(), 5);

    // In a real scenario, you would use this to resume from sequence > 5
    // For this test, we just verify the checkpoint exists
}

/// Projection that tracks multiple aggregate types
struct MultiAggregateProjection {
    name: String,
    user_count: u64,
    order_count: u64,
}

impl MultiAggregateProjection {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            user_count: 0,
            order_count: 0,
        }
    }

    fn user_count(&self) -> u64 {
        self.user_count
    }

    fn order_count(&self) -> u64 {
        self.order_count
    }
}

#[async_trait]
impl Projection for MultiAggregateProjection {
    fn name(&self) -> &str {
        &self.name
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        match event.aggregate_type.as_str() {
            "User" => self.user_count += 1,
            "Order" => self.order_count += 1,
            _ => {}
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_projection_tracks_multiple_aggregates() {
    let bus = InMemoryEventBus::new();
    let projection = MultiAggregateProjection::new("multi_tracker");
    let mut runner = ProjectionRunner::new(projection);

    let subscription = bus.subscribe(EventFilter::all()).await.unwrap();

    // Publish events for different aggregates
    bus.publish(create_test_envelope("UserCreated", "User"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("OrderPlaced", "Order"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("UserUpdated", "User"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("OrderShipped", "Order"))
        .await
        .unwrap();
    bus.publish(create_test_envelope("UserDeleted", "User"))
        .await
        .unwrap();

    let limited_stream = subscription.take(5);
    runner.run(limited_stream).await.unwrap();

    assert_eq!(runner.projection().user_count(), 3);
    assert_eq!(runner.projection().order_count(), 2);
}

#[tokio::test]
async fn test_checkpoint_store_isolation() {
    let store = InMemoryCheckpointStore::new();

    // Save checkpoints for different projections
    let checkpoint1 = Checkpoint::new("proj1", Uuid::new_v4(), 100);
    let checkpoint2 = Checkpoint::new("proj2", Uuid::new_v4(), 200);
    let checkpoint3 = Checkpoint::new("proj3", Uuid::new_v4(), 300);

    store.save("proj1", checkpoint1).await.unwrap();
    store.save("proj2", checkpoint2).await.unwrap();
    store.save("proj3", checkpoint3).await.unwrap();

    // Verify each projection has its own checkpoint
    assert_eq!(store.load("proj1").await.unwrap().unwrap().sequence(), 100);
    assert_eq!(store.load("proj2").await.unwrap().unwrap().sequence(), 200);
    assert_eq!(store.load("proj3").await.unwrap().unwrap().sequence(), 300);

    // Delete one shouldn't affect others
    store.delete("proj2").await.unwrap();
    assert!(store.load("proj1").await.unwrap().is_some());
    assert!(store.load("proj2").await.unwrap().is_none());
    assert!(store.load("proj3").await.unwrap().is_some());
}
