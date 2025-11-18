//! Payment Saga Example (Choreography Pattern)
//!
//! This example demonstrates the saga pattern for handling payment processing
//! across multiple aggregates with compensation logic.
//!
//! Run with: cargo run -p event-sauce --example saga-payment --features "memory,sagas"

use async_trait::async_trait;
use event_sauce::event_sauce_memory::InMemoryEventBus;
use event_sauce::event_sauce_sagas;
use event_sauce::{EventBus, EventEnvelope, Version};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// Re-export saga types for convenience
use event_sauce_sagas::{Result, Saga, SagaRunner};

// ============================================================================
// Payment Saga - Reacts to order and payment events
// ============================================================================

struct PaymentSaga {
    payments_processed: Arc<Mutex<Vec<PaymentInfo>>>,
    refunds_issued: Arc<Mutex<Vec<String>>>,
}

#[derive(Debug, Clone)]
struct PaymentInfo {
    order_id: String,
    amount: f64,
}

impl PaymentSaga {
    fn new() -> Self {
        Self {
            payments_processed: Arc::new(Mutex::new(Vec::new())),
            refunds_issued: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn payments(&self) -> Arc<Mutex<Vec<PaymentInfo>>> {
        Arc::clone(&self.payments_processed)
    }

    fn refunds(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.refunds_issued)
    }
}

#[async_trait]
impl Saga for PaymentSaga {
    fn interested_in(&self, event: &EventEnvelope) -> bool {
        matches!(
            event.event_type.as_str(),
            "OrderPlaced" | "OrderCancelled" | "ShipmentFailed"
        )
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        match event.event_type.as_str() {
            "OrderPlaced" => {
                let order_id = event
                    .event_data
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                let amount = event
                    .event_data
                    .get("total")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);

                println!(
                    "💳 Processing payment for order {} (${:.2})",
                    order_id, amount
                );

                // Simulate payment processing
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                self.payments_processed
                    .lock()
                    .await
                    .push(PaymentInfo { order_id, amount });

                println!("✓ Payment processed successfully");
                Ok(())
            }
            "OrderCancelled" | "ShipmentFailed" => {
                let order_id = event
                    .event_data
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                println!(
                    "⚠️  Order {} cancelled/failed - initiating refund",
                    order_id
                );
                self.compensate(event).await
            }
            _ => Ok(()),
        }
    }

    async fn compensate(&mut self, event: &EventEnvelope) -> Result<()> {
        let order_id = event
            .event_data
            .get("order_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        println!("💸 Issuing refund for order {}", order_id);

        // Simulate refund processing
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        self.refunds_issued.lock().await.push(order_id.clone());

        println!("✓ Refund issued for order {}", order_id);
        Ok(())
    }

    fn name(&self) -> &str {
        "PaymentSaga"
    }
}

// ============================================================================
// Helper function to create events
// ============================================================================

fn create_event(event_type: &str, data: serde_json::Value) -> EventEnvelope {
    EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        "Order".to_string(),
        event_type.to_string(),
        Version::new(1),
        data,
    )
}

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!("🎯 Payment Saga Example (Choreography Pattern)\n");
    println!("This demonstrates a saga that reacts to order events");
    println!("and compensates when things go wrong.\n");
    println!("{}", "=".repeat(60));
    println!();

    // Create saga
    let saga = PaymentSaga::new();
    let payments = saga.payments();
    let refunds = saga.refunds();

    // Create event bus with pre-loaded events
    let event_bus = Arc::new(InMemoryEventBus::new());

    // Publish events in sequence
    let events = vec![
        create_event(
            "OrderPlaced",
            json!({"order_id": "order-123", "total": 99.99}),
        ),
        create_event(
            "OrderPlaced",
            json!({"order_id": "order-456", "total": 149.99}),
        ),
        create_event(
            "OrderCancelled",
            json!({"order_id": "order-456", "reason": "customer request"}),
        ),
        create_event(
            "OrderPlaced",
            json!({"order_id": "order-789", "total": 249.99}),
        ),
        create_event(
            "ShipmentFailed",
            json!({"order_id": "order-789", "reason": "out of stock"}),
        ),
    ];

    for event in events {
        event_bus.publish(event).await?;
    }

    // Run saga
    let mut runner = SagaRunner::new(saga, event_bus);

    println!("📢 Starting saga...\n");

    runner.run().await?;

    // Print results
    println!();
    println!("{}", "=".repeat(60));
    println!("\n📊 Results:\n");

    let payments_list = payments.lock().await;
    println!("Payments processed: {}", payments_list.len());
    for payment in payments_list.iter() {
        println!("  • Order {}: ${:.2}", payment.order_id, payment.amount);
    }

    println!();
    let refunds_list = refunds.lock().await;
    println!("Refunds issued: {}", refunds_list.len());
    for refund in refunds_list.iter() {
        println!("  • Order {}", refund);
    }

    println!();
    println!("✅ Saga demonstration complete!");

    Ok(())
}
