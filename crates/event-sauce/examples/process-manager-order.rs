//! Order Fulfillment Process Manager Example (Orchestration Pattern)
//!
//! This example demonstrates the process manager pattern for orchestrating
//! a complex multi-step order fulfillment workflow.
//!
//! Run with: cargo run -p event-sauce --example process-manager-order --features "memory,sagas"

use async_trait::async_trait;
use event_sauce::event_sauce_memory::InMemoryEventBus;
use event_sauce::event_sauce_sagas;
use event_sauce::{EventBus, EventEnvelope, Version};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// Re-export saga types for convenience
use event_sauce_sagas::{Command, CommandExecutor, ProcessManager, ProcessRunner, Result};

// ============================================================================
// Commands - Actions the process manager can issue
// ============================================================================

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum FulfillmentCommand {
    ProcessPayment { order_id: String, amount: f64 },
    ReserveInventory { order_id: String, items: Vec<String> },
    CreateShipment { order_id: String, address: String },
    SendConfirmation { order_id: String, email: String },
    CancelOrder { order_id: String, reason: String },
}

impl Command for FulfillmentCommand {}

// ============================================================================
// Process Manager - Orchestrates the order fulfillment workflow
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum FulfillmentState {
    Created,
    PaymentRequested,
    InventoryReserving,
    ShipmentCreating,
    ConfirmationSending,
    Completed,
    Failed,
}

struct OrderFulfillmentProcess {
    order_id: String,
    state: FulfillmentState,
    amount: f64,
    items: Vec<String>,
    email: String,
    address: String,
    completed: bool,
    failed: bool,
}

impl OrderFulfillmentProcess {
    fn new(order_id: String) -> Self {
        Self {
            order_id,
            state: FulfillmentState::Created,
            amount: 0.0,
            items: Vec::new(),
            email: String::new(),
            address: String::new(),
            completed: false,
            failed: false,
        }
    }
}

#[async_trait]
impl ProcessManager for OrderFulfillmentProcess {
    type Command = FulfillmentCommand;

    async fn handle_event(&mut self, event: &EventEnvelope) -> Result<Vec<Self::Command>> {
        println!(
            "🔄 Process Manager [{}] handling event: {}",
            self.current_state(),
            event.event_type
        );

        match event.event_type.as_str() {
            "OrderPlaced" => {
                self.state = FulfillmentState::PaymentRequested;
                self.amount = event
                    .event_data
                    .get("total")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                self.email = event
                    .event_data
                    .get("email")
                    .and_then(|v| v.as_str())
                    .unwrap_or("customer@example.com")
                    .to_string();
                self.address = event
                    .event_data
                    .get("address")
                    .and_then(|v| v.as_str())
                    .unwrap_or("123 Main St")
                    .to_string();

                println!("  → Requesting payment of ${:.2}", self.amount);

                Ok(vec![FulfillmentCommand::ProcessPayment {
                    order_id: self.order_id.clone(),
                    amount: self.amount,
                }])
            }

            "PaymentProcessed" => {
                self.state = FulfillmentState::InventoryReserving;
                self.items = vec!["Item1".to_string(), "Item2".to_string()]; // Simplified

                println!("  → Reserving inventory for {} items", self.items.len());

                Ok(vec![FulfillmentCommand::ReserveInventory {
                    order_id: self.order_id.clone(),
                    items: self.items.clone(),
                }])
            }

            "InventoryReserved" => {
                self.state = FulfillmentState::ShipmentCreating;

                println!("  → Creating shipment to {}", self.address);

                Ok(vec![FulfillmentCommand::CreateShipment {
                    order_id: self.order_id.clone(),
                    address: self.address.clone(),
                }])
            }

            "ShipmentCreated" => {
                self.state = FulfillmentState::ConfirmationSending;

                println!("  → Sending confirmation to {}", self.email);

                Ok(vec![FulfillmentCommand::SendConfirmation {
                    order_id: self.order_id.clone(),
                    email: self.email.clone(),
                }])
            }

            "ConfirmationSent" => {
                self.state = FulfillmentState::Completed;
                self.completed = true;

                println!("  ✓ Order fulfillment complete!");

                Ok(vec![])
            }

            "PaymentFailed" | "InventoryUnavailable" | "ShipmentFailed" => {
                self.state = FulfillmentState::Failed;
                self.failed = true;

                let reason = event
                    .event_data
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown error")
                    .to_string();

                println!("  ✗ Failure detected: {}", reason);
                println!("  → Cancelling order");

                Ok(vec![FulfillmentCommand::CancelOrder {
                    order_id: self.order_id.clone(),
                    reason,
                }])
            }

            _ => Ok(vec![]),
        }
    }

    fn is_complete(&self) -> bool {
        self.completed
    }

    fn is_failed(&self) -> bool {
        self.failed
    }

    fn current_state(&self) -> String {
        format!("{:?}", self.state)
    }

    fn name(&self) -> &str {
        "OrderFulfillmentProcess"
    }
}

// ============================================================================
// Command Executor - Executes commands issued by the process manager
// ============================================================================

struct FulfillmentCommandExecutor {
    commands_log: Arc<Mutex<Vec<String>>>,
    event_bus: Arc<InMemoryEventBus>,
}

impl FulfillmentCommandExecutor {
    fn new(event_bus: Arc<InMemoryEventBus>) -> Self {
        Self {
            commands_log: Arc::new(Mutex::new(Vec::new())),
            event_bus,
        }
    }

    fn log(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.commands_log)
    }
}

#[async_trait]
impl CommandExecutor<FulfillmentCommand> for FulfillmentCommandExecutor {
    async fn execute(&self, command: FulfillmentCommand) -> Result<()> {
        let cmd_str = format!("{:?}", command);
        self.commands_log.lock().await.push(cmd_str.clone());

        println!("📤 Executing command: {}", cmd_str);

        // Simulate command execution and publish resulting events
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let event = match command {
            FulfillmentCommand::ProcessPayment { .. } => Some(create_event(
                "PaymentProcessed",
                json!({"status": "success"}),
            )),
            FulfillmentCommand::ReserveInventory { .. } => {
                Some(create_event("InventoryReserved", json!({"status": "reserved"})))
            }
            FulfillmentCommand::CreateShipment { .. } => Some(create_event(
                "ShipmentCreated",
                json!({"tracking": "TRACK123"}),
            )),
            FulfillmentCommand::SendConfirmation { .. } => {
                Some(create_event("ConfirmationSent", json!({"sent": true})))
            }
            FulfillmentCommand::CancelOrder { .. } => {
                Some(create_event("OrderCancelled", json!({"cancelled": true})))
            }
        };

        if let Some(evt) = event {
            self.event_bus.publish(evt).await.map_err(|e| {
                event_sauce_sagas::Error::CommandFailed(format!("Failed to publish event: {}", e))
            })?;
        }

        Ok(())
    }
}

// ============================================================================
// Helper functions
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
    println!("🎯 Order Fulfillment Process Manager (Orchestration Pattern)\n");
    println!("This demonstrates a process manager orchestrating a complex");
    println!("multi-step workflow with explicit state management.\n");
    println!("{}", "=".repeat(60));
    println!();

    // Create event bus
    let event_bus = Arc::new(InMemoryEventBus::new());

    // Create process manager
    let process = OrderFulfillmentProcess::new("order-123".to_string());

    // Create command executor
    let executor = Arc::new(FulfillmentCommandExecutor::new(Arc::clone(&event_bus)));
    let commands_log = executor.log();

    // Create process runner
    let mut runner = ProcessRunner::new(process, Arc::clone(&event_bus), executor);

    println!("📢 Starting order fulfillment process...\n");

    // Publish initial event
    let order_placed = create_event(
        "OrderPlaced",
        json!({
            "order_id": "order-123",
            "total": 199.99,
            "email": "customer@example.com",
            "address": "123 Main St, City, ST 12345"
        }),
    );

    event_bus.publish(order_placed).await?;

    // Run process
    runner.run().await?;

    // Print results
    println!();
    println!("{}", "=".repeat(60));
    println!("\n📊 Process Execution Summary:\n");

    let commands = commands_log.lock().await;
    println!("Commands executed: {}", commands.len());
    for (i, cmd) in commands.iter().enumerate() {
        println!("  {}. {}", i + 1, cmd);
    }

    println!();
    println!("✅ Process manager demonstration complete!");
    println!("\nThe process manager successfully orchestrated:");
    println!("  • Payment processing");
    println!("  • Inventory reservation");
    println!("  • Shipment creation");
    println!("  • Customer notification");

    Ok(())
}
