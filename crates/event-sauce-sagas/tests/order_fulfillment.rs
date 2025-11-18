//! Integration test demonstrating order fulfillment with saga and process manager patterns

use async_trait::async_trait;
use event_sauce_core::{EventBus, EventEnvelope, EventFilter, Version};
use event_sauce_sagas::{
    Command, CommandExecutor, ProcessManager, ProcessRunner, Result, Saga, SagaRunner,
};
use futures::{stream, Stream};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// ============================================================================
// Domain Events
// ============================================================================

fn create_event(event_type: &str, aggregate_type: &str, data: serde_json::Value) -> EventEnvelope {
    EventEnvelope::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        aggregate_type.to_string(),
        event_type.to_string(),
        Version::new(1),
        data,
    )
}

// ============================================================================
// Saga Pattern Test: Payment Processing
// ============================================================================

struct PaymentSaga {
    processed_orders: Arc<Mutex<Vec<String>>>,
    failed_orders: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Saga for PaymentSaga {
    fn interested_in(&self, event: &EventEnvelope) -> bool {
        matches!(event.event_type.as_str(), "OrderPlaced" | "OrderCancelled")
    }

    async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
        match event.event_type.as_str() {
            "OrderPlaced" => {
                let order_id = event
                    .event_data
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");

                // Simulate payment processing
                self.processed_orders
                    .lock()
                    .await
                    .push(order_id.to_string());
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn compensate(&mut self, event: &EventEnvelope) -> Result<()> {
        let order_id = event
            .event_data
            .get("order_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Simulate refund
        self.failed_orders.lock().await.push(order_id.to_string());
        Ok(())
    }
}

// ============================================================================
// Process Manager Pattern Test: Order Fulfillment
// ============================================================================

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum FulfillmentCommand {
    ProcessPayment { order_id: String, amount: f64 },
    CreateShipment { order_id: String },
    SendConfirmation { order_id: String },
    CancelOrder { order_id: String },
}

impl Command for FulfillmentCommand {}

#[derive(Debug)]
enum FulfillmentState {
    Created,
    PaymentRequested,
    PaymentCompleted,
    ShipmentCreated,
    Completed,
    Failed,
}

struct OrderFulfillmentProcess {
    order_id: String,
    state: FulfillmentState,
    completed: bool,
    failed: bool,
}

impl OrderFulfillmentProcess {
    fn new(order_id: String) -> Self {
        Self {
            order_id,
            state: FulfillmentState::Created,
            completed: false,
            failed: false,
        }
    }
}

#[async_trait]
impl ProcessManager for OrderFulfillmentProcess {
    type Command = FulfillmentCommand;

    async fn handle_event(&mut self, event: &EventEnvelope) -> Result<Vec<Self::Command>> {
        match event.event_type.as_str() {
            "OrderPlaced" => {
                self.state = FulfillmentState::PaymentRequested;
                let amount = event
                    .event_data
                    .get("total")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);

                Ok(vec![FulfillmentCommand::ProcessPayment {
                    order_id: self.order_id.clone(),
                    amount,
                }])
            }
            "PaymentProcessed" => {
                self.state = FulfillmentState::PaymentCompleted;
                Ok(vec![FulfillmentCommand::CreateShipment {
                    order_id: self.order_id.clone(),
                }])
            }
            "ShipmentCreated" => {
                self.state = FulfillmentState::ShipmentCreated;
                Ok(vec![FulfillmentCommand::SendConfirmation {
                    order_id: self.order_id.clone(),
                }])
            }
            "ConfirmationSent" => {
                self.state = FulfillmentState::Completed;
                self.completed = true;
                Ok(vec![])
            }
            "PaymentFailed" | "ShipmentFailed" => {
                self.state = FulfillmentState::Failed;
                self.failed = true;
                Ok(vec![FulfillmentCommand::CancelOrder {
                    order_id: self.order_id.clone(),
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
}

// ============================================================================
// Test Infrastructure
// ============================================================================

struct TestEventBus {
    events: Arc<Mutex<Vec<EventEnvelope>>>,
}

impl TestEventBus {
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn with_events(events: Vec<EventEnvelope>) -> Self {
        Self {
            events: Arc::new(Mutex::new(events)),
        }
    }
}

#[async_trait]
impl EventBus for TestEventBus {
    async fn publish(&self, event: EventEnvelope) -> event_sauce_core::Result<()> {
        self.events.lock().await.push(event);
        Ok(())
    }

    async fn subscribe(
        &self,
        _filter: EventFilter,
    ) -> event_sauce_core::Result<impl Stream<Item = EventEnvelope> + Send> {
        let events = self.events.lock().await.clone();
        Ok(stream::iter(events))
    }
}

struct TestCommandExecutor {
    commands_executed: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl CommandExecutor<FulfillmentCommand> for TestCommandExecutor {
    async fn execute(&self, command: FulfillmentCommand) -> Result<()> {
        let cmd_str = format!("{:?}", command);
        self.commands_executed.lock().await.push(cmd_str);
        Ok(())
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[tokio::test]
async fn test_saga_payment_processing() {
    let processed = Arc::new(Mutex::new(Vec::new()));
    let failed = Arc::new(Mutex::new(Vec::new()));

    let saga = PaymentSaga {
        processed_orders: Arc::clone(&processed),
        failed_orders: Arc::clone(&failed),
    };

    let events = vec![
        create_event(
            "OrderPlaced",
            "Order",
            json!({"order_id": "order-123", "total": 99.99}),
        ),
        create_event(
            "OrderPlaced",
            "Order",
            json!({"order_id": "order-456", "total": 149.99}),
        ),
    ];

    let event_bus = Arc::new(TestEventBus::with_events(events));
    let mut runner = SagaRunner::new(saga, event_bus);
    runner.run().await.unwrap();

    let processed_list = processed.lock().await;
    assert_eq!(processed_list.len(), 2);
    assert!(processed_list.contains(&"order-123".to_string()));
    assert!(processed_list.contains(&"order-456".to_string()));
}

#[tokio::test]
async fn test_process_manager_order_fulfillment() {
    let process = OrderFulfillmentProcess::new("order-789".to_string());

    let events = vec![
        create_event(
            "OrderPlaced",
            "Order",
            json!({"order_id": "order-789", "total": 199.99}),
        ),
        create_event("PaymentProcessed", "Payment", json!({})),
        create_event("ShipmentCreated", "Shipment", json!({})),
        create_event("ConfirmationSent", "Notification", json!({})),
    ];

    let event_bus = Arc::new(TestEventBus::with_events(events));
    let commands_executed = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(TestCommandExecutor {
        commands_executed: Arc::clone(&commands_executed),
    });

    let mut runner = ProcessRunner::new(process, event_bus, executor);
    runner.run().await.unwrap();

    let commands = commands_executed.lock().await;
    assert_eq!(commands.len(), 3);
    assert!(commands[0].contains("ProcessPayment"));
    assert!(commands[1].contains("CreateShipment"));
    assert!(commands[2].contains("SendConfirmation"));
}

#[tokio::test]
async fn test_process_manager_handles_payment_failure() {
    let process = OrderFulfillmentProcess::new("order-999".to_string());

    let events = vec![
        create_event(
            "OrderPlaced",
            "Order",
            json!({"order_id": "order-999", "total": 299.99}),
        ),
        create_event(
            "PaymentFailed",
            "Payment",
            json!({"reason": "insufficient funds"}),
        ),
    ];

    let event_bus = Arc::new(TestEventBus::with_events(events));
    let commands_executed = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(TestCommandExecutor {
        commands_executed: Arc::clone(&commands_executed),
    });

    let mut runner = ProcessRunner::new(process, event_bus, executor);
    let result = runner.run().await;

    // Process should fail
    assert!(result.is_err());

    // But should have executed cancel command
    let commands = commands_executed.lock().await;
    assert_eq!(commands.len(), 2);
    assert!(commands[0].contains("ProcessPayment"));
    assert!(commands[1].contains("CancelOrder"));
}

#[tokio::test]
async fn test_saga_compensation() {
    let processed = Arc::new(Mutex::new(Vec::new()));
    let failed = Arc::new(Mutex::new(Vec::new()));

    // Saga that fails on second event
    struct FailingSaga {
        processed_orders: Arc<Mutex<Vec<String>>>,
        failed_orders: Arc<Mutex<Vec<String>>>,
        fail_count: usize,
    }

    #[async_trait]
    impl Saga for FailingSaga {
        fn interested_in(&self, event: &EventEnvelope) -> bool {
            event.event_type == "OrderPlaced"
        }

        async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
            if self.fail_count > 0 {
                self.fail_count -= 1;
                return Err(event_sauce_sagas::Error::ExecutionFailed(
                    "Payment declined".into(),
                ));
            }

            let order_id = event
                .event_data
                .get("order_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            self.processed_orders
                .lock()
                .await
                .push(order_id.to_string());
            Ok(())
        }

        async fn compensate(&mut self, event: &EventEnvelope) -> Result<()> {
            let order_id = event
                .event_data
                .get("order_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            self.failed_orders.lock().await.push(order_id.to_string());
            Ok(())
        }
    }

    let saga = FailingSaga {
        processed_orders: Arc::clone(&processed),
        failed_orders: Arc::clone(&failed),
        fail_count: 1, // Fail on first event
    };

    let events = vec![create_event(
        "OrderPlaced",
        "Order",
        json!({"order_id": "order-fail", "total": 99.99}),
    )];

    let event_bus = Arc::new(TestEventBus::with_events(events));
    let mut runner = SagaRunner::new(saga, event_bus);
    let result = runner.run().await;

    // Should fail but compensate
    assert!(result.is_err());

    let failed_list = failed.lock().await;
    assert_eq!(failed_list.len(), 1);
    assert!(failed_list.contains(&"order-fail".to_string()));
}
