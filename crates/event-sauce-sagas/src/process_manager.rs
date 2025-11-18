//! Process Manager pattern implementation for orchestration-based coordination
//!
//! Process managers represent centralized workflows with explicit state machines
//! that coordinate multiple aggregates.

use crate::error::Result;
use async_trait::async_trait;
use event_sauce_core::EventEnvelope;
use serde::{Deserialize, Serialize};

/// Command trait for process manager commands
///
/// Commands are actions that a process manager wants to execute
/// on aggregates or external systems.
pub trait Command: Send + Sync + std::fmt::Debug {}

/// Process Manager trait for implementing orchestration-based workflows
///
/// A process manager maintains explicit state and coordinates workflows
/// by issuing commands to aggregates and reacting to their events.
///
/// # Examples
///
/// ```
/// use event_sauce_sagas::{ProcessManager, Command, Result};
/// use event_sauce_core::{Aggregate, EventEnvelope};
/// use async_trait::async_trait;
///
/// #[derive(Debug)]
/// enum OrderCommand {
///     ProcessPayment,
///     CreateShipment,
/// }
///
/// impl Command for OrderCommand {}
///
/// struct OrderFulfillmentProcess {
///     // ... process state
/// }
///
/// #[async_trait]
/// impl ProcessManager for OrderFulfillmentProcess {
///     type Command = OrderCommand;
///
///     async fn handle_event(&mut self, event: &EventEnvelope) -> Result<Vec<Self::Command>> {
///         // React to event and return commands
///         Ok(vec![])
///     }
///
///     fn is_complete(&self) -> bool {
///         false
///     }
/// }
/// ```
#[async_trait]
pub trait ProcessManager: Send + Sync {
    /// The type of commands this process manager can issue
    type Command: Command;

    /// Handles an event and returns commands to execute
    ///
    /// This method is called when an event occurs that affects this process.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be processed or if state transition is invalid.
    async fn handle_event(&mut self, event: &EventEnvelope) -> Result<Vec<Self::Command>>;

    /// Returns true if the process has completed
    fn is_complete(&self) -> bool;

    /// Returns true if the process has failed
    fn is_failed(&self) -> bool {
        false
    }

    /// Returns the name of this process manager
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Returns the current state of the process for debugging
    fn current_state(&self) -> String {
        "Running".to_string()
    }
}

/// Process state tracking for persistence
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessState {
    /// Unique identifier for the process instance
    pub process_id: String,
    /// Current state name
    pub state: String,
    /// Whether the process is complete
    pub completed: bool,
    /// Whether the process has failed
    pub failed: bool,
    /// When the process started
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// When the process completed or failed
    pub ended_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Additional state data as JSON
    pub data: serde_json::Value,
}

impl ProcessState {
    /// Creates a new process state
    pub fn new(process_id: impl Into<String>, state: impl Into<String>) -> Self {
        Self {
            process_id: process_id.into(),
            state: state.into(),
            completed: false,
            failed: false,
            started_at: chrono::Utc::now(),
            ended_at: None,
            data: serde_json::json!({}),
        }
    }

    /// Transitions to a new state
    pub fn transition_to(&mut self, new_state: impl Into<String>) {
        self.state = new_state.into();
    }

    /// Marks the process as completed
    pub fn complete(&mut self) {
        self.completed = true;
        self.ended_at = Some(chrono::Utc::now());
    }

    /// Marks the process as failed
    pub fn fail(&mut self) {
        self.failed = true;
        self.ended_at = Some(chrono::Utc::now());
    }

    /// Sets additional data
    pub fn set_data(&mut self, data: serde_json::Value) {
        self.data = data;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use event_sauce_core::Version;
    use serde_json::json;
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    enum TestCommand {
        DoSomething,
        DoSomethingElse,
    }

    impl Command for TestCommand {}

    struct TestProcessManager {
        state: String,
        completed: bool,
        failed: bool,
        events_processed: Vec<String>,
    }

    #[async_trait]
    impl ProcessManager for TestProcessManager {
        type Command = TestCommand;

        async fn handle_event(&mut self, event: &EventEnvelope) -> Result<Vec<Self::Command>> {
            self.events_processed.push(event.event_type.clone());

            match event.event_type.as_str() {
                "Started" => {
                    self.state = "Running".to_string();
                    Ok(vec![TestCommand::DoSomething])
                }
                "StepCompleted" => {
                    self.state = "Completing".to_string();
                    Ok(vec![TestCommand::DoSomethingElse])
                }
                "Completed" => {
                    self.state = "Done".to_string();
                    self.completed = true;
                    Ok(vec![])
                }
                "Failed" => {
                    self.state = "Failed".to_string();
                    self.failed = true;
                    Err(Error::ExecutionFailed("Process failed".into()))
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

        fn name(&self) -> &str {
            "TestProcessManager"
        }

        fn current_state(&self) -> String {
            self.state.clone()
        }
    }

    fn create_test_event(event_type: &str) -> EventEnvelope {
        EventEnvelope::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "Test".to_string(),
            event_type.to_string(),
            Version::new(1),
            json!({}),
        )
    }

    #[tokio::test]
    async fn test_process_manager_handle_event() {
        let mut pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events_processed: vec![],
        };

        let event = create_test_event("Started");
        let commands = pm.handle_event(&event).await.unwrap();

        assert_eq!(pm.current_state(), "Running");
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], TestCommand::DoSomething));
    }

    #[tokio::test]
    async fn test_process_manager_completion() {
        let mut pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events_processed: vec![],
        };

        assert!(!pm.is_complete());

        let event = create_test_event("Completed");
        pm.handle_event(&event).await.unwrap();

        assert!(pm.is_complete());
        assert_eq!(pm.current_state(), "Done");
    }

    #[tokio::test]
    async fn test_process_manager_failure() {
        let mut pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events_processed: vec![],
        };

        let event = create_test_event("Failed");
        let result = pm.handle_event(&event).await;

        assert!(result.is_err());
        assert!(pm.is_failed());
        assert_eq!(pm.current_state(), "Failed");
    }

    #[tokio::test]
    async fn test_process_manager_name() {
        let pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events_processed: vec![],
        };

        assert_eq!(pm.name(), "TestProcessManager");
    }

    #[tokio::test]
    async fn test_process_manager_workflow() {
        let mut pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events_processed: vec![],
        };

        // Step 1: Start
        let event1 = create_test_event("Started");
        let commands1 = pm.handle_event(&event1).await.unwrap();
        assert_eq!(commands1.len(), 1);
        assert_eq!(pm.current_state(), "Running");

        // Step 2: Continue
        let event2 = create_test_event("StepCompleted");
        let commands2 = pm.handle_event(&event2).await.unwrap();
        assert_eq!(commands2.len(), 1);
        assert_eq!(pm.current_state(), "Completing");

        // Step 3: Complete
        let event3 = create_test_event("Completed");
        let commands3 = pm.handle_event(&event3).await.unwrap();
        assert_eq!(commands3.len(), 0);
        assert!(pm.is_complete());

        assert_eq!(
            pm.events_processed,
            vec!["Started", "StepCompleted", "Completed"]
        );
    }

    #[test]
    fn test_process_state_creation() {
        let state = ProcessState::new("proc-1", "Initial");
        assert_eq!(state.process_id, "proc-1");
        assert_eq!(state.state, "Initial");
        assert!(!state.completed);
        assert!(!state.failed);
        assert!(state.ended_at.is_none());
    }

    #[test]
    fn test_process_state_transition() {
        let mut state = ProcessState::new("proc-1", "Initial");
        state.transition_to("Running");
        assert_eq!(state.state, "Running");
    }

    #[test]
    fn test_process_state_completion() {
        let mut state = ProcessState::new("proc-1", "Running");
        assert!(!state.completed);
        assert!(state.ended_at.is_none());

        state.complete();
        assert!(state.completed);
        assert!(state.ended_at.is_some());
    }

    #[test]
    fn test_process_state_failure() {
        let mut state = ProcessState::new("proc-1", "Running");
        assert!(!state.failed);

        state.fail();
        assert!(state.failed);
        assert!(state.ended_at.is_some());
    }

    #[test]
    fn test_process_state_data() {
        let mut state = ProcessState::new("proc-1", "Running");
        let data = json!({"order_id": "order-123", "total": 100});
        state.set_data(data.clone());
        assert_eq!(state.data, data);
    }

    #[test]
    fn test_process_state_serialization() {
        let state = ProcessState::new("proc-1", "Running");
        let json = serde_json::to_string(&state).unwrap();
        let deserialized: ProcessState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, deserialized);
    }
}
