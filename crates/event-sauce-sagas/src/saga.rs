//! Saga pattern implementation for choreography-based coordination
//!
//! Sagas represent decentralized workflows where each step reacts to events
//! and can compensate for failures.

use crate::error::Result;
use async_trait::async_trait;
use event_sauce_core::EventEnvelope;

/// Saga trait for implementing choreography-based workflows
///
/// A saga reacts to events and can compensate when failures occur.
/// Each saga is responsible for coordinating a specific workflow
/// by listening to events and performing actions.
///
/// # Examples
///
/// ```
/// use event_sauce_sagas::{Saga, Result};
/// use event_sauce_core::EventEnvelope;
/// use async_trait::async_trait;
///
/// struct PaymentSaga {
///     // ... saga state
/// }
///
/// #[async_trait]
/// impl Saga for PaymentSaga {
///     fn interested_in(&self, event: &EventEnvelope) -> bool {
///         event.event_type.starts_with("Order")
///     }
///
///     async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
///         // Process the event
///         Ok(())
///     }
///
///     async fn compensate(&mut self, event: &EventEnvelope) -> Result<()> {
///         // Rollback changes if needed
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Saga: Send + Sync {
    /// Determines if this saga is interested in the given event
    ///
    /// Returns `true` if the saga should handle this event.
    fn interested_in(&self, event: &EventEnvelope) -> bool;

    /// Handles an event
    ///
    /// This method is called when an event occurs that the saga is interested in.
    ///
    /// # Errors
    ///
    /// Returns `Error::ExecutionFailed` if the saga cannot handle the event.
    async fn handle(&mut self, event: &EventEnvelope) -> Result<()>;

    /// Compensates for a failed saga step
    ///
    /// This method is called when a saga step fails and needs to be rolled back.
    ///
    /// # Errors
    ///
    /// Returns `Error::CompensationFailed` if compensation cannot be performed.
    async fn compensate(&mut self, event: &EventEnvelope) -> Result<()>;

    /// Returns the name of this saga for logging and debugging
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

/// Saga step represents a single action in a saga workflow
#[derive(Debug, Clone)]
pub struct SagaStep {
    /// Name of the step
    pub name: String,
    /// Whether the step was successful
    pub success: bool,
    /// Timestamp when the step was executed
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Error message if the step failed
    pub error: Option<String>,
}

impl SagaStep {
    /// Creates a successful saga step
    pub fn success(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            success: true,
            timestamp: chrono::Utc::now(),
            error: None,
        }
    }

    /// Creates a failed saga step
    pub fn failure(name: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            success: false,
            timestamp: chrono::Utc::now(),
            error: Some(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use event_sauce_core::Version;
    use serde_json::json;
    use uuid::Uuid;

    struct TestSaga {
        events_handled: Vec<String>,
        should_fail: bool,
    }

    #[async_trait]
    impl Saga for TestSaga {
        fn interested_in(&self, event: &EventEnvelope) -> bool {
            event.event_type.starts_with("Test")
        }

        async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
            if self.should_fail {
                return Err(Error::ExecutionFailed("forced failure".into()));
            }
            self.events_handled.push(event.event_type.clone());
            Ok(())
        }

        async fn compensate(&mut self, event: &EventEnvelope) -> Result<()> {
            self.events_handled.push(format!("compensate:{}", event.event_type));
            Ok(())
        }

        fn name(&self) -> &str {
            "TestSaga"
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
    async fn test_saga_interested_in() {
        let saga = TestSaga {
            events_handled: vec![],
            should_fail: false,
        };

        let event1 = create_test_event("TestEvent");
        let event2 = create_test_event("OtherEvent");

        assert!(saga.interested_in(&event1));
        assert!(!saga.interested_in(&event2));
    }

    #[tokio::test]
    async fn test_saga_handle_success() {
        let mut saga = TestSaga {
            events_handled: vec![],
            should_fail: false,
        };

        let event = create_test_event("TestEvent");
        let result = saga.handle(&event).await;

        assert!(result.is_ok());
        assert_eq!(saga.events_handled, vec!["TestEvent"]);
    }

    #[tokio::test]
    async fn test_saga_handle_failure() {
        let mut saga = TestSaga {
            events_handled: vec![],
            should_fail: true,
        };

        let event = create_test_event("TestEvent");
        let result = saga.handle(&event).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::ExecutionFailed(_)));
    }

    #[tokio::test]
    async fn test_saga_compensate() {
        let mut saga = TestSaga {
            events_handled: vec![],
            should_fail: false,
        };

        let event = create_test_event("TestEvent");
        let result = saga.compensate(&event).await;

        assert!(result.is_ok());
        assert_eq!(saga.events_handled, vec!["compensate:TestEvent"]);
    }

    #[tokio::test]
    async fn test_saga_name() {
        let saga = TestSaga {
            events_handled: vec![],
            should_fail: false,
        };

        assert_eq!(saga.name(), "TestSaga");
    }

    #[test]
    fn test_saga_step_success() {
        let step = SagaStep::success("payment");
        assert_eq!(step.name, "payment");
        assert!(step.success);
        assert!(step.error.is_none());
    }

    #[test]
    fn test_saga_step_failure() {
        let step = SagaStep::failure("payment", "insufficient funds");
        assert_eq!(step.name, "payment");
        assert!(!step.success);
        assert_eq!(step.error, Some("insufficient funds".to_string()));
    }
}
