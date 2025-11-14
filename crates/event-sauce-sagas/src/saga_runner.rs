//! Saga runner for executing sagas in response to events

use crate::error::{Error, Result};
use crate::saga::{Saga, SagaStep};
use event_sauce_core::{EventBus, EventEnvelope, EventFilter};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Runner for executing sagas
///
/// The saga runner subscribes to events and dispatches them to interested sagas.
/// It handles failures by attempting compensation.
///
/// # Examples
///
/// ```no_run
/// use event_sauce_sagas::{Saga, SagaRunner};
/// use event_sauce_core::EventBus;
/// use std::sync::Arc;
///
/// # async fn example<E: EventBus + 'static>(saga: impl Saga + 'static, event_bus: Arc<E>) {
/// let mut runner = SagaRunner::new(saga, event_bus);
///
/// // Run the saga (this will block)
/// runner.run().await.unwrap();
/// # }
/// ```
pub struct SagaRunner<S: Saga, E: EventBus> {
    saga: Arc<RwLock<S>>,
    event_bus: Arc<E>,
    history: Vec<SagaStep>,
}

impl<S: Saga + 'static, E: EventBus + 'static> SagaRunner<S, E> {
    /// Creates a new saga runner
    pub fn new(saga: S, event_bus: Arc<E>) -> Self {
        Self {
            saga: Arc::new(RwLock::new(saga)),
            event_bus,
            history: Vec::new(),
        }
    }

    /// Runs the saga, processing events until the stream ends
    ///
    /// # Errors
    ///
    /// Returns an error if saga execution or compensation fails.
    pub async fn run(&mut self) -> Result<()> {
        let saga_name = {
            let saga = self.saga.read().await;
            saga.name().to_string()
        };

        info!(saga = %saga_name, "Starting saga runner");

        let filter = EventFilter::all();
        let event_bus = Arc::clone(&self.event_bus);
        let mut stream = Box::pin(event_bus.subscribe(filter).await?);

        while let Some(event) = stream.next().await {
            if let Err(e) = self.process_event(event).await {
                error!(saga = %saga_name, error = %e, "Failed to process event");
                return Err(e);
            }
        }

        info!(saga = %saga_name, "Saga runner finished");
        Ok(())
    }

    /// Processes a single event
    async fn process_event(&mut self, event: EventEnvelope) -> Result<()> {
        let interested = {
            let saga = self.saga.read().await;
            saga.interested_in(&event)
        };

        if !interested {
            debug!(event_type = %event.event_type, "Saga not interested in event");
            return Ok(());
        }

        let saga_name = {
            let saga = self.saga.read().await;
            saga.name().to_string()
        };

        info!(
            saga = %saga_name,
            event_type = %event.event_type,
            event_id = %event.id,
            "Processing event"
        );

        let mut saga = self.saga.write().await;

        match saga.handle(&event).await {
            Ok(()) => {
                info!(saga = %saga_name, event_type = %event.event_type, "Event handled successfully");
                self.history.push(SagaStep::success(event.event_type.clone()));
                Ok(())
            }
            Err(e) => {
                error!(
                    saga = %saga_name,
                    event_type = %event.event_type,
                    error = %e,
                    "Event handling failed, attempting compensation"
                );

                self.history.push(SagaStep::failure(
                    event.event_type.clone(),
                    e.to_string(),
                ));

                match saga.compensate(&event).await {
                    Ok(()) => {
                        warn!(saga = %saga_name, "Compensation successful");
                        Err(e)
                    }
                    Err(comp_err) => {
                        error!(saga = %saga_name, error = %comp_err, "Compensation failed");
                        Err(Error::CompensationFailed(format!(
                            "Original error: {e}, Compensation error: {comp_err}"
                        )))
                    }
                }
            }
        }
    }

    /// Returns the saga execution history
    #[must_use] 
    pub fn history(&self) -> &[SagaStep] {
        &self.history
    }

    /// Returns a reference to the saga
    #[must_use] 
    pub fn saga(&self) -> Arc<RwLock<S>> {
        Arc::clone(&self.saga)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::saga::Saga;
    use async_trait::async_trait;
    use event_sauce_core::Version;
    use futures::{stream, Stream};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    // Mock EventBus for testing
    struct MockEventBus {
        events: Arc<Mutex<Vec<EventEnvelope>>>,
    }

    #[async_trait]
    impl EventBus for MockEventBus {
        async fn publish(&self, _event: EventEnvelope) -> event_sauce_core::Result<()> {
            Ok(())
        }

        async fn subscribe(
            &self,
            _filter: EventFilter,
        ) -> event_sauce_core::Result<impl Stream<Item = EventEnvelope> + Send> {
            let events = self.events.lock().await;
            let stream = stream::iter(events.clone().into_iter());
            Ok(stream)
        }
    }

    struct TestSaga {
        events_handled: Arc<Mutex<Vec<String>>>,
        should_fail: bool,
        compensations: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl Saga for TestSaga {
        fn interested_in(&self, event: &EventEnvelope) -> bool {
            event.event_type.starts_with("Test")
        }

        async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
            if self.should_fail {
                return Err(Error::ExecutionFailed("intentional failure".into()));
            }
            let mut handled = self.events_handled.lock().await;
            handled.push(event.event_type.clone());
            Ok(())
        }

        async fn compensate(&mut self, event: &EventEnvelope) -> Result<()> {
            let mut comps = self.compensations.lock().await;
            comps.push(event.event_type.clone());
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
    async fn test_saga_runner_processes_events() {
        let events = Arc::new(Mutex::new(vec![
            create_test_event("TestEvent1"),
            create_test_event("TestEvent2"),
            create_test_event("OtherEvent"), // Should be ignored
        ]));

        let event_bus = Arc::new(MockEventBus {
            events: Arc::clone(&events),
        });

        let events_handled = Arc::new(Mutex::new(Vec::new()));
        let saga = TestSaga {
            events_handled: Arc::clone(&events_handled),
            should_fail: false,
            compensations: Arc::new(Mutex::new(Vec::new())),
        };

        let mut runner = SagaRunner::new(saga, event_bus);
        runner.run().await.unwrap();

        let handled = events_handled.lock().await;
        assert_eq!(*handled, vec!["TestEvent1", "TestEvent2"]);
    }

    #[tokio::test]
    async fn test_saga_runner_handles_failure_with_compensation() {
        let events = Arc::new(Mutex::new(vec![create_test_event("TestEvent")]));

        let event_bus = Arc::new(MockEventBus {
            events: Arc::clone(&events),
        });

        let compensations = Arc::new(Mutex::new(Vec::new()));
        let saga = TestSaga {
            events_handled: Arc::new(Mutex::new(Vec::new())),
            should_fail: true,
            compensations: Arc::clone(&compensations),
        };

        let mut runner = SagaRunner::new(saga, event_bus);
        let result = runner.run().await;

        assert!(result.is_err());
        let comps = compensations.lock().await;
        assert_eq!(*comps, vec!["TestEvent"]);
    }

    #[tokio::test]
    async fn test_saga_runner_tracks_history() {
        let events = Arc::new(Mutex::new(vec![
            create_test_event("TestEvent1"),
            create_test_event("TestEvent2"),
        ]));

        let event_bus = Arc::new(MockEventBus {
            events: Arc::clone(&events),
        });

        let saga = TestSaga {
            events_handled: Arc::new(Mutex::new(Vec::new())),
            should_fail: false,
            compensations: Arc::new(Mutex::new(Vec::new())),
        };

        let mut runner = SagaRunner::new(saga, event_bus);
        runner.run().await.unwrap();

        let history = runner.history();
        assert_eq!(history.len(), 2);
        assert!(history[0].success);
        assert_eq!(history[0].name, "TestEvent1");
        assert!(history[1].success);
        assert_eq!(history[1].name, "TestEvent2");
    }

    #[tokio::test]
    async fn test_saga_runner_empty_event_stream() {
        let events = Arc::new(Mutex::new(vec![]));
        let event_bus = Arc::new(MockEventBus { events });

        let saga = TestSaga {
            events_handled: Arc::new(Mutex::new(Vec::new())),
            should_fail: false,
            compensations: Arc::new(Mutex::new(Vec::new())),
        };

        let mut runner = SagaRunner::new(saga, event_bus);
        let result = runner.run().await;

        assert!(result.is_ok());
        assert_eq!(runner.history().len(), 0);
    }
}
