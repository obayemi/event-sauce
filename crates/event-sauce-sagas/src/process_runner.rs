//! Process runner for executing process managers

use crate::error::{Error, Result};
use crate::process_manager::{Command, ProcessManager};
use async_trait::async_trait;
use event_sauce_core::{EventBus, EventEnvelope, EventFilter};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Command executor trait
///
/// Implementations define how to execute commands issued by process managers.
#[async_trait]
pub trait CommandExecutor<C: Command>: Send + Sync {
    /// Executes a command
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be executed.
    async fn execute(&self, command: C) -> Result<()>;
}

/// Runner for executing process managers
///
/// The process runner subscribes to events, dispatches them to the process manager,
/// and executes the resulting commands.
///
/// # Examples
///
/// ```no_run
/// use event_sauce_sagas::{ProcessManager, ProcessRunner, CommandExecutor};
/// use event_sauce_core::EventBus;
/// use std::sync::Arc;
///
/// # async fn example<PM, EB, CE>(
/// #     process_manager: PM,
/// #     event_bus: Arc<EB>,
/// #     executor: Arc<CE>
/// # )
/// # where
/// #     PM: ProcessManager + 'static,
/// #     EB: EventBus + 'static,
/// #     CE: CommandExecutor<PM::Command> + 'static,
/// # {
/// let mut runner = ProcessRunner::new(process_manager, event_bus, executor);
///
/// // Run the process manager
/// runner.run().await.unwrap();
/// # }
/// ```
pub struct ProcessRunner<PM: ProcessManager, EB: EventBus, CE: CommandExecutor<PM::Command>> {
    process_manager: Arc<RwLock<PM>>,
    event_bus: Arc<EB>,
    executor: Arc<CE>,
    events_processed: usize,
}

impl<PM, EB, CE> ProcessRunner<PM, EB, CE>
where
    PM: ProcessManager + 'static,
    EB: EventBus + 'static,
    CE: CommandExecutor<PM::Command> + 'static,
{
    /// Creates a new process runner
    pub fn new(process_manager: PM, event_bus: Arc<EB>, executor: Arc<CE>) -> Self {
        Self {
            process_manager: Arc::new(RwLock::new(process_manager)),
            event_bus,
            executor,
            events_processed: 0,
        }
    }

    /// Runs the process manager until completion or failure
    ///
    /// # Errors
    ///
    /// Returns an error if event processing or command execution fails.
    pub async fn run(&mut self) -> Result<()> {
        let pm_name = {
            let pm = self.process_manager.read().await;
            pm.name().to_string()
        };

        info!(process = %pm_name, "Starting process runner");

        let filter = EventFilter::all();
        let event_bus = Arc::clone(&self.event_bus);
        let mut stream = Box::pin(event_bus.subscribe(filter).await?);

        while let Some(event) = stream.next().await {
            if let Err(e) = self.process_event(event).await {
                error!(process = %pm_name, error = %e, "Failed to process event");
                return Err(e);
            }

            // Check if process is complete
            let pm = self.process_manager.read().await;
            if pm.is_complete() {
                info!(process = %pm_name, "Process completed successfully");
                return Ok(());
            }
            if pm.is_failed() {
                error!(process = %pm_name, "Process failed");
                return Err(Error::ExecutionFailed("Process failed".into()));
            }
        }

        info!(process = %pm_name, "Process runner finished");
        Ok(())
    }

    /// Processes a single event
    async fn process_event(&mut self, event: EventEnvelope) -> Result<()> {
        let pm_name = {
            let pm = self.process_manager.read().await;
            pm.name().to_string()
        };

        info!(
            process = %pm_name,
            event_type = %event.event_type,
            event_id = %event.id,
            "Processing event"
        );

        let commands = {
            let mut pm = self.process_manager.write().await;
            pm.handle_event(&event).await?
        };

        debug!(
            process = %pm_name,
            command_count = commands.len(),
            "Generated commands"
        );

        for (idx, command) in commands.into_iter().enumerate() {
            info!(
                process = %pm_name,
                command_index = idx,
                command = ?command,
                "Executing command"
            );

            self.executor.execute(command).await?;
        }

        self.events_processed += 1;
        Ok(())
    }

    /// Returns the number of events processed
    #[must_use]
    pub fn events_processed(&self) -> usize {
        self.events_processed
    }

    /// Returns a reference to the process manager
    #[must_use]
    pub fn process_manager(&self) -> Arc<RwLock<PM>> {
        Arc::clone(&self.process_manager)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_manager::{Command, ProcessManager};
    use async_trait::async_trait;
    use event_sauce_core::Version;
    use futures::{stream, Stream};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    enum TestCommand {
        Action1,
        Action2,
    }

    impl Command for TestCommand {}

    struct TestProcessManager {
        state: String,
        completed: bool,
        failed: bool,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ProcessManager for TestProcessManager {
        type Command = TestCommand;

        async fn handle_event(&mut self, event: &EventEnvelope) -> Result<Vec<Self::Command>> {
            self.events.lock().await.push(event.event_type.clone());

            match event.event_type.as_str() {
                "Start" => {
                    self.state = "Running".to_string();
                    Ok(vec![TestCommand::Action1])
                }
                "Continue" => {
                    self.state = "Continuing".to_string();
                    Ok(vec![TestCommand::Action2])
                }
                "Complete" => {
                    self.state = "Done".to_string();
                    self.completed = true;
                    Ok(vec![])
                }
                "Fail" => {
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

    struct TestCommandExecutor {
        commands_executed: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl CommandExecutor<TestCommand> for TestCommandExecutor {
        async fn execute(&self, command: TestCommand) -> Result<()> {
            let cmd_str = format!("{:?}", command);
            self.commands_executed.lock().await.push(cmd_str);
            Ok(())
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
    async fn test_process_runner_executes_commands() {
        let events = Arc::new(Mutex::new(vec![
            create_test_event("Start"),
            create_test_event("Continue"),
            create_test_event("Complete"),
        ]));

        let event_bus = Arc::new(MockEventBus {
            events: Arc::clone(&events),
        });

        let pm_events = Arc::new(Mutex::new(Vec::new()));
        let pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events: Arc::clone(&pm_events),
        };

        let commands_executed = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(TestCommandExecutor {
            commands_executed: Arc::clone(&commands_executed),
        });

        let mut runner = ProcessRunner::new(pm, event_bus, executor);
        runner.run().await.unwrap();

        let executed = commands_executed.lock().await;
        assert_eq!(*executed, vec!["Action1", "Action2"]);
        assert_eq!(runner.events_processed(), 3);
    }

    #[tokio::test]
    async fn test_process_runner_stops_on_completion() {
        let events = Arc::new(Mutex::new(vec![
            create_test_event("Start"),
            create_test_event("Complete"),
            create_test_event("ShouldNotProcess"),
        ]));

        let event_bus = Arc::new(MockEventBus {
            events: Arc::clone(&events),
        });

        let pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events: Arc::new(Mutex::new(Vec::new())),
        };

        let executor = Arc::new(TestCommandExecutor {
            commands_executed: Arc::new(Mutex::new(Vec::new())),
        });

        let mut runner = ProcessRunner::new(pm, event_bus, executor);
        runner.run().await.unwrap();

        // Should only process 2 events before completion
        assert_eq!(runner.events_processed(), 2);
    }

    #[tokio::test]
    async fn test_process_runner_handles_failure() {
        let events = Arc::new(Mutex::new(vec![
            create_test_event("Start"),
            create_test_event("Fail"),
        ]));

        let event_bus = Arc::new(MockEventBus {
            events: Arc::clone(&events),
        });

        let pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events: Arc::new(Mutex::new(Vec::new())),
        };

        let executor = Arc::new(TestCommandExecutor {
            commands_executed: Arc::new(Mutex::new(Vec::new())),
        });

        let mut runner = ProcessRunner::new(pm, event_bus, executor);
        let result = runner.run().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_runner_empty_stream() {
        let events = Arc::new(Mutex::new(vec![]));
        let event_bus = Arc::new(MockEventBus { events });

        let pm = TestProcessManager {
            state: "Initial".to_string(),
            completed: false,
            failed: false,
            events: Arc::new(Mutex::new(Vec::new())),
        };

        let executor = Arc::new(TestCommandExecutor {
            commands_executed: Arc::new(Mutex::new(Vec::new())),
        });

        let mut runner = ProcessRunner::new(pm, event_bus, executor);
        let result = runner.run().await;

        assert!(result.is_ok());
        assert_eq!(runner.events_processed(), 0);
    }
}
