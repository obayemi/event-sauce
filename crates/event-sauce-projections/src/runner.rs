//! Projection runner for executing projections from event streams.

use crate::Projection;
use event_sauce_core::EventEnvelope;
use futures::Stream;
use std::future::Future;

/// Executes a projection by consuming events from a stream.
///
/// The runner manages the lifecycle of processing events through a projection,
/// including error handling and graceful shutdown.
///
/// # Examples
///
/// ```ignore
/// use event_sauce_projections::{Projection, ProjectionRunner};
/// use futures::StreamExt;
///
/// let runner = ProjectionRunner::new(my_projection);
/// runner.run(event_stream).await?;
/// ```
pub struct ProjectionRunner<P: Projection> {
    projection: P,
}

impl<P: Projection> ProjectionRunner<P> {
    /// Creates a new projection runner.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use event_sauce_projections::ProjectionRunner;
    ///
    /// let runner = ProjectionRunner::new(my_projection);
    /// ```
    pub fn new(projection: P) -> Self {
        Self { projection }
    }

    /// Runs the projection by consuming events from the stream.
    ///
    /// This method will process events until the stream ends or an error occurs.
    ///
    /// # Errors
    ///
    /// Returns an error if event processing fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// runner.run(event_stream).await?;
    /// ```
    pub async fn run<S>(&mut self, stream: S) -> event_sauce_core::Result<()>
    where
        S: Stream<Item = EventEnvelope> + Send,
    {
        use futures::StreamExt;

        futures::pin_mut!(stream);

        while let Some(event) = stream.next().await {
            self.projection.handle(&event).await?;
        }

        Ok(())
    }

    /// Runs the projection with a callback invoked after each event.
    ///
    /// The callback receives the processed event and can be used for
    /// checkpointing, logging, or other side effects.
    ///
    /// # Errors
    ///
    /// Returns an error if event processing or the callback fails.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// runner.run_with_callback(event_stream, |event| async {
    ///     println!("Processed: {:?}", event);
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn run_with_callback<S, F, Fut>(
        &mut self,
        stream: S,
        mut callback: F,
    ) -> event_sauce_core::Result<()>
    where
        S: Stream<Item = EventEnvelope> + Send,
        F: FnMut(EventEnvelope) -> Fut + Send,
        Fut: Future<Output = event_sauce_core::Result<()>> + Send,
    {
        use futures::StreamExt;

        futures::pin_mut!(stream);

        while let Some(event) = stream.next().await {
            self.projection.handle(&event).await?;
            callback(event).await?;
        }

        Ok(())
    }

    /// Returns a reference to the projection.
    pub fn projection(&self) -> &P {
        &self.projection
    }

    /// Returns a mutable reference to the projection.
    pub fn projection_mut(&mut self) -> &mut P {
        &mut self.projection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use event_sauce_core::{EventEnvelope, Result, Version};
    use futures::stream;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    /// Test projection that counts events
    struct CounterProjection {
        count: Arc<Mutex<u64>>,
    }

    impl CounterProjection {
        fn new(count: Arc<Mutex<u64>>) -> Self {
            Self { count }
        }
    }

    #[async_trait]
    impl Projection for CounterProjection {
        fn name(&self) -> &str {
            "counter"
        }

        async fn handle(&mut self, _event: &EventEnvelope) -> Result<()> {
            let mut count = self.count.lock().await;
            *count += 1;
            Ok(())
        }
    }

    fn create_test_envelope(event_type: &str) -> EventEnvelope {
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
    async fn test_runner_creates() {
        let count = Arc::new(Mutex::new(0));
        let projection = CounterProjection::new(count);
        let _runner = ProjectionRunner::new(projection);
    }

    #[tokio::test]
    async fn test_runner_processes_empty_stream() {
        let count = Arc::new(Mutex::new(0));
        let projection = CounterProjection::new(Arc::clone(&count));
        let mut runner = ProjectionRunner::new(projection);

        let events: Vec<EventEnvelope> = vec![];
        let stream = stream::iter(events);

        runner.run(stream).await.unwrap();

        assert_eq!(*count.lock().await, 0);
    }

    #[tokio::test]
    async fn test_runner_processes_single_event() {
        let count = Arc::new(Mutex::new(0));
        let projection = CounterProjection::new(Arc::clone(&count));
        let mut runner = ProjectionRunner::new(projection);

        let events = vec![create_test_envelope("Event1")];
        let stream = stream::iter(events);

        runner.run(stream).await.unwrap();

        assert_eq!(*count.lock().await, 1);
    }

    #[tokio::test]
    async fn test_runner_processes_multiple_events() {
        let count = Arc::new(Mutex::new(0));
        let projection = CounterProjection::new(Arc::clone(&count));
        let mut runner = ProjectionRunner::new(projection);

        let events = vec![
            create_test_envelope("Event1"),
            create_test_envelope("Event2"),
            create_test_envelope("Event3"),
        ];
        let stream = stream::iter(events);

        runner.run(stream).await.unwrap();

        assert_eq!(*count.lock().await, 3);
    }

    #[tokio::test]
    async fn test_runner_with_callback() {
        let count = Arc::new(Mutex::new(0));
        let projection = CounterProjection::new(Arc::clone(&count));
        let mut runner = ProjectionRunner::new(projection);

        let callback_count = Arc::new(Mutex::new(0));
        let callback_count_clone = Arc::clone(&callback_count);

        let events = vec![
            create_test_envelope("Event1"),
            create_test_envelope("Event2"),
        ];
        let stream = stream::iter(events);

        runner
            .run_with_callback(stream, move |_event| {
                let callback_count = Arc::clone(&callback_count_clone);
                async move {
                    let mut count = callback_count.lock().await;
                    *count += 1;
                    Ok(())
                }
            })
            .await
            .unwrap();

        assert_eq!(*count.lock().await, 2);
        assert_eq!(*callback_count.lock().await, 2);
    }

    #[tokio::test]
    async fn test_runner_access_projection() {
        let count = Arc::new(Mutex::new(0));
        let projection = CounterProjection::new(Arc::clone(&count));
        let mut runner = ProjectionRunner::new(projection);

        assert_eq!(runner.projection().name(), "counter");
        assert_eq!(runner.projection_mut().name(), "counter");
    }

    /// Projection that fails on specific event types
    struct FailingProjection {
        fail_on: String,
    }

    impl FailingProjection {
        fn new(fail_on: String) -> Self {
            Self { fail_on }
        }
    }

    #[async_trait]
    impl Projection for FailingProjection {
        fn name(&self) -> &str {
            "failing"
        }

        async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
            if event.event_type == self.fail_on {
                return Err(event_sauce_core::Error::custom("Intentional failure"));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_runner_stops_on_projection_error() {
        let projection = FailingProjection::new("Event2".to_string());
        let mut runner = ProjectionRunner::new(projection);

        let events = vec![
            create_test_envelope("Event1"),
            create_test_envelope("Event2"),
            create_test_envelope("Event3"),
        ];
        let stream = stream::iter(events);

        let result = runner.run(stream).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_runner_callback_error_stops_processing() {
        let count = Arc::new(Mutex::new(0));
        let projection = CounterProjection::new(Arc::clone(&count));
        let mut runner = ProjectionRunner::new(projection);

        let events = vec![
            create_test_envelope("Event1"),
            create_test_envelope("Event2"),
            create_test_envelope("Event3"),
        ];
        let stream = stream::iter(events);

        let mut callback_invocations = 0;
        let result = runner
            .run_with_callback(stream, |_event| {
                callback_invocations += 1;
                async move {
                    if callback_invocations == 2 {
                        Err(event_sauce_core::Error::custom("Callback failure"))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;

        assert!(result.is_err());
        // Should have processed 2 events before callback failure
        assert_eq!(*count.lock().await, 2);
    }
}
