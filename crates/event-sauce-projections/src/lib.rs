//! # event-sauce-projections
//!
//! Projection building helpers with streaming support.
//!
//! This crate provides tools for building read models from event streams:
//!
//! - **`Projection`** trait - Define how events update read models
//! - **`ProjectionRunner`** - Execute projections from event streams
//! - **`Checkpoint`** - Track and resume projection progress
//! - **Error handling** - Retry logic and error recovery
//!
//! # Examples
//!
//! ```
//! use event_sauce_projections::Projection;
//! use event_sauce_core::{EventEnvelope, Result};
//! use async_trait::async_trait;
//!
//! struct UserCountProjection {
//!     count: u64,
//! }
//!
//! #[async_trait]
//! impl Projection for UserCountProjection {
//!     fn name(&self) -> &str {
//!         "user_count"
//!     }
//!
//!     async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
//!         if event.event_type == "UserCreated" {
//!             self.count += 1;
//!         }
//!         Ok(())
//!     }
//! }
//! ```

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod checkpoint;
mod projection;
mod runner;

pub use checkpoint::{Checkpoint, CheckpointStore, InMemoryCheckpointStore};
pub use projection::Projection;
pub use runner::ProjectionRunner;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use event_sauce_core::{EventEnvelope, Result, Version};
    use serde_json::json;
    use uuid::Uuid;

    /// Test projection that counts events
    struct CounterProjection {
        name: String,
        count: u64,
    }

    impl CounterProjection {
        fn new(name: impl Into<String>) -> Self {
            Self {
                name: name.into(),
                count: 0,
            }
        }

        fn count(&self) -> u64 {
            self.count
        }
    }

    #[async_trait]
    impl Projection for CounterProjection {
        fn name(&self) -> &str {
            &self.name
        }

        async fn handle(&mut self, _event: &EventEnvelope) -> Result<()> {
            self.count += 1;
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
    async fn test_projection_has_name() {
        let projection = CounterProjection::new("test_projection");
        assert_eq!(projection.name(), "test_projection");
    }

    #[tokio::test]
    async fn test_projection_handles_events() {
        let mut projection = CounterProjection::new("test");
        let event = create_test_envelope("TestEvent");

        projection.handle(&event).await.unwrap();

        assert_eq!(projection.count(), 1);
    }

    #[tokio::test]
    async fn test_projection_handles_multiple_events() {
        let mut projection = CounterProjection::new("test");

        for i in 0..10 {
            let event = create_test_envelope(&format!("Event{}", i));
            projection.handle(&event).await.unwrap();
        }

        assert_eq!(projection.count(), 10);
    }

    /// Test projection that filters events by type
    struct FilteringProjection {
        name: String,
        matched_count: u64,
        target_event_type: String,
    }

    impl FilteringProjection {
        fn new(name: impl Into<String>, target_event_type: impl Into<String>) -> Self {
            Self {
                name: name.into(),
                matched_count: 0,
                target_event_type: target_event_type.into(),
            }
        }

        fn matched_count(&self) -> u64 {
            self.matched_count
        }
    }

    #[async_trait]
    impl Projection for FilteringProjection {
        fn name(&self) -> &str {
            &self.name
        }

        async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
            if event.event_type == self.target_event_type {
                self.matched_count += 1;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_projection_can_filter_events() {
        let mut projection = FilteringProjection::new("filter", "UserCreated");

        projection.handle(&create_test_envelope("UserCreated")).await.unwrap();
        projection.handle(&create_test_envelope("UserUpdated")).await.unwrap();
        projection.handle(&create_test_envelope("UserCreated")).await.unwrap();

        assert_eq!(projection.matched_count(), 2);
    }
}
