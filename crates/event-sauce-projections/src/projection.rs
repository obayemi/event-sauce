//! Projection trait for building read models from events.

use async_trait::async_trait;
use event_sauce_core::{EventEnvelope, Result};

/// Trait for projections that build read models from event streams.
///
/// Projections consume events and update read models, enabling
/// different views of the data optimized for querying.
///
/// # Design Principles
///
/// - **Idempotent** - Handling the same event multiple times should be safe
/// - **Async** - Projections may need to perform I/O (database updates, etc.)
/// - **Error handling** - Projections can fail and should communicate errors
/// - **Stateful** - Projections maintain state (the read model)
///
/// # Examples
///
/// ```
/// use event_sauce_projections::Projection;
/// use event_sauce_core::{EventEnvelope, Result};
/// use async_trait::async_trait;
///
/// struct UserCountProjection {
///     count: u64,
/// }
///
/// #[async_trait]
/// impl Projection for UserCountProjection {
///     fn name(&self) -> &str {
///         "user_count"
///     }
///
///     async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
///         if event.event_type == "UserCreated" {
///             self.count += 1;
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Projection: Send + Sync {
    /// Returns the unique name of this projection.
    ///
    /// The name is used for checkpointing and identifying the projection.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_projections::Projection;
    /// # use event_sauce_core::{EventEnvelope, Result};
    /// # use async_trait::async_trait;
    /// # struct MyProjection;
    /// # #[async_trait]
    /// # impl Projection for MyProjection {
    /// fn name(&self) -> &str {
    ///     "my_projection_v1"
    /// }
    /// # async fn handle(&mut self, event: &EventEnvelope) -> Result<()> { Ok(()) }
    /// # }
    /// ```
    fn name(&self) -> &str;

    /// Handles an event and updates the read model.
    ///
    /// This method is called for each event in the stream.
    /// Implementations should update their internal state based on the event.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be processed.
    /// The projection runner may retry failed events depending on configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// # use event_sauce_projections::Projection;
    /// # use event_sauce_core::{EventEnvelope, Result};
    /// # use async_trait::async_trait;
    /// # struct MyProjection { count: u64 }
    /// # #[async_trait]
    /// # impl Projection for MyProjection {
    /// # fn name(&self) -> &str { "my_projection" }
    /// async fn handle(&mut self, event: &EventEnvelope) -> Result<()> {
    ///     // Update read model based on event
    ///     if event.event_type == "UserCreated" {
    ///         self.count += 1;
    ///     }
    ///     Ok(())
    /// }
    /// # }
    /// ```
    async fn handle(&mut self, event: &EventEnvelope) -> Result<()>;
}
