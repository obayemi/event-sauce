//! Checkpoint support for tracking projection progress.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sauce_core::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Represents a checkpoint in an event stream.
///
/// Checkpoints track the last processed event for a projection,
/// enabling resumption after restarts or failures.
///
/// # Examples
///
/// ```
/// use event_sauce_projections::Checkpoint;
/// use uuid::Uuid;
///
/// let checkpoint = Checkpoint::new(
///     "my_projection",
///     Uuid::new_v4(),
///     100
/// );
///
/// assert_eq!(checkpoint.projection_name(), "my_projection");
/// assert_eq!(checkpoint.sequence(), 100);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    projection_name: String,
    event_id: Uuid,
    sequence: i64,
    timestamp: DateTime<Utc>,
}

impl Checkpoint {
    /// Creates a new checkpoint.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_projections::Checkpoint;
    /// use uuid::Uuid;
    ///
    /// let checkpoint = Checkpoint::new(
    ///     "my_projection",
    ///     Uuid::new_v4(),
    ///     100
    /// );
    /// ```
    #[must_use]
    pub fn new(projection_name: impl Into<String>, event_id: Uuid, sequence: i64) -> Self {
        Self {
            projection_name: projection_name.into(),
            event_id,
            sequence,
            timestamp: Utc::now(),
        }
    }

    /// Returns the projection name.
    #[must_use]
    pub fn projection_name(&self) -> &str {
        &self.projection_name
    }

    /// Returns the last processed event ID.
    #[must_use]
    pub fn event_id(&self) -> Uuid {
        self.event_id
    }

    /// Returns the sequence number.
    #[must_use]
    pub fn sequence(&self) -> i64 {
        self.sequence
    }

    /// Returns the checkpoint timestamp.
    #[must_use]
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
}

/// Trait for checkpoint storage implementations.
///
/// Checkpoint stores persist projection progress, enabling resumption
/// after restarts or failures.
///
/// # Examples
///
/// ```
/// use event_sauce_projections::{CheckpointStore, InMemoryCheckpointStore, Checkpoint};
/// use uuid::Uuid;
///
/// # async fn example() -> event_sauce_core::Result<()> {
/// let store = InMemoryCheckpointStore::new();
///
/// // Save checkpoint
/// let checkpoint = Checkpoint::new("my_projection", Uuid::new_v4(), 100);
/// store.save("my_projection", checkpoint).await?;
///
/// // Load checkpoint
/// let loaded = store.load("my_projection").await?;
/// assert!(loaded.is_some());
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Saves a checkpoint for a projection.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be saved.
    async fn save(&self, projection_name: &str, checkpoint: Checkpoint) -> Result<()>;

    /// Loads the latest checkpoint for a projection.
    ///
    /// Returns `None` if no checkpoint exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be loaded.
    async fn load(&self, projection_name: &str) -> Result<Option<Checkpoint>>;

    /// Deletes the checkpoint for a projection.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be deleted.
    async fn delete(&self, projection_name: &str) -> Result<()>;
}

/// In-memory checkpoint store for testing and development.
///
/// This implementation stores checkpoints in memory and is not persistent.
/// For production use, implement `CheckpointStore` with a database backend.
///
/// # Examples
///
/// ```
/// use event_sauce_projections::{InMemoryCheckpointStore, CheckpointStore, Checkpoint};
/// use uuid::Uuid;
///
/// # async fn example() -> event_sauce_core::Result<()> {
/// let store = InMemoryCheckpointStore::new();
/// let checkpoint = Checkpoint::new("my_projection", Uuid::new_v4(), 100);
///
/// store.save("my_projection", checkpoint).await?;
/// let loaded = store.load("my_projection").await?;
/// assert!(loaded.is_some());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct InMemoryCheckpointStore {
    checkpoints: Arc<RwLock<HashMap<String, Checkpoint>>>,
}

impl InMemoryCheckpointStore {
    /// Creates a new in-memory checkpoint store.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_projections::InMemoryCheckpointStore;
    ///
    /// let store = InMemoryCheckpointStore::new();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn save(&self, projection_name: &str, checkpoint: Checkpoint) -> Result<()> {
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.insert(projection_name.to_string(), checkpoint);
        Ok(())
    }

    async fn load(&self, projection_name: &str) -> Result<Option<Checkpoint>> {
        let checkpoints = self.checkpoints.read().await;
        Ok(checkpoints.get(projection_name).cloned())
    }

    async fn delete(&self, projection_name: &str) -> Result<()> {
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.remove(projection_name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_new() {
        let event_id = Uuid::new_v4();
        let checkpoint = Checkpoint::new("test_projection", event_id, 100);

        assert_eq!(checkpoint.projection_name(), "test_projection");
        assert_eq!(checkpoint.event_id(), event_id);
        assert_eq!(checkpoint.sequence(), 100);
    }

    #[test]
    fn test_checkpoint_timestamp() {
        let checkpoint = Checkpoint::new("test", Uuid::new_v4(), 1);
        let now = Utc::now();

        // Timestamp should be close to now
        let diff = now.signed_duration_since(checkpoint.timestamp());
        assert!(diff.num_seconds() < 1);
    }

    #[test]
    fn test_checkpoint_clone() {
        let checkpoint = Checkpoint::new("test", Uuid::new_v4(), 1);
        let cloned = checkpoint.clone();

        assert_eq!(checkpoint, cloned);
    }

    #[test]
    fn test_checkpoint_debug() {
        let checkpoint = Checkpoint::new("test", Uuid::new_v4(), 1);
        let debug = format!("{:?}", checkpoint);

        assert!(debug.contains("Checkpoint"));
        assert!(debug.contains("test"));
    }

    #[tokio::test]
    async fn test_in_memory_store_new() {
        let _store = InMemoryCheckpointStore::new();
    }

    #[tokio::test]
    async fn test_in_memory_store_default() {
        let _store = InMemoryCheckpointStore::default();
    }

    #[tokio::test]
    async fn test_in_memory_store_save_and_load() {
        let store = InMemoryCheckpointStore::new();
        let event_id = Uuid::new_v4();
        let checkpoint = Checkpoint::new("test_projection", event_id, 100);

        store.save("test_projection", checkpoint).await.unwrap();

        let loaded = store.load("test_projection").await.unwrap();
        assert!(loaded.is_some());

        let loaded = loaded.unwrap();
        assert_eq!(loaded.projection_name(), "test_projection");
        assert_eq!(loaded.event_id(), event_id);
        assert_eq!(loaded.sequence(), 100);
    }

    #[tokio::test]
    async fn test_in_memory_store_load_nonexistent() {
        let store = InMemoryCheckpointStore::new();
        let loaded = store.load("nonexistent").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_store_overwrite() {
        let store = InMemoryCheckpointStore::new();
        let event_id1 = Uuid::new_v4();
        let event_id2 = Uuid::new_v4();

        let checkpoint1 = Checkpoint::new("test", event_id1, 100);
        store.save("test", checkpoint1).await.unwrap();

        let checkpoint2 = Checkpoint::new("test", event_id2, 200);
        store.save("test", checkpoint2).await.unwrap();

        let loaded = store.load("test").await.unwrap().unwrap();
        assert_eq!(loaded.event_id(), event_id2);
        assert_eq!(loaded.sequence(), 200);
    }

    #[tokio::test]
    async fn test_in_memory_store_delete() {
        let store = InMemoryCheckpointStore::new();
        let checkpoint = Checkpoint::new("test", Uuid::new_v4(), 100);

        store.save("test", checkpoint).await.unwrap();
        assert!(store.load("test").await.unwrap().is_some());

        store.delete("test").await.unwrap();
        assert!(store.load("test").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_in_memory_store_delete_nonexistent() {
        let store = InMemoryCheckpointStore::new();
        // Should not error
        store.delete("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn test_in_memory_store_multiple_projections() {
        let store = InMemoryCheckpointStore::new();

        let checkpoint1 = Checkpoint::new("proj1", Uuid::new_v4(), 100);
        let checkpoint2 = Checkpoint::new("proj2", Uuid::new_v4(), 200);

        store.save("proj1", checkpoint1).await.unwrap();
        store.save("proj2", checkpoint2).await.unwrap();

        let loaded1 = store.load("proj1").await.unwrap().unwrap();
        let loaded2 = store.load("proj2").await.unwrap().unwrap();

        assert_eq!(loaded1.sequence(), 100);
        assert_eq!(loaded2.sequence(), 200);
    }

    #[tokio::test]
    async fn test_in_memory_store_is_cloneable() {
        let store = InMemoryCheckpointStore::new();
        let store_clone = store.clone();

        let checkpoint = Checkpoint::new("test", Uuid::new_v4(), 100);
        store.save("test", checkpoint).await.unwrap();

        // Clone should see the same data
        let loaded = store_clone.load("test").await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().sequence(), 100);
    }

    #[tokio::test]
    async fn test_checkpoint_store_trait() {
        let store: Box<dyn CheckpointStore> = Box::new(InMemoryCheckpointStore::new());
        let checkpoint = Checkpoint::new("test", Uuid::new_v4(), 100);

        store.save("test", checkpoint).await.unwrap();
        let loaded = store.load("test").await.unwrap();
        assert!(loaded.is_some());
    }
}
