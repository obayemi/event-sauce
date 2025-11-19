//! In-memory checkpoint store implementation.
//!
//! Provides a fast, thread-safe in-memory implementation of `CheckpointStore`
//! suitable for testing and development.

use async_trait::async_trait;
use event_sauce_core::{CheckpointStore, Position, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory checkpoint store for testing and development.
///
/// This implementation stores all checkpoints in memory using thread-safe
/// data structures. It provides full `CheckpointStore` functionality.
///
/// # Thread Safety
///
/// This store is thread-safe and can be cloned cheaply (uses `Arc` internally).
///
/// # Examples
///
/// ```
/// use event_sauce_memory::InMemoryCheckpointStore;
/// use event_sauce_core::{CheckpointStore, Position};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let store = InMemoryCheckpointStore::new();
///
///     // Save a checkpoint
///     store.save_checkpoint("my-subscription", Position::new(42)).await?;
///
///     // Load it back
///     let position = store.load_checkpoint("my-subscription").await?;
///     assert_eq!(position, Some(Position::new(42)));
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct InMemoryCheckpointStore {
    inner: Arc<RwLock<HashMap<String, Position>>>,
}

impl InMemoryCheckpointStore {
    /// Creates a new empty in-memory checkpoint store.
    ///
    /// # Examples
    ///
    /// ```
    /// use event_sauce_memory::InMemoryCheckpointStore;
    ///
    /// let store = InMemoryCheckpointStore::new();
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
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
    async fn save_checkpoint(&self, subscription_name: &str, position: Position) -> Result<()> {
        self.inner
            .write()
            .await
            .insert(subscription_name.to_string(), position);
        Ok(())
    }

    async fn load_checkpoint(&self, subscription_name: &str) -> Result<Option<Position>> {
        Ok(self.inner.read().await.get(subscription_name).copied())
    }

    async fn delete_checkpoint(&self, subscription_name: &str) -> Result<()> {
        self.inner.write().await.remove(subscription_name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::{CheckpointStore, Position};

    #[tokio::test]
    async fn test_create_store() {
        let _store = InMemoryCheckpointStore::new();
    }

    #[tokio::test]
    async fn test_save_and_load_checkpoint() {
        let store = InMemoryCheckpointStore::new();

        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(42)));
    }

    #[tokio::test]
    async fn test_load_nonexistent_checkpoint() {
        let store = InMemoryCheckpointStore::new();

        let position = store
            .load_checkpoint("nonexistent-subscription")
            .await
            .unwrap();
        assert_eq!(position, None);
    }

    #[tokio::test]
    async fn test_delete_checkpoint() {
        let store = InMemoryCheckpointStore::new();

        // Save checkpoint
        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        // Verify it exists
        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(42)));

        // Delete it
        store.delete_checkpoint("test-subscription").await.unwrap();

        // Verify it's gone
        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, None);
    }

    #[tokio::test]
    async fn test_overwrite_checkpoint() {
        let store = InMemoryCheckpointStore::new();

        // Save initial checkpoint
        store
            .save_checkpoint("test-subscription", Position::new(10))
            .await
            .unwrap();

        // Overwrite with new position
        store
            .save_checkpoint("test-subscription", Position::new(20))
            .await
            .unwrap();

        // Should get the latest position
        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(20)));
    }

    #[tokio::test]
    async fn test_multiple_subscriptions() {
        let store = InMemoryCheckpointStore::new();

        // Save checkpoints for multiple subscriptions
        store
            .save_checkpoint("subscription-1", Position::new(10))
            .await
            .unwrap();
        store
            .save_checkpoint("subscription-2", Position::new(20))
            .await
            .unwrap();
        store
            .save_checkpoint("subscription-3", Position::new(30))
            .await
            .unwrap();

        // Load each checkpoint
        let pos1 = store.load_checkpoint("subscription-1").await.unwrap();
        let pos2 = store.load_checkpoint("subscription-2").await.unwrap();
        let pos3 = store.load_checkpoint("subscription-3").await.unwrap();

        assert_eq!(pos1, Some(Position::new(10)));
        assert_eq!(pos2, Some(Position::new(20)));
        assert_eq!(pos3, Some(Position::new(30)));
    }

    #[tokio::test]
    async fn test_delete_nonexistent_checkpoint() {
        let store = InMemoryCheckpointStore::new();

        // Deleting a nonexistent checkpoint should succeed (no-op)
        let result = store.delete_checkpoint("nonexistent-subscription").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_store_is_cloneable() {
        let store = InMemoryCheckpointStore::new();
        let store_clone = store.clone();

        // Save via original store
        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        // Read via clone - should see the same data (Arc semantics)
        let position = store_clone
            .load_checkpoint("test-subscription")
            .await
            .unwrap();
        assert_eq!(position, Some(Position::new(42)));
    }

    #[tokio::test]
    async fn test_default_trait() {
        let store = InMemoryCheckpointStore::default();

        // Should work the same as new()
        store
            .save_checkpoint("test-subscription", Position::new(42))
            .await
            .unwrap();

        let position = store.load_checkpoint("test-subscription").await.unwrap();
        assert_eq!(position, Some(Position::new(42)));
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let store = InMemoryCheckpointStore::new();
        let store1 = store.clone();
        let store2 = store.clone();

        // Concurrent writes to different subscriptions
        let handle1 = tokio::spawn(async move {
            store1
                .save_checkpoint("subscription-1", Position::new(10))
                .await
                .unwrap();
        });

        let handle2 = tokio::spawn(async move {
            store2
                .save_checkpoint("subscription-2", Position::new(20))
                .await
                .unwrap();
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        // Both checkpoints should be saved
        let pos1 = store.load_checkpoint("subscription-1").await.unwrap();
        let pos2 = store.load_checkpoint("subscription-2").await.unwrap();

        assert_eq!(pos1, Some(Position::new(10)));
        assert_eq!(pos2, Some(Position::new(20)));
    }
}
