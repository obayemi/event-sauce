//! In-memory checkpoint store implementation.
//!
//! Provides a fast, thread-safe in-memory implementation of `CheckpointStore`
//! suitable for testing and development.

use async_trait::async_trait;
use event_sauce_core::{CheckpointStore, Error, Position, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    inner: Arc<RwLock<HashMap<String, Entry>>>,
}

#[derive(Clone)]
struct Entry {
    position: Position,
    lease: Option<Lease>,
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            position: Position::start(),
            lease: None,
        }
    }
}

#[derive(Clone)]
struct Lease {
    worker_id: String,
    expires_at: Instant,
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
        let mut guard = self.inner.write().await;
        let entry = guard.entry(subscription_name.to_string()).or_default();
        entry.position = position;
        Ok(())
    }

    async fn load_checkpoint(&self, subscription_name: &str) -> Result<Option<Position>> {
        Ok(self
            .inner
            .read()
            .await
            .get(subscription_name)
            .map(|entry| entry.position))
    }

    async fn delete_checkpoint(&self, subscription_name: &str) -> Result<()> {
        self.inner.write().await.remove(subscription_name);
        Ok(())
    }

    async fn try_acquire_lease(
        &self,
        subscription_name: &str,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<Position>> {
        let mut guard = self.inner.write().await;
        let now = Instant::now();
        let entry = guard.entry(subscription_name.to_string()).or_default();

        let can_take = match &entry.lease {
            None => true,
            Some(lease) => lease.expires_at <= now || lease.worker_id == worker_id,
        };
        if !can_take {
            return Ok(None);
        }
        entry.lease = Some(Lease {
            worker_id: worker_id.to_string(),
            expires_at: now + lease_duration,
        });
        Ok(Some(entry.position))
    }

    async fn renew_lease(
        &self,
        subscription_name: &str,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<()> {
        let mut guard = self.inner.write().await;
        let now = Instant::now();
        let Some(entry) = guard.get_mut(subscription_name) else {
            return Err(Error::custom(format!(
                "lease lost: no entry for {subscription_name}"
            )));
        };
        match &entry.lease {
            Some(lease) if lease.worker_id == worker_id && lease.expires_at > now => {
                entry.lease = Some(Lease {
                    worker_id: worker_id.to_string(),
                    expires_at: now + lease_duration,
                });
                Ok(())
            }
            _ => Err(Error::custom(format!(
                "lease lost: not held by {worker_id}"
            ))),
        }
    }

    async fn release_lease(&self, subscription_name: &str, worker_id: &str) -> Result<()> {
        let mut guard = self.inner.write().await;
        if let Some(entry) = guard.get_mut(subscription_name) {
            if let Some(lease) = &entry.lease {
                if lease.worker_id == worker_id {
                    entry.lease = None;
                }
            }
        }
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
    async fn test_acquire_lease_on_empty_creates_entry_at_start() {
        let store = InMemoryCheckpointStore::new();
        let pos = store
            .try_acquire_lease("sub", "worker-a", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(pos, Some(Position::start()));
    }

    #[tokio::test]
    async fn test_second_worker_cannot_acquire_active_lease() {
        let store = InMemoryCheckpointStore::new();
        store
            .try_acquire_lease("sub", "worker-a", Duration::from_secs(60))
            .await
            .unwrap();
        let result = store
            .try_acquire_lease("sub", "worker-b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(result.is_none(), "second worker should be blocked");
    }

    #[tokio::test]
    async fn test_same_worker_can_re_acquire_own_lease() {
        let store = InMemoryCheckpointStore::new();
        store
            .try_acquire_lease("sub", "worker-a", Duration::from_secs(60))
            .await
            .unwrap();
        let result = store
            .try_acquire_lease("sub", "worker-a", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(result.is_some(), "same worker should refresh its lease");
    }

    #[tokio::test]
    async fn test_expired_lease_can_be_taken() {
        let store = InMemoryCheckpointStore::new();
        // Acquire with a very short duration
        store
            .try_acquire_lease("sub", "worker-a", Duration::from_millis(10))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = store
            .try_acquire_lease("sub", "worker-b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "expired lease should be reclaimable by another worker"
        );
    }

    #[tokio::test]
    async fn test_renew_extends_lease() {
        let store = InMemoryCheckpointStore::new();
        store
            .try_acquire_lease("sub", "worker-a", Duration::from_millis(50))
            .await
            .unwrap();
        store
            .renew_lease("sub", "worker-a", Duration::from_secs(60))
            .await
            .unwrap();
        // Wait past the original expiry
        tokio::time::sleep(Duration::from_millis(100)).await;
        let result = store
            .try_acquire_lease("sub", "worker-b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(result.is_none(), "renewed lease should still be active");
    }

    #[tokio::test]
    async fn test_renew_errors_when_not_held() {
        let store = InMemoryCheckpointStore::new();
        let result = store
            .renew_lease("sub", "worker-a", Duration::from_secs(60))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_release_allows_other_worker() {
        let store = InMemoryCheckpointStore::new();
        store
            .try_acquire_lease("sub", "worker-a", Duration::from_secs(60))
            .await
            .unwrap();
        store.release_lease("sub", "worker-a").await.unwrap();
        let result = store
            .try_acquire_lease("sub", "worker-b", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            result.is_some(),
            "released lease should be acquirable by another worker"
        );
    }

    #[tokio::test]
    async fn test_release_is_idempotent() {
        let store = InMemoryCheckpointStore::new();
        // Releasing without ever acquiring is a no-op
        store.release_lease("sub", "worker-a").await.unwrap();
        // Releasing twice is a no-op
        store
            .try_acquire_lease("sub", "worker-a", Duration::from_secs(60))
            .await
            .unwrap();
        store.release_lease("sub", "worker-a").await.unwrap();
        store.release_lease("sub", "worker-a").await.unwrap();
    }

    #[tokio::test]
    async fn test_lease_returns_existing_position() {
        let store = InMemoryCheckpointStore::new();
        store
            .save_checkpoint("sub", Position::new(100))
            .await
            .unwrap();
        let pos = store
            .try_acquire_lease("sub", "worker-a", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(pos, Some(Position::new(100)));
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
