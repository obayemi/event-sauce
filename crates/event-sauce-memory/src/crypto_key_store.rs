//! In-memory crypto key store implementation.
//!
//! Provides a thread-safe in-memory implementation of [`CryptoKeyStore`]
//! for testing and development.

use async_trait::async_trait;
use event_sauce_core::{CryptoKeyStore, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use uuid::Uuid;

/// In-memory crypto key store for testing and development.
///
/// Stores encryption keys in a `HashMap` behind a `RwLock`.
///
/// # Examples
///
/// ```
/// use event_sauce_memory::InMemoryCryptoKeyStore;
///
/// let store = InMemoryCryptoKeyStore::new();
/// ```
#[derive(Debug, Default)]
pub struct InMemoryCryptoKeyStore {
    keys: RwLock<HashMap<Uuid, Vec<u8>>>,
}

impl InMemoryCryptoKeyStore {
    /// Creates a new empty in-memory crypto key store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl CryptoKeyStore for InMemoryCryptoKeyStore {
    async fn get_key(&self, aggregate_id: Uuid) -> Result<Option<Vec<u8>>> {
        Ok(self.keys.read().get(&aggregate_id).cloned())
    }

    async fn upsert_key(&self, aggregate_id: Uuid, key: Vec<u8>) -> Result<()> {
        self.keys.write().insert(aggregate_id, key);
        Ok(())
    }

    async fn delete_key(&self, aggregate_id: Uuid) -> Result<()> {
        self.keys.write().remove(&aggregate_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::CryptoKeyStore;

    #[tokio::test]
    async fn get_returns_none_for_unknown_id() {
        let store = InMemoryCryptoKeyStore::new();
        let result = store.get_key(Uuid::new_v4()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn upsert_then_get_returns_key() {
        let store = InMemoryCryptoKeyStore::new();
        let id = Uuid::new_v4();
        let key = vec![1, 2, 3, 4];

        store.upsert_key(id, key.clone()).await.unwrap();
        let retrieved = store.get_key(id).await.unwrap();

        assert_eq!(retrieved, Some(key));
    }

    #[tokio::test]
    async fn upsert_overwrites_existing_key() {
        let store = InMemoryCryptoKeyStore::new();
        let id = Uuid::new_v4();

        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();
        store.upsert_key(id, vec![4, 5, 6]).await.unwrap();

        let retrieved = store.get_key(id).await.unwrap().unwrap();
        assert_eq!(retrieved, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let store = InMemoryCryptoKeyStore::new();
        let id = Uuid::new_v4();

        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();
        assert!(store.get_key(id).await.unwrap().is_some());

        store.delete_key(id).await.unwrap();
        assert!(store.get_key(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_key_is_idempotent() {
        let store = InMemoryCryptoKeyStore::new();
        let id = Uuid::new_v4();

        // Should not error
        store.delete_key(id).await.unwrap();
    }

    #[tokio::test]
    async fn keys_are_isolated_per_aggregate() {
        let store = InMemoryCryptoKeyStore::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        store.upsert_key(id1, vec![1]).await.unwrap();
        store.upsert_key(id2, vec![2]).await.unwrap();

        assert_eq!(store.get_key(id1).await.unwrap(), Some(vec![1]));
        assert_eq!(store.get_key(id2).await.unwrap(), Some(vec![2]));

        store.delete_key(id1).await.unwrap();

        assert!(store.get_key(id1).await.unwrap().is_none());
        assert_eq!(store.get_key(id2).await.unwrap(), Some(vec![2]));
    }

    #[test]
    fn default_creates_empty_store() {
        let store = InMemoryCryptoKeyStore::default();
        assert!(store.keys.read().is_empty());
    }
}
