//! In-memory crypto key store implementation.
//!
//! Provides a thread-safe in-memory implementation of [`CryptoKeyStore`]
//! for testing and development.

use async_trait::async_trait;
use event_sauce_core::{CryptoKeyStore, Error, Result};
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use uuid::Uuid;

/// One aggregate's key-store slot: either an active key, or a tombstone left
/// by [`delete_key`](CryptoKeyStore::delete_key).
///
/// Modeling both states in one enum, behind one lock, makes "has a key AND
/// is shredded" unrepresentable — the bug a pair of separate `keys` /
/// `shredded` collections invited, since each was mutated under its own lock
/// and a reader could observe them out of sync.
#[derive(Debug, Clone)]
enum KeySlot {
    /// An active encryption key.
    Active(Vec<u8>),
    /// The aggregate was crypto-shredded; no key is stored.
    Shredded,
}

/// In-memory crypto key store for testing and development.
///
/// Stores each aggregate's key slot (active or shredded) in a `HashMap`
/// behind a single `RwLock`, so every operation reads or writes a
/// consistent state.
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
    keys: RwLock<HashMap<Uuid, KeySlot>>,
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
        Ok(match self.keys.read().get(&aggregate_id) {
            Some(KeySlot::Active(key)) => Some(key.clone()),
            Some(KeySlot::Shredded) | None => None,
        })
    }

    async fn upsert_key(&self, aggregate_id: Uuid, key: Vec<u8>) -> Result<()> {
        self.keys.write().insert(aggregate_id, KeySlot::Active(key));
        Ok(())
    }

    async fn delete_key(&self, aggregate_id: Uuid) -> Result<()> {
        self.keys.write().insert(aggregate_id, KeySlot::Shredded);
        Ok(())
    }

    async fn get_or_insert_key(&self, aggregate_id: Uuid, candidate: Vec<u8>) -> Result<Vec<u8>> {
        match self.keys.write().entry(aggregate_id) {
            Entry::Occupied(entry) => match entry.get() {
                KeySlot::Active(key) => Ok(key.clone()),
                KeySlot::Shredded => Err(Error::key_not_found(aggregate_id)),
            },
            Entry::Vacant(entry) => {
                entry.insert(KeySlot::Active(candidate.clone()));
                Ok(candidate)
            }
        }
    }

    async fn is_shredded(&self, aggregate_id: Uuid) -> Result<bool> {
        Ok(matches!(
            self.keys.read().get(&aggregate_id),
            Some(KeySlot::Shredded)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_sauce_core::CryptoKeyStore;
    use std::sync::Arc;

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

    #[tokio::test]
    async fn is_shredded_is_false_before_any_delete() {
        let store = InMemoryCryptoKeyStore::new();
        let id = Uuid::new_v4();
        assert!(!store.is_shredded(id).await.unwrap());
    }

    #[tokio::test]
    async fn delete_key_marks_the_aggregate_shredded() {
        let store = InMemoryCryptoKeyStore::new();
        let id = Uuid::new_v4();
        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();

        store.delete_key(id).await.unwrap();

        assert!(store.is_shredded(id).await.unwrap());
    }

    #[tokio::test]
    async fn get_or_insert_key_fails_for_a_shredded_aggregate() {
        let store = InMemoryCryptoKeyStore::new();
        let id = Uuid::new_v4();
        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();
        store.delete_key(id).await.unwrap();

        let result = store.get_or_insert_key(id, vec![9, 9, 9]).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().is_key_not_found());
        assert!(store.get_key(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn upsert_key_clears_a_shredded_marker() {
        let store = InMemoryCryptoKeyStore::new();
        let id = Uuid::new_v4();
        store.upsert_key(id, vec![1, 2, 3]).await.unwrap();
        store.delete_key(id).await.unwrap();

        store.upsert_key(id, vec![4, 5, 6]).await.unwrap();

        assert!(!store.is_shredded(id).await.unwrap());
        assert_eq!(store.get_key(id).await.unwrap(), Some(vec![4, 5, 6]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_get_or_insert_key_agree_on_one_winner() {
        const RACERS: u8 = 32;
        const ROUNDS: usize = 20;

        for _ in 0..ROUNDS {
            let store = Arc::new(InMemoryCryptoKeyStore::new());
            let id = Uuid::new_v4();
            let barrier = Arc::new(tokio::sync::Barrier::new(RACERS as usize));

            let handles = (0..RACERS).map(|n| {
                let store = store.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    store.get_or_insert_key(id, vec![n; 32]).await.unwrap()
                })
            });

            let winners: Vec<Vec<u8>> = futures::future::join_all(handles)
                .await
                .into_iter()
                .map(|joined| joined.unwrap())
                .collect();

            let first = winners[0].clone();
            assert!(
                winners.iter().all(|w| *w == first),
                "every racer must agree on the same winning key, got {winners:?}"
            );
            assert_eq!(store.get_key(id).await.unwrap().unwrap(), first);
        }
    }
}
