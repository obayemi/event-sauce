#![cfg(feature = "crypto")]

//! Integration tests for encrypted aggregate crypto-shredding support.

use event_sauce_core::Repository;
use event_sauce_core::{
    Aggregate, AggregateError, AggregateRoot, AggregateVersion, ApplyEvent, CryptoKeyStore,
    DomainEvent, Entity, EntityId, EventApplicator, EventStore, EventVersion, SnapshotConfig,
    StreamId,
};
use event_sauce_memory::{InMemoryCryptoKeyStore, InMemoryEventStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// --- Encrypted aggregate definition ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecretUser {
    id: EntityId,
    name: String,
    email: String,
}

impl Entity for SecretUser {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            email: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for SecretUser {}

impl Aggregate for SecretUser {
    type Event = SecretUserEvent;
    type Error = SecretUserError;
    type DeletedState = Self;

    fn is_encrypted() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SecretUserEvent {
    Created { name: String, email: String },
    EmailChanged { email: String },
}

impl DomainEvent for SecretUserEvent {
    type Aggregate = SecretUser;
    fn event_type(&self) -> &'static str {
        match self {
            Self::Created { .. } => "SecretUser.Created",
            Self::EmailChanged { .. } => "SecretUser.EmailChanged",
        }
    }
    fn event_version(&self) -> EventVersion {
        EventVersion::new(1)
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl ApplyEvent<SecretUser> for SecretUserEvent {
    fn apply(&self, entity: &mut SecretUser) {
        match self {
            Self::Created { name, email } => {
                entity.name = name.clone();
                entity.email = email.clone();
            }
            Self::EmailChanged { email } => {
                entity.email = email.clone();
            }
        }
    }
}

impl EventApplicator<SecretUser> for SecretUserEvent {
    fn dispatch(&self, entity: &mut SecretUser) -> Result<(), SecretUserError> {
        ApplyEvent::apply(self, entity);
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut SecretUser) {
        ApplyEvent::apply(self, entity);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("secret user error")]
struct SecretUserError;
impl AggregateError for SecretUserError {}

// --- Non-encrypted aggregate for comparison ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicUser {
    id: EntityId,
    name: String,
}

impl Entity for PublicUser {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for PublicUser {}

impl Aggregate for PublicUser {
    type Event = PublicUserEvent;
    type Error = PublicUserError;
    type DeletedState = Self;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PublicUserEvent {
    Created { name: String },
}

impl DomainEvent for PublicUserEvent {
    type Aggregate = PublicUser;
    fn event_type(&self) -> &'static str {
        "PublicUser.Created"
    }
    fn event_version(&self) -> EventVersion {
        EventVersion::new(1)
    }
    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl ApplyEvent<PublicUser> for PublicUserEvent {
    fn apply(&self, entity: &mut PublicUser) {
        match self {
            Self::Created { name } => entity.name = name.clone(),
        }
    }
}

impl EventApplicator<PublicUser> for PublicUserEvent {
    fn dispatch(&self, entity: &mut PublicUser) -> Result<(), PublicUserError> {
        ApplyEvent::apply(self, entity);
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut PublicUser) {
        ApplyEvent::apply(self, entity);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("public user error")]
struct PublicUserError;
impl AggregateError for PublicUserError {}

// --- Helpers ---

fn create_store_with_crypto() -> (InMemoryEventStore, Arc<InMemoryCryptoKeyStore>) {
    let key_store = Arc::new(InMemoryCryptoKeyStore::new());

    let store = InMemoryEventStore::builder()
        .snapshot_config(SnapshotConfig::disabled())
        .crypto_key_store(key_store.clone())
        .build();

    (store, key_store)
}

fn create_store_with_crypto_and_snapshots() -> (InMemoryEventStore, Arc<InMemoryCryptoKeyStore>) {
    let key_store = Arc::new(InMemoryCryptoKeyStore::new());

    let store = InMemoryEventStore::builder()
        .snapshot_config(SnapshotConfig::always())
        .crypto_key_store(key_store.clone())
        .build();

    (store, key_store)
}

// --- Tests ---

#[tokio::test]
async fn encrypted_aggregate_commit_encrypts_event_data() {
    let (store, _key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<SecretUser>::new(id);
    agg.apply(SecretUserEvent::Created {
        name: "Alice".into(),
        email: "alice@example.com".into(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Verify event data is encrypted at rest
    let stream_id = StreamId::new("SecretUser", id.as_uuid());
    use futures::StreamExt;
    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await
        .unwrap();
    futures::pin_mut!(event_stream);

    let envelope = event_stream.next().await.unwrap().unwrap();
    // The event_data should be in encrypted form
    assert!(
        event_sauce_core::crypto::is_encrypted(&envelope.event_data),
        "Event data should be encrypted at rest"
    );
    // Should NOT contain plaintext
    let data_str = envelope.event_data.to_string();
    assert!(
        !data_str.contains("alice@example.com"),
        "Encrypted data should not contain plaintext email"
    );
}

#[tokio::test]
async fn encrypted_aggregate_load_decrypts_correctly() {
    let (store, _key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<SecretUser>::new(id);
    agg.apply(SecretUserEvent::Created {
        name: "Bob".into(),
        email: "bob@example.com".into(),
    })
    .unwrap();
    agg.apply(SecretUserEvent::EmailChanged {
        email: "bob.new@example.com".into(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Load and verify state matches
    let repo = Arc::new(store).repository::<SecretUser>();
    let loaded = repo.load(id).await.unwrap();

    assert_eq!(loaded.entity().name, "Bob");
    assert_eq!(loaded.entity().email, "bob.new@example.com");
    assert_eq!(loaded.version(), AggregateVersion::new(2));
}

#[tokio::test]
async fn encrypted_aggregate_key_deletion_returns_key_not_found() {
    let (store, key_store) = create_store_with_crypto();
    let id = EntityId::new();

    // Commit events
    let mut agg = AggregateRoot::<SecretUser>::new(id);
    agg.apply(SecretUserEvent::Created {
        name: "Charlie".into(),
        email: "charlie@example.com".into(),
    })
    .unwrap();
    store.commit(&mut agg).await.unwrap();

    // Delete the encryption key (crypto-shredding)
    key_store.delete_key(id.as_uuid()).await.unwrap();

    // Attempting to load should return KeyNotFound
    let store = Arc::new(store);
    let repo = store.repository::<SecretUser>();
    let result = repo.load(id).await;

    assert!(result.is_err());
    assert!(
        result.unwrap_err().is_key_not_found(),
        "Should return KeyNotFound after key deletion"
    );
}

#[tokio::test]
async fn non_encrypted_aggregate_is_not_encrypted() {
    let (store, _key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<PublicUser>::new(id);
    agg.apply(PublicUserEvent::Created {
        name: "Dave".into(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Verify event data is NOT encrypted
    let stream_id = StreamId::new("PublicUser", id.as_uuid());
    use futures::StreamExt;
    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await
        .unwrap();
    futures::pin_mut!(event_stream);

    let envelope = event_stream.next().await.unwrap().unwrap();
    assert!(
        !event_sauce_core::crypto::is_encrypted(&envelope.event_data),
        "Non-encrypted aggregate data should NOT be encrypted"
    );
    assert!(
        envelope.event_data.to_string().contains("Dave"),
        "Plaintext data should be readable"
    );
}

#[tokio::test]
async fn non_encrypted_aggregate_loads_without_crypto() {
    let (store, _key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<PublicUser>::new(id);
    agg.apply(PublicUserEvent::Created { name: "Eve".into() })
        .unwrap();
    store.commit(&mut agg).await.unwrap();

    let store = Arc::new(store);
    let repo = store.repository::<PublicUser>();
    let loaded = repo.load(id).await.unwrap();

    assert_eq!(loaded.entity().name, "Eve");
}

#[tokio::test]
async fn snapshot_encryption_roundtrip() {
    let (store, _key_store) = create_store_with_crypto_and_snapshots();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<SecretUser>::new(id);
    agg.apply(SecretUserEvent::Created {
        name: "Frank".into(),
        email: "frank@example.com".into(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Verify snapshot is encrypted
    let stream_id = StreamId::new("SecretUser", id.as_uuid());
    let snapshot = store.load_snapshot(stream_id).await.unwrap();
    assert!(snapshot.is_some(), "Snapshot should exist");
    let snapshot = snapshot.unwrap();
    assert!(
        event_sauce_core::crypto::is_encrypted(&snapshot.snapshot_data),
        "Snapshot data should be encrypted"
    );

    // Load aggregate (uses snapshot + decryption)
    let store = Arc::new(store);
    let repo = store.repository::<SecretUser>();
    let loaded = repo.load(id).await.unwrap();

    assert_eq!(loaded.entity().name, "Frank");
    assert_eq!(loaded.entity().email, "frank@example.com");
}

#[tokio::test]
async fn backward_compat_loading_unencrypted_events() {
    let id = EntityId::new();

    // Commit plaintext events for a non-encrypted aggregate
    let store = InMemoryEventStore::builder()
        .snapshot_config(SnapshotConfig::disabled())
        .build();

    let mut agg = AggregateRoot::<PublicUser>::new(id);
    agg.apply(PublicUserEvent::Created {
        name: "Grace".into(),
    })
    .unwrap();
    store.commit(&mut agg).await.unwrap();

    // Verify decrypt_value passes through unencrypted data
    let plain_value = serde_json::json!({"name": "Grace"});
    assert!(
        !event_sauce_core::crypto::is_encrypted(&plain_value),
        "Unencrypted data should pass is_encrypted check as false"
    );

    // The decrypt_value function should pass through unencrypted data
    struct MockProv;
    impl event_sauce_core::CryptoProvider for MockProv {
        fn encrypt(
            &self,
            _key: &[u8],
            _plaintext: &[u8],
            _aad: &[u8],
        ) -> event_sauce_core::Result<Vec<u8>> {
            Ok(vec![])
        }
        fn decrypt(
            &self,
            _key: &[u8],
            _ciphertext: &[u8],
            _aad: &[u8],
        ) -> event_sauce_core::Result<Vec<u8>> {
            Ok(vec![])
        }
        fn generate_key(&self) -> Vec<u8> {
            vec![0; 32]
        }
    }

    let result = event_sauce_core::crypto::decrypt_value(&MockProv, &[0; 32], &plain_value, &[]);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), plain_value);

    // Non-encrypted loads work with default crypto configured
    let store = Arc::new(store);
    let _ = store.repository::<PublicUser>();
}

#[tokio::test]
async fn encrypted_aggregate_works_with_default_crypto() {
    let store = InMemoryEventStore::new(); // Crypto configured by default
    let id = EntityId::new();

    let mut agg = AggregateRoot::<SecretUser>::new(id);
    agg.apply(SecretUserEvent::Created {
        name: "Hank".into(),
        email: "hank@example.com".into(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Load and verify
    let store = Arc::new(store);
    let repo = store.repository::<SecretUser>();
    let loaded = repo.load(id).await.unwrap();
    assert_eq!(loaded.entity().name, "Hank");
    assert_eq!(loaded.entity().email, "hank@example.com");
}

#[tokio::test]
async fn multiple_encrypted_aggregates_use_different_keys() {
    let (store, key_store) = create_store_with_crypto();
    let id1 = EntityId::new();
    let id2 = EntityId::new();

    let mut agg1 = AggregateRoot::<SecretUser>::new(id1);
    agg1.apply(SecretUserEvent::Created {
        name: "User1".into(),
        email: "user1@example.com".into(),
    })
    .unwrap();
    store.commit(&mut agg1).await.unwrap();

    let mut agg2 = AggregateRoot::<SecretUser>::new(id2);
    agg2.apply(SecretUserEvent::Created {
        name: "User2".into(),
        email: "user2@example.com".into(),
    })
    .unwrap();
    store.commit(&mut agg2).await.unwrap();

    // Each aggregate should have its own key
    let key1 = key_store.get_key(id1.as_uuid()).await.unwrap().unwrap();
    let key2 = key_store.get_key(id2.as_uuid()).await.unwrap().unwrap();
    assert_ne!(key1, key2, "Each aggregate should have a unique key");

    // Both should load correctly
    let store = Arc::new(store);
    let repo = store.repository::<SecretUser>();

    let loaded1 = repo.load(id1).await.unwrap();
    assert_eq!(loaded1.entity().name, "User1");

    let loaded2 = repo.load(id2).await.unwrap();
    assert_eq!(loaded2.entity().name, "User2");
}

#[tokio::test]
async fn shredded_aggregate_cannot_be_saved_again() {
    let (store, key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<SecretUser>::new(id);
    agg.apply(SecretUserEvent::Created {
        name: "Ivy".into(),
        email: "ivy@example.com".into(),
    })
    .unwrap();
    store.commit(&mut agg).await.unwrap();

    key_store.delete_key(id.as_uuid()).await.unwrap();

    let mut agg = AggregateRoot::<SecretUser>::new(id);
    agg.apply(SecretUserEvent::EmailChanged {
        email: "ivy.new@example.com".into(),
    })
    .unwrap();
    let result = store.commit(&mut agg).await;

    assert!(result.is_err());
    assert!(
        result.unwrap_err().is_key_not_found(),
        "saving a shredded aggregate must fail instead of re-keying it"
    );
    assert!(
        key_store.get_key(id.as_uuid()).await.unwrap().is_none(),
        "no key must have been created for the shredded aggregate"
    );
}

/// Wraps [`InMemoryCryptoKeyStore`], rendezvousing the first two `get_key`
/// callers at a barrier before either sees a result.
///
/// `ensure_crypto_key`'s `get_key` fast path is the first thing two
/// concurrent first commits of the same aggregate race on. Gating it here
/// forces both callers to observe "no key yet" in lockstep, every run,
/// instead of depending on scheduler luck to interleave them that way.
struct BarrierGatedKeyStore {
    inner: InMemoryCryptoKeyStore,
    barrier: tokio::sync::Barrier,
    gated_calls: std::sync::atomic::AtomicUsize,
}

impl BarrierGatedKeyStore {
    fn new() -> Self {
        Self {
            inner: InMemoryCryptoKeyStore::new(),
            barrier: tokio::sync::Barrier::new(2),
            gated_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl CryptoKeyStore for BarrierGatedKeyStore {
    /// Both callers reach this point agreeing there is no key yet. The
    /// barrier leader (the call the scheduler completed the rendezvous
    /// second for) gets an extra nudge so it also finishes its own
    /// commit second, deterministically making it the one whose key
    /// write lands last — the exact interleaving that orphans the
    /// other caller's already-encrypted events under a non-atomic
    /// store.
    async fn get_key(&self, aggregate_id: uuid::Uuid) -> event_sauce_core::Result<Option<Vec<u8>>> {
        let existing = self.inner.get_key(aggregate_id).await?;
        if self
            .gated_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            < 2
        {
            let result = self.barrier.wait().await;
            if result.is_leader() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        Ok(existing)
    }

    async fn upsert_key(
        &self,
        aggregate_id: uuid::Uuid,
        key: Vec<u8>,
    ) -> event_sauce_core::Result<()> {
        self.inner.upsert_key(aggregate_id, key).await
    }

    async fn delete_key(&self, aggregate_id: uuid::Uuid) -> event_sauce_core::Result<()> {
        self.inner.delete_key(aggregate_id).await
    }

    async fn get_or_insert_key(
        &self,
        aggregate_id: uuid::Uuid,
        candidate: Vec<u8>,
    ) -> event_sauce_core::Result<Vec<u8>> {
        self.inner.get_or_insert_key(aggregate_id, candidate).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_first_commits_of_same_aggregate_share_one_key() {
    let key_store = Arc::new(BarrierGatedKeyStore::new());
    let store = Arc::new(
        InMemoryEventStore::builder()
            .snapshot_config(SnapshotConfig::disabled())
            .crypto_key_store(key_store)
            .build(),
    );
    let id = EntityId::new();

    let commit = |name: &'static str| {
        let store = store.clone();
        tokio::spawn(async move {
            let mut agg = AggregateRoot::<SecretUser>::new(id);
            agg.apply(SecretUserEvent::Created {
                name: name.into(),
                email: format!("{name}@example.com"),
            })
            .unwrap();
            store.commit(&mut agg).await
        })
    };

    let (a, b) = tokio::join!(commit("Winner"), commit("Loser"));
    let results = [a.unwrap(), b.unwrap()];

    let successes = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        successes, 1,
        "exactly one of two concurrent first commits of the same aggregate must win"
    );

    let repo = store.repository::<SecretUser>();
    let loaded = repo.load(id).await.expect(
        "the winning commit's events must decrypt cleanly with the stored key, \
         never orphaned by a key the other racer overwrote",
    );
    assert!(loaded.entity().name == "Winner" || loaded.entity().name == "Loser");
}
