#![cfg(feature = "crypto")]

//! Integration tests for field-level encryption support.

use event_sauce_core::Repository;
use event_sauce_core::{
    define_events, Aggregate, AggregateError, AggregateRoot, AggregateVersion, CryptoKeyStore,
    DomainEvent, Entity, EntityId, EventStore, SnapshotConfig, StreamId,
};
use event_sauce_memory::{InMemoryCryptoKeyStore, InMemoryEventStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// --- Field-encrypted aggregate definition (NOT fully encrypted) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Patient {
    id: EntityId,
    name: String,
    diagnosis: String,
    visit_count: i32,
}

impl Entity for Patient {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            diagnosis: String::new(),
            visit_count: 0,
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for Patient {}

#[derive(Debug, thiserror::Error)]
#[error("patient error")]
struct PatientError;
impl AggregateError for PatientError {}

impl Aggregate for Patient {
    type Event = PatientEvent;
    type Error = PatientError;
    type DeletedState = Self;
}

define_events! {
    enum PatientEvent for Patient {
        Registered {
            name: String,
            diagnosis: String,
            visit_count: i32,
        }
        @encrypted_fields(name, diagnosis)
        => |patient, event| {
            patient.name = event.name.clone();
            patient.diagnosis = event.diagnosis.clone();
            patient.visit_count = event.visit_count;
        },

        VisitRecorded {
            visit_count: i32,
        }
        => |patient, event| {
            patient.visit_count = event.visit_count;
        },
    }
}

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
async fn field_encrypted_commit_encrypts_only_specified_fields() {
    let (store, _key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<Patient>::new(id);
    agg.apply(PatientEvent::Registered {
        name: "Alice".into(),
        diagnosis: "flu".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Read raw event data from store
    let stream_id = StreamId::new("Patient", id.as_uuid());
    use futures::StreamExt;
    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await
        .unwrap();
    futures::pin_mut!(event_stream);

    let envelope = event_stream.next().await.unwrap().unwrap();

    // The whole event should NOT be fully encrypted
    assert!(
        !event_sauce_core::crypto::is_encrypted(&envelope.event_data),
        "Field-encrypted event should NOT be fully encrypted"
    );

    let data_str = envelope.event_data.to_string();

    // name and diagnosis should be encrypted (not readable as plaintext)
    assert!(
        !data_str.contains("Alice"),
        "name should be encrypted (no plaintext 'Alice')"
    );
    assert!(
        !data_str.contains("flu"),
        "diagnosis should be encrypted (no plaintext 'flu')"
    );

    // visit_count should be in plaintext
    assert!(
        data_str.contains("1"),
        "visit_count should remain in plaintext"
    );

    // Encrypted fields should have the __encrypted marker
    assert!(
        event_sauce_core::crypto::is_encrypted(&envelope.event_data["name"]),
        "name field should be encrypted"
    );
    assert!(
        event_sauce_core::crypto::is_encrypted(&envelope.event_data["diagnosis"]),
        "diagnosis field should be encrypted"
    );
}

#[tokio::test]
async fn field_encrypted_non_encrypted_event_stored_plaintext() {
    let (store, _key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<Patient>::new(id);
    agg.apply(PatientEvent::Registered {
        name: "Alice".into(),
        diagnosis: "flu".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();
    agg.apply(PatientEvent::VisitRecorded {
        visit_count: 2,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Read raw events
    let stream_id = StreamId::new("Patient", id.as_uuid());
    use futures::StreamExt;
    let event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await
        .unwrap();
    futures::pin_mut!(event_stream);

    // Skip first event (Registered)
    let _first = event_stream.next().await.unwrap().unwrap();

    // Second event (VisitRecorded) should be fully plaintext
    let second = event_stream.next().await.unwrap().unwrap();
    assert!(
        !event_sauce_core::crypto::has_encrypted_fields(&second.event_data),
        "VisitRecorded event should have no encrypted fields"
    );
    assert!(
        second.event_data.to_string().contains('2'),
        "visit_count should be plaintext"
    );
}

#[tokio::test]
async fn field_encrypted_load_roundtrip() {
    let (store, _key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<Patient>::new(id);
    agg.apply(PatientEvent::Registered {
        name: "Bob".into(),
        diagnosis: "cold".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();
    agg.apply(PatientEvent::VisitRecorded {
        visit_count: 2,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Load and verify state is fully restored
    let store = Arc::new(store);
    let repo = store.repository::<Patient>();
    let loaded = repo.load(id).await.unwrap();

    assert_eq!(loaded.entity().name, "Bob");
    assert_eq!(loaded.entity().diagnosis, "cold");
    assert_eq!(loaded.entity().visit_count, 2);
    assert_eq!(loaded.version(), AggregateVersion::new(2));
}

#[tokio::test]
async fn field_encrypted_crypto_shredding() {
    let (store, key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<Patient>::new(id);
    agg.apply(PatientEvent::Registered {
        name: "Charlie".into(),
        diagnosis: "headache".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Delete the crypto key (crypto-shredding)
    key_store.delete_key(id.as_uuid()).await.unwrap();

    // Loading should fail because the encrypted fields cannot be decrypted.
    // Crypto-shredding a field-encrypted aggregate surfaces as `KeyNotFound`,
    // unified with full encryption.
    let store = Arc::new(store);
    let repo = store.repository::<Patient>();
    let result = repo.load(id).await;

    assert!(
        result.unwrap_err().is_key_not_found(),
        "Load should fail with KeyNotFound after crypto-shredding field-encrypted aggregate"
    );
}

#[tokio::test]
async fn field_encrypted_snapshot_encryption() {
    let (store, _key_store) = create_store_with_crypto_and_snapshots();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<Patient>::new(id);
    agg.apply(PatientEvent::Registered {
        name: "Diana".into(),
        diagnosis: "flu".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    store.commit(&mut agg).await.unwrap();

    // Verify snapshot is encrypted (full encryption to prevent plaintext leaks)
    let stream_id = StreamId::new("Patient", id.as_uuid());
    let snapshot = store.load_snapshot(stream_id).await.unwrap();
    assert!(snapshot.is_some(), "Snapshot should exist");
    let snapshot = snapshot.unwrap();
    assert!(
        event_sauce_core::crypto::is_encrypted(&snapshot.snapshot_data),
        "Snapshot for field-encrypted aggregate should be fully encrypted"
    );

    // Load via snapshot should still work
    let store = Arc::new(store);
    let repo = store.repository::<Patient>();
    let loaded = repo.load(id).await.unwrap();

    assert_eq!(loaded.entity().name, "Diana");
    assert_eq!(loaded.entity().diagnosis, "flu");
    assert_eq!(loaded.entity().visit_count, 1);
}

/// Crypto provider whose `encrypt` always fails — used to prove the snapshot
/// encryption path fails closed (never persists plaintext) when encryption is
/// impossible.
struct FailingEncryptProvider;

impl event_sauce_core::CryptoProvider for FailingEncryptProvider {
    fn encrypt(
        &self,
        _key: &[u8],
        _plaintext: &[u8],
        _aad: &[u8],
    ) -> event_sauce_core::Result<Vec<u8>> {
        Err(event_sauce_core::Error::encryption("encrypt always fails"))
    }
    fn decrypt(
        &self,
        _key: &[u8],
        _ciphertext: &[u8],
        _aad: &[u8],
    ) -> event_sauce_core::Result<Vec<u8>> {
        Err(event_sauce_core::Error::encryption("decrypt always fails"))
    }
    fn generate_key(&self) -> Vec<u8> {
        vec![0x42; 32]
    }
}

/// H3: snapshot encryption for an aggregate that REQUIRES encryption must FAIL
/// CLOSED — never persist plaintext.
///
/// We commit only a `VisitRecorded` event, which has NO `@encrypted_fields`, so
/// the event-encryption path is skipped entirely (`encrypt_fields` is never
/// called). Yet because `PatientEvent::has_any_encrypted_fields()` is true, the
/// snapshot MUST be fully encrypted. With a provider whose `encrypt` always
/// fails and snapshots-on-every-commit, the snapshot cannot be encrypted, so
/// `commit()` MUST return an error and MUST NOT persist a plaintext snapshot.
///
/// Today `encrypt_snapshot_data` swallows the encrypt error (warn + fall
/// through), so commit succeeds and a PLAINTEXT snapshot containing `name`,
/// `diagnosis`, etc. is stored — a confidentiality hole.
#[tokio::test]
async fn snapshot_fails_closed_when_encryption_fails() {
    let key_store = Arc::new(InMemoryCryptoKeyStore::new());
    let store = InMemoryEventStore::builder()
        .snapshot_config(SnapshotConfig::always())
        .crypto_key_store(key_store)
        .crypto_provider(Arc::new(FailingEncryptProvider))
        .build();

    let id = EntityId::new();
    let mut agg = AggregateRoot::<Patient>::new(id);
    // VisitRecorded has no encrypted fields: event-encryption is a no-op, so the
    // only crypto operation is snapshot encryption — isolating the H3 path.
    agg.apply(PatientEvent::VisitRecorded {
        visit_count: 5,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    let result = store.commit(&mut agg).await;
    assert!(
        result.is_err(),
        "commit must fail closed when snapshot encryption fails for an encryption-required aggregate"
    );
    let err = result.unwrap_err();
    assert!(
        err.is_encryption() || err.is_key_not_found(),
        "expected encryption/key-not-found error, got: {err:?}"
    );

    // Any persisted snapshot must be ciphertext, never plaintext.
    let stream_id = StreamId::new("Patient", id.as_uuid());
    if let Some(snapshot) = store.load_snapshot(stream_id).await.unwrap() {
        assert!(
            event_sauce_core::crypto::is_encrypted(&snapshot.snapshot_data),
            "any persisted snapshot for an encryption-required aggregate must be ciphertext, never plaintext"
        );
    }
}

/// H4: crypto-shredding a field-encrypted aggregate must surface as
/// `KeyNotFound`, exactly like full encryption. Snapshots disabled, so the
/// failure must come from the event stream's embedded `__encrypted` blobs.
///
/// Today load succeeds with `__encrypted` blobs left in the events (key treated
/// as optional, decrypt becomes a no-op) — so this returns Ok, not KeyNotFound.
#[tokio::test]
async fn field_encrypted_crypto_shredding_returns_key_not_found() {
    let (store, key_store) = create_store_with_crypto();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<Patient>::new(id);
    agg.apply(PatientEvent::Registered {
        name: "Frank".into(),
        diagnosis: "headache".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();
    store.commit(&mut agg).await.unwrap();

    // Delete the crypto key out from under existing data (crypto-shredding).
    key_store.delete_key(id.as_uuid()).await.unwrap();

    let store = Arc::new(store);
    let repo = store.repository::<Patient>();
    let result = repo.load(id).await;

    assert!(result.is_err(), "load must fail after key deletion");
    assert!(
        result.unwrap_err().is_key_not_found(),
        "field-encrypted shred must surface as KeyNotFound (unified with full encryption)"
    );
}

/// H4: same as above but a fully-encrypted snapshot exists. Load must still
/// surface `KeyNotFound`, not an opaque `Error::custom` from `from_value`
/// failing to deserialize ciphertext.
#[tokio::test]
async fn field_encrypted_crypto_shredding_with_snapshot_returns_key_not_found() {
    let (store, key_store) = create_store_with_crypto_and_snapshots();
    let id = EntityId::new();

    let mut agg = AggregateRoot::<Patient>::new(id);
    agg.apply(PatientEvent::Registered {
        name: "Grace".into(),
        diagnosis: "flu".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();
    store.commit(&mut agg).await.unwrap();

    key_store.delete_key(id.as_uuid()).await.unwrap();

    let store = Arc::new(store);
    let repo = store.repository::<Patient>();
    let result = repo.load(id).await;

    assert!(result.is_err(), "load must fail after key deletion");
    let err = result.unwrap_err();
    assert!(
        err.is_key_not_found(),
        "expected KeyNotFound for shredded field-encrypted aggregate with snapshot, got: {err:?}"
    );
}

/// H4 GUARD (must stay green): a field-encrypted aggregate with NO committed
/// data and an absent key must NOT report `KeyNotFound`. With no events and no
/// snapshot, there is genuinely nothing to read — the correct outcome is
/// `NotFound` (after F7a), never a spurious key-not-found.
#[tokio::test]
async fn field_encrypted_no_data_no_key_is_not_key_not_found() {
    let (store, _key_store) = create_store_with_crypto();
    let id = EntityId::new(); // never committed; key store present but empty

    let store = Arc::new(store);
    let repo = store.repository::<Patient>();
    let result = repo.load(id).await;

    assert!(
        result.is_err(),
        "loading a never-committed aggregate should be an error (NotFound)"
    );
    let err = result.unwrap_err();
    assert!(
        !err.is_key_not_found(),
        "no data + no key must NOT be KeyNotFound (nothing to shred), got: {err:?}"
    );
    assert!(
        err.is_not_found(),
        "no data should surface as NotFound, got: {err:?}"
    );
}

#[tokio::test]
async fn field_encrypted_works_with_default_crypto() {
    let store = InMemoryEventStore::new(); // Crypto included by default
    let id = EntityId::new();

    let mut agg = AggregateRoot::<Patient>::new(id);
    agg.apply(PatientEvent::Registered {
        name: "Eve".into(),
        diagnosis: "flu".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })
    .unwrap();

    let result = store.commit(&mut agg).await;
    assert!(
        result.is_ok(),
        "Field-encrypted events should work with default crypto"
    );
}

#[tokio::test]
async fn field_encrypted_aggregate_is_not_fully_encrypted() {
    // Verify the aggregate itself is NOT marked as fully encrypted
    assert!(
        !Patient::is_encrypted(),
        "Field-encrypted aggregate should not be fully encrypted"
    );
    assert!(
        <PatientEvent as DomainEvent>::has_any_encrypted_fields(),
        "Should report having encrypted fields"
    );
}
