//! Integration tests for field-level encryption support.

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

    // Loading should fail because the encrypted fields cannot be decrypted
    let store = Arc::new(store);
    let repo = store.repository::<Patient>();
    let result = repo.load(id).await;

    assert!(
        result.is_err(),
        "Load should fail after crypto-shredding field-encrypted aggregate"
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
