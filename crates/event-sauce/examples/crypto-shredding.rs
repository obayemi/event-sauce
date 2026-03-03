//! # Crypto-Shredding Example — Privacy & Right to Be Forgotten
//!
//! This example demonstrates **crypto-shredding** for GDPR-style privacy:
//!
//! - **Encrypted aggregate** (`Patient`) with `is_encrypted() -> true` — event data is encrypted at rest
//! - **Public aggregate** (`Counter`) with default `is_encrypted() -> false` — data stored as plaintext
//! - **Transparent encryption/decryption** — `commit()` encrypts, `load()` decrypts automatically
//! - **Encryption verification** — raw event stream inspection proves data is encrypted
//! - **Crypto-shredding** — deleting the key makes the aggregate permanently unloadable
//!
//! ## How it works
//!
//! Each encrypted aggregate instance gets its own AES-256-GCM encryption key, stored in a
//! `CryptoKeyStore`. When you call `commit()`, event and snapshot data is encrypted before
//! storage. When you call `load()`, it is decrypted transparently. Deleting the key via
//! `delete_key()` makes the data permanently unreadable — satisfying GDPR Article 17.
//!
//! Run with:
//! ```bash
//! cargo run --example crypto-shredding
//! ```

use std::sync::Arc;

use chrono::{DateTime, Utc};
use event_sauce_core::{
    crypto, Aggregate, AggregateRoot, AggregateVersion, ApplyEvent, DefaultEntity, DomainEvent,
    Entity, EntityId, EventStore, SnapshotConfig, StreamId,
};
use event_sauce_macros::AggregateError;
use event_sauce_memory::InMemoryEventStore;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

// ============================================================================
// Encrypted Aggregate: Patient (sensitive medical data)
// ============================================================================

/// Domain errors for the Patient aggregate.
#[derive(AggregateError, Debug, thiserror::Error)]
enum PatientError {
    #[error("Patient name cannot be empty")]
    EmptyName,
}

/// A patient record containing sensitive medical data.
///
/// This aggregate is **encrypted**: all event and snapshot data is encrypted at rest.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Patient {
    id: EntityId,
    name: String,
    diagnosis: String,
}

impl Entity for Patient {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            diagnosis: String::new(),
        }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Patient {}

impl Aggregate for Patient {
    type Event = PatientEvent;
    type Error = PatientError;
    type DeletedState = Self;

    /// Mark this aggregate as encrypted — enables automatic encryption.
    fn is_encrypted() -> bool {
        true
    }
}

// -- Patient events --

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PatientRegisteredEvent {
    name: String,
    diagnosis: String,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<Patient> for PatientRegisteredEvent {
    fn validate(&self, _patient: &Patient) -> Result<(), PatientError> {
        if self.name.is_empty() {
            return Err(PatientError::EmptyName);
        }
        Ok(())
    }

    fn apply(&self, patient: &mut Patient) {
        patient.name = self.name.clone();
        patient.diagnosis = self.diagnosis.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiagnosisUpdatedEvent {
    diagnosis: String,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<Patient> for DiagnosisUpdatedEvent {
    fn validate(&self, _patient: &Patient) -> Result<(), PatientError> {
        Ok(())
    }

    fn apply(&self, patient: &mut Patient) {
        patient.diagnosis = self.diagnosis.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, event_sauce_macros::Event)]
#[event(version = 1, aggregate = "Patient")]
enum PatientEvent {
    Registered(PatientRegisteredEvent),
    DiagnosisUpdated(DiagnosisUpdatedEvent),
}

// ============================================================================
// Public Aggregate: Counter (non-sensitive data)
// ============================================================================

/// Domain errors for the Counter aggregate.
#[derive(AggregateError, Debug, thiserror::Error)]
enum CounterError {
    #[error("Cannot increment by zero")]
    ZeroIncrement,
}

/// A simple counter — **not** private, so data is stored as plaintext.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Counter {
    id: EntityId,
    value: i64,
}

impl Entity for Counter {
    fn new(id: EntityId) -> Self {
        Self { id, value: 0 }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DefaultEntity for Counter {}

impl Aggregate for Counter {
    type Event = CounterEvent;
    type Error = CounterError;
    type DeletedState = Self;
    // is_encrypted() defaults to false — no encryption
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncrementedEvent {
    amount: i64,
    timestamp: DateTime<Utc>,
}

impl ApplyEvent<Counter> for IncrementedEvent {
    fn validate(&self, _counter: &Counter) -> Result<(), CounterError> {
        if self.amount == 0 {
            return Err(CounterError::ZeroIncrement);
        }
        Ok(())
    }

    fn apply(&self, counter: &mut Counter) {
        counter.value += self.amount;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, event_sauce_macros::Event)]
#[event(version = 1, aggregate = "Counter")]
enum CounterEvent {
    Incremented(IncrementedEvent),
}

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Event Sauce - Crypto-Shredding Example\n");
    println!("This example demonstrates:");
    println!("  - Encrypted aggregates with automatic encryption at rest");
    println!("  - Public aggregates stored as plaintext for contrast");
    println!("  - Crypto-shredding: deleting a key to implement right-to-be-forgotten\n");

    // -- Setup --
    // InMemoryEventStore includes AES-256-GCM crypto provider and key store by default.

    let store = Arc::new(
        InMemoryEventStore::builder()
            .snapshot_config(SnapshotConfig::disabled())
            .build(),
    );

    println!("  Store configured with AES-256-GCM encryption and in-memory key store.\n");

    // ========================================================================
    // Step 1: Create and commit an encrypted aggregate (Patient)
    // ========================================================================

    println!("=== Step 1: Commit an Encrypted Aggregate (Patient) ===\n");

    let mut patient = AggregateRoot::<Patient>::new(EntityId::new());
    patient.apply(PatientRegisteredEvent {
        name: "Alice Smith".to_string(),
        diagnosis: "Hypertension Stage 2".to_string(),
        timestamp: Utc::now(),
    })?;
    patient.apply(DiagnosisUpdatedEvent {
        diagnosis: "Hypertension Stage 2, controlled with medication".to_string(),
        timestamp: Utc::now(),
    })?;

    store.commit(&mut patient).await?;

    println!("  Patient registered: {}", patient.name);
    println!("  Diagnosis: {}", patient.diagnosis);
    println!("  Version: {}", patient.version());

    // ========================================================================
    // Step 2: Create and commit a public aggregate (Counter)
    // ========================================================================

    println!("\n=== Step 2: Commit a Public Aggregate (Counter) ===\n");

    let mut counter = AggregateRoot::<Counter>::new(EntityId::new());
    counter.apply(IncrementedEvent {
        amount: 42,
        timestamp: Utc::now(),
    })?;

    store.commit(&mut counter).await?;

    println!("  Counter value: {}", counter.value);
    println!("  Version: {}", counter.version());

    // ========================================================================
    // Step 3: Verify encryption at rest
    // ========================================================================

    println!("\n=== Step 3: Verify Encryption at Rest ===\n");

    // Inspect raw Patient events — should be encrypted
    let patient_stream_id = StreamId::new("Patient", patient.entity_id().as_uuid());
    let mut patient_stream = store
        .load_stream(patient_stream_id, AggregateVersion::initial())
        .await?;

    println!("  Raw Patient event data (should be encrypted):");
    while let Some(Ok(envelope)) = patient_stream.next().await {
        let encrypted = crypto::is_encrypted(&envelope.event_data);
        println!(
            "    Event '{}': encrypted={}",
            envelope.event_type, encrypted
        );
        if encrypted {
            // Show the encrypted payload shape
            println!(
                "      Payload: {{\"__encrypted\": \"{}...\"}}",
                &envelope.event_data["__encrypted"]
                    .as_str()
                    .unwrap_or("")
                    .chars()
                    .take(32)
                    .collect::<String>()
            );
        }
    }

    // Inspect raw Counter events — should be plaintext
    let counter_stream_id = StreamId::new("Counter", counter.entity_id().as_uuid());
    let mut counter_stream = store
        .load_stream(counter_stream_id, AggregateVersion::initial())
        .await?;

    println!("\n  Raw Counter event data (should be plaintext):");
    while let Some(Ok(envelope)) = counter_stream.next().await {
        let encrypted = crypto::is_encrypted(&envelope.event_data);
        println!(
            "    Event '{}': encrypted={}",
            envelope.event_type, encrypted
        );
        println!("      Payload: {}", envelope.event_data);
    }

    // ========================================================================
    // Step 4: Transparent decryption on load
    // ========================================================================

    println!("\n=== Step 4: Transparent Decryption on Load ===\n");

    let repo = store.repository::<Patient>();
    let loaded_patient = repo.load(patient.entity_id()).await?;

    println!("  Loaded patient from store (decrypted transparently):");
    println!("    Name: {}", loaded_patient.name);
    println!("    Diagnosis: {}", loaded_patient.diagnosis);
    println!("    Version: {}", loaded_patient.version());

    // ========================================================================
    // Step 5: Crypto-shredding — delete the key
    // ========================================================================

    println!("\n=== Step 5: Crypto-Shredding (Right to Be Forgotten) ===\n");

    let patient_uuid = patient.entity_id().as_uuid();
    println!("  Deleting encryption key for patient {}...", patient_uuid);
    store
        .crypto_key_store()
        .expect("crypto key store configured")
        .delete_key(patient_uuid)
        .await?;
    println!("  Key deleted.\n");

    // Attempt to load — should fail with KeyNotFound
    println!("  Attempting to load patient after key deletion...");
    match repo.load(patient.entity_id()).await {
        Ok(_) => println!("    ERROR: Should have failed!"),
        Err(e) if e.is_key_not_found() => {
            println!("    KeyNotFound error (expected): {e}");
            println!("    The patient's data is now permanently unreadable.");
        }
        Err(e) => println!("    Unexpected error: {e}"),
    }

    // ========================================================================
    // Step 6: Public aggregate is unaffected
    // ========================================================================

    println!("\n=== Step 6: Public Aggregate Still Accessible ===\n");

    let counter_repo = store.repository::<Counter>();
    let loaded_counter = counter_repo.load(counter.entity_id()).await?;

    println!("  Counter loaded successfully (no encryption involved):");
    println!("    Value: {}", loaded_counter.value);
    println!("    Version: {}", loaded_counter.version());

    // ========================================================================
    // Summary
    // ========================================================================

    println!("\n=== Summary ===\n");
    println!("  Key Takeaways:");
    println!("    - Encrypted aggregates have is_encrypted() -> true");
    println!("    - Event data is encrypted automatically on commit()");
    println!("    - Decryption is transparent on load()");
    println!("    - Deleting the key makes data permanently unreadable");
    println!("    - Public aggregates are completely unaffected");
    println!("    - This satisfies GDPR Article 17 (right to erasure)");

    Ok(())
}
