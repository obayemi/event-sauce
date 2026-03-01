//! # Field-Level Encryption Example — Selective Privacy
//!
//! This example demonstrates **field-level encryption** for selective privacy:
//!
//! - Only sensitive fields (name, diagnosis) are encrypted — other fields stay in plaintext
//! - The aggregate is **not** fully encrypted (`is_encrypted() -> false`)
//! - Non-sensitive event variants are stored entirely in plaintext
//! - Crypto-shredding still works: deleting the key makes sensitive fields unreadable
//!
//! ## When to use field-level vs full encryption
//!
//! - **Full encryption** (`#[aggregate(encrypted)]`): All event/snapshot data is encrypted.
//!   Use when the entire aggregate is sensitive (e.g., medical records, financial data).
//! - **Field-level** (`@encrypted_fields(field1, field2)`): Only listed fields are encrypted.
//!   Use when some fields are sensitive but others should remain queryable (e.g., PII alongside
//!   operational counters).
//!
//! Run with:
//! ```bash
//! cargo run --example field-encryption --features crypto
//! ```

use std::sync::Arc;

use event_sauce_core::{
    crypto, define_events, Aggregate, AggregateError, AggregateRoot, AggregateVersion,
    CryptoKeyStore, Entity, EntityId, EventStore, SnapshotConfig, StreamId,
};
use event_sauce_crypto::Aes256GcmProvider;
use event_sauce_memory::{InMemoryCryptoKeyStore, InMemoryEventStore};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

// ============================================================================
// Field-encrypted aggregate: Patient
// ============================================================================

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
    // Note: is_encrypted() is NOT overridden — this is NOT a fully encrypted aggregate
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

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Event Sauce - Field-Level Encryption Example\n");
    println!("This example demonstrates:");
    println!("  - Encrypting only sensitive fields (name, diagnosis)");
    println!("  - Non-sensitive fields (visit_count) remain in plaintext");
    println!("  - Non-sensitive events stored entirely in plaintext");
    println!("  - Crypto-shredding for selective privacy\n");

    // -- Setup --
    let key_store = Arc::new(InMemoryCryptoKeyStore::new());
    let provider = Arc::new(Aes256GcmProvider);

    let store = Arc::new(
        InMemoryEventStore::builder()
            .snapshot_config(SnapshotConfig::disabled())
            .crypto_key_store(key_store.clone())
            .crypto_provider(provider)
            .build(),
    );

    // ========================================================================
    // Step 1: Commit events with encrypted fields
    // ========================================================================

    println!("=== Step 1: Commit Events ===\n");

    let id = EntityId::new();
    let mut patient = AggregateRoot::<Patient>::new(id);

    patient.apply(PatientEvent::Registered {
        name: "Alice Smith".into(),
        diagnosis: "Hypertension Stage 2".into(),
        visit_count: 1,
        timestamp: chrono::Utc::now(),
    })?;

    patient.apply(PatientEvent::VisitRecorded {
        visit_count: 2,
        timestamp: chrono::Utc::now(),
    })?;

    store.commit(&mut patient).await?;

    println!("  Patient registered: {}", patient.name);
    println!("  Diagnosis: {}", patient.diagnosis);
    println!("  Visit count: {}", patient.visit_count);
    println!("  Version: {}\n", patient.version());

    // ========================================================================
    // Step 2: Inspect raw event data
    // ========================================================================

    println!("=== Step 2: Raw Event Data at Rest ===\n");

    let stream_id = StreamId::new("Patient", id.as_uuid());
    let mut event_stream = store
        .load_stream(stream_id, AggregateVersion::initial())
        .await?;

    while let Some(Ok(envelope)) = event_stream.next().await {
        let is_full = crypto::is_encrypted(&envelope.event_data);
        let has_fields = crypto::has_encrypted_fields(&envelope.event_data);

        println!("  Event '{}':", envelope.event_type);
        println!("    Fully encrypted: {is_full}");
        println!("    Has encrypted fields: {has_fields}");
        println!("    Payload: {}\n", envelope.event_data);
    }

    // ========================================================================
    // Step 3: Transparent load
    // ========================================================================

    println!("=== Step 3: Transparent Load (Decryption) ===\n");

    let repo = store.repository::<Patient>();
    let loaded = repo.load(id).await?;

    println!("  Loaded patient:");
    println!("    Name: {}", loaded.name);
    println!("    Diagnosis: {}", loaded.diagnosis);
    println!("    Visit count: {}", loaded.visit_count);
    println!("    Version: {}\n", loaded.version());

    // ========================================================================
    // Step 4: Crypto-shredding
    // ========================================================================

    println!("=== Step 4: Crypto-Shredding ===\n");

    println!("  Deleting encryption key...");
    key_store.delete_key(id.as_uuid()).await?;
    println!("  Key deleted.\n");

    println!("  Attempting to load patient after key deletion...");
    match repo.load(id).await {
        Ok(_) => println!("    ERROR: Should have failed!"),
        Err(e) => {
            println!("    Load failed (expected): {e}");
            println!("    Sensitive fields are now permanently unreadable.");
        }
    }

    // ========================================================================
    // Summary
    // ========================================================================

    println!("\n=== Summary ===\n");
    println!("  Key Takeaways:");
    println!("    - @encrypted_fields(name, diagnosis) encrypts only listed fields");
    println!("    - visit_count stays in plaintext (queryable)");
    println!("    - VisitRecorded events are entirely plaintext (no @encrypted_fields)");
    println!("    - Deleting the key makes encrypted fields permanently unreadable");

    Ok(())
}
