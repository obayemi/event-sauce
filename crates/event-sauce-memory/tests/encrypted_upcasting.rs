//! Regression tests for M7: encrypted aggregates are schema-migratable via the
//! read-time upcast hook.
//!
//! M7's original claim was that "Encrypted aggregates can never be
//! schema-migrated: no upcasting hook AND no decrypt/transform/re-encrypt path,
//! so any event-shape change is unrecoverable while the key is live." Two of
//! those premises are now FALSE:
//!
//!   * the upcast seam exists (`DomainEvent::upcast`, called by `from_envelope`
//!     BEFORE `serde_json::from_value`), and
//!   * the load path DECRYPTS `event_data` BEFORE calling `from_envelope` (see
//!     `load_any` / `decrypt_event_data` in `event_store.rs`).
//!
//! Composed, the per-event load order for an encrypted aggregate is therefore
//! **decrypt -> upcast -> deserialize**. That means an encrypted aggregate whose
//! event shape changed CAN be migrated at read time today: upcast runs on the
//! decrypted PLAINTEXT, exactly as it does for an unencrypted aggregate.
//!
//! These tests prove that end-to-end on the in-memory backend, seeding a raw
//! historical envelope whose ENCRYPTED payload, once decrypted, predates a
//! field the current v2 struct requires (mirroring `upcasting.rs`, but with the
//! payload encrypted under the aggregate's key like `encrypted_aggregate.rs`).
//!
//!   * `encrypted_v1_payload_without_upcast_fails_to_deserialize` is the
//!     COMPILES-TODAY RED: it uses an encrypted aggregate with NO `upcast`
//!     override. Loading decrypts the stored v1 ciphertext to plaintext that
//!     lacks the `tier` field, and deserialization then fails on the missing
//!     field. This proves the decrypt step succeeds (the error is NOT
//!     `KeyNotFound`, NOT an opaque ciphertext-deserialize error) and isolates
//!     the genuine remaining gap that the upcast seam closes.
//!
//!   * `encrypted_v1_payload_with_upcast_migrates_on_load` is the MEANINGFUL
//!     proof: with an `upcast` override that injects `tier` for v1 payloads, the
//!     same encrypted v1 payload loads SUCCESSFULLY with the migrated value —
//!     closing the "unrecoverable" claim for encrypted aggregates.
//!
//!   * `crypto_shredded_encrypted_aggregate_stays_unrecoverable` guards the
//!     still-true caveat (and the F5 invariant): a shredded aggregate (key
//!     deleted) still returns `KeyNotFound`, never silently anything, even when
//!     an `upcast` override exists.

use event_sauce_core::{
    crypto, Aggregate, AggregateError, AggregateVersion, CryptoKeyStore, CryptoProvider,
    DomainEvent, Entity, EntityId, EventApplicator, EventEnvelope, EventStore, EventVersion,
    SnapshotConfig, StreamId,
};
use event_sauce_crypto::Aes256GcmProvider;
use event_sauce_memory::{InMemoryCryptoKeyStore, InMemoryEventStore};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

// ============================================================================
// Encrypted aggregate WITH an `upcast` override (the migratable case).
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("secret profile error")]
struct SecretProfileError;
impl AggregateError for SecretProfileError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecretProfile {
    id: EntityId,
    /// Present since v1.
    name: String,
    /// Added in event schema v2. NO serde default: a decrypted v1 payload that
    /// lacks this field cannot deserialize without migration.
    tier: String,
}

impl Entity for SecretProfile {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            tier: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for SecretProfile {}

impl Aggregate for SecretProfile {
    type Event = SecretProfileEvent;
    type Error = SecretProfileError;
    type DeletedState = Self;

    // This aggregate is FULLY ENCRYPTED: event data is encrypted at rest.
    fn is_encrypted() -> bool {
        true
    }
}

/// Current (v2) event shape: `tier` is required and has NO serde default.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum SecretProfileEvent {
    Registered { name: String, tier: String },
}

impl DomainEvent for SecretProfileEvent {
    type Aggregate = SecretProfile;

    fn event_type(&self) -> &'static str {
        match self {
            SecretProfileEvent::Registered { .. } => "SecretProfile.Registered",
        }
    }

    fn event_version(&self) -> EventVersion {
        EventVersion::new(2)
    }

    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    /// The hook under test. On load this runs AFTER decryption (the load path
    /// decrypts `event_data` before `from_envelope`), so it sees the v1
    /// PLAINTEXT and injects the missing `tier` field for `from_version == 1`.
    fn upcast(event_type: &str, from_version: EventVersion, data: &mut serde_json::Value) {
        if event_type == "SecretProfile.Registered" && from_version == EventVersion::new(1) {
            if let Some(obj) = data
                .get_mut("Registered")
                .and_then(serde_json::Value::as_object_mut)
            {
                obj.entry("tier")
                    .or_insert_with(|| json!("migrated-from-v1"));
            }
        }
    }
}

impl EventApplicator<SecretProfile> for SecretProfileEvent {
    fn dispatch(&self, entity: &mut SecretProfile) -> Result<(), SecretProfileError> {
        match self {
            SecretProfileEvent::Registered { name, tier } => {
                entity.name = name.clone();
                entity.tier = tier.clone();
            }
        }
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut SecretProfile) {
        match self {
            SecretProfileEvent::Registered { name, tier } => {
                entity.name = name.clone();
                entity.tier = tier.clone();
            }
        }
    }
}

// ============================================================================
// Encrypted aggregate WITHOUT an `upcast` override (compiles-today runtime RED).
// ============================================================================

#[derive(Debug, thiserror::Error)]
#[error("legacy secret error")]
struct LegacySecretError;
impl AggregateError for LegacySecretError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacySecret {
    id: EntityId,
    name: String,
    /// Required field with NO serde default — same shape change as above.
    tier: String,
}

impl Entity for LegacySecret {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            name: String::new(),
            tier: String::new(),
        }
    }
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl event_sauce_core::DefaultEntity for LegacySecret {}

impl Aggregate for LegacySecret {
    type Event = LegacySecretEvent;
    type Error = LegacySecretError;
    type DeletedState = Self;

    fn is_encrypted() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum LegacySecretEvent {
    Registered { name: String, tier: String },
}

impl DomainEvent for LegacySecretEvent {
    type Aggregate = LegacySecret;

    fn event_type(&self) -> &'static str {
        match self {
            LegacySecretEvent::Registered { .. } => "LegacySecret.Registered",
        }
    }

    fn event_version(&self) -> EventVersion {
        EventVersion::new(2)
    }

    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    // Intentionally NO `upcast` override: relies entirely on the production
    // default. A decrypted v1 payload that lacks `tier` cannot be migrated.
}

impl EventApplicator<LegacySecret> for LegacySecretEvent {
    fn dispatch(&self, entity: &mut LegacySecret) -> Result<(), LegacySecretError> {
        match self {
            LegacySecretEvent::Registered { name, tier } => {
                entity.name = name.clone();
                entity.tier = tier.clone();
            }
        }
        Ok(())
    }
    fn dispatch_unchecked(&self, entity: &mut LegacySecret) {
        match self {
            LegacySecretEvent::Registered { name, tier } => {
                entity.name = name.clone();
                entity.tier = tier.clone();
            }
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Builds a store with an explicit, caller-controlled crypto key store and the
/// real AES-256-GCM provider, so a test can pre-seed a known key and encrypt a
/// historical payload under it before appending.
fn create_encrypted_store() -> (Arc<InMemoryEventStore>, Arc<InMemoryCryptoKeyStore>) {
    let key_store = Arc::new(InMemoryCryptoKeyStore::new());
    let store = InMemoryEventStore::builder()
        .snapshot_config(SnapshotConfig::disabled())
        .crypto_key_store(key_store.clone())
        .crypto_provider(Arc::new(Aes256GcmProvider))
        .build();
    (Arc::new(store), key_store)
}

/// Seeds a raw, ENCRYPTED historical envelope.
///
/// Mirrors `upcasting.rs::append_raw` but encrypts the (plaintext) v1 payload
/// under a freshly generated key registered for `aggregate_id`, exactly as a
/// full-encryption `commit()` would have stored it at the time. The same key is
/// resolved by the load path, so loading decrypts back to this plaintext shape.
async fn seed_encrypted_raw(
    store: &Arc<InMemoryEventStore>,
    key_store: &Arc<InMemoryCryptoKeyStore>,
    aggregate_type: &'static str,
    aggregate_id: Uuid,
    event_type: &str,
    event_version: i64,
    plaintext_payload: serde_json::Value,
) {
    // Generate and register a key for this aggregate (as commit() would).
    let provider = Aes256GcmProvider;
    let key = provider.generate_key();
    key_store
        .upsert_key(aggregate_id, key.clone())
        .await
        .expect("seeding the crypto key must succeed");

    // Encrypt the historical plaintext payload under that key.
    let encrypted = crypto::encrypt_value(&provider, &key, &plaintext_payload)
        .expect("encrypting the historical payload must succeed");
    assert!(
        crypto::is_encrypted(&encrypted),
        "seeded payload must be stored as ciphertext"
    );

    let stream_id = StreamId::new(aggregate_type, aggregate_id);
    let envelope = EventEnvelope::new(
        Uuid::new_v4(),
        aggregate_id,
        aggregate_type,
        event_type.to_string(),
        EventVersion::new(event_version),
        encrypted,
    );
    store
        .append(
            stream_id,
            vec![envelope],
            AggregateVersion::initial(),
            Vec::new(),
            false,
        )
        .await
        .expect("appending a raw encrypted historical envelope must succeed");
}

// ============================================================================
// COMPILES-TODAY runtime RED
// ============================================================================

/// RED (runtime, compiles today): a stored ENCRYPTED v1 payload that decrypts to
/// plaintext missing the `tier` field cannot deserialize on load when no
/// `upcast` override is supplied. The failure must be a missing-FIELD
/// deserialize error — proving the decrypt step ran (it is NOT `KeyNotFound` and
/// NOT an opaque ciphertext-deserialize error) and that the only remaining gap
/// is the schema migration, which the upcast seam closes. The companion
/// `..._with_upcast_migrates_on_load` test proves that closing.
#[tokio::test]
async fn encrypted_v1_payload_without_upcast_fails_to_deserialize() {
    let (store, key_store) = create_encrypted_store();
    let id = EntityId::new();

    // Historical v1 payload: `tier` did not exist yet. Stored encrypted.
    seed_encrypted_raw(
        &store,
        &key_store,
        "LegacySecret",
        id.as_uuid(),
        "LegacySecret.Registered",
        1,
        json!({ "Registered": { "name": "alice" } }),
    )
    .await;

    let repo = store.repository::<LegacySecret>();
    let result = repo.load(id).await;

    let err = result.expect_err(
        "loading an encrypted v1 payload that decrypts to a shape lacking the v2 `tier` field \
         must fail without an upcast override to migrate it",
    );

    // The decrypt step must have succeeded: this is a missing-FIELD error, not a
    // key/crypto failure and not an opaque ciphertext-deserialize error.
    assert!(
        !err.is_key_not_found(),
        "the key is present, so this must NOT be KeyNotFound (decryption ran): {err:?}"
    );
    let msg = format!("{err:?}");
    assert!(
        msg.contains("tier") || msg.to_lowercase().contains("missing field"),
        "expected a missing-field deserialize error mentioning `tier` (proving decrypt ran and \
         only schema migration is missing), got: {msg}"
    );
}

// ============================================================================
// MEANINGFUL proof: decrypt-THEN-upcast migrates encrypted data end-to-end.
// ============================================================================

/// PRIMARY DELIVERABLE: an encrypted aggregate IS schema-migratable at read time.
///
/// With an `upcast` override that injects `tier` for v1 payloads, loading the
/// same encrypted v1 payload succeeds: the load path decrypts the ciphertext to
/// the v1 plaintext, upcast injects the missing field, and deserialization then
/// succeeds with the migrated value. This closes M7's "unrecoverable" claim.
#[tokio::test]
async fn encrypted_v1_payload_with_upcast_migrates_on_load() {
    let (store, key_store) = create_encrypted_store();
    let id = EntityId::new();

    seed_encrypted_raw(
        &store,
        &key_store,
        "SecretProfile",
        id.as_uuid(),
        "SecretProfile.Registered",
        1,
        json!({ "Registered": { "name": "bob" } }),
    )
    .await;

    let repo = store.repository::<SecretProfile>();
    let loaded = repo.load(id).await.expect(
        "an encrypted v1 payload must load after decrypt-then-upcast injects the missing `tier`",
    );

    assert_eq!(loaded.entity().name, "bob");
    assert_eq!(
        loaded.entity().tier,
        "migrated-from-v1",
        "decrypt-then-upcast must populate the field that did not exist at v1"
    );
}

/// A current-version (v2) ENCRYPTED payload already carries `tier`, so the
/// upcast must branch on `from_version` and leave the decrypted payload
/// untouched.
#[tokio::test]
async fn encrypted_current_payload_loads_untouched() {
    let (store, key_store) = create_encrypted_store();
    let id = EntityId::new();

    seed_encrypted_raw(
        &store,
        &key_store,
        "SecretProfile",
        id.as_uuid(),
        "SecretProfile.Registered",
        2,
        json!({ "Registered": { "name": "carol", "tier": "gold" } }),
    )
    .await;

    let repo = store.repository::<SecretProfile>();
    let loaded = repo
        .load(id)
        .await
        .expect("a current-version encrypted payload must load unchanged");

    assert_eq!(loaded.entity().name, "carol");
    assert_eq!(
        loaded.entity().tier,
        "gold",
        "upcast must not rewrite a current-version encrypted payload"
    );
}

// ============================================================================
// F5 GUARD (must stay green): crypto-shred remains unrecoverable, even with an
// upcast override present.
// ============================================================================

/// A crypto-shredded encrypted aggregate (key deleted) stays intentionally
/// unrecoverable: loading must return `KeyNotFound`, never a silent success and
/// never a migrated value. The upcast hook does not — and must not — provide a
/// back door around erasure.
#[tokio::test]
async fn crypto_shredded_encrypted_aggregate_stays_unrecoverable() {
    let (store, key_store) = create_encrypted_store();
    let id = EntityId::new();

    seed_encrypted_raw(
        &store,
        &key_store,
        "SecretProfile",
        id.as_uuid(),
        "SecretProfile.Registered",
        1,
        json!({ "Registered": { "name": "dave" } }),
    )
    .await;

    // Shred the key out from under the existing ciphertext.
    key_store.delete_key(id.as_uuid()).await.unwrap();

    let repo = store.repository::<SecretProfile>();
    let result = repo.load(id).await;

    assert!(
        result.unwrap_err().is_key_not_found(),
        "a crypto-shredded encrypted aggregate must stay unrecoverable (KeyNotFound), \
         the upcast hook must not bypass erasure"
    );
}
