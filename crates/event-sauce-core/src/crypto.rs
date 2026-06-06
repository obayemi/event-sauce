//! Cryptographic traits and helpers for encrypted aggregate encryption.
//!
//! This module provides the abstractions needed for crypto-shredding support:
//! - [`CryptoKeyStore`] — per-aggregate encryption key management
//! - [`CryptoProvider`] — pluggable encryption/decryption implementations
//! - Helper functions for encrypting/decrypting [`serde_json::Value`] payloads
//!
//! When an encrypted aggregate's encryption key is deleted, its event and snapshot
//! data becomes permanently unreadable (GDPR right-to-be-forgotten).

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use uuid::Uuid;

use crate::Result;

/// Per-aggregate encryption key storage.
///
/// Manages the lifecycle of encryption keys used for encrypted aggregates.
/// Deleting a key effectively "crypto-shreds" the aggregate's data.
///
/// # Examples
///
/// ```ignore
/// let key = store.get_key(aggregate_id).await?;
/// if key.is_none() {
///     return Err(Error::key_not_found(aggregate_id));
/// }
/// ```
#[async_trait]
pub trait CryptoKeyStore: Send + Sync {
    /// Returns the encryption key for an aggregate, or `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails.
    async fn get_key(&self, aggregate_id: Uuid) -> Result<Option<Vec<u8>>>;

    /// Inserts or updates the encryption key for an aggregate.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails.
    async fn upsert_key(&self, aggregate_id: Uuid, key: Vec<u8>) -> Result<()>;

    /// Deletes the encryption key for an aggregate (crypto-shredding).
    ///
    /// After deletion, the aggregate's encrypted data is permanently unreadable.
    /// This operation is idempotent — deleting a non-existent key succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails.
    async fn delete_key(&self, aggregate_id: Uuid) -> Result<()>;
}

/// Pluggable encryption/decryption provider.
///
/// Implementations supply the actual cryptographic operations.
/// The default implementation in `event-sauce-crypto` uses AES-256-GCM.
///
/// # Associated data (AAD)
///
/// `encrypt`/`decrypt` take an `aad: &[u8]` (additional authenticated data).
/// The AAD is authenticated but not encrypted: it is bound to the ciphertext so
/// that decryption fails unless the *same* AAD is supplied. event-sauce binds
/// each event's ciphertext to `aggregate_id || event_id` (and `|| field_name`
/// for field-level encryption). Because the per-aggregate key is shared by every
/// event of an aggregate, this binding prevents a ciphertext from one event being
/// relocated onto another event row of the same aggregate (replay/relocation
/// attacks). Implementations MUST authenticate the AAD; a different AAD on
/// decrypt MUST fail.
///
/// # On-disk envelope versioning
///
/// Stored ciphertext carries a version marker: `{"__encrypted": "<base64>",
/// "__enc_v": 2}`. Version `2` rows are decrypted with the bound AAD; rows with
/// no `__enc_v` marker (or `__enc_v: 1`) are *legacy* rows written before AAD
/// binding and are decrypted with an EMPTY AAD to stay readable. The `__encrypted`
/// and `__enc_v` keys are reserved and must not be used as user field names.
///
/// # Contract
///
/// - `decrypt(key, encrypt(key, plaintext, aad), aad)` must return the original plaintext
/// - `decrypt` with a wrong key, corrupted ciphertext, or a different `aad` must return an error
/// - `generate_key()` must produce keys suitable for `encrypt`/`decrypt`
pub trait CryptoProvider: Send + Sync {
    /// Encrypts plaintext bytes with the given key, binding `aad` as additional
    /// authenticated data.
    ///
    /// The output format is implementation-defined (e.g., nonce prepended to ciphertext).
    /// Pass an empty slice for `aad` when no binding is required.
    ///
    /// # Errors
    ///
    /// Returns an error if encryption fails.
    fn encrypt(&self, key: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>>;

    /// Decrypts ciphertext bytes with the given key, requiring `aad` to match the
    /// additional authenticated data supplied at encryption time.
    ///
    /// # Errors
    ///
    /// Returns an error if decryption fails (wrong key, corrupted data, mismatched
    /// `aad`, etc.).
    fn decrypt(&self, key: &[u8], ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>>;

    /// Generates a new random encryption key.
    fn generate_key(&self) -> Vec<u8>;
}

/// On-disk envelope version for AAD-bound ciphertext.
///
/// Version `1` (the *absence* of `__enc_v`) marks legacy rows written with no
/// associated data; version `2` marks rows whose ciphertext is bound to its
/// event via AAD.
const ENVELOPE_VERSION: u64 = 2;

/// Encrypts a JSON value, wrapping the result in a versioned envelope
/// `{"__encrypted": "<base64>", "__enc_v": 2}`.
///
/// The `aad` is bound into the ciphertext as additional authenticated data so
/// the result can only be decrypted in the same context (see
/// [`CryptoProvider`]).
///
/// # Errors
///
/// Returns an error if serialization or encryption fails.
pub fn encrypt_value(
    provider: &dyn CryptoProvider,
    key: &[u8],
    value: &serde_json::Value,
    aad: &[u8],
) -> Result<serde_json::Value> {
    let plaintext = serde_json::to_vec(value)?;
    let ciphertext = provider.encrypt(key, &plaintext, aad)?;
    let encoded = BASE64.encode(&ciphertext);
    Ok(serde_json::json!({ "__encrypted": encoded, "__enc_v": ENVELOPE_VERSION }))
}

/// Decrypts a JSON value that was encrypted with [`encrypt_value`].
///
/// If the value is not encrypted (no `__encrypted` key), it is returned as-is.
///
/// The envelope version drives AAD handling: version `2` rows are decrypted with
/// the supplied `aad`; legacy rows (no `__enc_v` marker, or `__enc_v: 1`) were
/// written before AAD binding and are decrypted with an EMPTY AAD so they remain
/// readable regardless of the `aad` the caller supplies for fresh rows.
///
/// # Errors
///
/// Returns an error if decryption or deserialization fails.
pub fn decrypt_value(
    provider: &dyn CryptoProvider,
    key: &[u8],
    value: &serde_json::Value,
    aad: &[u8],
) -> Result<serde_json::Value> {
    let Some(encoded) = value.get("__encrypted").and_then(serde_json::Value::as_str) else {
        // Not encrypted — return as-is (backward compatibility)
        return Ok(value.clone());
    };

    // Version-absent (or v1) rows predate AAD binding: decrypt with empty AAD to
    // reproduce exactly how they were written. v2 rows bind the supplied AAD.
    let effective_aad: &[u8] = match value.get("__enc_v").and_then(serde_json::Value::as_u64) {
        Some(v) if v >= ENVELOPE_VERSION => aad,
        _ => &[],
    };

    let ciphertext = BASE64
        .decode(encoded)
        .map_err(|e| crate::Error::encryption(format!("base64 decode failed: {e}")))?;
    let plaintext = provider.decrypt(key, &ciphertext, effective_aad)?;
    let decrypted: serde_json::Value = serde_json::from_slice(&plaintext)?;
    Ok(decrypted)
}

/// Encrypts specific named fields within a JSON object in-place.
///
/// Each field listed in `fields` is replaced with `{"__encrypted": "<base64>",
/// "__enc_v": 2}`. Fields that are not present in the object are silently skipped.
///
/// The per-field AAD is `aad || field_name`: because multiple fields of one event
/// share the same base `aad` (`aggregate_id || event_id`), binding the field name
/// additionally prevents intra-event field swapping (relocating one encrypted
/// field's ciphertext onto another field of the same event).
///
/// # Errors
///
/// Returns an error if the value is not a JSON object, or if encryption fails.
pub fn encrypt_fields(
    provider: &dyn CryptoProvider,
    key: &[u8],
    data: &mut serde_json::Value,
    fields: &[&str],
    aad: &[u8],
) -> Result<()> {
    let obj = data
        .as_object_mut()
        .ok_or_else(|| crate::Error::encryption("encrypt_fields requires a JSON object"))?;

    for &field in fields {
        if let Some(value) = obj.get(field) {
            let encrypted = encrypt_value(provider, key, value, &field_aad(aad, field))?;
            obj.insert(field.to_string(), encrypted);
        }
    }

    Ok(())
}

/// Decrypts any individually-encrypted fields within a JSON object in-place.
///
/// Walks all top-level fields; any value matching the encrypted-envelope shape is
/// decrypted and replaced with its plaintext form. Non-encrypted fields are
/// left untouched.
///
/// The per-field AAD mirrors [`encrypt_fields`] (`aad || field_name`). Legacy
/// rows with no `__enc_v` marker ignore the AAD (see [`decrypt_value`]).
///
/// # Errors
///
/// Returns an error if the value is not a JSON object, or if decryption fails.
pub fn decrypt_encrypted_fields(
    provider: &dyn CryptoProvider,
    key: &[u8],
    data: &mut serde_json::Value,
    aad: &[u8],
) -> Result<()> {
    let obj = data.as_object_mut().ok_or_else(|| {
        crate::Error::encryption("decrypt_encrypted_fields requires a JSON object")
    })?;

    let fields_to_decrypt: Vec<String> = obj
        .iter()
        .filter(|(_, v)| is_encrypted(v))
        .map(|(k, _)| k.clone())
        .collect();

    for field in fields_to_decrypt {
        if let Some(value) = obj.get(&field) {
            let decrypted = decrypt_value(provider, key, value, &field_aad(aad, &field))?;
            obj.insert(field, decrypted);
        }
    }

    Ok(())
}

/// Builds the canonical per-event AAD that binds an event's ciphertext to that
/// specific event: `aggregate_id (16 bytes) || event_id (16 bytes)`.
///
/// This is the exact associated data the event store threads into
/// [`encrypt_value`]/[`encrypt_fields`] when persisting an encrypted aggregate's
/// events (field-level encryption additionally appends the field name). The
/// per-event UUID makes the AAD unique per event, so a v2 ciphertext produced
/// for one event fails to authenticate if relocated onto another event row of
/// the same aggregate (the per-aggregate key alone cannot distinguish events).
///
/// Callers that load and decrypt event envelopes *outside* the aggregate replay
/// path — e.g. a projection or an audit viewer reading raw rows — MUST pass this
/// same AAD to [`decrypt_value`]/[`decrypt_encrypted_fields`], or v2 ciphertext
/// will fail to decrypt. (Legacy v1 rows ignore the AAD, so they stay readable
/// regardless.)
#[must_use]
pub fn event_aad(aggregate_id: Uuid, event_id: Uuid) -> [u8; 32] {
    let mut aad = [0u8; 32];
    aad[..16].copy_from_slice(aggregate_id.as_bytes());
    aad[16..].copy_from_slice(event_id.as_bytes());
    aad
}

/// Builds the per-field AAD (`base_aad || field_name`) used by field-level
/// encryption to defeat intra-event field swapping.
fn field_aad(base: &[u8], field: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(base.len() + field.len());
    aad.extend_from_slice(base);
    aad.extend_from_slice(field.as_bytes());
    aad
}

/// Returns `true` if any top-level field in a JSON object is individually encrypted.
///
/// Checks whether any value within the object matches the `{"__encrypted": "..."}` format.
/// Returns `false` for non-object values or objects with no encrypted fields.
///
/// # Examples
///
/// ```
/// use event_sauce_core::crypto::has_encrypted_fields;
/// use serde_json::json;
///
/// let data = json!({"name": {"__encrypted": "abc"}, "age": 30});
/// assert!(has_encrypted_fields(&data));
///
/// let plain = json!({"name": "Alice", "age": 30});
/// assert!(!has_encrypted_fields(&plain));
/// ```
#[must_use]
pub fn has_encrypted_fields(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .is_some_and(|obj| obj.values().any(is_encrypted))
}

/// Returns `true` if the JSON value is in encrypted form.
///
/// Encrypted values are objects whose keys are exactly `__encrypted` (a base64
/// string) and OPTIONALLY `__enc_v` (a numeric envelope version) — the shape
/// produced by [`encrypt_value`]. An object that carries a `__encrypted` key
/// alongside any *other* key, or whose `__encrypted` value is not a string, is a
/// legitimate user payload, not ciphertext, and is reported as `false`.
///
/// # Examples
///
/// ```
/// use event_sauce_core::crypto::is_encrypted;
/// use serde_json::json;
///
/// assert!(is_encrypted(&json!({"__encrypted": "abc"})));
/// // A versioned envelope is also ciphertext:
/// assert!(is_encrypted(&json!({"__encrypted": "abc", "__enc_v": 2})));
/// assert!(!is_encrypted(&json!({"name": "Alice"})));
/// // A non-`__enc_v` extra key or a non-string value mean it is not ciphertext:
/// assert!(!is_encrypted(&json!({"__encrypted": "abc", "name": "Alice"})));
/// assert!(!is_encrypted(&json!({"__encrypted": 5})));
/// ```
#[must_use]
pub fn is_encrypted(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|obj| {
        let encrypted_is_string = obj
            .get("__encrypted")
            .is_some_and(serde_json::Value::is_string);
        // Only `__encrypted` (string) plus an OPTIONAL numeric `__enc_v` are
        // allowed; any other key means this is a user payload, not ciphertext.
        let only_reserved_keys = obj.keys().all(|k| k == "__encrypted" || k == "__enc_v");
        // `__enc_v`, when present, must be numeric (a non-numeric companion makes
        // this a user payload, not ciphertext).
        let version_is_numeric = obj
            .get("__enc_v")
            .map_or(true, serde_json::Value::is_number);
        encrypted_is_string && only_reserved_keys && version_is_numeric
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple XOR-based mock provider for testing.
    /// NOT cryptographically secure — only for unit tests.
    struct MockCryptoProvider;

    impl CryptoProvider for MockCryptoProvider {
        fn encrypt(&self, key: &[u8], plaintext: &[u8], _aad: &[u8]) -> Result<Vec<u8>> {
            Ok(plaintext
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ key[i % key.len()])
                .collect())
        }

        fn decrypt(&self, key: &[u8], ciphertext: &[u8], _aad: &[u8]) -> Result<Vec<u8>> {
            // XOR is its own inverse
            self.encrypt(key, ciphertext, &[])
        }

        fn generate_key(&self) -> Vec<u8> {
            vec![0x42; 32]
        }
    }

    /// Mock provider that always fails.
    struct FailingCryptoProvider;

    impl CryptoProvider for FailingCryptoProvider {
        fn encrypt(&self, _key: &[u8], _plaintext: &[u8], _aad: &[u8]) -> Result<Vec<u8>> {
            Err(crate::Error::encryption("encryption failed"))
        }

        fn decrypt(&self, _key: &[u8], _ciphertext: &[u8], _aad: &[u8]) -> Result<Vec<u8>> {
            Err(crate::Error::encryption("decryption failed"))
        }

        fn generate_key(&self) -> Vec<u8> {
            vec![0; 32]
        }
    }

    #[test]
    fn encrypt_value_produces_encrypted_format() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"name": "Alice", "age": 30});

        let encrypted = encrypt_value(&provider, &key, &value, &[]).unwrap();

        assert!(is_encrypted(&encrypted));
        assert!(encrypted.get("__encrypted").unwrap().is_string());
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"order_id": "abc-123", "items": [1, 2, 3]});

        let encrypted = encrypt_value(&provider, &key, &value, b"aad").unwrap();
        let decrypted = decrypt_value(&provider, &key, &encrypted, b"aad").unwrap();

        assert_eq!(decrypted, value);
    }

    #[test]
    fn decrypt_value_passes_through_unencrypted_data() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"name": "Bob"});

        let result = decrypt_value(&provider, &key, &value, &[]).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn is_encrypted_returns_true_for_encrypted_value() {
        let value = serde_json::json!({"__encrypted": "YWJj"});
        assert!(is_encrypted(&value));
    }

    #[test]
    fn is_encrypted_returns_false_for_plain_value() {
        let value = serde_json::json!({"name": "Alice"});
        assert!(!is_encrypted(&value));
    }

    #[test]
    fn is_encrypted_returns_false_for_null() {
        let value = serde_json::Value::Null;
        assert!(!is_encrypted(&value));
    }

    #[test]
    fn is_encrypted_returns_false_for_array() {
        let value = serde_json::json!([1, 2, 3]);
        assert!(!is_encrypted(&value));
    }

    #[test]
    fn is_encrypted_returns_false_when_extra_keys_present() {
        // A legitimate user object that happens to carry a top-level
        // `__encrypted` key must NOT be misclassified as ciphertext.
        let value = serde_json::json!({"__encrypted": "abc", "name": "Alice"});
        assert!(
            !is_encrypted(&value),
            "an object with extra keys alongside __encrypted is not ciphertext"
        );
    }

    #[test]
    fn is_encrypted_returns_false_when_value_not_string() {
        // Real ciphertext stores a base64 string; a non-string value is not it.
        let value = serde_json::json!({"__encrypted": 5});
        assert!(
            !is_encrypted(&value),
            "__encrypted with a non-string value is not ciphertext"
        );
    }

    #[test]
    fn encrypt_value_fails_when_provider_fails() {
        let provider = FailingCryptoProvider;
        let key = vec![0; 32];
        let value = serde_json::json!({"data": "test"});

        let result = encrypt_value(&provider, &key, &value, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn decrypt_value_fails_when_provider_fails() {
        // First encrypt with working provider
        let mock = MockCryptoProvider;
        let key = mock.generate_key();
        let value = serde_json::json!({"data": "test"});
        let encrypted = encrypt_value(&mock, &key, &value, &[]).unwrap();

        // Then try to decrypt with failing provider
        let failing = FailingCryptoProvider;
        let result = decrypt_value(&failing, &key, &encrypted, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn decrypt_value_fails_on_invalid_base64() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"__encrypted": "not-valid-base64!!!"});

        let result = decrypt_value(&provider, &key, &value, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn generate_key_returns_32_bytes() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn encrypt_value_with_empty_object() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({});

        let encrypted = encrypt_value(&provider, &key, &value, b"aad").unwrap();
        let decrypted = decrypt_value(&provider, &key, &encrypted, b"aad").unwrap();
        assert_eq!(decrypted, value);
    }

    #[test]
    fn encrypt_value_with_nested_data() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({
            "user": {
                "name": "Alice",
                "address": {
                    "city": "Wonderland"
                }
            },
            "tags": ["admin", "user"]
        });

        let encrypted = encrypt_value(&provider, &key, &value, b"aad").unwrap();
        assert!(is_encrypted(&encrypted));
        let decrypted = decrypt_value(&provider, &key, &encrypted, b"aad").unwrap();
        assert_eq!(decrypted, value);
    }

    // --- Tests for encrypt_fields ---

    #[test]
    fn encrypt_fields_encrypts_named_fields() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!({
            "name": "Alice",
            "diagnosis": "flu",
            "visit_count": 3
        });

        encrypt_fields(&provider, &key, &mut data, &["name", "diagnosis"], b"aad").unwrap();

        assert!(is_encrypted(&data["name"]));
        assert!(is_encrypted(&data["diagnosis"]));
        assert_eq!(data["visit_count"], 3);
    }

    #[test]
    fn encrypt_fields_roundtrip() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let original = serde_json::json!({
            "name": "Alice",
            "diagnosis": "flu",
            "visit_count": 3
        });
        let mut data = original.clone();

        encrypt_fields(&provider, &key, &mut data, &["name", "diagnosis"], b"aad").unwrap();
        decrypt_encrypted_fields(&provider, &key, &mut data, b"aad").unwrap();

        assert_eq!(data, original);
    }

    #[test]
    fn encrypt_fields_skips_missing_fields() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!({"name": "Alice"});

        encrypt_fields(&provider, &key, &mut data, &["name", "nonexistent"], b"aad").unwrap();

        assert!(is_encrypted(&data["name"]));
        assert!(data.get("nonexistent").is_none());
    }

    #[test]
    fn encrypt_fields_fails_on_non_object() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!([1, 2, 3]);

        let result = encrypt_fields(&provider, &key, &mut data, &["name"], b"aad");
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn encrypt_fields_propagates_provider_error() {
        let provider = FailingCryptoProvider;
        let key = vec![0; 32];
        let mut data = serde_json::json!({"name": "Alice"});

        let result = encrypt_fields(&provider, &key, &mut data, &["name"], b"aad");
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    // --- Tests for decrypt_encrypted_fields ---

    #[test]
    fn decrypt_encrypted_fields_decrypts_only_encrypted() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!({
            "name": "Alice",
            "diagnosis": "flu",
            "visit_count": 3
        });

        encrypt_fields(&provider, &key, &mut data, &["name"], b"aad").unwrap();
        assert!(is_encrypted(&data["name"]));
        assert_eq!(data["diagnosis"], "flu");

        decrypt_encrypted_fields(&provider, &key, &mut data, b"aad").unwrap();

        assert_eq!(data["name"], "Alice");
        assert_eq!(data["diagnosis"], "flu");
        assert_eq!(data["visit_count"], 3);
    }

    #[test]
    fn decrypt_encrypted_fields_noop_when_no_encrypted() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!({"name": "Alice", "age": 30});
        let original = data.clone();

        decrypt_encrypted_fields(&provider, &key, &mut data, b"aad").unwrap();

        assert_eq!(data, original);
    }

    #[test]
    fn decrypt_encrypted_fields_fails_on_non_object() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!("a string");

        let result = decrypt_encrypted_fields(&provider, &key, &mut data, b"aad");
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    // --- Tests for has_encrypted_fields ---

    #[test]
    fn has_encrypted_fields_detects_encrypted_field() {
        let data = serde_json::json!({
            "name": {"__encrypted": "abc"},
            "age": 30
        });
        assert!(has_encrypted_fields(&data));
    }

    #[test]
    fn has_encrypted_fields_false_for_plain_object() {
        let data = serde_json::json!({"name": "Alice", "age": 30});
        assert!(!has_encrypted_fields(&data));
    }

    #[test]
    fn has_encrypted_fields_false_for_non_object() {
        assert!(!has_encrypted_fields(&serde_json::json!([1, 2, 3])));
        assert!(!has_encrypted_fields(&serde_json::json!("string")));
        assert!(!has_encrypted_fields(&serde_json::Value::Null));
    }

    #[test]
    fn has_encrypted_fields_false_for_empty_object() {
        assert!(!has_encrypted_fields(&serde_json::json!({})));
    }

    // --- RED (L5): versioned envelope `__enc_v` companion key ---
    //
    // Binding ciphertext to its event via AAD changes how new rows are written,
    // which would break decryption of already-stored ciphertext. The fix versions
    // the on-disk marker: going forward it writes
    // `{"__encrypted": "<base64>", "__enc_v": 2}`. The `is_encrypted` predicate
    // (tightened by F5 to require the SOLE key `__encrypted`) must be relaxed to
    // also accept an OPTIONAL numeric `__enc_v` companion key — and nothing else.
    #[test]
    fn is_encrypted_returns_true_with_version_marker() {
        // A v2 marker carries `__encrypted` (string) plus `__enc_v` (number).
        let value = serde_json::json!({"__encrypted": "abc", "__enc_v": 2});
        assert!(
            is_encrypted(&value),
            "a versioned envelope {{__encrypted, __enc_v}} must be recognized as ciphertext"
        );
    }

    #[test]
    fn is_encrypted_still_false_for_extra_non_version_key() {
        // F5 invariant preserved: a legitimate user object that carries
        // `__encrypted` alongside an unrelated key is NOT ciphertext.
        let value = serde_json::json!({"__encrypted": "abc", "name": "Alice"});
        assert!(
            !is_encrypted(&value),
            "an object with a non-`__enc_v` extra key is not ciphertext"
        );
    }

    #[test]
    fn is_encrypted_still_false_for_non_string_payload() {
        // F5 invariant preserved: a non-string `__encrypted` is not ciphertext,
        // even paired with a version marker.
        assert!(
            !is_encrypted(&serde_json::json!({"__encrypted": 5})),
            "`__encrypted` with a non-string value is not ciphertext"
        );
        assert!(
            !is_encrypted(&serde_json::json!({"__encrypted": 5, "__enc_v": 2})),
            "`__encrypted` with a non-string value is not ciphertext even with a version marker"
        );
    }

    // --- RED (L5): legacy v1 ciphertext stays readable via empty-AAD path ---
    //
    // Pre-existing rows were written with NO associated data and NO `__enc_v`
    // marker (version 1). After the fix threads `aad` through the helpers, those
    // legacy rows must still decrypt: the version is ABSENT, so `decrypt_value`
    // must reproduce how they were written (EMPTY aad), regardless of the `aad`
    // argument the caller supplies for fresh (v2) rows.
    //
    // An AAD-binding mock provider (mirrors `Aes256GcmProvider`, which lives in
    // a separate crate that core cannot depend on) so the v2 relocation assertion
    // is provider-driven, not a no-op.
    struct AadBindingProvider;

    impl CryptoProvider for AadBindingProvider {
        fn encrypt(&self, key: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
            // Bind aad by prefixing it; decrypt verifies the prefix matches.
            let mut out = Vec::new();
            #[allow(clippy::cast_possible_truncation)]
            out.extend_from_slice(&(aad.len() as u32).to_le_bytes());
            out.extend_from_slice(aad);
            out.extend(
                plaintext
                    .iter()
                    .enumerate()
                    .map(|(i, b)| b ^ key[i % key.len()]),
            );
            Ok(out)
        }

        fn decrypt(&self, key: &[u8], ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
            if ciphertext.len() < 4 {
                return Err(crate::Error::encryption("ciphertext too short"));
            }
            let (len_bytes, rest) = ciphertext.split_at(4);
            let aad_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            if rest.len() < aad_len {
                return Err(crate::Error::encryption("ciphertext too short"));
            }
            let (bound_aad, body) = rest.split_at(aad_len);
            if bound_aad != aad {
                return Err(crate::Error::encryption("aad mismatch"));
            }
            Ok(body
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ key[i % key.len()])
                .collect())
        }

        fn generate_key(&self) -> Vec<u8> {
            vec![0x42; 32]
        }
    }

    #[test]
    fn legacy_v1_ciphertext_decrypts_with_empty_aad() {
        let provider = AadBindingProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"order_id": "abc-123"});

        // Hand-craft a v1 marker: base64 of a ciphertext produced with EMPTY aad,
        // and NO `__enc_v` field — exactly how legacy rows look on disk.
        let plaintext = serde_json::to_vec(&value).unwrap();
        let ciphertext = provider.encrypt(&key, &plaintext, &[]).unwrap();
        let legacy = serde_json::json!({ "__encrypted": BASE64.encode(&ciphertext) });

        // A v1 row must decrypt even when the caller passes a non-empty aad
        // (the decoder reads the version and uses empty aad for legacy rows).
        let aad = b"aggregate-id||event-id";
        let decrypted = decrypt_value(&provider, &key, &legacy, aad).unwrap();
        assert_eq!(
            decrypted, value,
            "legacy v1 ciphertext must remain readable"
        );
    }

    // --- RED (L5): v2 roundtrip binds ciphertext to its aad ---
    #[test]
    fn v2_encrypt_value_binds_aad() {
        let provider = AadBindingProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"email": "alice@example.com"});

        let aad = b"aggregate||event-a";
        let other_aad = b"aggregate||event-b";

        let encrypted = encrypt_value(&provider, &key, &value, aad).unwrap();
        assert!(is_encrypted(&encrypted));

        // Same aad roundtrips.
        let ok = decrypt_value(&provider, &key, &encrypted, aad).unwrap();
        assert_eq!(ok, value);

        // A different aad (relocating the ciphertext to another event) must fail.
        let relocated = decrypt_value(&provider, &key, &encrypted, other_aad);
        assert!(
            relocated.is_err(),
            "a v2 ciphertext must not decrypt under a different aad (relocation)"
        );
    }

    // --- event_aad: the canonical per-event binding layout ---
    //
    // External callers that decrypt event envelopes manually (projections,
    // audit viewers) reconstruct the AAD via this helper, so its byte layout is
    // a stable contract: `aggregate_id (16) || event_id (16)`, in that order.
    #[test]
    fn event_aad_layout_is_aggregate_then_event() {
        let aggregate_id = Uuid::from_bytes([0xAA; 16]);
        let event_id = Uuid::from_bytes([0xEE; 16]);

        let aad = event_aad(aggregate_id, event_id);

        assert_eq!(aad.len(), 32, "AAD is aggregate_id (16) || event_id (16)");
        assert_eq!(
            &aad[..16],
            aggregate_id.as_bytes(),
            "first half is aggregate_id"
        );
        assert_eq!(&aad[16..], event_id.as_bytes(), "second half is event_id");
    }

    #[test]
    fn event_aad_is_deterministic_and_distinguishes_events() {
        let aggregate_id = Uuid::from_bytes([1; 16]);
        let event_a = Uuid::from_bytes([2; 16]);
        let event_b = Uuid::from_bytes([3; 16]);

        // Same inputs → same AAD (encrypt and decrypt sides must agree).
        assert_eq!(
            event_aad(aggregate_id, event_a),
            event_aad(aggregate_id, event_a)
        );
        // Different events of the same aggregate → different AAD (defeats
        // relocating one event's ciphertext onto another event row).
        assert_ne!(
            event_aad(aggregate_id, event_a),
            event_aad(aggregate_id, event_b)
        );
    }

    // The public helper must produce exactly what the AAD-binding provider
    // expects, so a value encrypted under `event_aad(agg, ev)` round-trips only
    // under the same (agg, ev) and is rejected when relocated to another event.
    #[test]
    fn event_aad_binds_value_to_its_event() {
        let provider = AadBindingProvider;
        let key = provider.generate_key();
        let aggregate_id = Uuid::from_bytes([7; 16]);
        let event_a = Uuid::from_bytes([8; 16]);
        let event_b = Uuid::from_bytes([9; 16]);
        let value = serde_json::json!({"email": "alice@example.com"});

        let encrypted =
            encrypt_value(&provider, &key, &value, &event_aad(aggregate_id, event_a)).unwrap();

        let same = decrypt_value(
            &provider,
            &key,
            &encrypted,
            &event_aad(aggregate_id, event_a),
        )
        .unwrap();
        assert_eq!(same, value);

        let relocated = decrypt_value(
            &provider,
            &key,
            &encrypted,
            &event_aad(aggregate_id, event_b),
        );
        assert!(
            relocated.is_err(),
            "ciphertext must not decrypt under another event's AAD"
        );
    }
}
