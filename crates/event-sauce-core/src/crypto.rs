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
/// # Contract
///
/// - `decrypt(key, encrypt(key, plaintext))` must return the original plaintext
/// - `decrypt` with a wrong key or corrupted ciphertext must return an error
/// - `generate_key()` must produce keys suitable for `encrypt`/`decrypt`
pub trait CryptoProvider: Send + Sync {
    /// Encrypts plaintext bytes with the given key.
    ///
    /// The output format is implementation-defined (e.g., nonce prepended to ciphertext).
    ///
    /// # Errors
    ///
    /// Returns an error if encryption fails.
    fn encrypt(&self, key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>>;

    /// Decrypts ciphertext bytes with the given key.
    ///
    /// # Errors
    ///
    /// Returns an error if decryption fails (wrong key, corrupted data, etc.).
    fn decrypt(&self, key: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>>;

    /// Generates a new random encryption key.
    fn generate_key(&self) -> Vec<u8>;
}

/// Encrypts a JSON value, wrapping the result in `{"__encrypted": "<base64>"}`.
///
/// # Errors
///
/// Returns an error if serialization or encryption fails.
pub fn encrypt_value(
    provider: &dyn CryptoProvider,
    key: &[u8],
    value: &serde_json::Value,
) -> Result<serde_json::Value> {
    let plaintext = serde_json::to_vec(value)?;
    let ciphertext = provider.encrypt(key, &plaintext)?;
    let encoded = BASE64.encode(&ciphertext);
    Ok(serde_json::json!({ "__encrypted": encoded }))
}

/// Decrypts a JSON value that was encrypted with [`encrypt_value`].
///
/// If the value is not encrypted (no `__encrypted` key), it is returned as-is.
///
/// # Errors
///
/// Returns an error if decryption or deserialization fails.
pub fn decrypt_value(
    provider: &dyn CryptoProvider,
    key: &[u8],
    value: &serde_json::Value,
) -> Result<serde_json::Value> {
    let Some(encoded) = value.get("__encrypted").and_then(serde_json::Value::as_str) else {
        // Not encrypted — return as-is (backward compatibility)
        return Ok(value.clone());
    };

    let ciphertext = BASE64
        .decode(encoded)
        .map_err(|e| crate::Error::encryption(format!("base64 decode failed: {e}")))?;
    let plaintext = provider.decrypt(key, &ciphertext)?;
    let decrypted: serde_json::Value = serde_json::from_slice(&plaintext)?;
    Ok(decrypted)
}

/// Encrypts specific named fields within a JSON object in-place.
///
/// Each field listed in `fields` is replaced with `{"__encrypted": "<base64>"}`.
/// Fields that are not present in the object are silently skipped.
///
/// # Errors
///
/// Returns an error if the value is not a JSON object, or if encryption fails.
pub fn encrypt_fields(
    provider: &dyn CryptoProvider,
    key: &[u8],
    data: &mut serde_json::Value,
    fields: &[&str],
) -> Result<()> {
    let obj = data
        .as_object_mut()
        .ok_or_else(|| crate::Error::encryption("encrypt_fields requires a JSON object"))?;

    for &field in fields {
        if let Some(value) = obj.get(field) {
            let encrypted = encrypt_value(provider, key, value)?;
            obj.insert(field.to_string(), encrypted);
        }
    }

    Ok(())
}

/// Decrypts any individually-encrypted fields within a JSON object in-place.
///
/// Walks all top-level fields; any value matching `{"__encrypted": "..."}` is
/// decrypted and replaced with its plaintext form. Non-encrypted fields are
/// left untouched.
///
/// # Errors
///
/// Returns an error if the value is not a JSON object, or if decryption fails.
pub fn decrypt_encrypted_fields(
    provider: &dyn CryptoProvider,
    key: &[u8],
    data: &mut serde_json::Value,
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
            let decrypted = decrypt_value(provider, key, value)?;
            obj.insert(field, decrypted);
        }
    }

    Ok(())
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
/// Encrypted values have the shape `{"__encrypted": "<base64>"}`.
///
/// # Examples
///
/// ```
/// use event_sauce_core::crypto::is_encrypted;
/// use serde_json::json;
///
/// assert!(is_encrypted(&json!({"__encrypted": "abc"})));
/// assert!(!is_encrypted(&json!({"name": "Alice"})));
/// ```
#[must_use]
pub fn is_encrypted(value: &serde_json::Value) -> bool {
    value.get("__encrypted").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple XOR-based mock provider for testing.
    /// NOT cryptographically secure — only for unit tests.
    struct MockCryptoProvider;

    impl CryptoProvider for MockCryptoProvider {
        fn encrypt(&self, key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
            Ok(plaintext
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ key[i % key.len()])
                .collect())
        }

        fn decrypt(&self, key: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
            // XOR is its own inverse
            self.encrypt(key, ciphertext)
        }

        fn generate_key(&self) -> Vec<u8> {
            vec![0x42; 32]
        }
    }

    /// Mock provider that always fails.
    struct FailingCryptoProvider;

    impl CryptoProvider for FailingCryptoProvider {
        fn encrypt(&self, _key: &[u8], _plaintext: &[u8]) -> Result<Vec<u8>> {
            Err(crate::Error::encryption("encryption failed"))
        }

        fn decrypt(&self, _key: &[u8], _ciphertext: &[u8]) -> Result<Vec<u8>> {
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

        let encrypted = encrypt_value(&provider, &key, &value).unwrap();

        assert!(is_encrypted(&encrypted));
        assert!(encrypted.get("__encrypted").unwrap().is_string());
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"order_id": "abc-123", "items": [1, 2, 3]});

        let encrypted = encrypt_value(&provider, &key, &value).unwrap();
        let decrypted = decrypt_value(&provider, &key, &encrypted).unwrap();

        assert_eq!(decrypted, value);
    }

    #[test]
    fn decrypt_value_passes_through_unencrypted_data() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"name": "Bob"});

        let result = decrypt_value(&provider, &key, &value).unwrap();
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
    fn encrypt_value_fails_when_provider_fails() {
        let provider = FailingCryptoProvider;
        let key = vec![0; 32];
        let value = serde_json::json!({"data": "test"});

        let result = encrypt_value(&provider, &key, &value);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn decrypt_value_fails_when_provider_fails() {
        // First encrypt with working provider
        let mock = MockCryptoProvider;
        let key = mock.generate_key();
        let value = serde_json::json!({"data": "test"});
        let encrypted = encrypt_value(&mock, &key, &value).unwrap();

        // Then try to decrypt with failing provider
        let failing = FailingCryptoProvider;
        let result = decrypt_value(&failing, &key, &encrypted);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn decrypt_value_fails_on_invalid_base64() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let value = serde_json::json!({"__encrypted": "not-valid-base64!!!"});

        let result = decrypt_value(&provider, &key, &value);
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

        let encrypted = encrypt_value(&provider, &key, &value).unwrap();
        let decrypted = decrypt_value(&provider, &key, &encrypted).unwrap();
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

        let encrypted = encrypt_value(&provider, &key, &value).unwrap();
        assert!(is_encrypted(&encrypted));
        let decrypted = decrypt_value(&provider, &key, &encrypted).unwrap();
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

        encrypt_fields(&provider, &key, &mut data, &["name", "diagnosis"]).unwrap();

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

        encrypt_fields(&provider, &key, &mut data, &["name", "diagnosis"]).unwrap();
        decrypt_encrypted_fields(&provider, &key, &mut data).unwrap();

        assert_eq!(data, original);
    }

    #[test]
    fn encrypt_fields_skips_missing_fields() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!({"name": "Alice"});

        encrypt_fields(&provider, &key, &mut data, &["name", "nonexistent"]).unwrap();

        assert!(is_encrypted(&data["name"]));
        assert!(data.get("nonexistent").is_none());
    }

    #[test]
    fn encrypt_fields_fails_on_non_object() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!([1, 2, 3]);

        let result = encrypt_fields(&provider, &key, &mut data, &["name"]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn encrypt_fields_propagates_provider_error() {
        let provider = FailingCryptoProvider;
        let key = vec![0; 32];
        let mut data = serde_json::json!({"name": "Alice"});

        let result = encrypt_fields(&provider, &key, &mut data, &["name"]);
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

        encrypt_fields(&provider, &key, &mut data, &["name"]).unwrap();
        assert!(is_encrypted(&data["name"]));
        assert_eq!(data["diagnosis"], "flu");

        decrypt_encrypted_fields(&provider, &key, &mut data).unwrap();

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

        decrypt_encrypted_fields(&provider, &key, &mut data).unwrap();

        assert_eq!(data, original);
    }

    #[test]
    fn decrypt_encrypted_fields_fails_on_non_object() {
        let provider = MockCryptoProvider;
        let key = provider.generate_key();
        let mut data = serde_json::json!("a string");

        let result = decrypt_encrypted_fields(&provider, &key, &mut data);
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
}
