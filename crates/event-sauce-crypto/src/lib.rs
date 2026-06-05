//! # event-sauce-crypto
//!
//! AES-256-GCM encryption provider for event-sauce privacy support.
//!
//! Provides [`Aes256GcmProvider`], a [`CryptoProvider`] implementation using
//! AES-256-GCM authenticated encryption. Used with encrypted aggregates for
//! GDPR-style crypto-shredding.
//!
//! # Examples
//!
//! ```
//! use event_sauce_crypto::Aes256GcmProvider;
//! use event_sauce_core::CryptoProvider;
//!
//! let provider = Aes256GcmProvider;
//! let key = provider.generate_key();
//! assert_eq!(key.len(), 32); // AES-256 requires 32-byte keys
//!
//! let plaintext = b"sensitive event data";
//! // `aad` binds the ciphertext to its context (e.g. aggregate_id || event_id);
//! // an empty slice means no binding. Decryption must supply the same `aad`.
//! let aad = b"aggregate-id||event-id";
//! let ciphertext = provider.encrypt(&key, plaintext, aad).unwrap();
//! let decrypted = provider.decrypt(&key, &ciphertext, aad).unwrap();
//! assert_eq!(decrypted, plaintext);
//! ```

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng, Payload},
    AeadCore, Aes256Gcm, Nonce,
};
use event_sauce_core::CryptoProvider;

pub use event_sauce_core::{CryptoKeyStore, CryptoKeyStoreRef, CryptoProviderRef};

/// AES-256-GCM encryption provider.
///
/// Uses 32-byte keys and 12-byte nonces. The ciphertext format is:
/// `nonce (12 bytes) || ciphertext (variable length, includes 16-byte auth tag)`.
///
/// # Security
///
/// - AES-256-GCM provides both confidentiality and integrity
/// - Each encryption uses a random nonce generated from OS entropy
/// - Authentication tag prevents tampering with ciphertext
/// - The `aad` argument is bound as additional authenticated data: it is
///   authenticated (not encrypted) so a ciphertext only decrypts when the same
///   `aad` is supplied, defeating relocation/replay of a ciphertext onto a
///   different event of the same aggregate
///
/// # Nonce ceiling
///
/// The 96-bit random nonce risks collision as the message count under a single
/// key grows; stay well under ~2^32 encryptions per key for a <2^-32 collision
/// bound. event-sauce uses one key per aggregate, so this is reached only by
/// aggregates with billions of events. See `docs/privacy.md` for rotation
/// guidance and nonce-misuse-resistant alternatives.
///
/// # Examples
///
/// ```
/// use event_sauce_crypto::Aes256GcmProvider;
/// use event_sauce_core::CryptoProvider;
///
/// let provider = Aes256GcmProvider;
/// let key = provider.generate_key();
///
/// let data = b"hello world";
/// let aad = b"aggregate-id||event-id";
/// let encrypted = provider.encrypt(&key, data, aad).unwrap();
/// let decrypted = provider.decrypt(&key, &encrypted, aad).unwrap();
/// assert_eq!(decrypted, data);
/// ```
pub struct Aes256GcmProvider;

/// Nonce size for AES-256-GCM (96 bits / 12 bytes).
const NONCE_SIZE: usize = 12;

impl CryptoProvider for Aes256GcmProvider {
    fn encrypt(
        &self,
        key: &[u8],
        plaintext: &[u8],
        aad: &[u8],
    ) -> event_sauce_core::Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| event_sauce_core::Error::encryption(format!("invalid key: {e}")))?;

        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        // Bind `aad` as additional authenticated data: it is authenticated but
        // not stored, so decryption fails unless the same AAD is supplied.
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|e| event_sauce_core::Error::encryption(format!("encryption failed: {e}")))?;

        // Prepend nonce to ciphertext: nonce (12) || ciphertext
        let mut output = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    fn decrypt(
        &self,
        key: &[u8],
        ciphertext: &[u8],
        aad: &[u8],
    ) -> event_sauce_core::Result<Vec<u8>> {
        if ciphertext.len() < NONCE_SIZE {
            return Err(event_sauce_core::Error::encryption(
                "ciphertext too short: missing nonce",
            ));
        }

        let (nonce_bytes, encrypted) = ciphertext.split_at(NONCE_SIZE);
        let nonce = Nonce::from_slice(nonce_bytes);

        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| event_sauce_core::Error::encryption(format!("invalid key: {e}")))?;

        cipher
            .decrypt(
                nonce,
                Payload {
                    msg: encrypted,
                    aad,
                },
            )
            .map_err(|e| event_sauce_core::Error::encryption(format!("decryption failed: {e}")))
    }

    fn generate_key(&self) -> Vec<u8> {
        Aes256Gcm::generate_key(&mut OsRng).to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let plaintext = b"hello, event-sauce crypto!";

        let ciphertext = provider.encrypt(&key, plaintext, &[]).unwrap();
        let decrypted = provider.decrypt(&key, &ciphertext, &[]).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_produces_different_ciphertext_each_time() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let plaintext = b"same data";

        let ct1 = provider.encrypt(&key, plaintext, &[]).unwrap();
        let ct2 = provider.encrypt(&key, plaintext, &[]).unwrap();

        // Different nonces mean different ciphertexts
        assert_ne!(ct1, ct2);

        // Both decrypt to the same plaintext
        assert_eq!(
            provider.decrypt(&key, &ct1, &[]).unwrap(),
            provider.decrypt(&key, &ct2, &[]).unwrap()
        );
    }

    #[test]
    fn wrong_key_fails_decryption() {
        let provider = Aes256GcmProvider;
        let key1 = provider.generate_key();
        let key2 = provider.generate_key();
        let plaintext = b"secret";

        let ciphertext = provider.encrypt(&key1, plaintext, &[]).unwrap();
        let result = provider.decrypt(&key2, &ciphertext, &[]);

        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn corrupted_ciphertext_fails_decryption() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let plaintext = b"important data";

        let mut ciphertext = provider.encrypt(&key, plaintext, &[]).unwrap();
        // Corrupt a byte in the encrypted portion (after the nonce)
        if let Some(byte) = ciphertext.get_mut(NONCE_SIZE + 1) {
            *byte ^= 0xFF;
        }

        let result = provider.decrypt(&key, &ciphertext, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn ciphertext_too_short_fails() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();

        // Less than NONCE_SIZE bytes
        let result = provider.decrypt(&key, &[0u8; 5], &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn generate_key_produces_32_bytes() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn generate_key_produces_different_keys() {
        let provider = Aes256GcmProvider;
        let key1 = provider.generate_key();
        let key2 = provider.generate_key();
        assert_ne!(key1, key2);
    }

    #[test]
    fn invalid_key_length_fails_encrypt() {
        let provider = Aes256GcmProvider;
        let short_key = vec![0u8; 16]; // AES-256 needs 32 bytes

        let result = provider.encrypt(&short_key, b"data", &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn invalid_key_length_fails_decrypt() {
        let provider = Aes256GcmProvider;
        let short_key = vec![0u8; 16];
        // Create a fake ciphertext with valid nonce length
        let fake_ct = vec![0u8; NONCE_SIZE + 32];

        let result = provider.decrypt(&short_key, &fake_ct, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn encrypt_empty_plaintext() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();

        let ciphertext = provider.encrypt(&key, b"", &[]).unwrap();
        let decrypted = provider.decrypt(&key, &ciphertext, &[]).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn encrypt_large_payload() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let large_data = vec![0x42u8; 1_000_000]; // 1 MB

        let ciphertext = provider.encrypt(&key, &large_data, &[]).unwrap();
        let decrypted = provider.decrypt(&key, &ciphertext, &[]).unwrap();
        assert_eq!(decrypted, large_data);
    }

    #[test]
    fn ciphertext_includes_nonce_prefix() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let plaintext = b"test";

        let ciphertext = provider.encrypt(&key, plaintext, &[]).unwrap();
        // Ciphertext should be: nonce(12) + encrypted_data + auth_tag(16)
        assert!(ciphertext.len() > NONCE_SIZE);
    }

    // --- RED (L5): ciphertext must be bound to its event via AEAD associated data ---
    //
    // The per-aggregate key is shared by every event of that aggregate. Without
    // associated-data (AAD) binding, GCM authenticates only that a blob was
    // produced under the key — NOT that it belongs to a specific event. An
    // attacker (or a buggy migration) with write access can COPY one encrypted
    // payload onto a different event row of the same aggregate and it decrypts +
    // authenticates cleanly, silently corrupting replayed state.
    //
    // The fix binds an `aad` (aggregate_id || event_id) into encrypt/decrypt so
    // that a ciphertext produced for event A FAILS to authenticate when presented
    // in the context of event B.
    #[test]
    fn relocated_ciphertext_fails_auth_with_aad() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let plaintext = b"sensitive event payload";

        // AAD for two distinct events of the SAME aggregate (same key).
        let aggregate_id = [0xAAu8; 16];
        let event_a = [0x01u8; 16];
        let event_b = [0x02u8; 16];

        let mut aad_a = Vec::new();
        aad_a.extend_from_slice(&aggregate_id);
        aad_a.extend_from_slice(&event_a);

        let mut aad_b = Vec::new();
        aad_b.extend_from_slice(&aggregate_id);
        aad_b.extend_from_slice(&event_b);

        // Encrypt the payload bound to event A.
        let ciphertext = provider.encrypt(&key, plaintext, &aad_a).unwrap();

        // Relocating event A's ciphertext onto event B (different AAD, same key)
        // MUST fail authentication.
        let relocated = provider.decrypt(&key, &ciphertext, &aad_b);
        assert!(
            relocated.is_err(),
            "relocating a ciphertext to a different event (different AAD) must fail authentication"
        );

        // Decrypting with the ORIGINAL AAD must still succeed.
        let original = provider.decrypt(&key, &ciphertext, &aad_a).unwrap();
        assert_eq!(original, plaintext);
    }
}
