//! # event-sauce-crypto
//!
//! AES-256-GCM encryption provider for event-sauce privacy support.
//!
//! Provides [`Aes256GcmProvider`], a [`CryptoProvider`] implementation using
//! AES-256-GCM authenticated encryption. Used with private aggregates for
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
//! let ciphertext = provider.encrypt(&key, plaintext).unwrap();
//! let decrypted = provider.decrypt(&key, &ciphertext).unwrap();
//! assert_eq!(decrypted, plaintext);
//! ```

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
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
/// let encrypted = provider.encrypt(&key, data).unwrap();
/// let decrypted = provider.decrypt(&key, &encrypted).unwrap();
/// assert_eq!(decrypted, data);
/// ```
pub struct Aes256GcmProvider;

/// Nonce size for AES-256-GCM (96 bits / 12 bytes).
const NONCE_SIZE: usize = 12;

impl CryptoProvider for Aes256GcmProvider {
    fn encrypt(&self, key: &[u8], plaintext: &[u8]) -> event_sauce_core::Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| event_sauce_core::Error::encryption(format!("invalid key: {e}")))?;

        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| event_sauce_core::Error::encryption(format!("encryption failed: {e}")))?;

        // Prepend nonce to ciphertext: nonce (12) || ciphertext
        let mut output = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        output.extend_from_slice(&nonce);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    fn decrypt(&self, key: &[u8], ciphertext: &[u8]) -> event_sauce_core::Result<Vec<u8>> {
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
            .decrypt(nonce, encrypted)
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

        let ciphertext = provider.encrypt(&key, plaintext).unwrap();
        let decrypted = provider.decrypt(&key, &ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn encrypt_produces_different_ciphertext_each_time() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let plaintext = b"same data";

        let ct1 = provider.encrypt(&key, plaintext).unwrap();
        let ct2 = provider.encrypt(&key, plaintext).unwrap();

        // Different nonces mean different ciphertexts
        assert_ne!(ct1, ct2);

        // Both decrypt to the same plaintext
        assert_eq!(
            provider.decrypt(&key, &ct1).unwrap(),
            provider.decrypt(&key, &ct2).unwrap()
        );
    }

    #[test]
    fn wrong_key_fails_decryption() {
        let provider = Aes256GcmProvider;
        let key1 = provider.generate_key();
        let key2 = provider.generate_key();
        let plaintext = b"secret";

        let ciphertext = provider.encrypt(&key1, plaintext).unwrap();
        let result = provider.decrypt(&key2, &ciphertext);

        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn corrupted_ciphertext_fails_decryption() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let plaintext = b"important data";

        let mut ciphertext = provider.encrypt(&key, plaintext).unwrap();
        // Corrupt a byte in the encrypted portion (after the nonce)
        if let Some(byte) = ciphertext.get_mut(NONCE_SIZE + 1) {
            *byte ^= 0xFF;
        }

        let result = provider.decrypt(&key, &ciphertext);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn ciphertext_too_short_fails() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();

        // Less than NONCE_SIZE bytes
        let result = provider.decrypt(&key, &[0u8; 5]);
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

        let result = provider.encrypt(&short_key, b"data");
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn invalid_key_length_fails_decrypt() {
        let provider = Aes256GcmProvider;
        let short_key = vec![0u8; 16];
        // Create a fake ciphertext with valid nonce length
        let fake_ct = vec![0u8; NONCE_SIZE + 32];

        let result = provider.decrypt(&short_key, &fake_ct);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_encryption());
    }

    #[test]
    fn encrypt_empty_plaintext() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();

        let ciphertext = provider.encrypt(&key, b"").unwrap();
        let decrypted = provider.decrypt(&key, &ciphertext).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn encrypt_large_payload() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let large_data = vec![0x42u8; 1_000_000]; // 1 MB

        let ciphertext = provider.encrypt(&key, &large_data).unwrap();
        let decrypted = provider.decrypt(&key, &ciphertext).unwrap();
        assert_eq!(decrypted, large_data);
    }

    #[test]
    fn ciphertext_includes_nonce_prefix() {
        let provider = Aes256GcmProvider;
        let key = provider.generate_key();
        let plaintext = b"test";

        let ciphertext = provider.encrypt(&key, plaintext).unwrap();
        // Ciphertext should be: nonce(12) + encrypted_data + auth_tag(16)
        assert!(ciphertext.len() > NONCE_SIZE);
    }
}
