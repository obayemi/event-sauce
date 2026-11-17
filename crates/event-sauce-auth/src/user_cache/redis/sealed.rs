//! The sealed form of a cached user: its byte layout and its encryption.

use std::sync::Arc;

use event_sauce_core::{
    AggregateRoot, AggregateVersion, CryptoKeyStore, CryptoProvider, DomainEvent, EntityId,
    EventStore, StoredVersion,
};
use serde::{de::DeserializeOwned, Serialize};
use zeroize::Zeroizing;

use crate::{CacheError, UserAggregate};

const VERSION_LEN: usize = 8;

const DOMAIN: &[u8] = b"user-cache";

/// What a cached user is stored as: its version in the clear, then its state.
///
/// The version is not secret, and having it up front lets a read that does
/// not match skip decryption altogether.
struct Entry {
    version: AggregateVersion,
    body: Vec<u8>,
}

impl Entry {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VERSION_LEN + self.body.len());
        bytes.extend_from_slice(&self.version.as_i64().to_be_bytes());
        bytes.extend_from_slice(&self.body);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, CacheError> {
        let (version, body) = bytes
            .split_first_chunk::<VERSION_LEN>()
            .ok_or(CacheError::Malformed)?;
        Ok(Self {
            version: AggregateVersion::new(i64::from_be_bytes(*version)),
            body: body.to_vec(),
        })
    }
}

/// Binds a ciphertext to the user and the version it was cached at, and
/// separates it from every other ciphertext made with the user's key.
fn associated_data(id: EntityId, version: AggregateVersion) -> Vec<u8> {
    let mut data = Vec::with_capacity(16 + VERSION_LEN + DOMAIN.len());
    data.extend_from_slice(id.as_uuid().as_bytes());
    data.extend_from_slice(&version.as_i64().to_be_bytes());
    data.extend_from_slice(DOMAIN);
    data
}

/// What the event store the users live in offers for encryption.
trait CryptoSource: Send + Sync {
    fn provider(&self) -> Option<&dyn CryptoProvider>;
    fn key_store(&self) -> Option<&dyn CryptoKeyStore>;
}

impl<S: EventStore> CryptoSource for S {
    fn provider(&self) -> Option<&dyn CryptoProvider> {
        self.crypto_provider()
    }

    fn key_store(&self) -> Option<&dyn CryptoKeyStore> {
        self.crypto_key_store()
    }
}

/// Turns users into the bytes a cache keeps, and back.
///
/// A user that is encrypted, fully or by fields, is sealed with the key its
/// events use, so shredding the key also shreds the cached bytes.
#[derive(Default)]
pub(super) struct Sealer {
    crypto: Option<Arc<dyn CryptoSource>>,
}

impl Sealer {
    /// A sealer that encrypts with the crypto provider and key store of
    /// `store`.
    pub(super) fn over<S: EventStore + 'static>(store: Arc<S>) -> Self {
        Self {
            crypto: Some(store),
        }
    }

    pub(super) fn has_crypto(&self) -> bool {
        self.crypto.is_some()
    }

    fn is_encrypted<U: UserAggregate>() -> bool {
        U::is_encrypted() || U::Event::has_any_encrypted_fields()
    }

    async fn user_key(
        &self,
        id: EntityId,
    ) -> Result<Option<(&dyn CryptoProvider, Zeroizing<Vec<u8>>)>, CacheError> {
        let (provider, key_store) = self
            .crypto
            .as_deref()
            .and_then(|source| source.provider().zip(source.key_store()))
            .ok_or_else(|| {
                event_sauce_core::Error::invalid_state(
                    "an encrypted user needs a crypto provider and key store, see with_crypto_from",
                )
            })?;
        let key = key_store.get_key(id.as_uuid()).await?;
        Ok(key.map(|key| (provider, Zeroizing::new(key))))
    }

    /// The bytes to cache `user` as, or `None` when its key is missing.
    pub(super) async fn seal<U>(
        &self,
        user: &AggregateRoot<U>,
    ) -> Result<Option<Vec<u8>>, CacheError>
    where
        U: UserAggregate + Serialize,
    {
        let state = serde_json::to_vec(&**user)?;
        let body = if Self::is_encrypted::<U>() {
            let Some((provider, key)) = self.user_key(user.entity_id()).await? else {
                return Ok(None);
            };
            let data = associated_data(user.entity_id(), user.version());
            provider.encrypt(&key, &state, &data)?
        } else {
            state
        };
        Ok(Some(
            Entry {
                version: user.version(),
                body,
            }
            .encode(),
        ))
    }

    /// The user `bytes` hold, or `None` when they are not at `version` or
    /// their key is gone.
    pub(super) async fn open<U>(
        &self,
        id: EntityId,
        version: AggregateVersion,
        bytes: &[u8],
    ) -> Result<Option<AggregateRoot<U>>, CacheError>
    where
        U: UserAggregate + DeserializeOwned,
    {
        let entry = Entry::decode(bytes)?;
        if entry.version != version {
            return Ok(None);
        }
        let state = if Self::is_encrypted::<U>() {
            let Some((provider, key)) = self.user_key(id).await? else {
                return Ok(None);
            };
            provider.decrypt(&key, &entry.body, &associated_data(id, version))?
        } else {
            entry.body
        };
        let stored_version =
            StoredVersion::try_from(version).map_err(event_sauce_core::Error::from)?;
        let Ok(user) = AggregateRoot::restore(stored_version, serde_json::from_slice::<U>(&state)?);
        Ok(Some(user))
    }
}

#[cfg(test)]
mod tests;
