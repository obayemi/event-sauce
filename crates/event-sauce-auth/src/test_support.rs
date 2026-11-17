//! Fakes shared by the unit tests of the login methods.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use event_sauce_core::{AggregateClaim, EntityId};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::{
    ClaimLocator, Credentials, Digest, Email, FederatedId, HasSecondFactor, HashError, LocateError,
    LoginId, LoginKey, LoginStore, LoginStoreError, PasswordHash, PasswordHasher, Provider, Sha256,
};

pub(crate) const HASH_A: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";
pub(crate) const HASH_B: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$b3RoZXJzYWx0c2FsdA$b3RoZXJoYXNob3RoZXJoYXNob3RoZXI";

pub(crate) fn email(raw: &str) -> Email {
    Email::parse(raw).unwrap()
}

pub(crate) fn google(subject: &str) -> FederatedId {
    FederatedId::new(Provider::GOOGLE, subject)
}

pub(crate) fn secret(raw: &str) -> SecretString {
    SecretString::from(raw)
}

/// A deterministic hasher that counts what it is asked to do.
#[derive(Debug, Clone, Default)]
pub(crate) struct FakeHasher {
    hashes: Arc<AtomicUsize>,
    verifies: Arc<AtomicUsize>,
    broken: bool,
}

impl FakeHasher {
    pub(crate) fn broken() -> Self {
        Self {
            broken: true,
            ..Self::default()
        }
    }

    pub(crate) fn hashed(&self) -> usize {
        self.hashes.load(Ordering::SeqCst)
    }

    pub(crate) fn verified(&self) -> usize {
        self.verifies.load(Ordering::SeqCst)
    }
}

impl PasswordHasher for FakeHasher {
    fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError> {
        self.hashes.fetch_add(1, Ordering::SeqCst);
        if self.broken {
            return Err(HashError("broken".into()));
        }
        PasswordHash::new(fake_phc(password)).map_err(|e| HashError(e.to_string()))
    }

    fn verify(&self, password: &SecretString, hash: &PasswordHash) -> Result<bool, HashError> {
        self.verifies.fetch_add(1, Ordering::SeqCst);
        if self.broken {
            return Err(HashError("broken".into()));
        }
        Ok(hash.as_str() == fake_phc(password))
    }
}

/// A claim index held in a map.
#[derive(Debug, Default)]
pub(crate) struct FakeLocator(HashMap<(&'static str, String), EntityId>);

impl FakeLocator {
    pub(crate) fn holding(claim: &AggregateClaim, holder: EntityId) -> Self {
        let mut index = HashMap::new();
        index.insert((claim.claim_type, claim.claim_key.to_string()), holder);
        Self(index)
    }
}

#[async_trait]
impl ClaimLocator for FakeLocator {
    async fn locate_claim(&self, claim: &AggregateClaim) -> Result<Option<EntityId>, LocateError> {
        Ok(self
            .0
            .get(&(claim.claim_type, claim.claim_key.to_string()))
            .copied())
    }
}

/// The PHC string a [`FakeHasher`] makes of `password`.
pub(crate) fn fake_phc(password: &SecretString) -> String {
    let digest = Sha256::digest(password.expose_secret().as_bytes());
    format!("$fake$c29tZXNhbHRzYWx0${}", hex::encode(digest))
}

/// A login store held in a map that remembers the ttl of every entry.
#[derive(Debug, Default)]
pub(crate) struct FakeLoginStore {
    entries: Mutex<HashMap<String, (Vec<u8>, Duration)>>,
    broken: bool,
}

impl FakeLoginStore {
    pub(crate) fn broken() -> Self {
        Self {
            broken: true,
            ..Self::default()
        }
    }

    pub(crate) fn ttl_of(&self, key: &LoginKey) -> Option<Duration> {
        self.entries
            .lock()
            .unwrap()
            .get(key.as_str())
            .map(|(_, ttl)| *ttl)
    }

    #[cfg(feature = "totp")]
    pub(crate) fn ttls(&self) -> Vec<Duration> {
        self.entries
            .lock()
            .unwrap()
            .values()
            .map(|(_, ttl)| *ttl)
            .collect()
    }

    #[cfg(feature = "totp")]
    pub(crate) fn keys(&self) -> Vec<String> {
        self.entries.lock().unwrap().keys().cloned().collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    fn check(&self) -> Result<(), LoginStoreError> {
        if self.broken {
            return Err(LoginStoreError::backend(
                "fake store",
                std::io::Error::other("broken"),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl LoginStore for FakeLoginStore {
    async fn put(
        &self,
        key: &LoginKey,
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), LoginStoreError> {
        self.check()?;
        self.entries
            .lock()
            .unwrap()
            .insert(key.as_str().to_owned(), (value.to_vec(), ttl));
        Ok(())
    }

    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        self.check()?;
        Ok(self
            .entries
            .lock()
            .unwrap()
            .remove(key.as_str())
            .map(|(value, _)| value))
    }

    async fn mark_once(&self, key: &LoginKey, ttl: Duration) -> Result<bool, LoginStoreError> {
        self.check()?;
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(key.as_str()) {
            return Ok(false);
        }
        entries.insert(key.as_str().to_owned(), (Vec::new(), ttl));
        Ok(true)
    }
}

/// Credentials that demand a second factor when `second_factor` is set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondFactorCredentials {
    pub(crate) second_factor: bool,
}

impl Credentials for SecondFactorCredentials {
    fn claims(&self) -> Vec<AggregateClaim> {
        Vec::new()
    }

    fn fingerprint(&self, digest: &mut Sha256) {
        digest.update([u8::from(self.second_factor)]);
    }
}

impl HasSecondFactor for SecondFactorCredentials {
    fn requires_second_factor(&self) -> bool {
        self.second_factor
    }
}

#[cfg(test)]
mod tests;
