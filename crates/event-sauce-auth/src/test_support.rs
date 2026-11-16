//! Fakes shared by the unit tests of the login methods.

use std::{
    collections::HashMap,
    fmt::Write as _,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use event_sauce_core::{AggregateClaim, EntityId};
use secrecy::{ExposeSecret, SecretString};

use crate::{ClaimLocator, Digest, HashError, LocateError, PasswordHash, PasswordHasher, Sha256};

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
    let hex = digest.iter().fold(String::new(), |mut hex, byte| {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
        hex
    });
    format!("$fake$c29tZXNhbHRzYWx0${hex}")
}
