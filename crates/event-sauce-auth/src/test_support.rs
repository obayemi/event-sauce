//! Fakes shared by the unit tests of the login methods.

use std::{
    collections::HashMap,
    fmt::Write as _,
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
    ClaimLocator, Credentials, Digest, HasSecondFactor, HashError, LocateError, LoginKey,
    LoginStore, LoginStoreError, PasswordHash, PasswordHasher, Sha256,
};

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

/// A user aggregate with just enough state to tell two versions apart.
#[cfg(feature = "moka")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheUser {
    pub id: EntityId,
    pub identity: crate::Identity<SecondFactorCredentials>,
    pub name: String,
}

#[cfg(feature = "moka")]
impl event_sauce_core::Entity for CacheUser {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

#[cfg(feature = "moka")]
#[derive(Debug, thiserror::Error)]
#[error("cache user error")]
pub struct CacheUserError;

#[cfg(feature = "moka")]
impl event_sauce_core::AggregateError for CacheUserError {}

#[cfg(feature = "moka")]
impl event_sauce_core::Aggregate for CacheUser {
    type Event = CacheUserEvent;
    type Error = CacheUserError;
    type DeletedState = Self;
}

#[cfg(feature = "moka")]
impl crate::UserAggregate for CacheUser {
    type Credentials = SecondFactorCredentials;

    fn identity(&self) -> &crate::Identity<SecondFactorCredentials> {
        &self.identity
    }
}

#[cfg(feature = "moka")]
event_sauce_core::define_events! {
    pub enum CacheUserEvent for CacheUser {
        Renamed {
            name: String,
        } => |user, event| {
            user.name.clone_from(&event.name);
        },
    }
}

/// A stored user at `version`, as a repository would load it.
#[cfg(feature = "moka")]
pub(crate) fn cache_user_at(
    id: EntityId,
    name: &str,
    version: i64,
) -> event_sauce_core::AggregateRoot<CacheUser> {
    let version =
        event_sauce_core::StoredVersion::try_from(event_sauce_core::AggregateVersion::new(version))
            .expect("a stored version is positive");
    event_sauce_core::AggregateRoot::restore(
        version,
        CacheUser {
            id,
            identity: crate::Identity::Anonymous,
            name: name.to_owned(),
        },
    )
    .expect("a user restores from itself")
}
