//! Fakes shared by the unit tests of the login methods.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, AggregateRoot,
    Entity, EntityId, StoredVersion,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use sha2::{Digest, Sha256};

use crate::{
    login::users::{Found, Users},
    ClaimLocator, Email, FederatedId, HashError, Identity, LocateError, LoginId, LoginKey,
    LoginStore, LoginStoreError, LoginTtl, Password, PasswordHash, PasswordHasher, Provider,
    SignUpError, UserAggregate, UserCache,
};

#[cfg(any(feature = "moka", feature = "redis"))]
use crate::{Credentials, Fingerprint};

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

#[async_trait]
impl PasswordHasher for FakeHasher {
    async fn hash(&self, password: &SecretString) -> Result<PasswordHash, HashError> {
        self.hashes.fetch_add(1, Ordering::SeqCst);
        if self.broken {
            return Err(HashError("broken".into()));
        }
        PasswordHash::new(fake_phc(password)).map_err(|e| HashError(e.to_string()))
    }

    async fn verify(
        &self,
        password: &SecretString,
        hash: &PasswordHash,
    ) -> Result<bool, HashError> {
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
    entries: Mutex<HashMap<String, (Vec<u8>, LoginTtl)>>,
    broken: bool,
}

impl FakeLoginStore {
    pub(crate) fn broken() -> Self {
        Self {
            broken: true,
            ..Self::default()
        }
    }

    pub(crate) fn ttl_of(&self, key: &LoginKey) -> Option<LoginTtl> {
        self.entries
            .lock()
            .unwrap()
            .get(key.as_str())
            .map(|(_, ttl)| *ttl)
    }

    #[cfg(feature = "totp")]
    pub(crate) fn ttls(&self) -> Vec<LoginTtl> {
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
        ttl: LoginTtl,
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

    async fn mark_once(&self, key: &LoginKey, ttl: LoginTtl) -> Result<bool, LoginStoreError> {
        self.check()?;
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(key.as_str()) {
            return Ok(false);
        }
        entries.insert(key.as_str().to_owned(), (Vec::new(), ttl));
        Ok(true)
    }
}

/// Credentials holding one flag, so that two values fingerprint differently.
#[cfg(any(feature = "moka", feature = "redis"))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondFactorCredentials {
    pub(crate) second_factor: bool,
}

#[cfg(any(feature = "moka", feature = "redis"))]
impl Credentials for SecondFactorCredentials {
    fn claims(&self) -> Vec<AggregateClaim> {
        Vec::new()
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        fingerprint.field(&[u8::from(self.second_factor)]);
    }
}

/// The credentials of [`TestUser`].
pub(crate) type TestCredentials = Password<Email>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TestUserError {}

impl AggregateError for TestUserError {}

/// A user aggregate with password credentials and an active flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TestUser {
    id: EntityId,
    identity: Identity<TestCredentials>,
    active: bool,
}

impl Entity for TestUser {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for TestUser {
    type Event = TestUserEvent;
    type Error = TestUserError;
    type DeletedState = Self;
}

impl UserAggregate for TestUser {
    type Credentials = TestCredentials;

    fn identity(&self) -> &Identity<TestCredentials> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

define_events! {
    pub(crate) enum TestUserEvent for TestUser {
        Registered {
            credentials: TestCredentials,
        }
        @init
        => |id, event| {
            TestUser {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                active: true,
            }
        },

        Deactivated {}
        => |user, _event| {
            user.active = false;
        },
    }
}

aggregate_command! {
    impl TestUser {
        @clock @init fn register(credentials: TestCredentials) -> RegisteredEvent { credentials };
    }
}

pub(crate) fn credentials(login: &str, password: &str) -> TestCredentials {
    Password::new(
        email(login),
        PasswordHash::new(fake_phc(&secret(password))).unwrap(),
    )
}

/// A user as the event store holds it, at its first version.
pub(crate) fn stored_user(
    credentials: TestCredentials,
    active: bool,
) -> Arc<AggregateRoot<TestUser>> {
    restored_user(EntityId::new(), credentials, active)
}

/// The user `id` as the event store holds it, at its first version.
pub(crate) fn restored_user(
    id: EntityId,
    credentials: TestCredentials,
    active: bool,
) -> Arc<AggregateRoot<TestUser>> {
    let user = TestUser {
        id,
        identity: Identity::Registered(credentials),
        active,
    };
    Arc::new(AggregateRoot::restore(StoredVersion::FIRST, user).unwrap())
}

/// The users of a [`FakeUsers`], shared between its copies.
#[derive(Debug, Default)]
struct FakeUsersState {
    users: Mutex<HashMap<EntityId, Arc<AggregateRoot<TestUser>>>>,
}

/// Users held in a map, standing in for the event store.
#[derive(Debug, Default, Clone)]
pub(crate) struct FakeUsers {
    state: Arc<FakeUsersState>,
    locator: Arc<FakeLocator>,
}

impl FakeUsers {
    /// Holds `user`, locatable by its password login.
    pub(crate) fn holding(user: &Arc<AggregateRoot<TestUser>>) -> Self {
        let credentials = user.identity().credentials().unwrap();
        let fake = Self {
            state: Arc::default(),
            locator: Arc::new(FakeLocator::holding(
                &Password::claim_for(credentials.login()),
                user.entity_id(),
            )),
        };
        fake.replace(user.clone());
        fake
    }

    pub(crate) fn replace(&self, user: Arc<AggregateRoot<TestUser>>) {
        self.state
            .users
            .lock()
            .unwrap()
            .insert(user.entity_id(), user);
    }

    pub(crate) fn remove(&self, id: EntityId) {
        self.state.users.lock().unwrap().remove(&id);
    }

    fn lookup(&self, id: EntityId) -> Found<TestUser> {
        match self.state.users.lock().unwrap().get(&id) {
            Some(user) if user.is_active() => Found::Active(user.clone()),
            Some(_) => Found::Inactive,
            None => Found::Missing,
        }
    }
}

#[async_trait]
impl Users<TestUser> for FakeUsers {
    fn locator(&self) -> &dyn ClaimLocator {
        &*self.locator
    }

    fn with_cache(&self, _cache: Arc<dyn UserCache<TestUser>>) -> Arc<dyn Users<TestUser>> {
        Arc::new(self.clone())
    }

    async fn find(&self, id: EntityId) -> Result<Found<TestUser>, event_sauce_core::Error> {
        Ok(self.lookup(id))
    }

    async fn current(
        &self,
        held: &Arc<AggregateRoot<TestUser>>,
    ) -> Result<Found<TestUser>, event_sauce_core::Error> {
        Ok(self.lookup(held.entity_id()))
    }

    async fn create(
        &self,
        user: AggregateRoot<TestUser>,
    ) -> Result<Arc<AggregateRoot<TestUser>>, SignUpError> {
        let mut users = self.state.users.lock().unwrap();
        if users.contains_key(&user.entity_id()) || user.pending_events().is_empty() {
            return Err(SignUpError::NotNew);
        }
        let stored = Arc::new(user);
        users.insert(stored.entity_id(), stored.clone());
        Ok(stored)
    }
}

#[cfg(test)]
mod tests;
