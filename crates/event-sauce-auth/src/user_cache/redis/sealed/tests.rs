use std::sync::Arc;

use event_sauce_core::{AggregateRoot, EntityId, EventStore, StoredVersion};
use event_sauce_memory::InMemoryEventStore;
use vault::Vault;

use super::*;
use crate::user_cache::test_support::{cache_user_at, CacheUser};
use crate::Identity;

#[test]
fn an_entry_round_trips_its_version_and_body() {
    let entry = Entry {
        version: AggregateVersion::new(42),
        body: b"body".to_vec(),
    };

    let decoded = Entry::decode(&entry.encode()).unwrap();

    assert_eq!(decoded.version, AggregateVersion::new(42));
    assert_eq!(decoded.body, b"body");
}

#[test]
fn an_entry_may_have_an_empty_body() {
    let decoded = Entry::decode(
        &Entry {
            version: AggregateVersion::new(1),
            body: Vec::new(),
        }
        .encode(),
    );

    assert!(decoded.unwrap().body.is_empty());
}

#[test]
fn an_entry_shorter_than_its_header_is_malformed() {
    assert!(matches!(
        Entry::decode(&[0; VERSION_LEN - 1]),
        Err(CacheError::Malformed)
    ));
}

#[test]
fn the_associated_data_binds_the_user_and_the_version() {
    let (ada, grace) = (EntityId::new(), EntityId::new());
    let at = |id, n| associated_data(id, AggregateVersion::new(n));

    assert_eq!(at(ada, 3), at(ada, 3));
    assert_ne!(at(ada, 3), at(ada, 4));
    assert_ne!(at(ada, 3), at(grace, 3));
}

mod vault {
    use event_sauce_core::{define_events, Aggregate, AggregateError, Entity, EntityId};
    use serde::{Deserialize, Serialize};

    use crate::test_support::SecondFactorCredentials;
    use crate::{Identity, UserAggregate};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Vault {
        pub id: EntityId,
        pub identity: Identity<SecondFactorCredentials>,
        pub secret: String,
    }

    impl Entity for Vault {
        fn entity_id(&self) -> EntityId {
            self.id
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("vault error")]
    pub struct VaultError;

    impl AggregateError for VaultError {}

    impl Aggregate for Vault {
        type Event = VaultEvent;
        type Error = VaultError;
        type DeletedState = Self;

        fn is_encrypted() -> bool {
            true
        }
    }

    impl UserAggregate for Vault {
        type Credentials = SecondFactorCredentials;

        fn identity(&self) -> &Identity<SecondFactorCredentials> {
            &self.identity
        }
    }

    define_events! {
        pub enum VaultEvent for Vault {
            Locked {
                secret: String,
            } => |vault, event| {
                vault.secret.clone_from(&event.secret);
            },
        }
    }
}

const SECRET: &str = "correct horse battery staple";

fn vault_at(id: EntityId, version: i64) -> AggregateRoot<Vault> {
    let version = StoredVersion::try_from(AggregateVersion::new(version)).unwrap();
    AggregateRoot::restore(
        version,
        Vault {
            id,
            identity: Identity::Anonymous,
            secret: SECRET.to_owned(),
        },
    )
    .unwrap()
}

fn store() -> Arc<InMemoryEventStore> {
    Arc::new(InMemoryEventStore::builder().build())
}

async fn give_key(store: &InMemoryEventStore, id: EntityId) {
    let key = store.crypto_provider().unwrap().generate_key();
    store
        .crypto_key_store()
        .unwrap()
        .upsert_key(id.as_uuid(), key)
        .await
        .unwrap();
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

#[tokio::test]
async fn a_plain_user_round_trips_without_crypto() {
    let sealer = Sealer::default();
    let id = EntityId::new();
    let user = cache_user_at(id, "ada", 3);

    let bytes = sealer.seal(&user).await.unwrap().unwrap();
    let opened: AggregateRoot<CacheUser> = sealer
        .open(id, AggregateVersion::new(3), &bytes)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(*opened, *user);
    assert_eq!(opened.version(), user.version());
    assert!(contains(&bytes, "ada"));
}

#[tokio::test]
async fn a_user_is_a_miss_at_any_other_version_without_decrypting() {
    let sealer = Sealer::default();
    let id = EntityId::new();
    let bytes = sealer
        .seal(&cache_user_at(id, "ada", 3))
        .await
        .unwrap()
        .unwrap();

    let other: Option<AggregateRoot<Vault>> = sealer
        .open(id, AggregateVersion::new(4), &bytes)
        .await
        .unwrap();

    assert!(other.is_none());
}

#[tokio::test]
async fn bytes_shorter_than_an_entry_are_malformed() {
    let opened = Sealer::default()
        .open::<CacheUser>(EntityId::new(), AggregateVersion::new(1), &[0; 3])
        .await;

    assert!(matches!(opened, Err(CacheError::Malformed)));
}

#[tokio::test]
async fn an_encrypted_user_round_trips_as_ciphertext() {
    let store = store();
    let sealer = Sealer::over(store.clone());
    let id = EntityId::new();
    give_key(&store, id).await;
    let user = vault_at(id, 5);

    let bytes = sealer.seal(&user).await.unwrap().unwrap();
    let opened: AggregateRoot<Vault> = sealer
        .open(id, AggregateVersion::new(5), &bytes)
        .await
        .unwrap()
        .unwrap();

    assert!(!contains(&bytes, SECRET));
    assert_eq!(*opened, *user);
    assert_eq!(opened.version(), user.version());
}

#[tokio::test]
async fn an_encrypted_user_without_a_key_is_neither_sealed_nor_opened() {
    let store = store();
    let sealer = Sealer::over(store.clone());
    let id = EntityId::new();
    assert!(sealer.seal(&vault_at(id, 5)).await.unwrap().is_none());

    give_key(&store, id).await;
    let bytes = sealer.seal(&vault_at(id, 5)).await.unwrap().unwrap();
    store
        .crypto_key_store()
        .unwrap()
        .delete_key(id.as_uuid())
        .await
        .unwrap();

    let opened: Option<AggregateRoot<Vault>> = sealer
        .open(id, AggregateVersion::new(5), &bytes)
        .await
        .unwrap();
    assert!(opened.is_none());
}

#[tokio::test]
async fn an_encrypted_user_needs_crypto_to_be_sealed_or_opened() {
    let sealer = Sealer::default();
    let id = EntityId::new();

    assert!(matches!(
        sealer.seal(&vault_at(id, 5)).await,
        Err(CacheError::Crypto(_))
    ));
    let entry = Entry {
        version: AggregateVersion::new(5),
        body: Vec::new(),
    }
    .encode();
    assert!(matches!(
        sealer
            .open::<Vault>(id, AggregateVersion::new(5), &entry)
            .await,
        Err(CacheError::Crypto(_))
    ));
}

#[tokio::test]
async fn a_ciphertext_does_not_open_under_another_user() {
    let store = store();
    let sealer = Sealer::over(store.clone());
    let (ada, grace) = (EntityId::new(), EntityId::new());
    give_key(&store, ada).await;
    let key = store
        .crypto_key_store()
        .unwrap()
        .get_key(ada.as_uuid())
        .await
        .unwrap()
        .unwrap();
    store
        .crypto_key_store()
        .unwrap()
        .upsert_key(grace.as_uuid(), key)
        .await
        .unwrap();
    let bytes = sealer.seal(&vault_at(ada, 5)).await.unwrap().unwrap();

    let opened = sealer
        .open::<Vault>(grace, AggregateVersion::new(5), &bytes)
        .await;

    assert!(matches!(opened, Err(CacheError::Crypto(_))));
}

#[test]
fn a_sealer_reports_whether_it_has_crypto() {
    assert!(!Sealer::default().has_crypto());
    assert!(Sealer::over(store()).has_crypto());
}
