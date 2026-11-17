//! The user aggregate, its credentials and second factor, shared by the
//! login and app integration tests.

use argon2::Params;
use async_trait::async_trait;
use event_sauce_auth::{
    Argon2Hasher, Credentials, Digest, Email, HasPassword, HasSecondFactor, Identity, LoginId,
    LoginStore, Password, PasswordHash, PasswordHasher, Rejected, SecondFactor, Sha256,
    UserAggregate,
};
use std::sync::Arc;

use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, Entity, EntityId,
    EventStore, Repository,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

pub const ADA: &str = "ada@example.com";
pub const PASSWORD: &str = "correct-horse";
pub const CODE: &str = "424242";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub password: Password<Email>,
    pub code: Option<String>,
}

impl Credentials for Account {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.password.claims()
    }

    fn fingerprint(&self, digest: &mut Sha256) {
        self.password.fingerprint(digest);
        if let Some(code) = &self.code {
            digest.update(code.as_bytes());
        }
    }
}

impl HasPassword for Account {
    type Login = Email;

    fn password(&self) -> Option<&Password<Email>> {
        Some(&self.password)
    }
}

impl HasSecondFactor for Account {
    fn requires_second_factor(&self) -> bool {
        self.code.is_some()
    }
}

pub struct FixedCode;

#[async_trait]
impl SecondFactor<Account> for FixedCode {
    type Challenge = ();
    type Response = String;

    fn challenge(&self, _stored: &Account) {}

    async fn verify(
        &self,
        stored: &Account,
        _challenge: &(),
        response: &String,
        _store: &dyn LoginStore,
    ) -> Result<(), Rejected> {
        if stored.code.as_ref() == Some(response) {
            Ok(())
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    id: EntityId,
    identity: Identity<Account>,
    active: bool,
}

impl Entity for User {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for User {
    type Event = UserEvent;
    type Error = UserError;
    type DeletedState = Self;

    fn claims(&self) -> Vec<AggregateClaim> {
        self.identity
            .credentials()
            .map(Credentials::claims)
            .unwrap_or_default()
    }
}

impl UserAggregate for User {
    type Credentials = Account;

    fn identity(&self) -> &Identity<Account> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

define_events! {
    pub enum UserEvent for User {
        Registered {
            credentials: Account,
        }
        @init
        => |id, event| {
            User {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                active: true,
            }
        },

        Anonymous {}
        @init
        => |id, _event| {
            User { id, identity: Identity::Anonymous, active: true }
        },

        PasswordChanged {
            hash: PasswordHash,
        }
        => |user, event| {
            if let Identity::Registered(account) = &mut user.identity {
                account.password.set_hash(event.hash.clone());
            }
        },

        SecondFactorEnrolled {
            code: String,
        }
        => |user, event| {
            if let Identity::Registered(account) = &mut user.identity {
                account.code = Some(event.code.clone());
            }
        },

        Deactivated {}
        => |user, _event| {
            user.active = false;
        },
    }
}

aggregate_command! {
    impl User {
        @clock @init fn register(credentials: Account) -> RegisteredEvent { credentials };
        @clock @init fn anonymous() -> AnonymousEvent { };
        @clock fn change_password(hash: PasswordHash) -> PasswordChangedEvent { hash };
        @clock fn enroll_second_factor(code: String) -> SecondFactorEnrolledEvent { code };
        @clock fn deactivate() -> DeactivatedEvent { };
    }
}

pub fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

pub fn hash(password: &str) -> PasswordHash {
    cheap_hasher().hash(&SecretString::from(password)).unwrap()
}

#[derive(Deserialize)]
pub struct LoginForm {
    pub email: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct CodeForm {
    pub code: String,
}

/// Saves Ada, with `code` as her second factor when there is one.
pub async fn register<S: EventStore + 'static>(store: &Arc<S>, code: Option<&str>) -> EntityId {
    let account = Account {
        password: Password::new(Email::parse(ADA).unwrap(), hash(PASSWORD)),
        code: code.map(str::to_owned),
    };
    let mut user = User::register(account).unwrap();
    let id = user.entity_id();
    store.repository::<User>().save(&mut user).await.unwrap();
    id
}

/// Changes the password of the user `id` to `password`.
pub async fn change_password<S: EventStore + 'static>(
    store: &Arc<S>,
    id: EntityId,
    password: &str,
) {
    let hash = hash(password);
    store
        .repository::<User>()
        .modify(id, |user| user.change_password(hash.clone()))
        .await
        .unwrap();
}
