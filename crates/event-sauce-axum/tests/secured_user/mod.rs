//! A user who signs in with a password and may enrol a TOTP key, shared by
//! the totp integration tests.

use std::sync::Arc;

use argon2::Params;
use event_sauce_auth::{
    Argon2Hasher, Credentials, Email, Fingerprint, HasPassword, HasTotp, Identity, LoginId,
    Password, PasswordHash, PasswordHasher, TotpSecret, UserAggregate,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, Entity, EntityId,
    EventStore, Repository,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

pub const ADA: &str = "ada@example.com";
pub const PASSWORD: &str = "correct-horse";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Secured {
    password: Password<Email>,
    totp: Option<TotpSecret>,
}

impl Credentials for Secured {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.password.claims()
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        self.password.fingerprint(fingerprint);
        self.feed_totp_fingerprint(fingerprint);
    }
}

impl HasPassword for Secured {
    type Login = Email;

    fn password(&self) -> Option<&Password<Email>> {
        Some(&self.password)
    }
}

impl HasTotp for Secured {
    fn totp_secret(&self) -> Option<&TotpSecret> {
        self.totp.as_ref()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    id: EntityId,
    identity: Identity<Secured>,
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
    type Credentials = Secured;

    fn identity(&self) -> &Identity<Secured> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        true
    }
}

define_events! {
    pub enum UserEvent for User {
        Registered {
            credentials: Secured,
        }
        @init
        => |id, event| {
            User {
                id,
                identity: Identity::Registered(event.credentials.clone()),
            }
        },

        TotpEnrolled {
            secret: TotpSecret,
        }
        => |user, event| {
            if let Identity::Registered(credentials) = &mut user.identity {
                credentials.totp = Some(event.secret.clone());
            }
        },

        TotpDisabled {}
        => |user, _event| {
            if let Identity::Registered(credentials) = &mut user.identity {
                credentials.totp = None;
            }
        },

        PasswordChanged {
            hash: PasswordHash,
        }
        => |user, event| {
            if let Identity::Registered(credentials) = &mut user.identity {
                credentials.password.set_hash(event.hash.clone());
            }
        },
    }
}

aggregate_command! {
    impl User {
        @clock @init fn register(credentials: Secured) -> RegisteredEvent { credentials };
        @clock fn enroll_totp(secret: TotpSecret) -> TotpEnrolledEvent { secret };
        @clock fn disable_totp() -> TotpDisabledEvent { };
        @clock fn change_password(hash: PasswordHash) -> PasswordChangedEvent { hash };
    }
}

pub fn cheap_hasher() -> Argon2Hasher {
    Argon2Hasher::new(Params::new(8, 1, 1, None).unwrap())
}

/// Saves Ada, with `totp` as her second factor when there is one.
pub async fn register<S: EventStore + 'static>(
    store: &Arc<S>,
    totp: Option<&TotpSecret>,
) -> EntityId {
    let hash = cheap_hasher()
        .hash(&SecretString::from(PASSWORD))
        .await
        .unwrap();
    let credentials = Secured {
        password: Password::new(Email::parse(ADA).unwrap(), hash),
        totp: totp.cloned(),
    };
    let mut user = User::register(credentials).unwrap();
    let id = user.entity_id();
    store.repository::<User>().save(&mut user).await.unwrap();
    id
}

pub async fn enroll_totp<S: EventStore + 'static>(
    store: &Arc<S>,
    id: EntityId,
    secret: &TotpSecret,
) {
    store
        .repository::<User>()
        .modify(id, |user| user.enroll_totp(secret.clone()))
        .await
        .unwrap();
}

pub async fn disable_totp<S: EventStore + 'static>(store: &Arc<S>, id: EntityId) {
    store
        .repository::<User>()
        .modify(id, |user| user.disable_totp())
        .await
        .unwrap();
}

pub async fn totp_of<S: EventStore + 'static>(store: &Arc<S>, id: EntityId) -> Option<TotpSecret> {
    let user = store.repository::<User>().load(id).await.unwrap();
    user.identity()
        .credentials()
        .and_then(|credentials| credentials.totp.clone())
}

pub async fn change_password<S: EventStore + 'static>(
    store: &Arc<S>,
    id: EntityId,
    hash: &PasswordHash,
) {
    store
        .repository::<User>()
        .modify(id, |user| user.change_password(hash.clone()))
        .await
        .unwrap();
}
