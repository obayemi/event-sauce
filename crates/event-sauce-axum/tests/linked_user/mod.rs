//! A user who signs in through an identity provider and may enrol a TOTP
//! key, shared by the oauth integration tests.

use std::sync::Arc;

use event_sauce_auth::{
    Credentials, Federated, FederatedId, Fingerprint, HasFederated, HasTotp, Identity,
    LoginMethodError, Provider, TotpSecret, UserAggregate,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, Entity, EntityId,
    EventStore, Repository,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Linked {
    federated: Federated,
    totp: Option<TotpSecret>,
}

impl Credentials for Linked {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.federated.claims()
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        self.federated.fingerprint(fingerprint);
        self.feed_totp_fingerprint(fingerprint);
    }
}

impl HasFederated for Linked {
    fn federated(&self) -> Option<&Federated> {
        Some(&self.federated)
    }
}

impl HasTotp for Linked {
    fn totp_secret(&self) -> Option<&TotpSecret> {
        self.totp.as_ref()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UserError {
    #[error(transparent)]
    Link(#[from] LoginMethodError),
}

impl AggregateError for UserError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    id: EntityId,
    identity: Identity<Linked>,
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
    type Credentials = Linked;

    fn identity(&self) -> &Identity<Linked> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

define_events! {
    pub enum UserEvent for User {
        Registered {
            credentials: Linked,
        }
        @init
        => |id, event| {
            User {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                active: true,
            }
        },

        AccountLinked {
            id: FederatedId,
        }
        => |user, event| {
            if let Identity::Registered(credentials) = &mut user.identity {
                let _ = credentials.federated.link(event.id.clone());
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

        Deactivated {}
        => |user, _event| {
            user.active = false;
        },
    }
}

aggregate_command! {
    impl User {
        @clock @init fn register(credentials: Linked) -> RegisteredEvent { credentials };
        @clock fn link(id: FederatedId) -> AccountLinkedEvent { id };
        @clock fn enroll_totp(secret: TotpSecret) -> TotpEnrolledEvent { secret };
        @clock fn deactivate() -> DeactivatedEvent { };
    }
}

pub fn guest(first: FederatedId) -> Linked {
    Linked {
        federated: Federated::new(first),
        totp: None,
    }
}

pub fn google(subject: &str) -> FederatedId {
    FederatedId::new(Provider::GOOGLE, subject)
}

pub async fn register<S: EventStore + 'static>(
    store: &Arc<S>,
    first: FederatedId,
    totp: Option<&TotpSecret>,
) -> EntityId {
    let credentials = Linked {
        federated: Federated::new(first),
        totp: totp.cloned(),
    };
    let mut user = User::register(credentials).unwrap();
    let id = user.entity_id();
    store.repository::<User>().save(&mut user).await.unwrap();
    id
}

pub async fn link<S: EventStore + 'static>(store: &Arc<S>, id: EntityId, account: FederatedId) {
    store
        .repository::<User>()
        .modify(id, |user| user.link(account))
        .await
        .unwrap();
}

pub async fn deactivate<S: EventStore + 'static>(store: &Arc<S>, id: EntityId) {
    store
        .repository::<User>()
        .modify(id, |user| user.deactivate())
        .await
        .unwrap();
}

pub async fn links_of<S: EventStore + 'static>(store: &Arc<S>, id: EntityId) -> Vec<FederatedId> {
    let user = store.repository::<User>().load(id).await.unwrap();
    user.identity()
        .credentials()
        .map(|credentials| credentials.federated.links().to_vec())
        .unwrap_or_default()
}
