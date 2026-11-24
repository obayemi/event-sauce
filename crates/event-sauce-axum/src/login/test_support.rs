//! A user aggregate the unit tests of the login modules share.

use std::sync::Arc;

use event_sauce_auth::{
    Authenticated, Email, Identity, LoginId, Logins, Password, PasswordHash, UserAggregate,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateError, AggregateRoot, Entity, EntityId,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};

const PHC: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";
const OTHER_PHC: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$b3RoZXJzYWx0b3RoZXI$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";

pub type Credentials = Password<Email>;

#[derive(Debug, thiserror::Error)]
pub enum MemberError {}

impl AggregateError for MemberError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Member {
    id: EntityId,
    identity: Identity<Credentials>,
}

impl Entity for Member {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Member {
    type Event = MemberEvent;
    type Error = MemberError;
    type DeletedState = Self;
}

impl UserAggregate for Member {
    type Credentials = Credentials;

    fn identity(&self) -> &Identity<Credentials> {
        &self.identity
    }
}

define_events! {
    pub enum MemberEvent for Member {
        Joined {
            credentials: Credentials,
        }
        @init
        => |id, event| {
            Member { id, identity: Identity::Registered(event.credentials.clone()) }
        },

        JoinedAnonymously {}
        @init
        => |id, _event| {
            Member { id, identity: Identity::Anonymous }
        },

        Touched {}
        => |_member, _event| {},
    }
}

aggregate_command! {
    impl Member {
        @clock @init fn join(credentials: Credentials) -> JoinedEvent { credentials };
        @clock @init fn join_anonymously() -> JoinedAnonymouslyEvent { };
    }
}

pub fn credentials(phc: &str) -> Credentials {
    Password::new(
        Email::parse("ada@example.com").unwrap(),
        PasswordHash::new(phc).unwrap(),
    )
}

pub fn registered() -> AggregateRoot<Member> {
    Member::join(credentials(PHC)).unwrap()
}

pub fn registered_with_another_password() -> AggregateRoot<Member> {
    Member::join(credentials(OTHER_PHC)).unwrap()
}

pub fn anonymous() -> AggregateRoot<Member> {
    Member::join_anonymously().unwrap()
}

pub async fn authenticated(root: AggregateRoot<Member>) -> Authenticated<Member> {
    Logins::<Member>::new(Arc::new(InMemoryEventStore::new()))
        .sign_up(root)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap()
}
