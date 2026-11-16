//! An encrypted user aggregate and the helpers the user cache integration
//! tests share.

use std::sync::Arc;

use event_sauce_auth::{Email, Identity, LoginId, Password, PasswordHash, UserAggregate};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateError, AggregateRoot, Entity, EntityId,
    EventStore, Repository,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};

const PHC: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";

pub type Credentials = Password<Email>;

#[derive(Debug, thiserror::Error)]
pub enum MemberError {}

impl AggregateError for MemberError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    id: EntityId,
    identity: Identity<Credentials>,
    pub nickname: String,
    active: bool,
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

    fn is_encrypted() -> bool {
        true
    }
}

impl UserAggregate for Member {
    type Credentials = Credentials;

    fn identity(&self) -> &Identity<Credentials> {
        &self.identity
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

define_events! {
    pub enum MemberEvent for Member {
        Joined {
            nickname: String,
            credentials: Credentials,
        }
        @init
        => |id, event| {
            Member {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                nickname: event.nickname.clone(),
                active: true,
            }
        },

        Renamed {
            nickname: String,
        }
        => |member, event| {
            member.nickname.clone_from(&event.nickname);
        },

        Suspended {}
        => |member, _event| {
            member.active = false;
        },
    }
}

aggregate_command! {
    impl Member {
        @clock @init fn join(nickname: String, credentials: Credentials) -> JoinedEvent {
            nickname, credentials
        };
        @clock fn rename(nickname: String) -> RenamedEvent { nickname };
        @clock fn suspend() -> SuspendedEvent { };
    }
}

pub fn credentials(login: &str) -> Credentials {
    Password::new(
        Email::parse(login).unwrap(),
        PasswordHash::new(PHC).unwrap(),
    )
}

pub async fn join(store: &Arc<InMemoryEventStore>, nickname: &str) -> EntityId {
    let mut member = Member::join(
        nickname.to_owned(),
        credentials(&format!("{nickname}@example.com")),
    )
    .unwrap();
    let id = member.entity_id();
    store
        .repository::<Member>()
        .save(&mut member)
        .await
        .unwrap();
    id
}

pub async fn rename(store: &Arc<InMemoryEventStore>, id: EntityId, nickname: &str) {
    store
        .repository::<Member>()
        .modify(id, |member| member.rename(nickname.to_owned()))
        .await
        .unwrap();
}

pub async fn suspend(store: &Arc<InMemoryEventStore>, id: EntityId) {
    store
        .repository::<Member>()
        .modify(id, |member| member.suspend())
        .await
        .unwrap();
}

pub async fn load(store: &Arc<InMemoryEventStore>, id: EntityId) -> Arc<AggregateRoot<Member>> {
    Arc::new(store.repository::<Member>().load_by_id(id).await.unwrap())
}
