//! The event-sourced `Member` user of the example.

use event_sauce::auth::{Email, Identity, Password, PasswordHash};
use event_sauce::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum MemberError {}

impl AggregateError for MemberError {}

#[aggregate(event = "MemberEvent", error = "MemberError", init, user)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Member {
    #[id]
    id: EntityId,
    #[user(identity)]
    identity: Identity<Password<Email>>,
    pub nickname: String,
}

define_events! {
    pub enum MemberEvent for Member {
        Joined {
            nickname: String,
            credentials: Password<Email>,
        }
        @init
        => |id, event| {
            Member {
                id,
                identity: Identity::Registered(event.credentials.clone()),
                nickname: event.nickname.clone(),
            }
        },

        PasswordChanged {
            hash: PasswordHash,
        }
        => |member, event| {
            if let Identity::Registered(credentials) = &mut member.identity {
                credentials.set_hash(event.hash.clone());
            }
        },
    }
}

aggregate_command! {
    impl Member {
        @clock @init fn join(nickname: String, credentials: Password<Email>) -> JoinedEvent {
            nickname, credentials
        };
        @clock fn change_password(hash: PasswordHash) -> PasswordChangedEvent { hash };
    }
}
