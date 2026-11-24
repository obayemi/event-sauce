//! The event-sourced `Member` user of the example.

use event_sauce::auth::{Email, Identity, Password, PasswordHash};
use event_sauce::prelude::*;
use serde::{Deserialize, Serialize};
use std::result::Result;

/// The shortest password a member may choose.
const MIN_PASSWORD_LEN: usize = 8;

/// Why a member's data was refused.
#[derive(Debug, thiserror::Error)]
pub enum MemberError {
    #[error("a nickname cannot be blank")]
    EmptyNickname,
    #[error("a password needs at least {MIN_PASSWORD_LEN} characters")]
    WeakPassword,
}

impl AggregateError for MemberError {}

/// The typed id of a [`Member`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AggregateId)]
#[serde(transparent)]
#[aggregate_id(Member)]
pub struct MemberId(EntityId);

/// A registered user who owns notes.
///
/// Snapshots every ten events keep the per-request user load short, and
/// the claims of the password identity make an email register only once.
#[aggregate(
    event = "MemberEvent",
    error = "MemberError",
    init,
    user,
    snapshots(every = 10)
)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Member {
    #[id]
    id: MemberId,
    #[user(identity)]
    identity: Identity<Password<Email>>,
    pub nickname: String,
}

impl Member {
    /// The email the member logs in with.
    pub fn email(&self) -> Option<&Email> {
        self.identity.credentials().map(Password::login)
    }
}

define_events! {
    pub enum MemberEvent for Member {
        Joined {
            nickname: String,
            credentials: Password<Email>,
        }
        @init
        @validate |event| {
            if event.nickname.trim().is_empty() {
                return Err(MemberError::EmptyNickname);
            }
            return Ok(());
        }
        => |id, event| {
            Member {
                id: id.into(),
                identity: Identity::Registered(event.credentials.clone()),
                nickname: event.nickname.trim().to_owned(),
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

/// Checks that `password` is long enough to be acceptable.
///
/// # Errors
///
/// [`MemberError::WeakPassword`] when the password is too short.
pub fn check_password(password: &str) -> Result<(), MemberError> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(MemberError::WeakPassword);
    }
    Ok(())
}
