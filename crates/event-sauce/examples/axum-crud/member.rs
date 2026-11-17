//! The event-sourced `Member` user of the example.

use event_sauce::auth::PasswordHasher;
use event_sauce::auth::{Argon2Hasher, Email, HashError, Identity, Password, PasswordHash};
use event_sauce::prelude::*;
use secrecy::SecretString;
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
    id: EntityId,
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
                id,
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

/// Why a password could not be turned into a hash.
#[derive(Debug, thiserror::Error)]
pub enum HashingError {
    #[error(transparent)]
    Refused(#[from] MemberError),
    #[error("the hasher failed: {0}")]
    Failed(#[from] HashError),
}

/// Hashes `password` unless it is too short to be acceptable.
///
/// # Errors
///
/// [`MemberError::WeakPassword`] for a short password, or the hasher's own
/// failure.
pub fn hash_password(
    hasher: &Argon2Hasher,
    password: String,
) -> Result<PasswordHash, HashingError> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(MemberError::WeakPassword.into());
    }
    Ok(hasher.hash(&SecretString::from(password))?)
}
