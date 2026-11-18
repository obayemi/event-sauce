//! The `auth` feature puts the login crate behind the facade.
//!
//! Nothing below names `event_sauce_auth` or `event_sauce_core`:
//! `#[aggregate(user)]` expands to paths rooted at `event_sauce::auth`, so a
//! downstream crate that depends on the facade alone compiles it.

use std::sync::Arc;

use event_sauce::auth::{
    Argon2Hasher, Authenticator, ClaimAuthenticator, Email, Identity, LoginId, Password,
    PasswordHash, PasswordHasher, PasswordLogin, UserAggregate,
};
use event_sauce::memory::InMemoryEventStore;
use event_sauce::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
#[error("member error")]
struct MemberError;

impl AggregateError for MemberError {}

#[aggregate(event = "MemberEvent", error = "MemberError", init, user)]
#[derive(Debug, Serialize, Deserialize)]
struct Member {
    #[id]
    id: EntityId,
    #[user(identity)]
    identity: Identity<Password<Email>>,
}

define_events! {
    enum MemberEvent for Member {
        Joined {
            credentials: Password<Email>,
        }
        @init
        => |id, event| {
            Member { id, identity: Identity::Registered(event.credentials.clone()) }
        },

        PasswordChanged {
            hash: PasswordHash,
        }
        => |member, event| {
            if let Identity::Registered(login) = &mut member.identity {
                login.set_hash(event.hash.clone());
            }
        },
    }
}

aggregate_command! {
    impl Member {
        @clock @init fn join(credentials: Password<Email>) -> JoinedEvent { credentials };
        @clock fn change_password(hash: PasswordHash) -> PasswordChangedEvent { hash };
    }
}

fn hasher() -> Argon2Hasher {
    Argon2Hasher::new(argon2::Params::new(8, 1, 1, None).unwrap())
}

async fn hash(password: &str) -> PasswordHash {
    hasher()
        .hash(&secrecy::SecretString::from(password))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_user_aggregate_registers_through_the_facade_alone() {
    let store = Arc::new(InMemoryEventStore::builder().build());
    let mut member = Member::join(Password::new(
        Email::parse("ada@example.com").unwrap(),
        hash("correct-horse").await,
    ))
    .unwrap();
    store
        .repository::<Member>()
        .save(&mut member)
        .await
        .unwrap();

    let loaded = store
        .repository::<Member>()
        .load(member.entity_id())
        .await
        .unwrap();

    assert!(loaded.is_active());
    assert!(matches!(loaded.identity(), Identity::Registered(_)));
}

#[tokio::test]
async fn the_facade_authenticator_logs_a_registered_user_in() {
    let store = Arc::new(InMemoryEventStore::builder().build());
    let login = Email::parse("ada@example.com").unwrap();
    let mut member =
        Member::join(Password::new(login.clone(), hash("correct-horse").await)).unwrap();
    store
        .repository::<Member>()
        .save(&mut member)
        .await
        .unwrap();
    let authenticator =
        ClaimAuthenticator::<Member, _, _>::new(store, PasswordLogin::new(hasher()));
    let attempt = event_sauce::auth::PasswordAttempt::new(
        login,
        secrecy::SecretString::from("correct-horse"),
    );

    let outcome = authenticator.authenticate(attempt).await.unwrap();

    assert!(matches!(
        outcome,
        event_sauce::auth::LoginOutcome::Authenticated(_)
    ));
}

#[tokio::test]
async fn changing_the_password_changes_the_session_fingerprint() {
    let login = Email::parse("ada@example.com").unwrap();
    let mut member = Member::join(Password::new(login, hash("correct-horse").await)).unwrap();
    let before = member.identity().session_fingerprint();

    member
        .change_password(hash("battery-staple").await)
        .unwrap();

    assert_ne!(member.identity().session_fingerprint(), before);
}
