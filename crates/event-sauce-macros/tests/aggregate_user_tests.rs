//! Tests for `#[aggregate(user)]`, which makes an aggregate a login user.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use event_sauce_auth::{
    Credentials, Email, Identity, LoginId, Password, PasswordHash, UserAggregate,
};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateClaim, AggregateError, DomainEvent,
    EntityId, EventApplicator, EventStore, EventVersion, Repository,
};
use event_sauce_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};

const PHC: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";

type Login = Password<Email>;

#[derive(Debug, thiserror::Error)]
#[error("user fixture error")]
struct FixtureError;

impl AggregateError for FixtureError {}

fn credentials(login: &str) -> Login {
    Password::new(
        Email::parse(login).unwrap(),
        PasswordHash::new(PHC).unwrap(),
    )
}

#[event_sauce_macros::aggregate(event = "MemberEvent", error = "FixtureError", init, claims, user)]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Member {
    #[id]
    id: EntityId,
    #[user(identity)]
    identity: Identity<Login>,
    #[user(active)]
    is_active: bool,
    nickname: String,
}

impl Member {
    fn aggregate_claims(&self) -> Vec<AggregateClaim> {
        vec![AggregateClaim::new(
            "member.nickname",
            serde_json::json!(self.nickname),
        )]
    }
}

define_events! {
    enum MemberEvent for Member {
        Joined {
            nickname: String,
            credentials: Option<Login>,
        }
        @init
        => |id, event| {
            Member {
                id,
                identity: event
                    .credentials
                    .clone()
                    .map_or(Identity::Anonymous, Identity::Registered),
                is_active: true,
                nickname: event.nickname.clone(),
            }
        },

        Suspended {}
        => |member, _event| {
            member.is_active = false;
        },
    }
}

aggregate_command! {
    impl Member {
        @clock @init fn join(nickname: String, credentials: Option<Login>) -> JoinedEvent {
            nickname, credentials
        };
        @clock fn suspend() -> SuspendedEvent { };
    }
}

macro_rules! plain_event {
    ($event:ident, $aggregate:ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct $event;

        impl DomainEvent for $event {
            type Aggregate = $aggregate;
            fn event_type(&self) -> &'static str {
                stringify!($event)
            }
            fn event_version(&self) -> EventVersion {
                EventVersion::new(1)
            }
            fn occurred_at(&self) -> DateTime<Utc> {
                Utc::now()
            }
        }

        impl EventApplicator<$aggregate> for $event {
            fn dispatch(&self, _entity: &mut $aggregate) -> Result<(), FixtureError> {
                Ok(())
            }
            fn dispatch_unchecked(&self, _entity: &mut $aggregate) {}
        }
    };
}

#[event_sauce_macros::aggregate(event = "GuestEvent", error = "FixtureError", init, user)]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Guest {
    #[id]
    id: EntityId,
    #[user(identity)]
    identity: Identity<Login>,
}

plain_event!(GuestEvent, Guest);

fn member(identity: Identity<Login>, is_active: bool) -> Member {
    Member {
        id: EntityId::new(),
        identity,
        is_active,
        nickname: "ada".to_string(),
    }
}

fn pairs(claims: &[AggregateClaim]) -> Vec<(&'static str, serde_json::Value)> {
    claims
        .iter()
        .map(|claim| (claim.claim_type, claim.claim_key.clone()))
        .collect()
}

fn claim_types(claims: &[AggregateClaim]) -> Vec<&'static str> {
    claims.iter().map(|claim| claim.claim_type).collect()
}

#[test]
fn the_user_impl_exposes_the_marked_identity() {
    let registered = Identity::Registered(credentials("ada@example.com"));
    assert_eq!(member(registered.clone(), true).identity(), &registered);
}

#[test]
fn the_active_marker_decides_whether_the_user_may_log_in() {
    assert!(member(Identity::Anonymous, true).is_active());
    assert!(!member(Identity::Anonymous, false).is_active());
}

#[test]
fn without_an_active_marker_the_user_is_always_active() {
    let guest = Guest {
        id: EntityId::new(),
        identity: Identity::Anonymous,
    };
    assert!(guest.is_active());
}

#[test]
fn an_anonymous_user_holds_only_its_own_claims() {
    let claims = member(Identity::Anonymous, true).claims();
    assert_eq!(claim_types(&claims), ["member.nickname"]);
}

#[test]
fn a_registered_user_adds_its_credential_claims_to_its_own() {
    let login = credentials("ada@example.com");
    let claims = member(Identity::Registered(login.clone()), true).claims();

    let mut expected = vec![AggregateClaim::new(
        "member.nickname",
        serde_json::json!("ada"),
    )];
    expected.extend(Credentials::claims(&login));
    assert_eq!(pairs(&claims), pairs(&expected));
    assert_eq!(claim_types(&claims), ["member.nickname", "auth.login"]);
}

#[test]
fn without_the_claims_flag_only_the_credential_claims_remain() {
    let login = credentials("ada@example.com");
    let guest = Guest {
        id: EntityId::new(),
        identity: Identity::Registered(login.clone()),
    };
    assert_eq!(pairs(&guest.claims()), pairs(&Credentials::claims(&login)));

    let anonymous = Guest {
        id: EntityId::new(),
        identity: Identity::Anonymous,
    };
    assert!(anonymous.claims().is_empty());
}

#[tokio::test]
async fn two_users_cannot_hold_the_same_login() {
    let store = Arc::new(InMemoryEventStore::new());
    let repository = store.repository::<Member>();
    let mut first = Member::join("ada".into(), Some(credentials("ada@example.com"))).unwrap();
    repository.save(&mut first).await.unwrap();

    let mut second = Member::join("eve".into(), Some(credentials("ADA@example.com"))).unwrap();
    let error = repository.save(&mut second).await.unwrap_err();

    let conflict = error.as_claim_conflict().expect("a claim conflict");
    assert_eq!(conflict.claim_type, "auth.login");
    assert_eq!(conflict.held_by, Some(first.entity_id().into()));
}

#[tokio::test]
async fn the_aggregates_own_claims_are_still_enforced() {
    let store = Arc::new(InMemoryEventStore::new());
    let repository = store.repository::<Member>();
    let mut first = Member::join("ada".into(), None).unwrap();
    repository.save(&mut first).await.unwrap();

    let mut second = Member::join("ada".into(), Some(credentials("eve@example.com"))).unwrap();
    let error = repository.save(&mut second).await.unwrap_err();

    assert_eq!(
        error
            .as_claim_conflict()
            .expect("a claim conflict")
            .claim_type,
        "member.nickname"
    );
}

#[tokio::test]
async fn distinct_logins_and_anonymous_users_coexist() {
    let store = Arc::new(InMemoryEventStore::new());
    let repository = store.repository::<Member>();
    for (nickname, login) in [
        ("ada", Some(credentials("ada@example.com"))),
        ("eve", Some(credentials("eve@example.com"))),
        ("bob", None),
        ("max", None),
    ] {
        let mut member = Member::join(nickname.into(), login).unwrap();
        repository.save(&mut member).await.unwrap();
    }
}
