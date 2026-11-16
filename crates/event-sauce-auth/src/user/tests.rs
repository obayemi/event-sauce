use event_sauce_core::{
    Aggregate, AggregateClaim, AggregateError, DomainEvent, Entity, EntityId, EventApplicator,
    EventVersion,
};
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Pass(String);

impl Credentials for Pass {
    fn claims(&self) -> Vec<AggregateClaim> {
        Vec::new()
    }

    fn fingerprint(&self, digest: &mut crate::Sha256) {
        crate::Digest::update(digest, self.0.as_bytes());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Member {
    id: EntityId,
    identity: Identity<Pass>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Noop;

#[derive(Debug, thiserror::Error)]
#[error("member error")]
struct MemberError;

impl AggregateError for MemberError {}

impl Entity for Member {
    fn new(id: EntityId) -> Self {
        Self {
            id,
            identity: Identity::Anonymous,
        }
    }

    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl DomainEvent for Noop {
    type Aggregate = Member;

    fn event_type(&self) -> &'static str {
        "Noop"
    }

    fn event_version(&self) -> EventVersion {
        EventVersion::new(1)
    }

    fn occurred_at(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

impl EventApplicator<Member> for Noop {
    fn dispatch(&self, _: &mut Member) -> Result<(), MemberError> {
        Ok(())
    }

    fn dispatch_unchecked(&self, _: &mut Member) {}
}

impl Aggregate for Member {
    type Event = Noop;
    type Error = MemberError;
    type DeletedState = Self;
}

impl UserAggregate for Member {
    type Credentials = Pass;

    fn identity(&self) -> &Identity<Pass> {
        &self.identity
    }
}

#[test]
fn a_user_is_active_unless_it_says_otherwise() {
    assert!(Member::new(EntityId::new()).is_active());
}

#[test]
fn identity_is_read_off_the_aggregate() {
    let mut member = Member::new(EntityId::new());
    assert_eq!(member.identity(), &Identity::Anonymous);
    member.identity.register(Pass("s".into())).unwrap();
    assert_eq!(member.identity().credentials(), Some(&Pass("s".into())));
}
