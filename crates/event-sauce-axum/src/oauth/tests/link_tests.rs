use chrono::{DateTime, TimeDelta};
use event_sauce_auth::OAuthState;
use event_sauce_core::EntityId;

use super::*;

const TTL: TimeDelta = TimeDelta::minutes(10);

fn link(proof: LinkProof, proved: DateTime<Utc>) -> PendingLink {
    PendingLink::new(
        EntityId::new(),
        OAuthState::new("state"),
        proof,
        proved,
        TTL,
    )
}

#[test]
fn a_proved_code_counts_until_its_link_expires() {
    let proved = DateTime::UNIX_EPOCH;
    let link = link(LinkProof::Proved, proved);

    assert_eq!(link.proof_at(proved), LinkProof::Proved);
    assert_eq!(link.proof_at(proved + TTL), LinkProof::Proved);
    let late = proved + TTL + TimeDelta::seconds(1);
    assert_eq!(link.proof_at(late), LinkProof::Unproved);
}

#[test]
fn a_link_without_a_proved_code_carries_no_proof() {
    let link = link(LinkProof::Unproved, DateTime::UNIX_EPOCH);

    assert_eq!(link.proof_at(DateTime::UNIX_EPOCH), LinkProof::Unproved);
}

#[test]
fn a_link_is_bound_to_the_state_of_its_flow() {
    let link = link(LinkProof::Unproved, DateTime::UNIX_EPOCH);

    assert!(link.answers(&OAuthState::new("state")));
    assert!(!link.answers(&OAuthState::new("other")));
}

#[test]
fn a_link_survives_the_session_record() {
    let proved = DateTime::UNIX_EPOCH;
    let link = link(LinkProof::Proved, proved);

    let stored = serde_json::to_value(&link).unwrap();
    let read: PendingLink = serde_json::from_value(stored).unwrap();

    assert_eq!(read.user(), link.user());
    assert_eq!(read.proof_at(proved), LinkProof::Proved);
}
