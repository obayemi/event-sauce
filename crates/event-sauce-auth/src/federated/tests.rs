use serde_json::json;

use super::*;
use crate::credentials::fingerprint_of;

fn id(provider: &Provider, subject: &str) -> FederatedId {
    FederatedId::new(provider.clone(), subject)
}

fn google(subject: &str) -> FederatedId {
    id(&Provider::GOOGLE, subject)
}

fn github(subject: &str) -> FederatedId {
    id(&Provider::GITHUB, subject)
}

#[test]
fn provider_constants_have_stable_names() {
    assert_eq!(Provider::GOOGLE.as_str(), "google");
    assert_eq!(Provider::GITHUB.as_str(), "github");
    assert_eq!(Provider::APPLE.as_str(), "apple");
    assert_eq!(Provider::MICROSOFT.as_str(), "microsoft");
}

#[test]
fn an_owned_provider_equals_the_constant_of_the_same_name() {
    assert_eq!(Provider::from("google".to_owned()), Provider::GOOGLE);
    assert_eq!(Provider::GOOGLE.to_string(), "google");
}

#[test]
fn provider_serializes_as_a_bare_string() {
    assert_eq!(
        serde_json::to_value(Provider::GOOGLE).unwrap(),
        json!("google")
    );
    let parsed: Provider = serde_json::from_value(json!("okta")).unwrap();
    assert_eq!(parsed.as_str(), "okta");
}

#[test]
fn federated_id_claims_the_provider_and_subject_pair() {
    let claim = google("sub-1").claim();
    assert_eq!(claim.claim_type, "auth.federated");
    assert_eq!(claim.claim_key, json!(["google", "sub-1"]));
}

#[test]
fn the_same_subject_at_another_provider_is_another_claim() {
    assert_ne!(google("1").claim().claim_key, github("1").claim().claim_key);
}

#[test]
fn link_adds_a_new_identity() {
    let mut federated = Federated::new(google("g1"));
    federated.link(github("h1")).unwrap();
    assert_eq!(federated.links(), [google("g1"), github("h1")]);
}

#[test]
fn link_refuses_an_identity_already_linked() {
    let mut federated = Federated::new(google("g1"));
    assert_eq!(
        federated.link(google("g1")),
        Err(LoginMethodError::AlreadyLinked)
    );
    assert_eq!(federated.links(), [google("g1")]);
}

#[test]
fn link_accepts_another_subject_of_a_known_provider() {
    let mut federated = Federated::new(google("g1"));
    federated.link(google("g2")).unwrap();
    assert_eq!(federated.links().len(), 2);
}

#[test]
fn unlink_removes_every_link_of_the_provider() {
    let mut federated = Federated::new(google("g1"));
    federated.link(google("g2")).unwrap();
    federated.link(github("h1")).unwrap();
    federated.unlink(&Provider::GOOGLE).unwrap();
    assert_eq!(federated.links(), [github("h1")]);
}

#[test]
fn unlink_refuses_to_remove_the_last_way_to_log_in() {
    let mut federated = Federated::new(google("g1"));
    assert_eq!(
        federated.unlink(&Provider::GOOGLE),
        Err(LoginMethodError::LastLoginMethod)
    );
    assert_eq!(federated.links(), [google("g1")]);
}

#[test]
fn unlink_refuses_a_provider_that_is_not_linked() {
    let mut federated = Federated::new(google("g1"));
    assert_eq!(
        federated.unlink(&Provider::GITHUB),
        Err(LoginMethodError::NotLinked)
    );
}

#[test]
fn claims_hold_one_claim_per_link() {
    let mut federated = Federated::new(google("g1"));
    federated.link(github("h1")).unwrap();
    let keys: Vec<_> = federated
        .claims()
        .into_iter()
        .map(|c| c.claim_key)
        .collect();
    assert_eq!(keys, [json!(["google", "g1"]), json!(["github", "h1"])]);
}

#[test]
fn federated_always_has_federated_links() {
    let federated = Federated::new(google("g1"));
    assert_eq!(HasFederated::federated(&federated), Some(&federated));
}

#[test]
fn fingerprint_changes_when_a_link_is_added_or_removed() {
    let one = Federated::new(google("g1"));
    let mut two = one.clone();
    two.link(github("h1")).unwrap();
    assert_ne!(fingerprint_of(&one), fingerprint_of(&two));
    two.unlink(&Provider::GITHUB).unwrap();
    assert_eq!(fingerprint_of(&one), fingerprint_of(&two));
}

#[test]
fn fingerprint_does_not_confuse_where_provider_ends_and_subject_begins() {
    let a = Federated::new(FederatedId::new(Provider::from("ab".to_owned()), "c"));
    let b = Federated::new(FederatedId::new(Provider::from("a".to_owned()), "bc"));
    assert_ne!(fingerprint_of(&a), fingerprint_of(&b));
}

#[test]
fn serde_round_trips_and_rejects_no_links() {
    let mut federated = Federated::new(google("g1"));
    federated.link(github("h1")).unwrap();
    let json = serde_json::to_string(&federated).unwrap();
    assert_eq!(serde_json::from_str::<Federated>(&json).unwrap(), federated);
    assert!(serde_json::from_str::<Federated>("[]").is_err());
}
