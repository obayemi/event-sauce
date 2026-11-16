use serde_json::json;

use super::*;
use crate::credentials::fingerprint_of;
use crate::{Email, FederatedId, PasswordHash, Provider};

const HASH_A: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";
const HASH_B: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$b3RoZXJzYWx0c2FsdA$b3RoZXJoYXNob3RoZXJoYXNob3RoZXJoYXNob3RoZXI";

type Creds = PasswordOrFederated<Email>;

fn password(hash: &str) -> Password<Email> {
    Password::new(
        Email::parse("ada@example.com").unwrap(),
        PasswordHash::new(hash).unwrap(),
    )
}

fn google() -> FederatedId {
    FederatedId::new(Provider::GOOGLE, "g1")
}

fn github() -> FederatedId {
    FederatedId::new(Provider::GITHUB, "h1")
}

fn password_only() -> Creds {
    Creds::Password(password(HASH_A))
}

fn federated_only() -> Creds {
    Creds::Federated(Federated::new(google()))
}

fn both() -> Creds {
    Creds::Both(password(HASH_A), Federated::new(google()))
}

#[test]
fn set_password_replaces_an_existing_password() {
    let mut credentials = password_only();
    credentials.set_password(password(HASH_B));
    assert_eq!(credentials, Creds::Password(password(HASH_B)));
}

#[test]
fn set_password_on_federated_becomes_both() {
    let mut credentials = federated_only();
    credentials.set_password(password(HASH_A));
    assert_eq!(credentials, both());
}

#[test]
fn set_password_on_both_keeps_the_links() {
    let mut credentials = both();
    credentials.set_password(password(HASH_B));
    assert_eq!(
        credentials,
        Creds::Both(password(HASH_B), Federated::new(google()))
    );
}

#[test]
fn remove_password_from_both_leaves_the_links() {
    let mut credentials = both();
    credentials.remove_password().unwrap();
    assert_eq!(credentials, federated_only());
}

#[test]
fn remove_password_is_refused_when_it_is_the_only_login_method() {
    let mut credentials = password_only();
    assert_eq!(
        credentials.remove_password(),
        Err(LoginMethodError::LastLoginMethod)
    );
    assert_eq!(credentials, password_only());
}

#[test]
fn remove_password_from_federated_changes_nothing() {
    let mut credentials = federated_only();
    credentials.remove_password().unwrap();
    assert_eq!(credentials, federated_only());
}

#[test]
fn link_on_password_becomes_both() {
    let mut credentials = password_only();
    credentials.link(google()).unwrap();
    assert_eq!(credentials, both());
}

#[test]
fn link_adds_to_the_existing_links() {
    let mut credentials = both();
    credentials.link(github()).unwrap();
    let Creds::Both(_, links) = credentials else {
        panic!("expected Both");
    };
    assert_eq!(links.links(), [google(), github()]);
}

#[test]
fn link_refuses_an_identity_already_linked() {
    for mut credentials in [federated_only(), both()] {
        assert_eq!(
            credentials.link(google()),
            Err(LoginMethodError::AlreadyLinked)
        );
    }
}

#[test]
fn unlink_removes_one_of_several_links() {
    let mut credentials = federated_only();
    credentials.link(github()).unwrap();
    credentials.unlink(&Provider::GOOGLE).unwrap();
    assert_eq!(credentials, Creds::Federated(Federated::new(github())));
}

#[test]
fn unlink_of_the_last_link_falls_back_to_the_password() {
    let mut credentials = both();
    credentials.unlink(&Provider::GOOGLE).unwrap();
    assert_eq!(credentials, password_only());
}

#[test]
fn unlink_of_the_last_link_without_a_password_is_refused() {
    let mut credentials = federated_only();
    assert_eq!(
        credentials.unlink(&Provider::GOOGLE),
        Err(LoginMethodError::LastLoginMethod)
    );
    assert_eq!(credentials, federated_only());
}

#[test]
fn unlink_of_an_unknown_provider_is_refused() {
    for mut credentials in [password_only(), federated_only(), both()] {
        assert_eq!(
            credentials.unlink(&Provider::GITHUB),
            Err(LoginMethodError::NotLinked)
        );
    }
}

#[test]
fn capabilities_follow_what_is_held() {
    assert!(password_only().password().is_some());
    assert!(password_only().federated().is_none());
    assert!(federated_only().password().is_none());
    assert!(federated_only().federated().is_some());
    assert!(both().password().is_some());
    assert!(both().federated().is_some());
}

#[test]
fn claims_cover_the_login_and_every_link() {
    let mut credentials = both();
    credentials.link(github()).unwrap();
    let claims: Vec<_> = credentials
        .claims()
        .into_iter()
        .map(|c| (c.claim_type, c.claim_key))
        .collect();
    assert_eq!(
        claims,
        [
            ("auth.login", json!("ada@example.com")),
            ("auth.federated", json!(["google", "g1"])),
            ("auth.federated", json!(["github", "h1"])),
        ]
    );
}

#[test]
fn a_user_without_a_password_claims_no_login() {
    assert!(federated_only()
        .claims()
        .iter()
        .all(|c| c.claim_type == "auth.federated"));
}

#[test]
fn fingerprint_changes_on_every_transition() {
    let mut credentials = password_only();
    let mut seen = vec![fingerprint_of(&credentials)];
    credentials.link(google()).unwrap();
    seen.push(fingerprint_of(&credentials));
    credentials.set_password(password(HASH_B));
    seen.push(fingerprint_of(&credentials));
    credentials.link(github()).unwrap();
    seen.push(fingerprint_of(&credentials));
    credentials.remove_password().unwrap();
    seen.push(fingerprint_of(&credentials));
    for (i, a) in seen.iter().enumerate() {
        for b in &seen[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn serde_round_trips_every_variant() {
    for credentials in [password_only(), federated_only(), both()] {
        let json = serde_json::to_string(&credentials).unwrap();
        assert_eq!(serde_json::from_str::<Creds>(&json).unwrap(), credentials);
    }
}
