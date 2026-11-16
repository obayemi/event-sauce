use serde_json::json;

use super::*;
use crate::credentials::fingerprint_of;
use crate::Email;

const HASH_A: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";
const HASH_B: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$b3RoZXJzYWx0c2FsdA$b3RoZXJoYXNob3RoZXJoYXNob3RoZXJoYXNob3RoZXI";

fn email(raw: &str) -> Email {
    Email::parse(raw).unwrap()
}

fn hash(raw: &str) -> PasswordHash {
    PasswordHash::new(raw).unwrap()
}

fn password(login: &str, hash_str: &str) -> Password<Email> {
    Password::new(email(login), hash(hash_str))
}

#[test]
fn new_exposes_its_login_and_hash() {
    let password = password("ada@example.com", HASH_A);
    assert_eq!(password.login(), &email("ada@example.com"));
    assert_eq!(password.hash(), &hash(HASH_A));
}

#[test]
fn set_hash_replaces_the_hash_only() {
    let mut password = password("ada@example.com", HASH_A);
    password.set_hash(hash(HASH_B));
    assert_eq!(password.hash(), &hash(HASH_B));
    assert_eq!(password.login(), &email("ada@example.com"));
}

#[test]
fn change_login_replaces_the_login_only() {
    let mut password = password("ada@example.com", HASH_A);
    password.change_login(email("eve@example.com"));
    assert_eq!(password.login(), &email("eve@example.com"));
    assert_eq!(password.hash(), &hash(HASH_A));
}

#[test]
fn claims_hold_the_login_under_auth_login() {
    let claims = password("ada@example.com", HASH_A).claims();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].claim_type, "auth.login");
    assert_eq!(claims[0].claim_key, json!("ada@example.com"));
}

#[test]
fn a_password_always_has_a_password() {
    let password = password("ada@example.com", HASH_A);
    assert_eq!(HasPassword::password(&password), Some(&password));
}

#[test]
fn fingerprint_changes_with_the_hash_and_with_the_login() {
    let base = fingerprint_of(&password("ada@example.com", HASH_A));
    assert_eq!(base, fingerprint_of(&password("ada@example.com", HASH_A)));
    assert_ne!(base, fingerprint_of(&password("ada@example.com", HASH_B)));
    assert_ne!(base, fingerprint_of(&password("eve@example.com", HASH_A)));
}

#[test]
fn serde_round_trips() {
    let password = password("ada@example.com", HASH_A);
    let json = serde_json::to_string(&password).unwrap();
    assert_eq!(
        serde_json::from_str::<Password<Email>>(&json).unwrap(),
        password
    );
}

#[test]
fn debug_does_not_leak_the_hash() {
    let shown = format!("{:?}", password("ada@example.com", HASH_A));
    assert!(!shown.contains("argon2id"));
}

#[test]
fn claim_for_a_login_is_the_claim_its_password_holds() {
    let claim = Password::claim_for(&email("ada@example.com"));
    let held = password("ada@example.com", HASH_A).claims().remove(0);
    assert_eq!(claim.claim_type, held.claim_type);
    assert_eq!(claim.claim_key, held.claim_key);
}
