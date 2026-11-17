use super::*;
use crate::test_support::HASH_A as ARGON2ID;

#[test]
fn accepts_a_phc_string() {
    assert_eq!(PasswordHash::new(ARGON2ID).unwrap().as_str(), ARGON2ID);
}

#[test]
fn rejects_what_is_not_a_phc_string() {
    for raw in ["", "hunter2", "$$", "argon2id$v=19"] {
        assert_eq!(PasswordHash::new(raw), Err(PasswordHashError), "{raw}");
    }
}

#[test]
fn debug_never_prints_the_hash() {
    let shown = format!("{:?}", PasswordHash::new(ARGON2ID).unwrap());
    assert!(!shown.contains("argon2id"));
    assert!(!shown.contains("aGFzaGhh"));
}

#[test]
fn serializes_as_the_bare_string() {
    let hash = PasswordHash::new(ARGON2ID).unwrap();
    assert_eq!(
        serde_json::to_string(&hash).unwrap(),
        format!("\"{ARGON2ID}\"")
    );
}

#[test]
fn deserializing_revalidates() {
    assert!(serde_json::from_str::<PasswordHash>("\"hunter2\"").is_err());
    let hash: PasswordHash = serde_json::from_str(&format!("\"{ARGON2ID}\"")).unwrap();
    assert_eq!(hash.as_str(), ARGON2ID);
}
