use chrono::DateTime;
use proptest::prelude::*;
use sha2::{Digest, Sha256};

use super::*;

const TTL: TimeDelta = TimeDelta::hours(1);

fn instant(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}

fn zero_token() -> ResetToken {
    ResetToken::parse(&"A".repeat(43)).unwrap()
}

#[test]
fn an_issued_token_is_43_base64url_characters() {
    let (token, _) = ResetToken::issue();

    assert_eq!(token.as_str().len(), 43);
    assert!(token
        .as_str()
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
}

#[test]
fn two_issued_tokens_differ() {
    let (first, first_hash) = ResetToken::issue();
    let (second, second_hash) = ResetToken::issue();

    assert_ne!(first.as_str(), second.as_str());
    assert_ne!(first_hash, second_hash);
}

#[test]
fn the_hash_is_the_sha256_of_the_token_text() {
    let hash = ResetTokenHash::of(&zero_token());

    let expected = BASE64.encode(Sha256::digest("A".repeat(43)));
    assert_eq!(
        serde_json::to_string(&hash).unwrap(),
        format!("\"{expected}\"")
    );
}

#[test]
fn the_issued_token_verifies_against_its_hash() {
    let (token, hash) = ResetToken::issue();

    assert_eq!(hash.verify(&token, instant(0), instant(10), TTL), Ok(()));
}

#[test]
fn another_token_does_not_verify() {
    let (_, hash) = ResetToken::issue();
    let (other, _) = ResetToken::issue();

    assert_eq!(
        hash.verify(&other, instant(0), instant(10), TTL),
        Err(ResetError::Mismatch)
    );
}

#[test]
fn a_token_is_valid_up_to_and_including_its_ttl() {
    let (token, hash) = ResetToken::issue();
    let issued_at = instant(1_000);

    assert_eq!(hash.verify(&token, issued_at, issued_at + TTL, TTL), Ok(()));
    assert_eq!(
        hash.verify(
            &token,
            issued_at,
            issued_at + TTL + TimeDelta::seconds(1),
            TTL
        ),
        Err(ResetError::Expired)
    );
}

#[test]
fn a_wrong_token_reports_a_mismatch_even_after_expiry() {
    let (_, hash) = ResetToken::issue();
    let (other, _) = ResetToken::issue();

    assert_eq!(
        hash.verify(&other, instant(0), instant(1_000_000), TTL),
        Err(ResetError::Mismatch)
    );
}

#[test]
fn a_token_is_valid_through_its_absolute_expiry_inclusive() {
    let (token, hash) = ResetToken::issue();
    let expires_at = instant(1_000);

    assert_eq!(hash.verify_until(&token, expires_at, expires_at), Ok(()));
    assert_eq!(
        hash.verify_until(&token, expires_at, expires_at + TimeDelta::seconds(1)),
        Err(ResetError::Expired)
    );
}

#[test]
fn a_token_before_its_expiry_is_valid_whatever_the_clock_skew() {
    let (token, hash) = ResetToken::issue();

    assert_eq!(
        hash.verify_until(&token, instant(1_000), instant(0)),
        Ok(())
    );
}

#[test]
fn a_wrong_token_reports_a_mismatch_even_past_the_absolute_expiry() {
    let (_, hash) = ResetToken::issue();
    let (other, _) = ResetToken::issue();

    assert_eq!(
        hash.verify_until(&other, instant(0), instant(1_000_000)),
        Err(ResetError::Mismatch)
    );
}

#[test]
fn parsing_the_shown_token_gives_back_a_token_that_verifies() {
    let (token, hash) = ResetToken::issue();

    let parsed = ResetToken::parse(token.as_str()).unwrap();

    assert_eq!(hash.verify(&parsed, instant(0), instant(0), TTL), Ok(()));
}

#[test]
fn parse_rejects_anything_but_32_canonical_base64url_bytes() {
    for raw in [
        "",
        "short",
        &"A".repeat(42),
        &"A".repeat(44),
        &format!("{}=", "A".repeat(43)),
        &format!("{}+", "A".repeat(42)),
        &format!("{}/", "A".repeat(42)),
        &format!("{}B", "A".repeat(42)),
        &format!(" {}", "A".repeat(42)),
    ] {
        assert_eq!(
            ResetToken::parse(raw).unwrap_err(),
            ResetTokenError,
            "{raw:?}"
        );
    }
}

#[test]
fn the_debug_output_never_prints_the_token() {
    let (token, _) = ResetToken::issue();

    assert!(!format!("{token:?}").contains(token.as_str()));
}

#[test]
fn a_hash_round_trips_through_json_as_a_string() {
    let (_, hash) = ResetToken::issue();

    let json = serde_json::to_string(&hash).unwrap();

    assert!(json.starts_with('"') && json.ends_with('"'));
    assert_eq!(serde_json::from_str::<ResetTokenHash>(&json).unwrap(), hash);
}

#[test]
fn a_malformed_hash_does_not_deserialize() {
    assert!(serde_json::from_str::<ResetTokenHash>("\"nope\"").is_err());
    assert!(serde_json::from_str::<ResetTokenHash>("42").is_err());
}

proptest! {
    #[test]
    fn validity_ends_exactly_at_the_ttl(issued in 0i64..1_000_000_000, ttl in 0i64..100_000, elapsed in 0i64..200_000) {
        let (token, hash) = ResetToken::issue();
        let outcome = hash.verify(
            &token,
            instant(issued),
            instant(issued + elapsed),
            TimeDelta::seconds(ttl),
        );

        prop_assert_eq!(outcome.is_ok(), elapsed <= ttl);
    }
}
