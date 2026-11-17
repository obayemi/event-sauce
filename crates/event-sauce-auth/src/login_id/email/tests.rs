use proptest::prelude::*;

use super::*;

#[test]
fn parse_trims_and_lowercases() {
    let email = Email::parse("  Ada.Lovelace@Example.COM \n").unwrap();
    assert_eq!(email.as_str(), "ada.lovelace@example.com");
}

#[test]
fn spellings_of_one_address_are_equal() {
    assert_eq!(
        Email::parse("ADA@example.com").unwrap(),
        Email::parse(" ada@EXAMPLE.com").unwrap()
    );
}

#[test]
fn parse_rejects_malformed_addresses() {
    for raw in ["", "   ", "ada", "ada@", "@example.com", "a da@example.com"] {
        assert!(Email::parse(raw).is_err(), "{raw:?} must be rejected");
    }
}

#[test]
fn error_names_the_rejected_input() {
    let error = Email::parse("nope").unwrap_err();
    assert_eq!(error.to_string(), "`nope` is not a valid email address");
}

#[test]
fn display_prints_the_normalised_address() {
    let email = Email::parse("Ada@Example.com").unwrap();
    assert_eq!(email.to_string(), "ada@example.com");
}

#[test]
fn serializes_as_a_bare_string() {
    let email = Email::parse("ada@example.com").unwrap();
    assert_eq!(
        serde_json::to_value(&email).unwrap(),
        serde_json::json!("ada@example.com")
    );
}

#[test]
fn deserializing_normalises() {
    let email: Email = serde_json::from_value(serde_json::json!(" ADA@example.com")).unwrap();
    assert_eq!(email.as_str(), "ada@example.com");
}

#[test]
fn deserializing_rejects_invalid_addresses() {
    assert!(serde_json::from_value::<Email>(serde_json::json!("nope")).is_err());
}

proptest! {
    #[test]
    fn parsing_a_parsed_email_changes_nothing(raw in "[ a-zA-Z0-9.@_+-]{0,40}") {
        if let Ok(email) = Email::parse(&raw) {
            prop_assert_eq!(Email::parse(email.as_str()).unwrap(), email);
        }
    }
}
