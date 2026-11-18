use proptest::prelude::*;

use super::*;

#[test]
fn parse_trims_and_lowercases() {
    assert_eq!(
        Username::parse("  Ada_L-99.x \n").unwrap().as_str(),
        "ada_l-99.x"
    );
}

#[test]
fn parse_enforces_the_length_bounds() {
    assert_eq!(Username::parse("ab"), Err(UsernameError::TooShort));
    assert_eq!(Username::parse(""), Err(UsernameError::TooShort));
    assert!(Username::parse("abc").is_ok());
    assert!(Username::parse(&"a".repeat(32)).is_ok());
    assert_eq!(
        Username::parse(&"a".repeat(33)),
        Err(UsernameError::TooLong)
    );
}

#[test]
fn parse_rejects_characters_outside_the_allowed_set() {
    assert_eq!(
        Username::parse("ada lovelace"),
        Err(UsernameError::InvalidCharacter(' '))
    );
    assert_eq!(
        Username::parse("ada@home"),
        Err(UsernameError::InvalidCharacter('@'))
    );
}

#[test]
fn parse_rejects_non_alphanumeric_boundaries() {
    for raw in ["_ada", "ada_", "-ada", ".ada", "ada."] {
        assert_eq!(
            Username::parse(raw),
            Err(UsernameError::InvalidBoundary),
            "{raw}"
        );
    }
}

#[test]
fn parse_rejects_cyrillic_lookalikes() {
    assert_eq!(
        Username::parse("\u{430}dmin"),
        Err(UsernameError::InvalidCharacter('\u{430}'))
    );
}

#[test]
fn parse_rejects_characters_that_fold_to_ascii() {
    assert_eq!(
        Username::parse("\u{212A}ing"),
        Err(UsernameError::InvalidCharacter('\u{212A}'))
    );
}

#[test]
fn spellings_of_one_username_are_equal() {
    assert_eq!(
        Username::parse("ADA99").unwrap(),
        Username::parse(" ada99").unwrap()
    );
}

#[test]
fn display_prints_the_normalised_username() {
    assert_eq!(Username::parse("Ada99").unwrap().to_string(), "ada99");
}

#[test]
fn serializes_as_a_bare_string() {
    let username = Username::parse("ada99").unwrap();
    assert_eq!(
        serde_json::to_value(&username).unwrap(),
        serde_json::json!("ada99")
    );
}

#[test]
fn deserializing_normalises_and_validates() {
    let username: Username = serde_json::from_value(serde_json::json!(" ADA99 ")).unwrap();
    assert_eq!(username.as_str(), "ada99");
    assert!(serde_json::from_value::<Username>(serde_json::json!("a")).is_err());
}

proptest! {
    #[test]
    fn parsing_a_parsed_username_changes_nothing(raw in "\\PC{0,40}") {
        if let Ok(username) = Username::parse(&raw) {
            prop_assert_eq!(Username::parse(username.as_str()).unwrap(), username);
        }
    }
}

#[test]
fn as_ref_is_the_normalised_username() {
    let username = Username::parse(" Ada_99 ").unwrap();
    assert_eq!(AsRef::<str>::as_ref(&username), "ada_99");
}
