use event_sauce::auth::{Email, LoginId, Password, PasswordHasher};
use event_sauce::prelude::*;
use secrecy::SecretString;

use super::{cheap_hasher, EMAIL, PASSWORD};
use crate::member::{check_password, Member, MemberError};
use crate::note::{Note, NoteCommands, NoteDeleteCommands, NoteError};

fn member(nickname: &str) -> AggregateRoot<Member> {
    let hash = cheap_hasher().hash(&SecretString::from(PASSWORD)).unwrap();
    let email = Email::parse(&format!("{nickname}@example.com")).unwrap();
    Member::join(nickname.to_owned(), Password::new(email, hash)).unwrap()
}

#[test]
fn a_member_needs_a_nickname() {
    let hash = cheap_hasher().hash(&SecretString::from(PASSWORD)).unwrap();
    let credentials = Password::new(Email::parse(EMAIL).unwrap(), hash);

    let result = Member::join("  ".to_owned(), credentials);

    assert!(matches!(result, Err(MemberError::EmptyNickname)));
}

#[test]
fn a_short_password_is_refused() {
    assert!(matches!(
        check_password("short"),
        Err(MemberError::WeakPassword)
    ));
}

#[test]
fn a_long_enough_password_is_accepted() {
    assert!(check_password(PASSWORD).is_ok());
}

#[test]
fn a_note_belongs_to_its_author() {
    let ada = member("ada");

    let note = Note::write(&ada, "Groceries".to_owned(), "milk".to_owned()).unwrap();

    assert!(note.ensure_owned_by(&ada).is_ok());
    assert!(matches!(
        note.ensure_owned_by(&member("grace")),
        Err(NoteError::NotOwner)
    ));
    assert_eq!(note.title, "Groceries");
}

#[test]
fn a_note_needs_a_title() {
    let ada = member("ada");

    let result = Note::write(&ada, " ".to_owned(), "milk".to_owned());

    assert!(matches!(result, Err(NoteError::EmptyTitle)));
}

#[test]
fn a_title_has_a_maximum_length() {
    let ada = member("ada");

    let result = Note::write(&ada, "x".repeat(201), String::new());

    assert!(matches!(result, Err(NoteError::TitleTooLong)));
}

#[test]
fn the_owner_edits_the_note() {
    let ada = member("ada");
    let mut note = Note::write(&ada, "Groceries".to_owned(), "milk".to_owned()).unwrap();

    note.edit(&ada, None, Some("milk, eggs".to_owned()))
        .unwrap();

    assert_eq!(note.title, "Groceries");
    assert_eq!(note.body, "milk, eggs");
}

#[test]
fn an_edit_must_change_something() {
    let ada = member("ada");
    let mut note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.edit(&ada, None, None);

    assert!(matches!(result, Err(NoteError::NothingToEdit)));
}

#[test]
fn another_member_cannot_edit_the_note() {
    let ada = member("ada");
    let grace = member("grace");
    let mut note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.edit(&grace, Some("Mine".to_owned()), None);

    assert!(matches!(result, Err(NoteError::NotOwner)));
    assert_eq!(note.title, "Groceries");
}

#[test]
fn another_member_cannot_delete_the_note() {
    let ada = member("ada");
    let grace = member("grace");
    let note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.delete(&grace);

    assert!(matches!(result, Err(NoteError::NotOwner)));
}

#[test]
fn the_owner_deletes_the_note() {
    let ada = member("ada");
    let note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    assert!(note.delete(&ada).is_ok());
}
