use event_sauce::auth::{Email, LoginId, Password, PasswordHasher};
use event_sauce::prelude::*;
use secrecy::SecretString;

use super::{cheap_hasher, EMAIL, PASSWORD};
use crate::member::{check_password, Member, MemberError};
use crate::note::{Note, NoteCommands, NoteDeleteCommands, NoteError};

async fn credentials(email: &str) -> Password<Email> {
    let hash = cheap_hasher()
        .hash(&SecretString::from(PASSWORD))
        .await
        .unwrap();
    Password::new(Email::parse(email).unwrap(), hash)
}

async fn member(nickname: &str) -> AggregateRoot<Member> {
    Member::join(
        nickname.to_owned(),
        credentials(&format!("{nickname}@example.com")).await,
    )
    .unwrap()
}

#[tokio::test]
async fn a_member_needs_a_nickname() {
    let result = Member::join("  ".to_owned(), credentials(EMAIL).await);

    assert!(matches!(result, Err(MemberError::EmptyNickname)));
}

#[tokio::test]
async fn a_short_password_is_refused() {
    assert!(matches!(
        check_password("short"),
        Err(MemberError::WeakPassword)
    ));
}

#[tokio::test]
async fn a_long_enough_password_is_accepted() {
    assert!(check_password(PASSWORD).is_ok());
}

#[tokio::test]
async fn a_note_belongs_to_its_author() {
    let ada = member("ada").await;

    let note = Note::write(&ada, "Groceries".to_owned(), "milk".to_owned()).unwrap();

    let grace = member("grace").await;

    assert!(note.ensure_owned_by(&ada).is_ok());
    assert!(matches!(
        note.ensure_owned_by(&grace),
        Err(NoteError::NotOwner)
    ));
    assert_eq!(note.title, "Groceries");
}

#[tokio::test]
async fn a_note_needs_a_title() {
    let ada = member("ada").await;

    let result = Note::write(&ada, " ".to_owned(), "milk".to_owned());

    assert!(matches!(result, Err(NoteError::EmptyTitle)));
}

#[tokio::test]
async fn a_title_has_a_maximum_length() {
    let ada = member("ada").await;

    let result = Note::write(&ada, "x".repeat(201), String::new());

    assert!(matches!(result, Err(NoteError::TitleTooLong)));
}

#[tokio::test]
async fn the_owner_edits_the_note() {
    let ada = member("ada").await;
    let mut note = Note::write(&ada, "Groceries".to_owned(), "milk".to_owned()).unwrap();

    note.edit(&ada, None, Some("milk, eggs".to_owned()))
        .unwrap();

    assert_eq!(note.title, "Groceries");
    assert_eq!(note.body, "milk, eggs");
}

#[tokio::test]
async fn an_edit_must_change_something() {
    let ada = member("ada").await;
    let mut note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.edit(&ada, None, None);

    assert!(matches!(result, Err(NoteError::NothingToEdit)));
}

#[tokio::test]
async fn another_member_cannot_edit_the_note() {
    let ada = member("ada").await;
    let grace = member("grace").await;
    let mut note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.edit(&grace, Some("Mine".to_owned()), None);

    assert!(matches!(result, Err(NoteError::NotOwner)));
    assert_eq!(note.title, "Groceries");
}

#[tokio::test]
async fn another_member_cannot_delete_the_note() {
    let ada = member("ada").await;
    let grace = member("grace").await;
    let note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    let result = note.delete(&grace);

    assert!(matches!(result, Err(NoteError::NotOwner)));
}

#[tokio::test]
async fn the_owner_deletes_the_note() {
    let ada = member("ada").await;
    let note = Note::write(&ada, "Groceries".to_owned(), String::new()).unwrap();

    assert!(note.delete(&ada).is_ok());
}
