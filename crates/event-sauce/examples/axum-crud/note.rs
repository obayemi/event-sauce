//! The event-sourced `Note` of the example, owned by the `Member` who wrote
//! it.

use crate::member::{Member, MemberId};
use event_sauce::prelude::*;
use serde::{Deserialize, Serialize};
use std::result::Result;

/// The longest title a note may have, in characters.
const MAX_TITLE_LEN: usize = 200;

/// Why a note command was refused.
#[derive(Debug, thiserror::Error)]
pub enum NoteError {
    #[error("a title cannot be blank")]
    EmptyTitle,
    #[error("a title has at most {MAX_TITLE_LEN} characters")]
    TitleTooLong,
    #[error("an edit must change the title or the body")]
    NothingToEdit,
    #[error("only the owner of a note may touch it")]
    NotOwner,
}

impl AggregateError for NoteError {}

fn check_title(title: &str) -> Result<(), NoteError> {
    if title.trim().is_empty() {
        return Err(NoteError::EmptyTitle);
    }
    if title.chars().count() > MAX_TITLE_LEN {
        return Err(NoteError::TitleTooLong);
    }
    Ok(())
}

/// The typed id of a [`Note`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AggregateId)]
#[serde(transparent)]
#[aggregate_id(Note)]
pub struct NoteId(EntityId);

/// A titled text owned by one member: only its owner reads, edits or
/// deletes it.
#[aggregate(event = "NoteEvent", error = "NoteError", init)]
#[derive(Debug, Serialize, Deserialize)]
pub struct Note {
    #[id]
    id: NoteId,
    owner: MemberId,
    pub title: String,
    pub body: String,
}

impl Note {
    /// Checks that `member` owns the note.
    ///
    /// # Errors
    ///
    /// [`NoteError::NotOwner`] for anybody else.
    pub fn ensure_owned_by(&self, member: &Member) -> Result<(), NoteError> {
        if self.owner == member.id() {
            Ok(())
        } else {
            Err(NoteError::NotOwner)
        }
    }

    /// Writes a new note on behalf of `author`, who becomes its owner.
    ///
    /// # Errors
    ///
    /// [`NoteError::EmptyTitle`] or [`NoteError::TitleTooLong`] for a bad
    /// title.
    pub fn write(
        author: &AggregateRoot<Member>,
        title: String,
        body: String,
    ) -> Result<AggregateRoot<Self>, NoteError> {
        Self::open(author, author.id(), title, body)
    }
}

define_events! {
    pub enum NoteEvent for Note {
        Created {
            owner: MemberId,
            title: String,
            body: String,
        }
        @init
        @actor(Member)
        @validate |author, event| {
            if event.owner != author.id() {
                return Err(NoteError::NotOwner);
            }
            return check_title(&event.title);
        }
        => |id, event| {
            Note {
                id: id.into(),
                owner: event.owner,
                title: event.title.trim().to_owned(),
                body: event.body.clone(),
            }
        },

        Edited {
            title: Option<String>,
            body: Option<String>,
        }
        @actor(Member)
        @validate |note, author, event| {
            note.ensure_owned_by(author)?;
            if event.title.is_none() && event.body.is_none() {
                return Err(NoteError::NothingToEdit);
            }
            if let Some(title) = &event.title {
                check_title(title)?;
            }
            return Ok(());
        }
        => |note, event| {
            if let Some(title) = &event.title {
                note.title = title.trim().to_owned();
            }
            if let Some(body) = &event.body {
                note.body.clone_from(body);
            }
        },

        Deleted {}
        @delete
        @actor(Member)
        @validate |note, author, _event| {
            return note.ensure_owned_by(author);
        }
        => |note, _event| note,
    }
}

aggregate_command! {
    impl Note {
        @clock @init @actor(Member)
        fn open(owner: MemberId, title: String, body: String) -> CreatedEvent {
            owner, title, body
        };
        @clock @actor(Member)
        fn edit(title: Option<String>, body: Option<String>) -> EditedEvent { title, body };
        @clock @delete @actor(Member)
        fn delete() -> DeletedEvent { };
    }
}
