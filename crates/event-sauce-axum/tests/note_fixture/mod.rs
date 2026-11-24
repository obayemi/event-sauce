//! A note aggregate owned by a user, and the three policies the tests of
//! `Authorized` use on it.

use event_sauce_axum::{Access, DeletedStatus, DeniedStatus, PathAggregate};
use event_sauce_core::{
    aggregate_command, define_events, Aggregate, AggregateError, AggregateRoot, Entity, EntityId,
};
use serde::{Deserialize, Serialize};

use crate::user_fixture::User;

#[derive(Debug, thiserror::Error)]
#[error("note error")]
pub struct NoteError;

impl AggregateError for NoteError {}

#[derive(Debug, Serialize, Deserialize)]
pub struct Note {
    id: EntityId,
    pub owner: EntityId,
}

impl Entity for Note {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Aggregate for Note {
    type Event = NoteEvent;
    type Error = NoteError;
    type DeletedState = Self;
}

impl PathAggregate for Note {
    const DELETED: DeletedStatus = DeletedStatus::Gone;
}

define_events! {
    pub enum NoteEvent for Note {
        Written {
            owner: EntityId,
        }
        @init
        => |id, event| Note { id, owner: event.owner },

        Touched {}
        => |_note, _event| {},

        Removed {}
        @delete
        => |note, _event| note,
    }
}

aggregate_command! {
    impl Note {
        @clock @init fn write(owner: EntityId) -> WrittenEvent { owner };
        @clock @delete fn remove() -> RemovedEvent { };
    }
}

pub struct Owner;

impl Access<Note> for Owner {
    type User = User;

    fn allows(note: &AggregateRoot<Note>, user: &AggregateRoot<User>) -> bool {
        note.owner == user.entity_id()
    }
}

pub struct OpenOwner;

impl Access<Note> for OpenOwner {
    type User = User;
    const DENIED: DeniedStatus = DeniedStatus::Forbidden;

    fn allows(note: &AggregateRoot<Note>, user: &AggregateRoot<User>) -> bool {
        Owner::allows(note, user)
    }
}

pub struct RegisteredOwner;

impl Access<Note> for RegisteredOwner {
    type User = User;
    const REGISTERED: bool = true;

    fn allows(note: &AggregateRoot<Note>, user: &AggregateRoot<User>) -> bool {
        Owner::allows(note, user)
    }
}
