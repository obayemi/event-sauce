//! The CRUD routes of the notes. Every one needs a registered member, and
//! only the owner of a note gets to see or change it.

use axum::{
    extract::{Path, State},
    http::{header::LOCATION, StatusCode},
    response::IntoResponse,
    Json,
};
use event_sauce::axum::{Access, Authorized, PathAggregate, Registered, Repo};
use event_sauce::postgres::PostgresEventStore;
use event_sauce::prelude::*;
use serde::Deserialize;
use sqlx::PgPool;
use std::result::Result;

use crate::error::ApiError;
use crate::member::Member;
use crate::note::{Note, NoteCommands, NoteDeleteCommands, NoteId};
use crate::projection::{self, NoteView};

type Notes = Repo<Note, PostgresEventStore>;
type Owned = Authorized<Note, Owner, PostgresEventStore>;

impl PathAggregate for Note {}

/// Only a registered member reads a note, and only the member who owns it:
/// anybody else gets the `404` of a note that does not exist.
pub struct Owner;

impl Access<Note> for Owner {
    type User = Member;
    const REGISTERED: bool = true;

    fn allows(note: &AggregateRoot<Note>, member: &AggregateRoot<Member>) -> bool {
        note.ensure_owned_by(member).is_ok()
    }
}

#[derive(Deserialize)]
pub struct NoteForm {
    title: String,
    body: String,
}

#[derive(Deserialize)]
pub struct NotePatch {
    title: Option<String>,
    body: Option<String>,
}

impl From<&AggregateRoot<Note>> for NoteView {
    fn from(note: &AggregateRoot<Note>) -> Self {
        Self {
            id: note.entity_id().into(),
            title: note.title.clone(),
            body: note.body.clone(),
        }
    }
}

pub async fn create(
    Repo(notes): Notes,
    author: Registered<Member>,
    Json(form): Json<NoteForm>,
) -> Result<impl IntoResponse, ApiError> {
    let mut note = Note::write(&author, form.title, form.body)?;
    notes.save(&mut note).await?;
    let view = NoteView::from(&note);
    let location = format!("/notes/{}", view.id);
    Ok((StatusCode::CREATED, [(LOCATION, location)], Json(view)))
}

pub async fn list(
    State(pool): State<PgPool>,
    member: Registered<Member>,
) -> Result<Json<Vec<NoteView>>, ApiError> {
    Ok(Json(projection::notes_of(&pool, member.id()).await?))
}

pub async fn read(note: Owned) -> Json<NoteView> {
    Json(NoteView::from(&*note))
}

pub async fn replace(
    notes: Notes,
    member: Registered<Member>,
    Path(id): Path<NoteId>,
    Json(form): Json<NoteForm>,
) -> Result<Json<NoteView>, ApiError> {
    edit(notes, &member, id, Some(form.title), Some(form.body)).await
}

pub async fn amend(
    notes: Notes,
    member: Registered<Member>,
    Path(id): Path<NoteId>,
    Json(patch): Json<NotePatch>,
) -> Result<Json<NoteView>, ApiError> {
    edit(notes, &member, id, patch.title, patch.body).await
}

pub async fn remove(
    Repo(notes): Notes,
    member: Registered<Member>,
    Path(id): Path<NoteId>,
) -> Result<StatusCode, ApiError> {
    let mut deleted = notes.load(id).await?.delete(&member)?;
    notes.save_deleted(&mut deleted).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn edit(
    Repo(notes): Notes,
    member: &Registered<Member>,
    id: NoteId,
    title: Option<String>,
    body: Option<String>,
) -> Result<Json<NoteView>, ApiError> {
    let view = notes
        .modify(id, |note| {
            note.edit(member, title, body)?;
            Ok(NoteView::from(&*note))
        })
        .await?;
    Ok(Json(view))
}
