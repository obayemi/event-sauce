//! The read model of the notes: one row per live note, kept up to date from
//! the `Note` events by a transactional Postgres projection.
//!
//! The runner applies each event and advances the projection's checkpoint in
//! one transaction, so a crash never leaves the table ahead of or behind its
//! checkpoint. The list is therefore eventually consistent with the writes:
//! a note shows up once the [`Projector`] has caught up with its events.

use std::sync::Arc;
use std::time::Duration;

use crate::member::MemberId;
use event_sauce::postgres::{
    LeaseOutcome, PostgresBackend, PostgresEventStore, PostgresProjection,
};
use event_sauce::prelude::{Error, EventEnvelope, EventType, Result};
use futures::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::note::{CreatedEvent, DeletedEvent, EditedEvent};

const LEASE: Duration = Duration::from_secs(30);
const IDLE: Duration = Duration::from_secs(1);

/// The API shape of a note: what the routes answer and what a row of the
/// read model holds.
#[derive(Serialize, FromRow)]
pub struct NoteView {
    pub id: Uuid,
    pub title: String,
    pub body: String,
}

fn sql_error(error: sqlx::Error) -> Error {
    Error::custom(format!("notes read model: {error}"))
}

fn payload<E: DeserializeOwned>(envelope: &EventEnvelope) -> Result<E> {
    serde_json::from_value(envelope.event_data.clone())
        .map_err(|error| Error::custom(format!("{}: {error}", envelope.event_type)))
}

/// The projection that maintains the `note_list` table.
pub struct NoteList;

impl NoteList {
    async fn migrate(pool: &PgPool) -> std::result::Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS note_list (
                id UUID PRIMARY KEY,
                owner UUID NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS note_list_owner ON note_list (owner, created_at)")
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn created(envelope: &EventEnvelope, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        let event: CreatedEvent = payload(envelope)?;
        sqlx::query(
            "INSERT INTO note_list (id, owner, title, body, created_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(envelope.aggregate_id)
        .bind(event.owner.as_uuid())
        .bind(event.title.trim())
        .bind(&event.body)
        .bind(envelope.created_at)
        .execute(&mut **tx)
        .await
        .map_err(sql_error)?;
        Ok(())
    }

    async fn edited(envelope: &EventEnvelope, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        let event: EditedEvent = payload(envelope)?;
        sqlx::query(
            "UPDATE note_list
             SET title = COALESCE($2, title), body = COALESCE($3, body)
             WHERE id = $1",
        )
        .bind(envelope.aggregate_id)
        .bind(event.title.as_deref().map(str::trim))
        .bind(event.body)
        .execute(&mut **tx)
        .await
        .map_err(sql_error)?;
        Ok(())
    }

    async fn deleted(envelope: &EventEnvelope, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        let _: DeletedEvent = payload(envelope)?;
        sqlx::query("DELETE FROM note_list WHERE id = $1")
            .bind(envelope.aggregate_id)
            .execute(&mut **tx)
            .await
            .map_err(sql_error)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PostgresProjection for NoteList {
    const NAME: &'static str = "NoteList";

    fn handled_event_types() -> Option<Vec<&'static str>> {
        Some(vec![
            CreatedEvent::EVENT_TYPE,
            EditedEvent::EVENT_TYPE,
            DeletedEvent::EVENT_TYPE,
        ])
    }

    async fn handle(
        &mut self,
        envelope: &EventEnvelope,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()> {
        match envelope.event_type.as_str() {
            CreatedEvent::EVENT_TYPE => Self::created(envelope, tx).await,
            EditedEvent::EVENT_TYPE => Self::edited(envelope, tx).await,
            DeletedEvent::EVENT_TYPE => Self::deleted(envelope, tx).await,
            other => Err(Error::custom(format!("unexpected event {other}"))),
        }
    }
}

/// The live notes of `owner`, oldest first.
///
/// # Errors
///
/// The database error, when the query fails.
pub async fn notes_of(
    pool: &PgPool,
    owner: MemberId,
) -> std::result::Result<Vec<NoteView>, sqlx::Error> {
    sqlx::query_as("SELECT id, title, body FROM note_list WHERE owner = $1 ORDER BY created_at, id")
        .bind(owner.as_uuid())
        .fetch_all(pool)
        .await
}

/// Keeps [`NoteList`] up to date from the events of the store it owns.
#[derive(Clone)]
pub struct Projector {
    backend: Arc<PostgresBackend>,
    worker_id: String,
}

impl Projector {
    /// Connects the event store on `pool` to the checkpoint store and creates
    /// the table of the read model.
    ///
    /// # Errors
    ///
    /// The store or database error, when a migration fails.
    pub async fn setup(pool: PgPool) -> anyhow::Result<Self> {
        let backend = PostgresBackend::builder()
            .pool(pool.clone())
            .build()
            .await?;
        NoteList::migrate(&pool).await?;
        Ok(Self {
            backend: Arc::new(backend),
            worker_id: Uuid::new_v4().to_string(),
        })
    }

    /// The event store the application must write through, since it is the
    /// one that wakes the projector up.
    pub fn event_store(&self) -> Arc<PostgresEventStore> {
        self.backend.event_store()
    }

    /// Applies every event the read model has not seen, under the lease of
    /// the projection, so any number of instances may run it.
    ///
    /// # Errors
    ///
    /// The store error, when an event cannot be read or applied.
    pub async fn catch_up(&self) -> Result<LeaseOutcome> {
        self.backend
            .run_leased_projection(&mut NoteList, &self.worker_id, LEASE)
            .await
    }

    /// Catches up, then sleeps until the next event is announced or a second
    /// passed, forever.
    ///
    /// # Errors
    ///
    /// The store error, when the announcements cannot be listened to. A
    /// failed catch-up is reported and retried on the next wake-up.
    pub async fn run(self) -> Result<()> {
        let mut announcements = Box::pin(self.event_store().listen_for_events().await?);
        loop {
            if let Err(error) = self.catch_up().await {
                eprintln!("note projection: {error}");
            }
            tokio::select! {
                _ = announcements.next() => {}
                () = tokio::time::sleep(IDLE) => {}
            }
        }
    }
}
