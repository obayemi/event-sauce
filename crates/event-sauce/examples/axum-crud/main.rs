//! # axum CRUD Example — Notes Behind a Password Login
//!
//! A full HTTP API on Postgres: members register and log in with a password,
//! and each of them creates, lists, reads, updates and deletes **their own**
//! notes.
//!
//! - `Member` is a user aggregate (`#[aggregate(user)]`) logged in through
//!   `EventSauceApp::postgres`, which wires the event store, the login
//!   store, the user cache and the correlation layer.
//! - `Note` is an aggregate whose events carry an actor: the member who
//!   creates it, edits it or deletes it. Ownership is the actor validation
//!   of the events, so another member's attempt fails with `404`, the same
//!   answer as for a note that does not exist.
//! - `GET /notes` is served from a read model, a Postgres projection of the
//!   `Note` events that a background task keeps up to date. The list lags
//!   the writes by the projector's latency, while `GET /notes/{id}` reads
//!   the aggregate itself and never lags.
//! - Every event is stored with the id of the request that caused it, taken
//!   from the `x-request-id` header or minted.
//!
//! | Route | What |
//! |---|---|
//! | `POST /register` | create a member: `{nickname, email, password}` |
//! | `POST /login`, `POST /logout` | open or end the session |
//! | `PUT /password` | change the password and end every session |
//! | `GET /me` | the logged-in member |
//! | `POST /notes` | create a note: `{title, body}` |
//! | `GET /notes` | the caller's notes, from the read model |
//! | `GET /notes/{id}` | one note |
//! | `PUT /notes/{id}` | replace `{title, body}` |
//! | `PATCH /notes/{id}` | change `{title}`, `{body}` or both |
//! | `DELETE /notes/{id}` | delete the note |
//!
//! Errors map to `401` (not logged in, wrong login), `404` (unknown, deleted
//! or foreign note), `409` (email already registered, concurrent edit) and
//! `422` (invalid input).
//!
//! Run it against a Postgres database:
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost/postgres \
//!   cargo run --example axum-crud --features axum-app
//! ```
//!
//! Then, from another terminal:
//! ```bash
//! curl -c jar -H 'content-type: application/json' \
//!   -d '{"nickname":"ada","email":"ada@example.com","password":"correct-horse"}' \
//!   localhost:3000/register
//! curl -c jar -H 'content-type: application/json' \
//!   -d '{"email":"ada@example.com","password":"correct-horse"}' \
//!   localhost:3000/login
//! curl -b jar -H 'content-type: application/json' \
//!   -d '{"title":"Groceries","body":"milk"}' localhost:3000/notes
//! curl -b jar localhost:3000/notes
//! ```
//!
//! The tests drive the router against a Postgres container, so they need
//! Docker.

mod app;
mod auth_routes;
mod error;
mod member;
mod note;
mod note_routes;
mod projection;
#[cfg(test)]
mod tests;

use event_sauce::auth::Argon2Hasher;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    let service = app::build(pool, Argon2Hasher::default()).await?;
    tokio::spawn(async move {
        let Err(error) = service.projector.run().await;
        eprintln!("note projection stopped: {error}");
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, service.router).await?;
    Ok(())
}
