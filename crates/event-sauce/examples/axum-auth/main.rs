//! # axum Auth Example — Password Login on an Event-Sourced User
//!
//! A `Member` aggregate carries its own password credentials. Registering
//! saves it, `POST /login` opens a session, and the guarded routes read the
//! logged-in member through the `CurrentUser` extractor. Changing the
//! password ends every session of the member, because the session is tied to
//! the credentials' fingerprint.
//!
//! Everything lives in memory, so no database is needed. Run with:
//! ```bash
//! cargo run --example axum-auth --features axum-login
//! ```
//!
//! Then, from another terminal:
//! ```bash
//! curl -c jar -H 'content-type: application/json' \
//!   -d '{"nickname":"ada","email":"ada@example.com","password":"correct-horse"}' \
//!   localhost:3000/members
//! curl -c jar -H 'content-type: application/json' \
//!   -d '{"email":"ada@example.com","password":"correct-horse"}' \
//!   localhost:3000/login
//! curl -b jar localhost:3000/me
//! ```

mod app;
mod member;
#[cfg(test)]
mod tests;

use event_sauce::auth::Argon2Hasher;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let router = app::router(Argon2Hasher::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router).await
}
