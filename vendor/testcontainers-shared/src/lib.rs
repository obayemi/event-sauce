//! # testcontainers-shared
//!
//! One [testcontainers] server per test binary, with an isolated slice of it
//! handed to every test.
//!
//! Starting a container per test makes a suite pay a container start per
//! test and run as many servers as it runs tests at once. Instead, the first
//! test that asks for a server starts it, every later test of the same
//! process reuses it, and each test gets a slice of its own: a fresh database
//! on the shared [`postgres`] server. Slices are released as soon as their
//! guard drops, and the servers are removed when the test process exits
//! normally, whether the run passed or failed; a run killed by a signal
//! leaves them behind, under a name carrying the process id
//! (`testcontainers_shared_<kind>_<pid>_<attempt>`).
//!
//! Tests whose side effects reach beyond their slice start a dedicated
//! container instead, with [`postgres::start_postgres_url`] or with
//! [`start_with_retry`] on a request built by [`postgres::test_postgres`].
//!
//! ```no_run
//! # async fn example() {
//! let (_database, url) = testcontainers_shared::postgres::fresh_database().await;
//! let pool = sqlx::PgPool::connect(&url).await.unwrap();
//! # }
//! ```
//!
//! [testcontainers]: https://docs.rs/testcontainers

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

pub mod postgres;
mod server;

pub use server::start_with_retry;
