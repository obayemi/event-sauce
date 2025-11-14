//! # event-sauce-postgres
//!
//! PostgreSQL backend for event-sauce with streaming support.
//!
//! Provides production-ready PostgreSQL implementations of:
//! - `EventStore` - Durable event persistence with ACID guarantees
//!
//! # Features
//!
//! - Full ACID transactions
//! - Optimistic concurrency control
//! - Efficient streaming queries
//! - Snapshot support
//! - Connection pooling
//!
//! # Example
//!
//! ```ignore
//! use event_sauce_postgres::PostgresEventStore;
//! use sqlx::PgPool;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgresql://localhost/events").await?;
//!     let store = PostgresEventStore::new(pool);
//!
//!     // Store is ready to use
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod event_store;

pub use event_store::PostgresEventStore;
