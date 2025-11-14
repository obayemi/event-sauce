//! # event-sauce-postgres
//!
//! `PostgreSQL` backend for event-sauce with streaming support.
//!
//! Provides production-ready `PostgreSQL` implementations of:
//! - `EventStore` - Durable event persistence with ACID guarantees
//! - `EventBus` - Real-time event delivery using `LISTEN`/`NOTIFY`
//!
//! # Features
//!
//! - Full ACID transactions
//! - Optimistic concurrency control
//! - Efficient streaming queries
//! - Snapshot support
//! - Connection pooling
//! - Real-time event notifications
//!
//! # Example
//!
//! ```ignore
//! use event_sauce_postgres::{PostgresEventStore, PostgresEventBus};
//! use sqlx::PgPool;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgresql://localhost/events").await?;
//!     let store = PostgresEventStore::new(pool.clone());
//!     let bus = PostgresEventBus::new(pool).await?;
//!
//!     // Store and bus are ready to use
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod event_bus;
mod event_store;

pub use event_bus::PostgresEventBus;
pub use event_store::PostgresEventStore;
