//! # event-sauce-postgres
//!
//! `PostgreSQL` backend for event-sauce with streaming support.
//!
//! Provides production-ready `PostgreSQL` implementations of:
//! - `EventStore` - Durable event persistence with ACID guarantees
//! - `CheckpointStore` - Durable checkpoint tracking for subscriptions
//!
//! # Features
//!
//! - Full ACID transactions
//! - Optimistic concurrency control
//! - Efficient streaming queries
//! - Snapshot support
//! - Connection pooling
//! - **Schema isolation** - Avoid migration conflicts with your application
//!
//! # Quick Start (Recommended)
//!
//! Use [`PostgresBackend`] for the simplest setup — it creates both event store
//! and checkpoint store, runs migrations, and wires everything together:
//!
//! ```ignore
//! use event_sauce_postgres::PostgresBackend;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let backend = PostgresBackend::setup("postgresql://localhost/events", "event_sauce").await?;
//!
//!     // Ready to use — event store, checkpoint store, and migrations all handled
//!     let event_store = backend.event_store();
//!     let checkpoint_store = backend.checkpoint_store();
//!     let pool = backend.pool();
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Builder Pattern
//!
//! For custom configuration:
//!
//! ```ignore
//! use event_sauce_postgres::PostgresBackend;
//! use event_sauce_core::SnapshotConfig;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let backend = PostgresBackend::builder()
//!         .database_url("postgresql://localhost/events")
//!         .schema("my_custom_schema")
//!         .snapshot_config(SnapshotConfig::disabled())
//!         .build()
//!         .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Individual Stores
//!
//! For fine-grained control, create stores individually:
//!
//! ```ignore
//! use event_sauce_postgres::PostgresEventStore;
//! use sqlx::PgPool;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgresql://localhost/events").await?;
//!
//!     let store = PostgresEventStore::builder()
//!         .pool(pool)
//!         .schema("event_sauce")
//!         .build();
//!
//!     store.migrate().await?;
//!
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod backend;
mod checkpoint_store;
mod event_store;

pub use backend::{PostgresBackend, PostgresBackendBuilder};
pub use checkpoint_store::{PostgresCheckpointStore, PostgresCheckpointStoreBuilder};
pub use event_store::{PostgresEventStore, PostgresEventStoreBuilder};
