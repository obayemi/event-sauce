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
//! - **Schema isolation** - Avoid migration conflicts with your application
//!
//! # Quick Start (Recommended)
//!
//! By default, event-sauce uses the `"event_sauce"` schema to isolate its tables
//! and migrations from your application:
//!
//! ```ignore
//! use event_sauce_postgres::PostgresEventStore;
//! use sqlx::PgPool;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgresql://localhost/events").await?;
//!
//!     // Simple constructor - uses "event_sauce" schema by default
//!     let store = PostgresEventStore::new(pool);
//!
//!     // Run migrations - creates event_sauce schema and tables
//!     store.migrate().await?;
//!
//!     // Now you have:
//!     // - event_sauce.events table
//!     // - event_sauce.snapshots table
//!     // - event_sauce._event_sauce_migrations table
//!     // Your application's public._sqlx_migrations remains separate!
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
//! use event_sauce_postgres::PostgresEventStore;
//! use sqlx::PgPool;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgresql://localhost/events").await?;
//!
//!     // Customize schema name, snapshot config, etc.
//!     let store = PostgresEventStore::builder()
//!         .pool(pool)
//!         .schema("my_custom_schema")  // Or use "public" for default schema
//!         .build();
//!
//!     store.migrate().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Custom Configuration
//!
//! ```ignore
//! use event_sauce_postgres::PostgresEventStore;
//! use event_sauce_core::SnapshotConfig;
//! use sqlx::PgPool;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgresql://localhost/events").await?;
//!
//!     let store = PostgresEventStore::builder()
//!         .pool(pool.clone())
//!         .schema("event_sauce")
//!         .snapshot_config(SnapshotConfig::disabled())
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

mod event_bus;
mod event_store;

pub use event_bus::PostgresEventBus;
pub use event_store::{PostgresEventStore, PostgresEventStoreBuilder};
