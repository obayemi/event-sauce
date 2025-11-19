//! # event-sauce-memory
//!
//! In-memory implementations of event-sauce traits for testing.
//!
//! This crate provides fast, in-memory implementations of:
//! - `EventStore`
//! - `SnapshotStore`
//! - `CheckpointStore`
//!
//! Perfect for unit tests and development.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod checkpoint_store;
mod event_store;

pub use checkpoint_store::InMemoryCheckpointStore;
pub use event_store::InMemoryEventStore;
