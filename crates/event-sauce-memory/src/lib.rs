//! # event-sauce-memory
//!
//! In-memory implementations of event-sauce traits for testing.
//!
//! This crate provides fast, in-memory implementations of:
//! - `EventStore`
//! - `SnapshotStore`
//! - `EventBus`
//!
//! Perfect for unit tests and development.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod event_store;

pub use event_store::InMemoryEventStore;
