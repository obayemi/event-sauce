//! # event-sauce-memory
//!
//! In-memory implementations of event-sauce traits for testing.
//!
//! This crate provides fast, in-memory implementations of:
//! - `EventStore`
//! - `StateStore`
//! - `SnapshotStore`
//! - `CheckpointStore`
//!
//! Perfect for unit tests and development.
//!
//! ## Feature flags
//!
//! - `event-sourcing` (default): `InMemoryEventStore`, `InMemoryCheckpointStore`,
//!   `InMemoryCryptoKeyStore` and `InMemoryEventLogQuery`, mirroring
//!   `event-sauce-core`'s feature of the same name.
//! - `state-store` (default): `InMemoryStateStore` and `InMemoryProjectionContext`.
//! - `crypto` (opt-in, off by default): installs `event-sauce-crypto`'s
//!   AES-256-GCM provider as the default crypto provider; implies
//!   `event-sourcing`.
//! - `unit-of-work` (opt-in, off by default): `InMemoryTransactionalStore`,
//!   committing an `event_sauce_core::AtomicCommit` across an
//!   `InMemoryEventStore` and an `InMemoryStateStore` atomically; implies
//!   `event-sourcing` and `state-store`.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

#[cfg(feature = "event-sourcing")]
mod checkpoint_store;
#[cfg(feature = "event-sourcing")]
mod crypto_key_store;
#[cfg(feature = "event-sourcing")]
mod event_log;
#[cfg(feature = "event-sourcing")]
mod event_store;
#[cfg(feature = "state-store")]
mod state_store;
#[cfg(feature = "unit-of-work")]
mod unit_of_work;

#[cfg(feature = "event-sourcing")]
pub use checkpoint_store::InMemoryCheckpointStore;
#[cfg(feature = "event-sourcing")]
pub use crypto_key_store::InMemoryCryptoKeyStore;
#[cfg(feature = "event-sourcing")]
pub use event_log::InMemoryEventLogQuery;
#[cfg(feature = "event-sourcing")]
pub use event_store::InMemoryEventStore;
#[cfg(feature = "state-store")]
pub use state_store::{InMemoryProjectionContext, InMemoryStateStore, InMemoryStateStoreBuilder};
#[cfg(feature = "unit-of-work")]
pub use unit_of_work::InMemoryTransactionalStore;
