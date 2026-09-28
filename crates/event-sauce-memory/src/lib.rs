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
