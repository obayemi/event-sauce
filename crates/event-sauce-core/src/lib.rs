//! # event-sauce-core
//!
//! Core traits and types for the event-sauce event-driven framework.
//!
//! This crate provides the fundamental building blocks for event-driven systems:
//! - `Entity` trait for domain objects with identity
//! - `Aggregate` trait for event-driven entities
//! - `AggregateRoot<A>` wrapper for infrastructure concerns
//! - `DomainEvent` trait for events
//! - `Repository` trait for persistence-style-agnostic aggregate persistence
//! - `EventStore` trait for event-sourced persistence
//! - `StateStore` trait for current-state persistence
//! - Core types like `EventEnvelope`, `StreamId`, `AggregateVersion`, `EventVersion`, `EntityId`
//! - Helper macros like `command_handler!` for reducing boilerplate
//!
//! ## Feature flags
//!
//! - `event-sourcing` (default): the event-sourced persistence style —
//!   `EventStore`, `EventSourcedRepository`, snapshots, checkpoints, policies,
//!   the audit log, and aggregate encryption.
//! - `state-store` (default): the current-state persistence style —
//!   `StateStore`, `StateStoredRepository`, and in-transaction
//!   `StateProjection`s.
//!
//! The domain layer (entities, aggregates, events, commands, the `Repository`
//! trait, and the macros) is always available; application code written
//! against it runs unchanged under either persistence style.
//!
//! ## TDD Approach
//!
//! This library is built using strict Test-Driven Development (TDD).
//! Every line of code has corresponding tests.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod actor_event;
mod aggregate;
mod aggregate_error;
mod aggregate_id;
mod aggregate_root;
mod aggregate_type;
mod apply_event;
#[cfg(feature = "event-sourcing")]
mod checkpoint;
/// Aggregate claims for cross-aggregate uniqueness constraints.
pub mod claims;
#[cfg(any(feature = "event-sourcing", feature = "state-store"))]
mod commit_source;
/// Cryptographic traits and helpers for encrypted aggregate encryption (crypto-shredding).
#[cfg(feature = "event-sourcing")]
pub mod crypto;
mod delete_event;
mod deleted_aggregate_root;
/// The dependency graph between published payloads and the aggregates they read.
#[cfg(feature = "dependencies")]
pub mod dependencies;
mod domain_event;
mod entity;
mod entity_id;
mod error;
mod event_applicator;
mod event_envelope;
mod event_filter;
/// Event log query types and trait for paginated, filtered audit log access.
#[cfg(feature = "event-sourcing")]
pub mod event_log;
#[cfg(feature = "event-sourcing")]
mod event_store;
mod init_event;
mod loaded;
mod macros;
mod modify_error;
/// Policy system for cross-aggregate event reactions with causation tracking.
#[cfg(feature = "event-sourcing")]
pub mod policy;
mod repository;
#[cfg(feature = "event-sourcing")]
mod snapshot_config;
#[cfg(feature = "event-sourcing")]
mod snapshot_strategy;
/// Specification pattern for composable, reusable business rule validation.
pub mod specification;
#[cfg(feature = "state-store")]
mod state_projection;
#[cfg(feature = "state-store")]
mod state_repository;
#[cfg(feature = "state-store")]
mod state_store;
mod types;
mod uninit_aggregate_root;
mod version;

#[cfg(test)]
mod test_fixtures;

/// Re-exports the exported macros expand through, so a caller depending only
/// on `event-sauce`/`event-sauce-core` needs no direct dependency on
/// `pastey`, `uuid`, `serde_json` or `async-trait` to use them. `chrono` is
/// also re-exported here for the macros' own internal use, but a caller
/// names it through the public [`chrono`](crate::chrono) re-export instead:
/// instants are part of the public API, not a macro-only detail.
#[doc(hidden)]
pub mod __private {
    pub use async_trait::async_trait;
    pub use chrono;
    pub use pastey::paste;
    pub use serde_json;
    pub use uuid;
}

/// The `chrono` crate the generated code's instants are built from.
///
/// Every command without `@clock` takes its instant as a parameter, and
/// every event carries its `occurred_at` through [`DomainEvent`], so
/// `chrono` types are part of this library's public API. Re-exported here
/// so a caller can spell
/// `chrono::DateTime<chrono::Utc>` as `event_sauce::chrono::DateTime<..>`
/// without adding `chrono` to its own `Cargo.toml`.
pub use chrono;

pub use actor_event::{ActorDeleteEvent, ActorEvent, ActorInitEvent};
pub use aggregate::Aggregate;
pub use aggregate_error::AggregateError;
pub use aggregate_id::{AggregateId, EntityIdFor};
pub use aggregate_root::AggregateRoot;
pub use aggregate_type::AggregateType;
pub use apply_event::ApplyEvent;
#[cfg(feature = "event-sourcing")]
pub use checkpoint::{wait_for_checkpoint, CheckpointStore};
pub use claims::AggregateClaim;
#[cfg(feature = "event-sourcing")]
pub use crypto::{CryptoKeyStore, CryptoProvider};
pub use delete_event::DeleteEvent;
pub use deleted_aggregate_root::DeletedAggregateRoot;
#[cfg(feature = "dependencies")]
pub use dependencies::{
    conflicting_triggers, Affected, Dependency, KeyClash, Node, NodeRef, Source, Stale, Trigger,
};
pub use domain_event::{DomainEvent, EventType};
pub use entity::{DefaultEntity, Entity};
pub use entity_id::EntityId;
pub use error::{ClaimConflict, Error, Result};
pub use event_applicator::EventApplicator;
pub use event_envelope::{EventEnvelope, EventMetadata};
pub use event_filter::EventFilter;
#[cfg(feature = "event-sourcing")]
pub use event_log::{EventLogEntry, EventLogOrder, EventLogPage, EventLogParams, EventLogQuery};
#[cfg(feature = "event-sourcing")]
pub use event_store::{EventStore, Snapshot, StreamCommit};
pub use init_event::InitEvent;
pub use loaded::Loaded;
pub use modify_error::ModifyError;
#[cfg(feature = "event-sourcing")]
pub use policy::{
    OnError, OnRetryExhausted, Policy, PolicyContext, PolicyRunner, RetryConfig, RetryLimit,
};
#[cfg(feature = "event-sourcing")]
pub use repository::EventSourcedRepository;
pub use repository::Repository;
#[cfg(feature = "event-sourcing")]
pub use snapshot_config::{SnapshotConfig, SnapshotConfigBuilder};
#[cfg(feature = "event-sourcing")]
pub use snapshot_strategy::{AlwaysSnapshot, EveryNEvents, NeverSnapshot, SnapshotStrategy};
pub use specification::{Spec, Specification, SpecificationError};
#[cfg(feature = "state-store")]
pub use state_projection::StateProjection;
#[cfg(feature = "state-store")]
pub use state_repository::StateStoredRepository;
#[cfg(feature = "state-store")]
pub use state_store::{StateCommit, StateStore, StoredState};
#[cfg(feature = "event-sourcing")]
pub use types::{CheckpointStoreRef, CryptoKeyStoreRef, CryptoProviderRef};
pub use types::{Position, StreamId};
pub use uninit_aggregate_root::UninitAggregateRoot;
pub use version::{AggregateVersion, EventVersion};
