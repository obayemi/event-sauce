//! # event-sauce-core
//!
//! Core traits and types for the event-sauce event sourcing library.
//!
//! This crate provides the fundamental building blocks for event-sourced systems:
//! - `Entity` trait for domain objects with identity
//! - `Aggregate` trait for event-sourced entities
//! - `AggregateRoot<A>` wrapper for infrastructure concerns
//! - `DomainEvent` trait for events
//! - `EventStore` trait for event persistence
//! - Core types like `EventEnvelope`, `StreamId`, `AggregateVersion`, `EventVersion`, `EntityId`
//! - Helper macros like `command_handler!` for reducing boilerplate
//!
//! ## TDD Approach
//!
//! This library is built using strict Test-Driven Development (TDD).
//! Every line of code has corresponding tests.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod aggregate;
mod aggregate_error;
mod aggregate_root;
mod aggregate_type;
mod apply_event;
mod domain_event;
mod entity;
mod entity_id;
mod error;
mod event_applicator;
mod event_envelope;
mod event_store;
mod init_event;
mod macros;
mod projection;
mod repository;
mod snapshot_config;
mod snapshot_strategy;
/// Specification pattern for composable, reusable business rule validation.
pub mod specification;
mod subscription;
mod types;
mod uninit_aggregate_root;
mod version;

#[doc(hidden)]
pub mod test_fixtures;

pub use aggregate::Aggregate;
pub use aggregate_error::AggregateError;
pub use aggregate_root::AggregateRoot;
pub use aggregate_type::AggregateType;
pub use apply_event::ApplyEvent;
pub use domain_event::{DomainEvent, EventType};
pub use entity::{DefaultEntity, Entity};
pub use entity_id::EntityId;
pub use error::{Error, Result};
pub use event_applicator::EventApplicator;
pub use event_envelope::{EventEnvelope, EventMetadata};
pub use event_store::{EventStore, Position, Snapshot, StreamId};
pub use init_event::InitEvent;
pub use projection::Projection;
pub use repository::Repository;
pub use snapshot_config::{SnapshotConfig, SnapshotConfigBuilder};
pub use snapshot_strategy::{AlwaysSnapshot, EveryNEvents, NeverSnapshot, SnapshotStrategy};
pub use specification::{Specification, SpecificationError};
pub use subscription::{
    CheckpointStore, CheckpointStrategy, ErrorPolicy, EventFilter, Subscription,
    SubscriptionBuilder, SubscriptionConfig,
};
pub use types::CheckpointStoreRef;
pub use uninit_aggregate_root::UninitAggregateRoot;
pub use version::{AggregateVersion, EventVersion};
