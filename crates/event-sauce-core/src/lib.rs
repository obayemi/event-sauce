//! # event-sauce-core
//!
//! Core traits and types for the event-sauce event sourcing library.
//!
//! This crate provides the fundamental building blocks for event-sourced systems:
//! - `Aggregate` trait for domain aggregates
//! - `DomainEvent` trait for events
//! - `EventStore` trait for event persistence
//! - `EventBus` trait for event publishing
//! - Core types like `EventEnvelope`, `StreamId`, `Version`
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
mod aggregate_id;
mod apply_event;
mod domain_event;
mod error;
mod event_envelope;
mod event_store;
mod macros;
mod snapshot_config;
mod snapshot_strategy;
mod subscription;
mod types;
mod version;

pub use aggregate::Aggregate;
pub use aggregate_error::AggregateError;
pub use aggregate_id::{AggregateId, DefaultAggregateId};
pub use apply_event::ApplyEvent;
pub use domain_event::DomainEvent;
pub use error::{Error, Result};
pub use event_envelope::{EventEnvelope, EventMetadata};
pub use event_store::{count_events, load, EventStore, Position, Snapshot, StreamId};
pub use snapshot_config::{SnapshotConfig, SnapshotConfigBuilder};
pub use snapshot_strategy::{AlwaysSnapshot, EveryNEvents, NeverSnapshot, SnapshotStrategy};
pub use subscription::{
    CheckpointStore, CheckpointStrategy, ErrorPolicy, EventFilter, Subscription,
    SubscriptionBuilder, SubscriptionConfig,
};
pub use types::{CheckpointStoreRef, EventStoreRef};
pub use version::Version;
