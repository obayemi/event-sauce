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
mod event_bus;
mod event_envelope;
mod event_store;
mod version;

pub use aggregate::Aggregate;
pub use aggregate_error::AggregateError;
pub use aggregate_id::AggregateId;
pub use apply_event::ApplyEvent;
pub use domain_event::DomainEvent;
pub use error::{Error, Result};
pub use event_bus::{EventBus, EventFilter};
pub use event_envelope::{EventEnvelope, EventMetadata};
pub use event_store::{EventStore, Position, Snapshot, StreamId};
pub use version::Version;
