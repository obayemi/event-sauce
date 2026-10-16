//! Helper macros for reducing boilerplate in event-sourced aggregates
//!
//! This module provides declarative macros that simplify common patterns in event sourcing:
//! - [`aggregate_command!`](crate::aggregate_command) - automatic command method generation
//! - [`define_events!`](crate::define_events) - declarative event enum and trait generation
//!
//! Each family lives in its own submodule. `spec!` lives beside
//! [`specification`](crate::specification) and `policy!` beside
//! [`policy`](crate::policy) instead, since that is what each one generates.

mod aggregate_command;
mod define_events;
mod validation;
