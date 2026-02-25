//! # event-sauce
//!
//! Production-ready event sourcing for Rust.
//!
//! Simple by default, powerful when needed.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use event_sauce::prelude::*;
//!
//! #[aggregate(event = "CounterEvent", error = "CounterError")]
//! struct Counter {
//!     #[id]
//!     id: EntityId,
//!     count: i32,
//! }
//! ```
//!
//! ## Features
//!
//! - **Streaming first**: Memory-efficient event processing
//! - **Type-safe**: Compile-time guarantees with derive macros
//! - **Multiple backends**: `PostgreSQL`, in-memory
//! - **100% test coverage**: Built with strict TDD
//!
//! ## TDD Philosophy
//!
//! This library is built following strict Test-Driven Development.
//! Every line of code has corresponding tests.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

// Re-export core types
pub use event_sauce_core::*;

#[cfg(feature = "macros")]
pub use event_sauce_macros::*;

#[cfg(feature = "memory")]
pub use event_sauce_memory;

#[cfg(feature = "postgres")]
pub use event_sauce_postgres;

/// Prelude module for convenient imports
///
/// Commonly used types and traits
pub mod prelude {

    pub use event_sauce_core::*;

    #[cfg(feature = "macros")]
    pub use event_sauce_macros::*;
}
