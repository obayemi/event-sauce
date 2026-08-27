//! # event-sauce
//!
//! Event-driven modeling and event sourcing for Rust.
//!
//! Aggregates express every state change as an explicit, validated event,
//! persisted behind the [`Repository`] trait. Simple by default, powerful
//! when needed.
//!
//! ## The only dependency you need
//!
//! This crate is the entry point to the whole library. The core traits and
//! types are re-exported at its root, the backends live under [`memory`] and
//! [`postgres`], and the bundled AES-256-GCM provider sits in [`crypto`]
//! next to the traits it implements. The derive macros expand to paths rooted
//! here, so `event-sauce` is the single line a downstream `Cargo.toml` needs:
//!
//! ```toml
//! [dependencies]
//! event-sauce = { version = "0.1", features = ["postgres"] }
//! ```
//!
//! Crates that depend on `event-sauce-core` directly keep working — the
//! macros resolve their generated paths to whichever of the two a crate
//! depends on, under whatever name it is renamed to.
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

/// Encryption traits and helpers, plus the bundled AES-256-GCM provider.
///
/// Shadows the `crypto` module re-exported from `event-sauce-core` so the
/// traits and the provider that implements them share one path.
#[cfg(feature = "event-sourcing")]
pub mod crypto {
    pub use event_sauce_core::crypto::*;

    #[cfg(feature = "crypto")]
    pub use event_sauce_crypto::Aes256GcmProvider;
}

/// In-memory backend: stores, projections, and test doubles.
#[cfg(feature = "memory")]
pub mod memory {
    pub use event_sauce_memory::*;
}

/// `PostgreSQL` backend: event store, state store, checkpoints, and outbox.
#[cfg(feature = "postgres")]
pub mod postgres {
    pub use event_sauce_postgres::*;
}

/// Prelude module for convenient imports
///
/// Commonly used types and traits
pub mod prelude {

    pub use event_sauce_core::*;

    #[cfg(feature = "macros")]
    pub use event_sauce_macros::*;
}
