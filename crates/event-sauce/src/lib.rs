//! # event-sauce
//!
//! Event-driven modeling and event sourcing for Rust.
//!
//! Aggregates express every state change as an explicit, validated event,
//! persisted behind the [`Repository`] trait. Simple by default, powerful
//! when needed.
//!
//! ## The only companion dependency you need
//!
//! This crate is the entry point to the whole library. The core traits and
//! types are re-exported at its root, the backends live under [`memory`] and
//! [`postgres`], and the bundled AES-256-GCM provider sits in [`crypto`]
//! next to the traits it implements. The derive macros expand to paths rooted
//! here, so `event-sauce` is the one line a downstream `Cargo.toml` needs
//! for the library itself:
//!
//! ```toml
//! [dependencies]
//! event-sauce = { version = "0.1", features = ["postgres"] }
//! ```
//!
//! [`define_events!`](crate::define_events) still expands to
//! `#[derive(Serialize, Deserialize)]` on the generated event structs, so a
//! caller using it also needs `serde` with the `derive` feature. `paste`,
//! `uuid`, `serde_json` and `async-trait` (needed only by `policy!`) are
//! macro-only: a caller adds none of them to use the macros.
//!
//! `chrono` is not macro-only — every command without `@clock` takes its
//! instant as a parameter, and every generated event carries a public
//! `DateTime<Utc>` field, so `chrono` types are part of the public API. A
//! caller still adds no `chrono` line of its own: the crate is reachable as
//! [`chrono`], the same one `event-sauce` already depends on. A
//! hand-written `impl` of one of the async traits (`StateProjection`,
//! `EventStore`, `CheckpointStore`) is a separate case: those traits are
//! themselves declared with `#[async_trait]`, so implementing one by hand
//! still needs the `async-trait` crate as a direct dependency, the same way
//! implementing any other trait needs whatever attribute macros that
//! trait's definition depends on.
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
///
/// Needs a persistence style (`event-sourcing` and/or `state-store`)
/// enabled alongside `memory`, otherwise the backend has nothing to export.
#[cfg(all(
    feature = "memory",
    any(feature = "event-sourcing", feature = "state-store")
))]
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
