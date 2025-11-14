//! # event-sauce-macros
//!
//! Derive macros for event-sauce to reduce boilerplate.
//!
//! Provides: #[derive(Aggregate)], #[derive(Event)], #[derive(Projection)]

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use proc_macro::TokenStream;

/// Derive macro for Aggregate trait
#[proc_macro_derive(Aggregate, attributes(aggregate, aggregate_id, aggregate_version, aggregate_events))]
pub fn derive_aggregate(_input: TokenStream) -> TokenStream {
    // Will be implemented following TDD
    TokenStream::new()
}

/// Derive macro for Event trait
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(_input: TokenStream) -> TokenStream {
    // Will be implemented following TDD
    TokenStream::new()
}
