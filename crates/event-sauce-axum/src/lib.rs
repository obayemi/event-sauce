//! # event-sauce-axum
//!
//! axum and tower integration for event-sauce.
//!
//! - [`CorrelationLayer`] runs every request inside an event context
//!   carrying its request id, so each event committed while serving the
//!   request is stored with that id as its correlation id.
//! - [`Store`], [`Repo`] and [`UnitOfWork`] are extractors over the
//!   application state: there is no registry, a repository is built per
//!   request from the shared store.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod correlation;
mod repo;
mod request_id;
mod store;
mod unit_of_work;

pub use correlation::{Correlation, CorrelationLayer, ResponseFuture, DEFAULT_HEADER};
pub use repo::Repo;
pub use request_id::RequestId;
pub use store::Store;
pub use unit_of_work::UnitOfWork;
