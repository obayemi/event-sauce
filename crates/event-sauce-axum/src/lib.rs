//! # event-sauce-axum
//!
//! axum and tower integration for event-sauce.
//!
//! [`CorrelationLayer`] runs every request inside an event context carrying
//! its request id, so each event committed while serving the request is
//! stored with that id as its correlation id.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod correlation;
mod request_id;

pub use correlation::{Correlation, CorrelationLayer, ResponseFuture, DEFAULT_HEADER};
pub use request_id::RequestId;
