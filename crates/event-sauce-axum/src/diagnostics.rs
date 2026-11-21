//! What a failed request logs, when the `tracing` feature is on.

use std::error::Error;

#[cfg(feature = "tracing")]
pub(crate) fn internal(source: &(dyn Error + 'static)) {
    tracing::error!(error = source, "request failed");
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn internal(_source: &(dyn Error + 'static)) {}

#[cfg(feature = "tracing")]
pub(crate) fn conflict(source: &(dyn Error + 'static)) {
    tracing::debug!(error = source, "request conflicted");
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn conflict(_source: &(dyn Error + 'static)) {}
