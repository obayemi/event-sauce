//! What a failed request logs, when the `tracing` feature is on.

use std::error::Error;

use axum::response::{IntoResponse, Response};
use http::StatusCode;

#[cfg(feature = "tracing")]
pub(crate) fn internal(source: &(dyn Error + 'static)) {
    tracing::error!(error = source, "request failed");
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn internal(_source: &(dyn Error + 'static)) {}

pub(crate) fn internal_error(source: &(dyn Error + 'static)) -> Response {
    internal(source);
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

#[cfg(feature = "tracing")]
pub(crate) fn conflict(source: &(dyn Error + 'static)) {
    tracing::debug!(error = source, "request conflicted");
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn conflict(_source: &(dyn Error + 'static)) {}

#[cfg(all(feature = "tracing", feature = "oauth"))]
pub(crate) fn refused(reason: &dyn std::fmt::Display) {
    tracing::warn!(%reason, "sign-in refused");
}

#[cfg(all(not(feature = "tracing"), feature = "oauth"))]
pub(crate) fn refused(_reason: &dyn std::fmt::Display) {}

#[cfg(all(feature = "tracing", feature = "oauth"))]
pub(crate) fn declined(error: Option<&str>) {
    tracing::info!(error, "the provider answered without a code");
}

#[cfg(all(not(feature = "tracing"), feature = "oauth"))]
pub(crate) fn declined(_error: Option<&str>) {}
