use std::time::Duration;

use axum::http::header::RETRY_AFTER;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[cfg(test)]
#[path = "tests/throttled_tests.rs"]
mod tests;

/// A request that must wait before it may try again.
///
/// It answers `429 Too Many Requests` with a `Retry-After` of the wait in
/// whole seconds, rounded up, and no body: an application that renders its own
/// page wraps it and sets the header from
/// [`retry_after_secs`](Self::retry_after_secs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("too many attempts, retry in {} seconds", self.retry_after_secs())]
pub struct Throttled(Duration);

impl Throttled {
    pub(crate) fn new(wait: Duration) -> Self {
        Self(wait)
    }

    /// The time left before the request may try again.
    #[must_use]
    pub fn wait(&self) -> Duration {
        self.0
    }

    /// The wait in whole seconds, rounded up: the `Retry-After` value.
    #[must_use]
    pub fn retry_after_secs(&self) -> u64 {
        self.0.as_secs() + u64::from(self.0.subsec_nanos() > 0)
    }
}

impl IntoResponse for Throttled {
    fn into_response(self) -> Response {
        (
            StatusCode::TOO_MANY_REQUESTS,
            [(RETRY_AFTER, self.retry_after_secs())],
        )
            .into_response()
    }
}
