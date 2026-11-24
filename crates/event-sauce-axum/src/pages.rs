//! [`Pages`]: what an application answers the steps of a flow with.

use axum::response::IntoResponse;

use crate::throttle::Throttled;

/// What an application answers the steps of a flow with.
///
/// A flow engine runs the steps and asks the application for every page and
/// redirect it ends in, through a trait that extends this one, so the
/// application keeps its templates, its translations and its routes. A value
/// is built per request by the handler, and may hold whatever the pages need:
/// the application state, the current user, a locale.
///
/// The only error a step escapes with is a throttled request, which the
/// application renders its own way, `429` with a `Retry-After`
/// ([`Throttled`] answers that on its own).
///
/// ```
/// use axum::response::Response;
/// use event_sauce_axum::{Pages, Throttled};
///
/// struct Html;
///
/// impl Pages for Html {
///     type Response = Response;
///     type Error = Throttled;
/// }
/// ```
pub trait Pages {
    /// Every page and redirect of the flow.
    type Response: IntoResponse;
    /// The error a step escapes with, of which a throttled request is the
    /// only one.
    type Error: From<Throttled>;
}
