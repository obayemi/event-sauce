//! [`Store`]: the shared handle on an event store that handlers extract.

use std::convert::Infallible;
use std::future::{ready, Future};
use std::ops::Deref;
use std::sync::Arc;

use axum::extract::{FromRef, FromRequestParts};
use http::request::Parts;

#[cfg(test)]
#[path = "tests/store_tests.rs"]
mod tests;

/// A cheaply cloneable handle on an event store, held in the application
/// state and extracted by handlers.
///
/// Cloning clones the [`Arc`], never the store, so `S` does not have to be
/// `Clone`. Derive `FromRef` on the application state to hand a `Store<S>`
/// to every handler that asks for one:
///
/// ```
/// use axum::extract::FromRef;
/// use event_sauce_axum::Store;
/// use event_sauce_memory::InMemoryEventStore;
///
/// #[derive(Clone, FromRef)]
/// struct AppState {
///     store: Store<InMemoryEventStore>,
/// }
///
/// async fn handler(store: Store<InMemoryEventStore>) -> usize {
///     std::sync::Arc::strong_count(store.arc())
/// }
/// # let _ = handler;
/// # let _ = AppState { store: Store::new(InMemoryEventStore::new()) };
/// ```
#[derive(Debug)]
pub struct Store<S>(pub Arc<S>);

impl<S> Store<S> {
    /// Wraps `store` in a new shared handle.
    #[must_use]
    pub fn new(store: S) -> Self {
        Self(Arc::new(store))
    }

    /// The shared store, for APIs that take an `Arc<S>`.
    #[must_use]
    pub fn arc(&self) -> &Arc<S> {
        &self.0
    }
}

impl<S> Clone for Store<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<S> From<Arc<S>> for Store<S> {
    fn from(store: Arc<S>) -> Self {
        Self(store)
    }
}

impl<S> Deref for Store<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.0
    }
}

impl<St, S> FromRequestParts<St> for Store<S>
where
    St: Send + Sync,
    S: Send + Sync,
    Self: FromRef<St>,
{
    type Rejection = Infallible;

    fn from_request_parts(
        _parts: &mut Parts,
        state: &St,
    ) -> impl Future<Output = Result<Self, Infallible>> {
        ready(Ok(Self::from_ref(state)))
    }
}
