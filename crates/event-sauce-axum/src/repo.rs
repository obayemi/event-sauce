//! [`Repo`]: the repository of one aggregate type, built per request.

use std::convert::Infallible;
use std::future::{ready, Future};
use std::ops::Deref;

use axum::extract::{FromRef, FromRequestParts};
use event_sauce_core::{Aggregate, EventSourcedRepository, EventStore};
use http::request::Parts;

use crate::Store;

/// The event-sourced repository of aggregate `A` over store `S`, extracted
/// by a handler.
///
/// The application state only has to yield a [`Store<S>`] through `FromRef`.
/// The repository is built on the fly from that store, so there is nothing
/// to register and extraction never fails.
///
/// ```
/// use axum::extract::FromRef;
/// use event_sauce_axum::{Repo, Store};
/// use event_sauce_core::{Aggregate, Repository};
/// use event_sauce_memory::InMemoryEventStore;
///
/// #[derive(Clone, FromRef)]
/// struct AppState {
///     store: Store<InMemoryEventStore>,
/// }
///
/// async fn handler<A>(Repo(repository): Repo<A, InMemoryEventStore>)
/// where
///     A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
///     A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
///     A::Event: serde::Serialize + serde::de::DeserializeOwned,
/// {
///     let _ = repository;
/// }
/// ```
#[derive(Debug)]
pub struct Repo<A, S>(pub EventSourcedRepository<S, A>);

impl<A, S> Deref for Repo<A, S> {
    type Target = EventSourcedRepository<S, A>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<St, A, S> FromRequestParts<St> for Repo<A, S>
where
    St: Send + Sync,
    Store<S>: FromRef<St>,
    S: EventStore + 'static,
    A: Aggregate + serde::Serialize + serde::de::DeserializeOwned,
    A::DeletedState: serde::Serialize + serde::de::DeserializeOwned,
    A::Event: serde::Serialize + serde::de::DeserializeOwned,
{
    type Rejection = Infallible;

    fn from_request_parts(
        _parts: &mut Parts,
        state: &St,
    ) -> impl Future<Output = Result<Self, Infallible>> {
        ready(Ok(Self(Store::from_ref(state).arc().repository())))
    }
}
