//! [`UnitOfWork`]: the typed unit-of-work store handlers extract.

use std::convert::Infallible;
use std::future::{ready, Future};
use std::ops::Deref;
use std::sync::Arc;

use axum::extract::{FromRef, FromRequestParts};
use event_sauce_core::{TransactionalStore, UnitOfWorkStore};
use http::request::Parts;

/// The shared [`UnitOfWorkStore`] over transactional store `T`, extracted by
/// a handler.
///
/// The application state yields an `Arc<UnitOfWorkStore<T>>` through
/// `FromRef`. The per-aggregate registration is already typed and checked
/// by the store's builder, so extraction never fails.
///
/// ```
/// use std::sync::Arc;
///
/// use axum::extract::FromRef;
/// use event_sauce_axum::UnitOfWork;
/// use event_sauce_core::UnitOfWorkStore;
/// use event_sauce_memory::InMemoryTransactionalStore;
///
/// #[derive(Clone, FromRef)]
/// struct AppState {
///     uow: Arc<UnitOfWorkStore<InMemoryTransactionalStore>>,
/// }
///
/// async fn handler(UnitOfWork(uow): UnitOfWork<InMemoryTransactionalStore>) {
///     let _ = uow;
/// }
/// ```
#[derive(Debug)]
pub struct UnitOfWork<T: TransactionalStore>(pub Arc<UnitOfWorkStore<T>>);

impl<T: TransactionalStore> Clone for UnitOfWork<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: TransactionalStore> Deref for UnitOfWork<T> {
    type Target = UnitOfWorkStore<T>;

    fn deref(&self) -> &UnitOfWorkStore<T> {
        &self.0
    }
}

impl<St, T> FromRequestParts<St> for UnitOfWork<T>
where
    St: Send + Sync,
    Arc<UnitOfWorkStore<T>>: FromRef<St>,
    T: TransactionalStore + 'static,
{
    type Rejection = Infallible;

    fn from_request_parts(
        _parts: &mut Parts,
        state: &St,
    ) -> impl Future<Output = Result<Self, Infallible>> {
        ready(Ok(Self(Arc::from_ref(state))))
    }
}
