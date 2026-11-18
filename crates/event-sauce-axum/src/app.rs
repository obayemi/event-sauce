//! [`App`]: an application wired on `PostgreSQL` in a few lines, built by
//! [`EventSauceApp`].

use std::sync::Arc;

use axum::Router;
use axum_login::{
    tower_sessions::{SessionManagerLayer, SessionStore},
    AuthManagerLayerBuilder, AuthnBackend,
};
use event_sauce_auth::{LoginStore, Logins, PendingLogins, UserAggregate};

use crate::{CorrelationLayer, EventSauceBackend, Store};

mod builder;

pub use builder::{AppBuilder, BuildError, EventSauceApp, UserAppBuilder};

/// A built application: the event store, the `axum-login` backend and the
/// pieces of a login, wired together.
///
/// `F` is the [`FirstFactor`](event_sauce_auth::FirstFactor) the application
/// logs users in with. Hold what handlers need in the application state,
/// [`store`](Self::store), [`logins`](Self::logins), [`pending`](Self::pending),
/// [`first_factor`](Self::first_factor) and the verifiers of the later steps
/// built from [`login_store`](Self::login_store), and apply
/// [`layer`](Self::layer) to the router.
pub struct App<U: UserAggregate, F, S> {
    store: Store<S>,
    backend: EventSauceBackend<U, S>,
    logins: Logins<U>,
    pending: PendingLogins<U>,
    first_factor: Arc<F>,
    login_store: Arc<dyn LoginStore>,
    correlation: CorrelationLayer,
}

impl<U: UserAggregate, F, S> std::fmt::Debug for App<U, F, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("correlation", &self.correlation)
            .finish_non_exhaustive()
    }
}

impl<U: UserAggregate, F, S> App<U, F, S> {
    /// The event store the application runs on.
    #[must_use]
    pub fn store(&self) -> Store<S> {
        self.store.clone()
    }

    /// The `axum-login` backend of the application, which turns a finished
    /// login into a session and reloads the user of a session.
    #[must_use]
    pub fn backend(&self) -> &EventSauceBackend<U, S> {
        &self.backend
    }

    /// Where logins start and end, over the application's store and user
    /// cache.
    #[must_use]
    pub fn logins(&self) -> Logins<U> {
        self.logins.clone()
    }

    /// The logins that wait for their next request, saved in the
    /// [`login_store`](Self::login_store).
    #[must_use]
    pub fn pending(&self) -> PendingLogins<U> {
        self.pending.clone()
    }

    /// The first factor, to pass to [`Logins::begin`] with the client's
    /// attempt.
    #[must_use]
    pub fn first_factor(&self) -> Arc<F> {
        Arc::clone(&self.first_factor)
    }

    /// The store that holds the logins in progress, which the verifiers of
    /// the later steps build on.
    #[must_use]
    pub fn login_store(&self) -> Arc<dyn LoginStore> {
        Arc::clone(&self.login_store)
    }
}

impl<U, F, S> App<U, F, S>
where
    U: UserAggregate,
    EventSauceBackend<U, S>: AuthnBackend + 'static,
{
    /// Applies to `router`, outermost first:
    ///
    /// 1. the [`CorrelationLayer`] on the configured header;
    /// 2. the `sessions` layer;
    /// 3. the `axum-login` authentication layer on the backend.
    ///
    /// The session store stays the application's choice.
    pub fn layer<St, Sess>(
        &self,
        router: Router<St>,
        sessions: SessionManagerLayer<Sess>,
    ) -> Router<St>
    where
        St: Clone + Send + Sync + 'static,
        Sess: SessionStore + Clone,
    {
        router
            .layer(AuthManagerLayerBuilder::new(self.backend.clone(), sessions).build())
            .layer(self.correlation.clone())
    }
}

#[cfg(test)]
mod tests;
