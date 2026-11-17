//! [`App`]: an application wired on `PostgreSQL` in a few lines, built by
//! [`EventSauceApp`].

use axum::Router;
use axum_login::{
    tower_sessions::{SessionManagerLayer, SessionStore},
    AuthManagerLayerBuilder, AuthnBackend,
};
use event_sauce_auth::UserAggregate;

use crate::{CorrelationLayer, EventSauceBackend, Store};

mod builder;
mod second_factor;

pub use builder::{AppBuilder, BuildError, EventSauceApp, UserAppBuilder};
pub use second_factor::{SecondFactorStage, WithSecondFactor};

/// A built application: the event store and the `axum-login` backend, wired
/// together.
///
/// Hold [`store`](Self::store) and [`backend`](Self::backend) in the
/// application state, and apply [`layer`](Self::layer) to the router.
pub struct App<U: UserAggregate, A, S> {
    store: Store<S>,
    backend: EventSauceBackend<U, A, S>,
    correlation: CorrelationLayer,
}

impl<U: UserAggregate, A, S> std::fmt::Debug for App<U, A, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("correlation", &self.correlation)
            .finish_non_exhaustive()
    }
}

impl<U: UserAggregate, A, S> App<U, A, S> {
    /// The event store the application runs on.
    #[must_use]
    pub fn store(&self) -> Store<S> {
        self.store.clone()
    }

    /// The `axum-login` backend of the application, which handlers use
    /// through `AuthSession` or call to start and complete logins.
    #[must_use]
    pub fn backend(&self) -> &EventSauceBackend<U, A, S> {
        &self.backend
    }
}

impl<U, A, S> App<U, A, S>
where
    U: UserAggregate,
    EventSauceBackend<U, A, S>: AuthnBackend + 'static,
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
