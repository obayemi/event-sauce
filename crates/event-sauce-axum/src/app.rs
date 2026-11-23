//! [`App`]: an application wired on `PostgreSQL` in a few lines, built by
//! [`EventSauceApp`].

use std::sync::Arc;

use axum::Router;
use event_sauce_auth::{LoginStore, Logins, PendingLogins, UserAggregate};
use tower_sessions::{service::CookieController, SessionManagerLayer, SessionStore};

use crate::{AuthLayer, CorrelationLayer, Store};

mod builder;
mod cookie;

pub use builder::{AppBuilder, BuildError, EventSauceApp, UserAppBuilder};
pub use cookie::{EncryptCookie, KeepCookie, SessionCookie, SignCookie};

/// A built application: the event store, the authentication layer and the
/// pieces of a login, wired together.
///
/// `F` is the [`FirstFactor`](event_sauce_auth::FirstFactor) the application
/// logs users in with. Hold what handlers need in the application state,
/// [`store`](Self::store), [`logins`](Self::logins), [`pending`](Self::pending),
/// [`first_factor`](Self::first_factor) and the verifiers of the later steps
/// built from [`login_store`](Self::login_store), and apply
/// [`layer`](Self::layer) to the router.
///
/// `K` is how [`layer`](Self::layer) protects the session cookie, chosen on
/// the builder: [`KeepCookie`] by default, [`SignCookie`] or
/// [`EncryptCookie`] with a key.
pub struct App<U: UserAggregate, F, S, K = KeepCookie> {
    store: Store<S>,
    auth: AuthLayer<U>,
    logins: Logins<U>,
    first_factor: Arc<F>,
    login_store: Arc<dyn LoginStore>,
    correlation: CorrelationLayer,
    cookie: K,
}

impl<U: UserAggregate, F, S, K> std::fmt::Debug for App<U, F, S, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("correlation", &self.correlation)
            .finish_non_exhaustive()
    }
}

impl<U: UserAggregate, F, S, K> App<U, F, S, K> {
    /// The event store the application runs on.
    #[must_use]
    pub fn store(&self) -> Store<S> {
        self.store.clone()
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
        self.logins.pending(self.login_store())
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

    /// Applies to `router`, outermost first:
    ///
    /// 1. the [`CorrelationLayer`] on the configured header;
    /// 2. the `sessions` layer, its cookie protected as `K` says;
    /// 3. the `axum-login` authentication layer, through the application's
    ///    [`AuthLayer`].
    ///
    /// The session store stays the application's choice. With the default
    /// [`KeepCookie`], so does the session cookie, as with
    /// [`AuthLayer::layer`]. An app built with a cookie key takes a plaintext
    /// session layer and protects its cookie with that key, so a session
    /// layer that already protects its cookie does not compile.
    pub fn layer<St, Sess, C>(
        &self,
        router: Router<St>,
        sessions: SessionManagerLayer<Sess, C>,
    ) -> Router<St>
    where
        St: Clone + Send + Sync + 'static,
        Sess: SessionStore + Clone,
        C: CookieController,
        K: SessionCookie<C>,
    {
        self.auth
            .layer(router, self.cookie.protect(sessions))
            .layer(self.correlation.clone())
    }
}

#[cfg(test)]
#[path = "tests/app_tests.rs"]
mod tests;
