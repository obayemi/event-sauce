//! [`AuthLayer`]: the only way to install the sealed `axum-login` backend.

use std::fmt;

use axum::Router;
use axum_login::{
    tower_sessions::{service::CookieController, SessionManagerLayer, SessionStore},
    AuthManagerLayerBuilder,
};
use event_sauce_auth::{CachedUserLoader, UserAggregate};
use event_sauce_core::EventStore;
use serde::{de::DeserializeOwned, Serialize};

use crate::login::backend::Backend;

/// Installs login sessions on a router, for the users of one aggregate.
///
/// It holds the `axum-login` backend, which nothing else exposes: the backend
/// type is crate-private, so the only way to open a session is
/// [`AuthSession::login`](crate::AuthSession::login) with a finished login.
/// [`layer`](Self::layer) returns a [`Router`], so the backend's type
/// appears in no signature an application can see.
///
/// The cache that spares a request the full load of its user is set on the
/// [`CachedUserLoader`]. Applications built with
/// [`EventSauceApp`](crate::EventSauceApp) never build one: the app holds it.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
///
/// use axum::Router;
/// use axum_login::tower_sessions::{MemoryStore, SessionManagerLayer};
/// use event_sauce_auth::{CachedUserLoader, MokaUserCache, UserAggregate};
/// use event_sauce_axum::AuthLayer;
/// use event_sauce_core::EventStore;
/// use serde::{de::DeserializeOwned, Serialize};
///
/// fn router<U, S>(store: Arc<S>) -> Router
/// where
///     S: EventStore + 'static,
///     U: UserAggregate,
///     U::DeletedState: Serialize + DeserializeOwned,
/// {
///     let loader = CachedUserLoader::<U, S>::new(store).with_cache(MokaUserCache::new(10_000));
///     let sessions = SessionManagerLayer::new(MemoryStore::default());
///     AuthLayer::new(loader).layer(Router::new(), sessions)
/// }
/// ```
pub struct AuthLayer<U: UserAggregate> {
    backend: Backend<U>,
}

impl<U: UserAggregate> AuthLayer<U> {
    /// A layer that resolves the user of every request through `loader`: a
    /// version check, then a cached user when the loader's cache holds
    /// exactly that version, otherwise a full load.
    #[must_use]
    pub fn new<S>(loader: CachedUserLoader<U, S>) -> Self
    where
        S: EventStore + 'static,
        U::DeletedState: Serialize + DeserializeOwned,
    {
        Self {
            backend: Backend::new(loader),
        }
    }

    /// Applies the `sessions` layer and the authentication layer to
    /// `router`, with the sessions outermost.
    ///
    /// The session store and the session cookie stay the application's
    /// choice: the cookie is plaintext from `SessionManagerLayer::new`,
    /// signed after `with_signed(key)` and encrypted after
    /// `with_private(key)`.
    pub fn layer<St, Sess, C>(
        &self,
        router: Router<St>,
        sessions: SessionManagerLayer<Sess, C>,
    ) -> Router<St>
    where
        St: Clone + Send + Sync + 'static,
        Sess: SessionStore + Clone,
        C: CookieController + Sync,
    {
        router.layer(AuthManagerLayerBuilder::new(self.backend.clone(), sessions).build())
    }
}

impl<U: UserAggregate> Clone for AuthLayer<U> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
        }
    }
}

impl<U: UserAggregate> fmt::Debug for AuthLayer<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthLayer").finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "tests/auth_layer_tests.rs"]
mod tests;
