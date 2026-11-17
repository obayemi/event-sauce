//! [`EventSauceApp`]: an application wired on `PostgreSQL` in a few lines.

use std::sync::Arc;

use axum::Router;
use axum_login::{
    tower_sessions::{SessionManagerLayer, SessionStore},
    AuthManagerLayerBuilder, AuthnBackend,
};
use event_sauce_auth::{
    ClaimAuthenticator, HasSecondFactor, InvalidSchemaName, LoginMethod, LoginStore,
    LoginStoreError, MokaUserCache, NoSecondFactor, PostgresLoginStore, SecondFactor, TwoFactor,
    UserAggregate, UserCache,
};
use event_sauce_core::EventStore;
use event_sauce_postgres::PostgresEventStore;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;

use crate::{CorrelationLayer, EventSauceBackend, Store};

const DEFAULT_CACHE_CAPACITY: u64 = 10_000;

/// Why [`UserAppBuilder::build`] failed.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// The schema given to [`UserAppBuilder::login_store_schema`] is not a
    /// valid schema name.
    #[error(transparent)]
    LoginSchema(#[from] InvalidSchemaName),
    /// The event store could not be built or migrated.
    #[error("event store: {0}")]
    EventStore(#[from] event_sauce_core::Error),
    /// The login store could not be migrated.
    #[error("login store: {0}")]
    LoginStore(#[from] LoginStoreError),
}

/// The entry point of the application builder.
///
/// [`EventSauceApp::postgres`] wires the common case with `PostgreSQL` behind
/// everything: the event store, the login store in a schema of its own, an
/// in-process user cache, the authenticator and the correlation layer.
/// [`UserAppBuilder::build`] runs every migration, and
/// [`App::layer`] applies the result to a router.
///
/// # Examples
///
/// ```no_run
/// use axum::Router;
/// use axum_login::tower_sessions::{MemoryStore, SessionManagerLayer};
/// use event_sauce_auth::{Argon2Hasher, Email, Password, PasswordLogin, UserAggregate};
/// use event_sauce_axum::EventSauceApp;
/// use serde::{de::DeserializeOwned, Serialize};
///
/// async fn router<U>(pool: sqlx::PgPool) -> Result<Router, Box<dyn std::error::Error>>
/// where
///     U: UserAggregate<Credentials = Password<Email>> + Serialize + DeserializeOwned + 'static,
///     U::DeletedState: Serialize + DeserializeOwned,
///     U::Event: Serialize + DeserializeOwned,
/// {
///     let app = EventSauceApp::postgres(pool)
///         .user::<U, _>(PasswordLogin::new(Argon2Hasher::default()))
///         .build()
///         .await?;
///     Ok(app.layer(Router::new(), SessionManagerLayer::new(MemoryStore::default())))
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct EventSauceApp;

impl EventSauceApp {
    /// Starts an application whose stores live in the database of `pool`.
    #[must_use]
    pub fn postgres(pool: PgPool) -> AppBuilder {
        AppBuilder { pool }
    }
}

/// An application builder that does not know its user aggregate yet.
#[derive(Debug, Clone)]
pub struct AppBuilder {
    pool: PgPool,
}

impl AppBuilder {
    /// Authenticates the users `U` with the login `method`.
    ///
    /// Call it as `user::<U, _>(method)`.
    #[must_use]
    pub fn user<U, M>(self, method: M) -> UserAppBuilder<U, M>
    where
        U: UserAggregate,
        M: LoginMethod<U::Credentials>,
    {
        UserAppBuilder {
            pool: self.pool,
            event_store: None,
            login_schema: None,
            method,
            second_factor: NoSecondFactor,
            cache: CacheChoice::Moka(DEFAULT_CACHE_CAPACITY),
            correlation: CorrelationLayer::new(),
        }
    }
}

enum CacheChoice<U: UserAggregate> {
    Moka(u64),
    Shared(Arc<dyn UserCache<U>>),
    Disabled,
}

/// What an application does between the first factor and the session.
///
/// It is [`NoSecondFactor`] until [`UserAppBuilder::second_factor`] says
/// otherwise.
pub trait SecondFactorStage<U: UserAggregate> {
    /// The stage the authenticator ends up with.
    type Stage;

    /// Gives `authenticator` its stage, with pending logins kept in `logins`.
    fn apply<M, S>(
        self,
        authenticator: ClaimAuthenticator<U, M, S>,
        logins: Arc<dyn LoginStore>,
    ) -> ClaimAuthenticator<U, M, S, Self::Stage>;
}

impl<U: UserAggregate> SecondFactorStage<U> for NoSecondFactor {
    type Stage = NoSecondFactor;

    fn apply<M, S>(
        self,
        authenticator: ClaimAuthenticator<U, M, S>,
        _logins: Arc<dyn LoginStore>,
    ) -> ClaimAuthenticator<U, M, S> {
        authenticator
    }
}

/// A second factor the application demands, held by the login store.
#[derive(Debug)]
pub struct WithSecondFactor<F>(F);

impl<U, F> SecondFactorStage<U> for WithSecondFactor<F>
where
    U: UserAggregate,
    U::Credentials: HasSecondFactor,
    F: SecondFactor<U::Credentials>,
{
    type Stage = TwoFactor<F>;

    fn apply<M, S>(
        self,
        authenticator: ClaimAuthenticator<U, M, S>,
        logins: Arc<dyn LoginStore>,
    ) -> ClaimAuthenticator<U, M, S, TwoFactor<F>> {
        authenticator.with_second_factor(TwoFactor::new(self.0, logins))
    }
}

/// The builder of an application with its user aggregate chosen.
///
/// The defaults are the ones of [`EventSauceApp`]. Every override is
/// optional, and the last call on a setting wins.
pub struct UserAppBuilder<U: UserAggregate, M, T = NoSecondFactor> {
    pool: PgPool,
    event_store: Option<Arc<PostgresEventStore>>,
    login_schema: Option<String>,
    method: M,
    second_factor: T,
    cache: CacheChoice<U>,
    correlation: CorrelationLayer,
}

impl<U: UserAggregate, M, T> UserAppBuilder<U, M, T> {
    /// Uses `store` as the event store instead of a default one on the pool.
    ///
    /// It is migrated by [`build`](Self::build) like the default one.
    #[must_use]
    pub fn event_store(mut self, store: Arc<PostgresEventStore>) -> Self {
        self.event_store = Some(store);
        self
    }

    /// Keeps the login store in `schema` instead of `event_sauce_login`.
    ///
    /// An invalid name makes [`build`](Self::build) fail with
    /// [`BuildError::LoginSchema`].
    #[must_use]
    pub fn login_store_schema(mut self, schema: impl Into<String>) -> Self {
        self.login_schema = Some(schema.into());
        self
    }

    /// Resolves the user of every request through `cache` instead of the
    /// default in-process one.
    #[must_use]
    pub fn user_cache(mut self, cache: impl UserCache<U> + 'static) -> Self {
        self.cache = CacheChoice::Shared(Arc::new(cache));
        self
    }

    /// Keeps up to `capacity` users in the default in-process cache
    /// instead of 10 000.
    #[must_use]
    pub fn user_cache_capacity(mut self, capacity: u64) -> Self {
        self.cache = CacheChoice::Moka(capacity);
        self
    }

    /// Loads the user of every request from the event store, with no cache.
    #[must_use]
    pub fn no_user_cache(mut self) -> Self {
        self.cache = CacheChoice::Disabled;
        self
    }

    /// Reads and echoes the request id in the header `name` instead of
    /// `x-request-id`.
    ///
    /// # Panics
    ///
    /// Panics when `name` is not a valid lowercase HTTP header name.
    #[must_use]
    pub fn correlation_header(mut self, name: &'static str) -> Self {
        self.correlation = CorrelationLayer::header(name);
        self
    }
}

impl<U: UserAggregate, M> UserAppBuilder<U, M> {
    /// Demands `factor` from the users whose credentials require one.
    ///
    /// Pending logins are kept in the login store, so they survive across
    /// instances.
    #[must_use]
    pub fn second_factor<F>(self, factor: F) -> UserAppBuilder<U, M, WithSecondFactor<F>>
    where
        U::Credentials: HasSecondFactor,
        F: SecondFactor<U::Credentials>,
    {
        UserAppBuilder {
            pool: self.pool,
            event_store: self.event_store,
            login_schema: self.login_schema,
            method: self.method,
            second_factor: WithSecondFactor(factor),
            cache: self.cache,
            correlation: self.correlation,
        }
    }
}

type Authenticator<U, M, S, T> = ClaimAuthenticator<U, M, S, <T as SecondFactorStage<U>>::Stage>;

impl<U, M, T> UserAppBuilder<U, M, T>
where
    U: UserAggregate + Serialize + DeserializeOwned + Send + Sync + 'static,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
    M: LoginMethod<U::Credentials>,
    T: SecondFactorStage<U>,
{
    /// Builds the application over `store` and `logins`.
    fn assemble<S: EventStore + 'static>(
        self,
        store: Arc<S>,
        logins: Arc<dyn LoginStore>,
    ) -> App<U, Authenticator<U, M, S, T>, S> {
        let authenticator = self.second_factor.apply(
            ClaimAuthenticator::new(store.clone(), self.method),
            logins.clone(),
        );
        let backend = EventSauceBackend::new(authenticator, store.clone());
        let backend = match self.cache {
            CacheChoice::Moka(capacity) => backend.with_cache(MokaUserCache::new(capacity)),
            CacheChoice::Shared(cache) => backend.with_shared_cache(cache),
            CacheChoice::Disabled => backend,
        };
        App {
            store: Store::from(store),
            backend,
            logins,
            correlation: self.correlation,
        }
    }

    /// Migrates the event store and the login store, then builds the
    /// application.
    ///
    /// Every migration is idempotent, so every instance may call this on
    /// start.
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] when the login schema is invalid or a store
    /// cannot be built or migrated.
    pub async fn build(
        self,
    ) -> Result<App<U, Authenticator<U, M, PostgresEventStore, T>, PostgresEventStore>, BuildError>
    {
        let logins = match &self.login_schema {
            Some(schema) => PostgresLoginStore::new(self.pool.clone()).with_schema(schema)?,
            None => PostgresLoginStore::new(self.pool.clone()),
        };
        let events = match &self.event_store {
            Some(store) => store.clone(),
            None => Arc::new(
                PostgresEventStore::builder()
                    .pool(self.pool.clone())
                    .build()?,
            ),
        };
        events.migrate().await?;
        logins.migrate().await?;
        Ok(self.assemble(events, Arc::new(logins)))
    }
}

/// A built application: the event store, the `axum-login` backend and the
/// login store, wired together.
///
/// Hold [`store`](Self::store) and [`backend`](Self::backend) in the
/// application state, and apply [`layer`](Self::layer) to the router.
pub struct App<U: UserAggregate, A, S> {
    store: Store<S>,
    backend: EventSauceBackend<U, A, S>,
    logins: Arc<dyn LoginStore>,
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

    /// The store of logins in progress.
    #[must_use]
    pub fn login_store(&self) -> Arc<dyn LoginStore> {
        self.logins.clone()
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
