//! The typestate builder of [`App`].

use std::sync::Arc;

use event_sauce_auth::{
    AlwaysDone, CachedUserLoader, FirstFactor, InvalidSchemaName, LoginMethod, LoginStore,
    LoginStoreError, Logins, MokaUserCache, PostgresLoginStore, Route, UserAggregate, UserCache,
};
use event_sauce_core::EventStore;
use event_sauce_postgres::PostgresEventStore;
use http::HeaderName;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;

use super::App;
use crate::{AuthLayer, CorrelationLayer, Store};

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
/// in-process user cache, the first factor with its route, and the correlation
/// layer.
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
///     U: UserAggregate<Credentials = Password<Email>> + 'static,
///     U::DeletedState: Serialize + DeserializeOwned,
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
    {
        UserAppBuilder {
            pool: self.pool,
            event_store: None,
            login_schema: None,
            method,
            route: AlwaysDone,
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

impl<U: UserAggregate> CacheChoice<U> {
    fn into_cache(self) -> Option<Arc<dyn UserCache<U>>> {
        match self {
            Self::Moka(capacity) => Some(Arc::new(MokaUserCache::new(capacity))),
            Self::Shared(cache) => Some(cache),
            Self::Disabled => None,
        }
    }
}

/// The builder of an application with its user aggregate chosen.
///
/// The defaults are the ones of [`EventSauceApp`]. Every override is
/// optional, and the last call on a setting wins.
pub struct UserAppBuilder<U: UserAggregate, M, R = AlwaysDone> {
    pool: PgPool,
    event_store: Option<Arc<PostgresEventStore>>,
    login_schema: Option<String>,
    method: M,
    route: R,
    cache: CacheChoice<U>,
    correlation: CorrelationLayer,
}

impl<U: UserAggregate, M, R> UserAppBuilder<U, M, R> {
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
    #[must_use]
    pub fn correlation_header(mut self, name: HeaderName) -> Self {
        self.correlation = CorrelationLayer::header(name);
        self
    }

    /// Sends the logins that passed the first factor through `route`, which
    /// decides the state each one continues in.
    ///
    /// The default, [`AlwaysDone`], ends every login there and fits only
    /// credentials that cannot hold a second factor. For the others there is
    /// no usable default: [`build`](Self::build) does not compile until
    /// `route` sends their users to the next step, so a login cannot skip a
    /// factor the application forgot.
    #[must_use]
    pub fn route<R2: Route<U>>(self, route: R2) -> UserAppBuilder<U, M, R2> {
        UserAppBuilder {
            pool: self.pool,
            event_store: self.event_store,
            login_schema: self.login_schema,
            method: self.method,
            route,
            cache: self.cache,
            correlation: self.correlation,
        }
    }
}

impl<U, M, R> UserAppBuilder<U, M, R>
where
    U: UserAggregate,
    U::DeletedState: Serialize + DeserializeOwned,
    M: LoginMethod<U::Credentials>,
    R: Route<U>,
{
    /// Builds the application over `store` and `login_store`.
    pub(super) fn assemble<S: EventStore + 'static>(
        self,
        store: Arc<S>,
        login_store: Arc<dyn LoginStore>,
    ) -> App<U, FirstFactor<M, R>, S> {
        let mut loader = CachedUserLoader::new(store.clone());
        let mut logins = Logins::new(store.clone());
        if let Some(cache) = self.cache.into_cache() {
            loader = loader.with_shared_cache(cache.clone());
            logins = logins.with_shared_cache(cache);
        }
        App {
            store: Store::from(store),
            auth: AuthLayer::new(loader),
            logins,
            first_factor: Arc::new(FirstFactor::new(self.method, self.route)),
            login_store,
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
    pub async fn build(self) -> Result<App<U, FirstFactor<M, R>, PostgresEventStore>, BuildError> {
        let login_store = match &self.login_schema {
            Some(schema) => PostgresLoginStore::new(self.pool.clone()).with_schema(schema)?,
            None => PostgresLoginStore::new(self.pool.clone()),
        };
        let event_store = match &self.event_store {
            Some(store) => store.clone(),
            None => Arc::new(
                PostgresEventStore::builder()
                    .pool(self.pool.clone())
                    .build()?,
            ),
        };
        event_store.migrate().await?;
        login_store.migrate().await?;
        Ok(self.assemble(event_store, Arc::new(login_store)))
    }
}

#[cfg(test)]
#[path = "tests/builder_tests.rs"]
mod tests;
