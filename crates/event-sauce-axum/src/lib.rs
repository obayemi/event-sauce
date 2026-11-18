//! # event-sauce-axum
//!
//! axum and tower integration for event-sauce.
//!
//! - [`CorrelationLayer`] runs every request inside an event context
//!   carrying its request id, so each event committed while serving the
//!   request is stored with that id as its correlation id.
//! - [`Store`], [`Repo`] and [`UnitOfWork`] are extractors over the
//!   application state: there is no registry, a repository is built per
//!   request from the shared store.
//! - With the `login` feature, `EventSauceBackend` plugs an
//!   `Authenticator` into `axum-login`.
//!   `SessionUser` ties a session to the identity's fingerprint,
//!   `PendingLogin` carries a login that waits for its second factor, and
//!   `CurrentUser`, `MaybeUser` and `Registered` hand the logged-in
//!   user to handlers.
//! - With the `app` feature, `EventSauceApp::postgres` builds the whole
//!   stack, stores, cache, authenticator and layers, on one pool.

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

#[cfg(feature = "app")]
mod app;
mod correlation;
#[cfg(feature = "login")]
mod login;
mod repo;
mod request_id;
mod store;
mod unit_of_work;

#[cfg(feature = "app")]
pub use app::{
    App, AppBuilder, BuildError, EventSauceApp, SecondFactorStage, UserAppBuilder, WithSecondFactor,
};
pub use correlation::{Correlation, CorrelationLayer, ResponseFuture, DEFAULT_HEADER};
pub use http::HeaderName;
#[cfg(feature = "login")]
pub use login::{
    AuthRejection, BackendError, CurrentUser, EventSauceBackend, MaybeUser, PendingLogin,
    Registered, SessionUser, UserBackend,
};
pub use repo::Repo;
pub use request_id::RequestId;
pub use store::Store;
pub use unit_of_work::UnitOfWork;
