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
//! - [`StoreError`] answers a failed store operation with the status of its
//!   kind and no detail, and [`SplitDomain`] hands the domain error of a
//!   modify, an execute or a unit of work back to the application.
//! - With the `login` feature, `AuthSession` opens an `axum-login` session
//!   from a finished login and from nothing else: the backend underneath is
//!   crate-private, so an application can neither name it nor obtain one.
//!   `AuthLayer` installs it, `AuthSession::finish` ends a login and
//!   `PendingLogin::hold` parks one that waits for its next step, both
//!   answering a `LoginStep`, and `CurrentUser`, `MaybeUser` and `Registered`
//!   hand the logged-in user to handlers. `LoginError` answers every refusal of a
//!   login with the same `401` and every outage with a `500`.
//! - With the `app` feature, `EventSauceApp::postgres` builds the whole
//!   stack, stores, cache, first factor and layers, on one pool.

#![deny(missing_docs)]
#![deny(unnameable_types)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

#[cfg(feature = "app")]
mod app;
mod correlation;
mod diagnostics;
#[cfg(feature = "login")]
mod login;
mod repo;
mod request_id;
mod store;
mod store_error;
mod unit_of_work;

#[cfg(feature = "app")]
pub use app::{App, AppBuilder, BuildError, EventSauceApp, UserAppBuilder};
pub use correlation::{Correlation, CorrelationLayer, ResponseFuture, DEFAULT_HEADER};
pub use http::HeaderName;
#[cfg(feature = "login")]
pub use login::{
    AuthLayer, AuthRejection, AuthSession, CurrentUser, LoginError, LoginStep, MaybeUser,
    PendingLogin, Registered, SessionError,
};
pub use repo::Repo;
pub use request_id::RequestId;
pub use store::Store;
pub use store_error::{DeletedStatus, Split, SplitDomain, StoreError, StoreFailure};
pub use unit_of_work::UnitOfWork;
