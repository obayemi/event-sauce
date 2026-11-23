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
//! - [`Found`] loads the aggregate a path names once per request, read only,
//!   and with the `login` feature `Authorized` also checks that the current
//!   user may reach it under an `Access` policy.
//! - With the `login` feature, `AuthSession` opens an `axum-login` session
//!   from a finished login and from nothing else: the backend underneath is
//!   crate-private, so an application can neither name it nor obtain one.
//!   `AuthLayer` installs it, and `AuthSession::finish` ends a login while
//!   `PendingLogin::hold` parks one that waits for its next step, both
//!   answering a `LoginStep`.
//! - With the `login` feature, `CurrentUser`, `MaybeUser` and `Registered`
//!   hand the logged-in user to handlers, and `Acting` builds the command
//!   context of that user from the application's `FromUser`.
//! - With the `login` feature, `LoginError` answers every refusal of a login
//!   with the same `401` and every outage with a `500`.
//! - With the `login` feature, `LoginRedirect` sends a browser that is not
//!   logged in to the login page with the page it asked for, on the routes
//!   it is applied to. `ReturnTo`, the same-origin target a form carries back
//!   after the login, needs no feature.
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
mod found;
#[cfg(feature = "login")]
mod login;
mod repo;
mod request_id;
mod return_to;
mod store;
mod store_error;
#[cfg(test)]
mod test_ticket;
mod unit_of_work;

#[cfg(feature = "app")]
pub use app::{App, AppBuilder, BuildError, EventSauceApp, UserAppBuilder};
pub use correlation::{Correlation, CorrelationLayer, ResponseFuture, DEFAULT_HEADER};
pub use found::{Found, FoundRejection, PathAggregate};
pub use http::HeaderName;
#[cfg(feature = "login")]
pub use login::{
    Access, AccessRejection, Acting, AuthLayer, AuthRejection, AuthSession, Authorized,
    CurrentUser, DeniedStatus, FromUser, LoginError, LoginRedirect, LoginStep, MaybeUser,
    PendingLogin, Registered, SessionError,
};
pub use repo::Repo;
pub use request_id::RequestId;
pub use return_to::ReturnTo;
pub use store::Store;
pub use store_error::{DeletedStatus, Split, SplitDomain, StoreError, StoreFailure};
pub use unit_of_work::UnitOfWork;
