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
//! - With the `throttle` feature, `LoginThrottles` slows a sign-in down with
//!   growing delays and never locks it out, `Throttled` answers a refused
//!   request with `429` and a `Retry-After`, and `ClientAddr` reads the
//!   address of the client, from the socket or from a proxy header.
//! - With the `totp` feature, `TotpChallenge` is the code step of a login whose
//!   route asked for a TOTP code, `TotpProof` checks the code of a signed-in
//!   user before it changes its own second factor, and the application renders
//!   each step through `TotpLoginPages`, a `Pages`. An HTML login flow keeps
//!   its in-between state in the session record, never in a cookie of its own.
//! - With the `reset` feature, `PasswordReset` runs a password reset by emailed
//!   link: it throttles and answers the request at once whatever the login,
//!   checks a link without using it, and redeems it, signing the user in along
//!   the route it was built with. The application keeps the reset in its own
//!   aggregate behind `ResetAccounts`, sends the mail with `ResetMailer`, and
//!   renders each step through `ResetPages`; a link is built from a configured
//!   `PublicOrigin`, never from `Host`.
//! - With the `oauth` feature, `OAuthSignIn` runs an OAuth sign-in with PKCE: it
//!   starts the flow, checks the state and the verifier of its callback, and
//!   signs the account the provider names in, through the TOTP code step its
//!   route asks for. The same callback links the account to the signed-in user
//!   (`start_link`, with the `LinkProof` of a code the application checked,
//!   valid for ten minutes and for that flow only) or claims it for an
//!   anonymous one (`mark_claim`). The application keeps every rule of whose
//!   account it is behind `OAuthAccounts`, and renders each step through
//!   `OAuthPages`.
//! - With the `app` feature, `EventSauceApp::postgres` builds the whole
//!   stack, stores, cache, first factor and layers, on one pool, and can sign
//!   or encrypt the session cookie with a key.
//! - `AuthLayer::layer` and `App::layer` take the application's session
//!   layer whatever protects its cookie: plaintext, signed or encrypted.

#![deny(missing_docs)]
#![deny(unnameable_types)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

#[cfg(feature = "app")]
mod app;
#[cfg(feature = "throttle")]
mod client_ip;
mod correlation;
mod diagnostics;
mod found;
#[cfg(feature = "login")]
mod login;
#[cfg(feature = "oauth")]
mod oauth;
#[cfg(feature = "throttle")]
mod pages;
mod repo;
mod request_id;
#[cfg(feature = "reset")]
mod reset;
mod return_to;
mod store;
mod store_error;
#[cfg(test)]
mod test_ticket;
#[cfg(feature = "throttle")]
mod throttle;
#[cfg(feature = "totp")]
mod totp;
mod unit_of_work;

#[cfg(feature = "app")]
pub use app::{
    App, AppBuilder, BuildError, EncryptCookie, EventSauceApp, KeepCookie, SessionCookie,
    SignCookie, UserAppBuilder,
};
#[cfg(feature = "throttle")]
pub use client_ip::{ClientAddr, ClientIpSource};
pub use correlation::{Correlation, CorrelationLayer, ResponseFuture, DEFAULT_HEADER};
pub use found::{Found, FoundRejection, PathAggregate};
pub use http::HeaderName;
#[cfg(feature = "login")]
pub use login::{
    Access, AccessRejection, Acting, AuthLayer, AuthRejection, AuthSession, Authorized,
    CurrentUser, DeniedStatus, FromUser, LoginError, LoginRedirect, LoginStep, MaybeUser,
    PendingLogin, Registered, SessionError,
};
#[cfg(feature = "oauth")]
pub use oauth::{
    CallbackError, LinkOutcome, LinkProof, OAuthAccounts, OAuthPages, OAuthSignIn, Returned,
};
#[cfg(feature = "throttle")]
pub use pages::Pages;
pub use repo::Repo;
pub use request_id::RequestId;
#[cfg(feature = "reset")]
pub use reset::{
    Finished, InvalidOrigin, NewPasswordError, PasswordReset, PendingReset, PublicOrigin,
    ResetAccounts, ResetMailer, ResetPages,
};
pub use return_to::ReturnTo;
pub use store::Store;
pub use store_error::{DeletedStatus, Split, SplitDomain, StoreError, StoreFailure};
#[cfg(feature = "throttle")]
pub use throttle::{Attempt, Backoff, Charge, LoginThrottles, Throttle, Throttled};
#[cfg(feature = "totp")]
pub use totp::{
    code_page, CodeRefusal, Enrolment, OpenError, Opened, Proof, TotpChallenge, TotpLoginPages,
    TotpProof,
};
pub use unit_of_work::UnitOfWork;
