//! The password reset flow of an HTML application.
//!
//! [`PasswordReset`] runs the HTTP side of a reset by emailed link and asks
//! the application for everything that is its own:
//!
//! - [`request`](PasswordReset::request) throttles the client, answers the
//!   same page whatever the login at once, and then, in a spawned task,
//!   finds the account, records a fresh [`ResetToken`] for it and hands the
//!   link to the [`ResetMailer`]. Whether an account exists never shows, in
//!   the answer or in its timing.
//! - [`page`](PasswordReset::page) checks a link without using it.
//! - [`redeem`](PasswordReset::redeem) hashes the new password, has the
//!   application consume the reset and set the password in one step, and
//!   signs the user in, unless its [`Route`](event_sauce_auth::Route) asks
//!   for a second factor: the user then signs in again.
//!
//! The reset itself, its cooldown and its validity, stays in the
//! application's aggregate behind [`ResetAccounts`], so a link issued
//! elsewhere (an admin) is redeemed by the same flow. A link is built from
//! the configured [`PublicOrigin`] and the application's path, never from the
//! `Host` of a request: anyone may ask for a reset, so a forged `Host` would
//! otherwise mail a genuine token behind the attacker's link.
//!
//! The application renders each step through [`ResetPages`], and its email
//! through the [`Draft`](ResetMailer::Draft) it prepares inside the request,
//! where its locale is known.

mod accounts;
mod origin;
mod pages;
mod redeem;
mod request;
mod signin;

use std::fmt;
use std::sync::Arc;

use event_sauce_auth::{
    Clock, FirstFactor, Logins, PasswordHasher, ResetToken, Route, SystemClock, UserAggregate,
};
use event_sauce_core::EntityId;

use crate::diagnostics;
use crate::throttle::LoginThrottles;

pub use accounts::{PendingReset, ResetAccounts, ResetMailer};
pub use origin::{InvalidOrigin, PublicOrigin};
pub use pages::{NewPasswordError, ResetPages};
pub use signin::Finished;

use signin::RedeemedLogin;

/// The password reset flow: a request for a link, the page of the link and
/// the redemption of the link.
///
/// It is cheap to clone when the hasher and the route are. Build it once at
/// startup and keep it in the application state: an application that has no
/// mailer or no public origin has no reset, and keeps an `Option` of it.
pub struct PasswordReset<U: UserAggregate, A, M, H, R> {
    accounts: Arc<A>,
    mailer: Arc<M>,
    hasher: H,
    origin: PublicOrigin,
    logins: Logins<U>,
    first: FirstFactor<RedeemedLogin, R>,
    throttles: Arc<LoginThrottles>,
    clock: Arc<dyn Clock>,
}

impl<U: UserAggregate, A, M, H: Clone, R: Clone> Clone for PasswordReset<U, A, M, H, R> {
    fn clone(&self) -> Self {
        Self {
            accounts: self.accounts.clone(),
            mailer: self.mailer.clone(),
            hasher: self.hasher.clone(),
            origin: self.origin.clone(),
            logins: self.logins.clone(),
            first: self.first.clone(),
            throttles: self.throttles.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<U: UserAggregate, A, M, H, R> fmt::Debug for PasswordReset<U, A, M, H, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PasswordReset")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

impl<U, A, M, H, R> PasswordReset<U, A, M, H, R>
where
    U: UserAggregate,
    A: ResetAccounts,
    M: ResetMailer,
    H: PasswordHasher,
    R: Route<U>,
    R::Next: Finished<U>,
{
    /// A reset flow over `accounts`, mailing links to `origin` with
    /// `mailer`, hashing new passwords with `hasher`, signing users in with
    /// `logins` along `route`, and counting the requests of a client in
    /// `throttles`.
    #[must_use]
    pub fn new(
        accounts: Arc<A>,
        mailer: Arc<M>,
        hasher: H,
        origin: PublicOrigin,
        logins: Logins<U>,
        route: R,
        throttles: Arc<LoginThrottles>,
    ) -> Self {
        Self {
            accounts,
            mailer,
            hasher,
            origin,
            logins,
            first: FirstFactor::new(RedeemedLogin, route),
            throttles,
            clock: Arc::new(SystemClock),
        }
    }

    /// Reads the time from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The form asking for a link.
    pub fn form<P: pages::ResetPages>(&self, pages: &P) -> P::Response {
        pages.request_form()
    }

    /// The new-password form when `token` matches a live reset of `user`,
    /// and the reset is not used.
    ///
    /// # Errors
    ///
    /// [`invalid_link`](ResetPages::invalid_link), for every way a link can
    /// be wrong: not a token, another token, used, expired, or the reset of
    /// no one.
    pub async fn page<P: ResetPages>(
        &self,
        pages: &P,
        user: EntityId,
        token: &str,
    ) -> Result<P::Response, P::Error> {
        if self.is_live(user, token).await {
            Ok(pages.new_password_form(None))
        } else {
            Err(pages.invalid_link())
        }
    }

    async fn is_live(&self, user: EntityId, token: &str) -> bool {
        let Ok(token) = ResetToken::parse(token) else {
            return false;
        };
        match self.accounts.pending(user).await {
            Ok(Some(PendingReset { hash, expires_at })) => hash
                .verify_until(&token, expires_at, self.clock.now())
                .is_ok(),
            Ok(None) => false,
            Err(error) => {
                diagnostics::internal(&error);
                false
            }
        }
    }
}
