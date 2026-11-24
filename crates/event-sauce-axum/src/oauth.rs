//! The OAuth sign-in flow of an HTML application: a redirect to the
//! provider's consent page, and the callback it comes back to.
//!
//! [`OAuthSignIn`] runs the flow and asks the application for everything that
//! is its own:
//!
//! - [`start`](OAuthSignIn::start) sends the browser to the consent page, and
//!   keeps the anti-forgery state, the PKCE verifier and the target of the
//!   flow in the session record.
//! - [`callback`](OAuthSignIn::callback) checks the state and the verifier,
//!   asks the provider who the code stands for, and then signs that account
//!   in. A user the provider's account does not belong to yet is the
//!   application's to link or create ([`OAuthAccounts`]); a sign-in goes on
//!   through the TOTP code step its [`Route`](event_sauce_auth::Route) asks
//!   for, like a password sign-in.
//! - [`start_link`](OAuthSignIn::start_link) starts a flow whose callback
//!   links the account to the user signed in instead of signing in, with the
//!   [`LinkProof`] of a code the application just checked. The link belongs
//!   to that flow alone, works once, and its proof counts for ten minutes.
//! - [`mark_claim`](OAuthSignIn::mark_claim) makes the next callback turn the
//!   anonymous user signed in into the account.
//!
//! A flow in flight keeps its state in the session record, never in a cookie
//! of its own, so a callback in another session finds nothing. Pages are the
//! application's, through [`OAuthPages`].

mod accounts;
mod callback;
mod flow;
mod link;
mod pages;

use std::fmt;
use std::sync::Arc;

use axum::response::Redirect;
use axum_login::tower_sessions::Session;
use chrono::TimeDelta;
use event_sauce_auth::{
    Authorization, Clock, FederatedLogin, FirstFactor, HasFederated, HasTotp, Logins,
    OAuthProvider, Route, SystemClock, TotpOrDone, UserAggregate,
};
use event_sauce_core::EntityId;

use crate::return_to::ReturnTo;
use crate::totp::TotpChallenge;

pub use accounts::{LinkOutcome, OAuthAccounts};
pub use callback::Returned;
pub use link::LinkProof;
pub use pages::{CallbackError, OAuthPages};

use link::PendingLink;

const LINK_PROOF_TTL: TimeDelta = TimeDelta::minutes(10);

/// The OAuth sign-in flow: a start, a callback, and the links and claims
/// that go through the same callback.
///
/// Build it once at startup and keep it in the application state: an
/// application that has no provider configured has no flow, and keeps an
/// `Option` of it.
///
/// ```no_run
/// use axum::response::Redirect;
/// use event_sauce_auth::{HasFederated, HasTotp, OAuthProvider, Route, TotpOrDone, UserAggregate};
/// use event_sauce_axum::{AuthSession, OAuthAccounts, OAuthPages, OAuthSignIn, Returned, ReturnTo};
/// use tower_sessions::Session;
///
/// async fn login<U, P, A, R>(oauth: &OAuthSignIn<U, P, A, R>, session: Session) -> Redirect
/// where
///     U: UserAggregate,
///     U::Credentials: HasFederated + HasTotp,
///     P: OAuthProvider,
///     A: OAuthAccounts<Profile = P::Profile>,
///     R: Route<U, Next = TotpOrDone<U>>,
/// {
///     oauth.start(&session, Some(&ReturnTo::root())).await
/// }
///
/// async fn callback<U, P, A, R, Pg>(
///     oauth: &OAuthSignIn<U, P, A, R>,
///     pages: &Pg,
///     mut auth: AuthSession<U>,
///     session: Session,
///     returned: Returned,
/// ) -> Pg::Response
/// where
///     U: UserAggregate,
///     U::Credentials: HasFederated + HasTotp,
///     P: OAuthProvider,
///     A: OAuthAccounts<Profile = P::Profile>,
///     R: Route<U, Next = TotpOrDone<U>>,
///     Pg: OAuthPages<P::Error, <A as OAuthAccounts>::Refusal>,
/// {
///     oauth.callback(pages, &mut auth, &session, returned).await
/// }
/// ```
pub struct OAuthSignIn<U: UserAggregate, P, A, R> {
    provider: Arc<P>,
    accounts: Arc<A>,
    logins: Logins<U>,
    first: FirstFactor<FederatedLogin, R>,
    challenge: TotpChallenge<U>,
    clock: Arc<dyn Clock>,
}

impl<U: UserAggregate, P, A, R: Clone> Clone for OAuthSignIn<U, P, A, R> {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            accounts: self.accounts.clone(),
            logins: self.logins.clone(),
            first: self.first.clone(),
            challenge: self.challenge.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<U: UserAggregate, P, A, R> fmt::Debug for OAuthSignIn<U, P, A, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthSignIn").finish_non_exhaustive()
    }
}

impl<U, P, A, R> OAuthSignIn<U, P, A, R>
where
    U: UserAggregate,
    U::Credentials: HasFederated + HasTotp,
    P: OAuthProvider,
    A: OAuthAccounts<Profile = P::Profile>,
    R: Route<U, Next = TotpOrDone<U>>,
{
    /// A flow with `provider`, over the accounts of `accounts`, starting
    /// logins with `logins` along `route`, and parking the ones that wait for
    /// a code with `challenge`.
    #[must_use]
    pub fn new(
        provider: Arc<P>,
        accounts: Arc<A>,
        logins: Logins<U>,
        route: R,
        challenge: TotpChallenge<U>,
    ) -> Self {
        Self {
            provider,
            accounts,
            logins,
            first: FirstFactor::new(FederatedLogin, route),
            challenge,
            clock: Arc::new(SystemClock),
        }
    }

    /// Reads the time from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Starts a flow that comes back to `return_to`; `None` forgets the
    /// target of an earlier flow. The redirect goes to the consent page.
    pub async fn start(&self, session: &Session, return_to: Option<&ReturnTo>) -> Redirect {
        let authorization = self.begin(session, return_to).await;
        Redirect::to(&authorization.url)
    }

    /// Starts a flow whose callback links the account to `user`, the user
    /// signed in on `session`, with `proof`, and comes back to `return_to`.
    ///
    /// The link works for this flow's callback only, once, and a proved code
    /// counts for ten minutes.
    pub async fn start_link(
        &self,
        session: &Session,
        user: EntityId,
        proof: LinkProof,
        return_to: &ReturnTo,
    ) -> Redirect {
        let authorization = self.begin(session, Some(return_to)).await;
        let link = PendingLink::new(
            user,
            authorization.state,
            proof,
            self.clock.now(),
            LINK_PROOF_TTL,
        );
        flow::put(session, flow::LINK, link).await;
        Redirect::to(&authorization.url)
    }

    /// Makes the next callback in `session` turn `user`, the anonymous user
    /// signed in on it, into the account. The application then starts the
    /// flow with [`start`](Self::start).
    pub async fn mark_claim(&self, session: &Session, user: EntityId) {
        flow::put(session, flow::CLAIM, user).await;
    }

    async fn begin(&self, session: &Session, return_to: Option<&ReturnTo>) -> Authorization {
        let authorization = self.provider.authorize();
        flow::start(session, &authorization, return_to).await;
        authorization
    }
}
