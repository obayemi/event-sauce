use axum_login::tower_sessions::Session;
use event_sauce_auth::{
    AuthError, AuthorizationCode, FederatedAccount, FederatedId, HasFederated, HasTotp,
    OAuthProvider, OAuthState, Route, TotpOrDone, UserAggregate, VerifiedFederated,
};
use event_sauce_core::EntityId;
use serde::Deserialize;

use super::accounts::{LinkOutcome, OAuthAccounts};
use super::flow::{self, take, take_flow};
use super::link::PendingLink;
use super::pages::{CallbackError, OAuthPages};
use super::OAuthSignIn;
use crate::diagnostics;
use crate::login::AuthSession;
use crate::return_to::ReturnTo;
use crate::totp::{renew, OpenError, Opened};

/// What the provider sends the browser back with: a `code`, or an `error`
/// instead when the user cancelled at its consent page or the provider
/// failed.
///
/// It deserializes from the query of the callback.
#[derive(Debug, Clone, Deserialize)]
pub struct Returned {
    /// The anti-forgery state the flow was started with.
    pub state: OAuthState,
    /// The one-time code, absent when the flow did not succeed.
    #[serde(default)]
    pub code: Option<AuthorizationCode>,
    /// What the provider says went wrong, logged and never shown.
    #[serde(default)]
    pub error: Option<String>,
}

type Refusal<A> = <A as OAuthAccounts>::Refusal;

impl<U, P, A, R> OAuthSignIn<U, P, A, R>
where
    U: UserAggregate,
    U::Credentials: HasFederated + HasTotp,
    P: OAuthProvider,
    A: OAuthAccounts<Profile = P::Profile>,
    R: Route<U, Next = TotpOrDone<U>>,
{
    /// The callback of a flow: links the account, claims for it, or signs it
    /// in, and answers with the application's page.
    ///
    /// In this order: the link of the session is taken, then the state is
    /// checked against the flow's (a refused state ends the flow), then the
    /// verifier and the target are taken. A callback without a code is a
    /// cancelled consent. The provider then names the account, and
    ///
    /// - when a link started with this flow's state waits, it links the
    ///   account to its user, if that user is the one signed in;
    /// - otherwise when a claim was marked, the signed-in user is turned into
    ///   the account, if it is the one marked;
    /// - otherwise the account signs in: a user that holds it goes through
    ///   the route, a user nobody holds is the application's to adopt, once.
    ///   A login that waits for a code goes to the code step.
    pub async fn callback<Pg: OAuthPages<P::Error, Refusal<A>>>(
        &self,
        pages: &Pg,
        auth: &mut AuthSession<U>,
        session: &Session,
        returned: Returned,
    ) -> Pg::Response {
        let link = take::<PendingLink>(session, flow::LINK).await;
        let flow = match take_flow(session, &returned.state).await {
            Ok(flow) => flow,
            Err(error) => return pages.failed(error, None),
        };
        let return_to = flow.return_to;
        let Some(code) = returned.code else {
            diagnostics::declined(returned.error.as_deref());
            return pages.failed(CallbackError::Cancelled, return_to.as_ref());
        };
        let account = match self.provider.identify(code, flow.pkce_verifier).await {
            Ok(account) => account,
            Err(error) => return pages.failed(CallbackError::Provider(error), return_to.as_ref()),
        };
        if let Some(link) = link.filter(|link| link.answers(&returned.state)) {
            return self.link(pages, auth, &link, account, return_to).await;
        }
        if let Some(user) = take::<EntityId>(session, flow::CLAIM).await {
            return self.claim(pages, auth, user, account, return_to).await;
        }
        self.sign_in(pages, auth, session, account, return_to.unwrap_or_default())
            .await
    }

    async fn link<Pg: OAuthPages<P::Error, Refusal<A>>>(
        &self,
        pages: &Pg,
        auth: &mut AuthSession<U>,
        link: &PendingLink,
        account: FederatedAccount<P::Profile>,
        return_to: Option<ReturnTo>,
    ) -> Pg::Response {
        if !is_signed_in(auth, link.user()) {
            return pages.not_session_user();
        }
        let proof = link.proof_at(self.clock.now());
        match self.accounts.link(link.user(), account, proof).await {
            LinkOutcome::Linked => renew(auth).await,
            LinkOutcome::NeedsCode => return pages.link_needs_code(),
            LinkOutcome::Failed => {}
        }
        pages.returned(return_to)
    }

    async fn claim<Pg: OAuthPages<P::Error, Refusal<A>>>(
        &self,
        pages: &Pg,
        auth: &mut AuthSession<U>,
        user: EntityId,
        account: FederatedAccount<P::Profile>,
        return_to: Option<ReturnTo>,
    ) -> Pg::Response {
        if !is_signed_in(auth, user) {
            return pages.not_session_user();
        }
        if self.accounts.claim(user, account).await {
            renew(auth).await;
        }
        pages.returned(return_to)
    }

    async fn sign_in<Pg: OAuthPages<P::Error, Refusal<A>>>(
        &self,
        pages: &Pg,
        auth: &mut AuthSession<U>,
        session: &Session,
        account: FederatedAccount<P::Profile>,
        return_to: ReturnTo,
    ) -> Pg::Response {
        let begun = match self.first_factor(&account).await {
            Ok(begun) => begun,
            Err(refusal) => {
                return pages.failed(CallbackError::Refused(refusal), Some(&return_to));
            }
        };
        match self
            .challenge
            .open(auth, session, begun, return_to.clone())
            .await
        {
            Ok(Opened::SignedIn(return_to)) => pages.done(return_to),
            Ok(Opened::AwaitingCode(return_to)) => pages.code_step(&return_to),
            Err(OpenError::Session(error)) => {
                diagnostics::internal(&error);
                pages.failed(CallbackError::Session, Some(&return_to))
            }
            Err(OpenError::Login(error)) => {
                diagnostics::refused(&error);
                pages.failed(CallbackError::Refused(None), Some(&return_to))
            }
        }
    }

    async fn first_factor(
        &self,
        account: &FederatedAccount<P::Profile>,
    ) -> Result<TotpOrDone<U>, Option<Refusal<A>>> {
        let mut begun = self.begin_login(&account.id).await;
        if is_unknown(&begun) {
            self.accounts.reconcile().await;
            begun = self.begin_login(&account.id).await;
        }
        if is_unknown(&begun) {
            self.accounts.unknown(account).await.map_err(Some)?;
            begun = self.begin_login(&account.id).await;
        }
        begun.map_err(|error| {
            diagnostics::refused(&error);
            None
        })
    }

    async fn begin_login(
        &self,
        id: &FederatedId,
    ) -> Result<TotpOrDone<U>, AuthError<VerifiedFederated>> {
        let attempt = VerifiedFederated {
            id: id.clone(),
            email: None,
        };
        self.logins.begin(&self.first, attempt).await
    }
}

fn is_unknown<T, A>(begun: &Result<T, AuthError<A>>) -> bool {
    matches!(begun, Err(AuthError::Unknown { .. }))
}

fn is_signed_in<U: UserAggregate>(auth: &AuthSession<U>, user: EntityId) -> bool {
    let signed_in = auth.user().map(|signed_in| signed_in.entity_id()) == Some(user);
    if !signed_in {
        diagnostics::refused(&"a link or a claim for a user other than the signed-in one");
    }
    signed_in
}
