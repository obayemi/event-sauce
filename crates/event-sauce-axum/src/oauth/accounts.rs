use async_trait::async_trait;
use event_sauce_auth::FederatedAccount;
use event_sauce_core::EntityId;

use super::link::LinkProof;

#[cfg(test)]
#[path = "tests/accounts_tests.rs"]
mod tests;

/// What the application did with a link to a signed-in user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkOutcome {
    /// The account is linked.
    Linked,
    /// The application wants a second factor code first and the link carried
    /// none: the user is sent to the page that asks for it.
    NeedsCode,
    /// The application could not link the account; it knows why and logged
    /// it. The user goes back where the flow was going.
    Failed,
}

/// The accounts of the application, as a federated sign-in, link or claim
/// changes them.
///
/// Every rule that decides whose account a provider identity is stays the
/// application's: linking by email, creating an account, asking a code before
/// a link. The flow only calls the hook its step reaches.
#[async_trait]
pub trait OAuthAccounts: Send + Sync + 'static {
    /// What the provider's account carries for the application, as its
    /// [`OAuthProvider`](event_sauce_auth::OAuthProvider) names it.
    type Profile: Send + Sync;

    /// Why the application refuses to sign an unknown account in, for its
    /// page.
    type Refusal: Send;

    /// Runs once when no user holds the account, before
    /// [`unknown`](Self::unknown), to settle what the first lookup could not
    /// see, such as links an older version left half written. The lookup then
    /// runs again. It does nothing by default.
    async fn reconcile(&self) {}

    /// No user holds the account, also after [`reconcile`](Self::reconcile):
    /// link it to a user, create one, or refuse. The lookup runs a last time
    /// when this succeeds.
    ///
    /// # Errors
    ///
    /// The application's refusal.
    async fn unknown(&self, account: &FederatedAccount<Self::Profile>)
        -> Result<(), Self::Refusal>;

    /// Links `account` to `user`, the user signed in on the session, with the
    /// `proof` the link carries.
    async fn link(
        &self,
        user: EntityId,
        account: FederatedAccount<Self::Profile>,
        proof: LinkProof,
    ) -> LinkOutcome;

    /// Turns `user`, the anonymous user signed in on the session, into the
    /// account. Answers whether it did.
    async fn claim(&self, user: EntityId, account: FederatedAccount<Self::Profile>) -> bool;
}
