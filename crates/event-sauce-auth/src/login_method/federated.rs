//! Federated login: [`VerifiedFederated`] and [`FederatedLogin`].

use async_trait::async_trait;
use event_sauce_core::EntityId;

use crate::{
    ClaimLocator, Email, FederatedId, HasFederated, Identity, LocateError, LoginMethod, Rejected,
};

/// A federated identity whose provider proof has already been checked.
///
/// The app builds it after its own OAuth callback has validated the
/// provider's token. The library runs no OAuth flow, so nothing here
/// verifies a token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedFederated {
    /// Who the provider says the user is.
    pub id: FederatedId,
    /// The email the provider vouched for, when it gave one.
    pub email: Option<Email>,
}

/// Logs in through a federated identity, for any credentials that
/// implement [`HasFederated`].
///
/// The user is found through the claim of the identity, and the attempt is
/// accepted only if that identity is linked to the user.
///
/// Configuring it for credentials without links does not compile:
///
/// ```compile_fail,E0277
/// use event_sauce_auth::{Credentials, Email, FederatedLogin, LoginMethod, Password};
///
/// fn needs_method<C: Credentials, M: LoginMethod<C>>() {}
/// needs_method::<Password<Email>, FederatedLogin>();
/// ```
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{
///     Federated, FederatedId, FederatedLogin, Identity, LoginMethod, Provider, VerifiedFederated,
/// };
///
/// # tokio_test::block_on(async {
/// let id = FederatedId::new(Provider::GOOGLE, "g-1");
/// let identity = Identity::Registered(Federated::new(id.clone()));
/// let attempt = VerifiedFederated { id, email: None };
/// assert!(FederatedLogin.verify(&identity, &attempt).await.is_ok());
/// # });
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct FederatedLogin;

#[async_trait]
impl<C: HasFederated> LoginMethod<C> for FederatedLogin {
    type Attempt = VerifiedFederated;

    async fn locate(
        &self,
        locator: &dyn ClaimLocator,
        attempt: &Self::Attempt,
    ) -> Result<Option<EntityId>, LocateError> {
        locator.locate_claim(&attempt.id.claim()).await
    }

    async fn verify(
        &self,
        identity: &Identity<C>,
        attempt: &Self::Attempt,
    ) -> Result<(), Rejected> {
        let linked = identity
            .credentials()
            .and_then(HasFederated::federated)
            .is_some_and(|federated| federated.links().contains(&attempt.id));
        if linked {
            Ok(())
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

#[cfg(test)]
#[path = "tests/federated_tests.rs"]
mod tests;
