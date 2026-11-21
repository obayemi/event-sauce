//! Combined login: [`PasswordOrFederatedAttempt`] and
//! [`PasswordOrFederatedLogin`].

use async_trait::async_trait;
use event_sauce_core::EntityId;

use crate::{
    ClaimLocator, FederatedLogin, HasFederated, HasPassword, Identity, LocateError, LoginId,
    LoginMethod, PasswordAttempt, PasswordHasher, PasswordLogin, Rejected, VerifiedFederated,
};

/// What a client submits to log in with a password or a federated identity.
#[derive(Debug)]
pub enum PasswordOrFederatedAttempt<L: LoginId> {
    /// A login identifier and its password.
    Password(PasswordAttempt<L>),
    /// A federated identity the app has already verified.
    Federated(VerifiedFederated),
}

/// Logs in with a password or a federated identity, for any credentials
/// that implement both [`HasPassword`] and [`HasFederated`].
///
/// Each attempt goes to [`PasswordLogin`] or [`FederatedLogin`].
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{
///     Federated, FederatedId, Identity, LoginMethod, PasswordHasher, PasswordOrFederated,
///     PasswordOrFederatedAttempt, PasswordOrFederatedLogin, Provider, VerifiedFederated, Email,
/// };
///
/// # #[cfg(feature = "argon2")]
/// # tokio_test::block_on(async {
/// let method = PasswordOrFederatedLogin::new(event_sauce_auth::Argon2Hasher::default());
/// let id = FederatedId::new(Provider::GOOGLE, "g-1");
/// let identity = Identity::Registered(PasswordOrFederated::<Email>::Federated(Federated::new(
///     id.clone(),
/// )));
/// let attempt = PasswordOrFederatedAttempt::Federated(VerifiedFederated { id, email: None });
/// assert!(method.verify(&identity, &attempt).await.is_ok());
/// # });
/// ```
#[derive(Debug)]
pub struct PasswordOrFederatedLogin<H> {
    password: PasswordLogin<H>,
}

impl<H: PasswordHasher> PasswordOrFederatedLogin<H> {
    /// A combined method checking passwords with `hasher`.
    #[must_use]
    pub fn new(hasher: H) -> Self {
        Self {
            password: PasswordLogin::new(hasher),
        }
    }
}

#[async_trait]
impl<C, H> LoginMethod<C> for PasswordOrFederatedLogin<H>
where
    C: HasPassword + HasFederated,
    H: PasswordHasher,
{
    type Attempt = PasswordOrFederatedAttempt<C::Login>;

    async fn locate(
        &self,
        locator: &dyn ClaimLocator,
        attempt: &Self::Attempt,
    ) -> Result<Option<EntityId>, LocateError> {
        match attempt {
            PasswordOrFederatedAttempt::Password(attempt) => {
                LoginMethod::<C>::locate(&self.password, locator, attempt).await
            }
            PasswordOrFederatedAttempt::Federated(attempt) => {
                LoginMethod::<C>::locate(&FederatedLogin, locator, attempt).await
            }
        }
    }

    async fn verify(
        &self,
        identity: &Identity<C>,
        attempt: &Self::Attempt,
    ) -> Result<(), Rejected> {
        match attempt {
            PasswordOrFederatedAttempt::Password(attempt) => {
                self.password.verify(identity, attempt).await
            }
            PasswordOrFederatedAttempt::Federated(attempt) => {
                FederatedLogin.verify(identity, attempt).await
            }
        }
    }

    async fn on_unknown(&self, attempt: &Self::Attempt) {
        if let PasswordOrFederatedAttempt::Password(attempt) = attempt {
            LoginMethod::<C>::on_unknown(&self.password, attempt).await;
        }
    }
}

#[cfg(test)]
mod tests;
