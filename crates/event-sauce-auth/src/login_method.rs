//! The [`LoginMethod`] trait and what it is built from.

use async_trait::async_trait;
use event_sauce_core::{AggregateClaim, EntityId, EventStore};

use crate::{Credentials, HashError, Identity, LoginStoreError};

mod federated;
mod password;
mod password_or_federated;

pub use federated::{FederatedLogin, VerifiedFederated};
pub use password::{PasswordAttempt, PasswordLogin};
pub use password_or_federated::{PasswordOrFederatedAttempt, PasswordOrFederatedLogin};

/// Why a user could not be located.
pub type LocateError = event_sauce_core::Error;

/// Why a login attempt was rejected by a method.
#[derive(Debug, thiserror::Error)]
pub enum Rejected {
    /// The attempt does not prove the identity it names.
    #[error("credentials do not match")]
    Mismatch,
    /// The attempt could not be checked because hashing failed.
    #[error(transparent)]
    Hash(#[from] HashError),
    /// The attempt could not be checked because the login store failed.
    #[error(transparent)]
    LoginStore(#[from] LoginStoreError),
    /// The attempt could not be checked at all: a mailer or a link store
    /// the verifier depends on failed.
    #[error("the attempt could not be checked")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl Rejected {
    /// A rejection for a verifier that could not check the attempt, wrapping
    /// what went wrong.
    #[must_use]
    pub fn unavailable(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Unavailable(Box::new(error))
    }
}

/// Finds the user holding a claim.
///
/// Every [`EventStore`] is one, through its claim index. A method that
/// looks users up elsewhere, such as a device link, ignores the locator.
#[async_trait]
pub trait ClaimLocator: Send + Sync {
    /// The user currently holding `claim`, if any.
    ///
    /// # Errors
    ///
    /// Returns [`LocateError`] when the lookup fails.
    async fn locate_claim(&self, claim: &AggregateClaim) -> Result<Option<EntityId>, LocateError>;
}

#[async_trait]
impl<S: EventStore + ?Sized> ClaimLocator for S {
    async fn locate_claim(&self, claim: &AggregateClaim) -> Result<Option<EntityId>, LocateError> {
        self.resolve_claim(claim).await
    }
}

/// How one kind of login attempt is located and verified.
///
/// Storing credentials ([`Credentials`]) and checking an attempt are
/// separate, so an app can swap how a method verifies without touching
/// what is stored.
#[async_trait]
pub trait LoginMethod<C: Credentials>: Send + Sync {
    /// What the client submits.
    type Attempt: Send + Sync;

    /// Finds the user this attempt targets.
    ///
    /// Credential methods resolve a claim through `locator`. Delegated
    /// methods look the user up in their own store.
    ///
    /// # Errors
    ///
    /// Returns [`LocateError`] when the lookup fails.
    async fn locate(
        &self,
        locator: &dyn ClaimLocator,
        attempt: &Self::Attempt,
    ) -> Result<Option<EntityId>, LocateError>;

    /// Checks the attempt against the located user's identity.
    ///
    /// The method decides whether [`Identity::Anonymous`] may log in this
    /// way: credential methods reject it, delegated ones may accept it.
    ///
    /// # Errors
    ///
    /// Returns [`Rejected`] when the attempt does not prove the identity.
    async fn verify(&self, identity: &Identity<C>, attempt: &Self::Attempt)
        -> Result<(), Rejected>;

    /// Runs when no user matches the attempt.
    ///
    /// Does nothing by default. Password methods override it to equalise
    /// the timing of known and unknown logins.
    async fn on_unknown(&self, _attempt: &Self::Attempt) {}
}

#[cfg(test)]
#[path = "tests/login_method_tests.rs"]
mod tests;
