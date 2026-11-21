//! Password login: [`PasswordAttempt`] and [`PasswordLogin`].

use async_trait::async_trait;
use event_sauce_core::EntityId;
use secrecy::SecretString;
use tokio::sync::OnceCell;

use crate::{
    ClaimLocator, HasPassword, HashError, Identity, LocateError, LoginId, LoginMethod, Password,
    PasswordHash, PasswordHasher, Rejected,
};

const DUMMY_PASSWORD: &str = "event-sauce-auth dummy password";

/// A password login attempt: who, and the password they typed.
#[derive(Debug)]
pub struct PasswordAttempt<L: LoginId> {
    /// The login identifier the client named.
    pub login: L,
    /// The password the client typed.
    pub password: SecretString,
}

impl<L: LoginId> PasswordAttempt<L> {
    /// An attempt to log in as `login` with `password`.
    #[must_use]
    pub fn new(login: L, password: SecretString) -> Self {
        Self { login, password }
    }
}

/// Logs in with a login identifier and a password, for any credentials
/// that implement [`HasPassword`].
///
/// An attempt on an unknown login, or on a user with no password, is
/// verified against a dummy hash made by the same hasher, so the timing
/// does not reveal which logins exist.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{
///     Email, Identity, LoginId, LoginMethod, Password, PasswordAttempt, PasswordHasher,
///     PasswordLogin,
/// };
/// use secrecy::SecretString;
///
/// # #[cfg(feature = "argon2")]
/// # tokio_test::block_on(async {
/// let method = PasswordLogin::new(event_sauce_auth::Argon2Hasher::default());
/// let secret = SecretString::from("correct horse");
/// let hash = event_sauce_auth::Argon2Hasher::default().hash(&secret).await?;
/// let identity = Identity::Registered(Password::new(Email::parse("ada@example.com")?, hash));
/// let attempt = PasswordAttempt::new(Email::parse("ada@example.com")?, secret);
/// assert!(method.verify(&identity, &attempt).await.is_ok());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # });
/// ```
#[derive(Debug)]
pub struct PasswordLogin<H> {
    hasher: H,
    dummy: OnceCell<Option<PasswordHash>>,
}

impl<H: PasswordHasher> PasswordLogin<H> {
    /// A password method checking passwords with `hasher`.
    #[must_use]
    pub fn new(hasher: H) -> Self {
        Self {
            hasher,
            dummy: OnceCell::new(),
        }
    }

    async fn equalise_timing(&self, password: &SecretString) {
        let dummy = self
            .dummy
            .get_or_init(|| async {
                self.hasher
                    .hash(&SecretString::from(DUMMY_PASSWORD))
                    .await
                    .ok()
            })
            .await;
        if let Some(dummy) = dummy {
            let _: Result<bool, HashError> = self.hasher.verify(password, dummy).await;
        }
    }
}

#[async_trait]
impl<C, H> LoginMethod<C> for PasswordLogin<H>
where
    C: HasPassword,
    H: PasswordHasher,
{
    type Attempt = PasswordAttempt<C::Login>;

    async fn locate(
        &self,
        locator: &dyn ClaimLocator,
        attempt: &Self::Attempt,
    ) -> Result<Option<EntityId>, LocateError> {
        locator
            .locate_claim(&Password::claim_for(&attempt.login))
            .await
    }

    async fn verify(
        &self,
        identity: &Identity<C>,
        attempt: &Self::Attempt,
    ) -> Result<(), Rejected> {
        let stored = identity
            .credentials()
            .and_then(HasPassword::password)
            .map(Password::hash);
        let Some(hash) = stored else {
            self.equalise_timing(&attempt.password).await;
            return Err(Rejected::Mismatch);
        };
        if self.hasher.verify(&attempt.password, hash).await? {
            Ok(())
        } else {
            Err(Rejected::Mismatch)
        }
    }

    async fn on_unknown(&self, attempt: &Self::Attempt) {
        self.equalise_timing(&attempt.password).await;
    }
}

#[cfg(test)]
mod tests;
