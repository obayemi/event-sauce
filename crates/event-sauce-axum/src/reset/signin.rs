use async_trait::async_trait;
use event_sauce_auth::{
    ClaimLocator, Credentials, Done, Identity, LocateError, Login, LoginMethod, Rejected,
    UserAggregate,
};
use event_sauce_core::EntityId;

/// A login that passed its first factor and needs nothing else to finish.
///
/// It is how [`PasswordReset`](super::PasswordReset) reads where a
/// [`Route`](event_sauce_auth::Route) sent the user it just reset the
/// password of: to a finished login, or to a step that asks for more, which
/// the user then does by signing in again.
pub trait Finished<U: UserAggregate>: Send {
    /// The login when it is done, `None` when it waits for another step.
    fn done(self) -> Option<Login<U, Done>>;
}

impl<U: UserAggregate> Finished<U> for Login<U, Done> {
    fn done(self) -> Option<Login<U, Done>> {
        Some(self)
    }
}

#[cfg(feature = "totp")]
impl<U: UserAggregate> Finished<U> for event_sauce_auth::TotpOrDone<U> {
    fn done(self) -> Option<Login<U, Done>> {
        match self {
            Self::Done(login) => Some(login),
            Self::Totp(_) => None,
        }
    }
}

/// A user whose reset link was just redeemed, which only the flow builds:
/// that is what lets it start a login without a password.
#[derive(Debug)]
pub(super) struct Redeemed(pub(super) EntityId);

/// The first factor of the login after a reset: the redeemed link.
#[derive(Debug, Clone, Copy)]
pub(super) struct RedeemedLogin;

#[async_trait]
impl<C: Credentials> LoginMethod<C> for RedeemedLogin {
    type Attempt = Redeemed;

    async fn locate(
        &self,
        _locator: &dyn ClaimLocator,
        attempt: &Redeemed,
    ) -> Result<Option<EntityId>, LocateError> {
        Ok(Some(attempt.0))
    }

    async fn verify(&self, _identity: &Identity<C>, _attempt: &Redeemed) -> Result<(), Rejected> {
        Ok(())
    }
}
