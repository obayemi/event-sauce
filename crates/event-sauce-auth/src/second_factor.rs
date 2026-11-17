//! The second factor: the [`SecondFactor`] trait, the [`HasSecondFactor`]
//! capability, and the pieces of the step between the first factor and the
//! session.

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::{Credentials, LoginStore, Rejected};

mod pending;
mod stage;
#[cfg(feature = "totp")]
mod totp;

pub use pending::{PendingLogin, PendingLoginError, PendingLoginId, PendingLogins};
pub use stage::{LoginStage, NoSecondFactor, TwoFactor};
#[cfg(feature = "totp")]
pub use totp::{HasTotp, Totp, TotpSecret};

/// Credentials that may demand a second factor after the first succeeds.
///
/// A second factor method needs `C: HasSecondFactor`, so configuring one for
/// credentials that cannot demand it fails to compile.
pub trait HasSecondFactor: Credentials {
    /// Whether a login with these credentials must also pass a second factor.
    fn requires_second_factor(&self) -> bool;
}

/// A way of proving a login a second time, once the first factor passed.
///
/// What the factor needs about the user lives in the credentials type `C`.
/// The authenticator keeps the [`SecondFactor::Challenge`] with the pending
/// login and hands it back to [`SecondFactor::verify`].
#[async_trait]
pub trait SecondFactor<C: HasSecondFactor>: Send + Sync {
    /// What the client is sent: nothing for a one-time password, a signed
    /// nonce for `WebAuthn`.
    type Challenge: Serialize + DeserializeOwned + Send + Sync;

    /// What the client answers with.
    type Response: Send + Sync;

    /// The challenge to issue for a login with `stored` credentials.
    fn challenge(&self, stored: &C) -> Self::Challenge;

    /// Checks `response` to `challenge` against the `stored` credentials.
    ///
    /// `store` holds whatever the factor must remember, such as the
    /// one-time-password steps already used.
    ///
    /// # Errors
    ///
    /// Returns [`Rejected`] when the response does not prove the second
    /// factor, or when the factor could not be checked.
    async fn verify(
        &self,
        stored: &C,
        challenge: &Self::Challenge,
        response: &Self::Response,
        store: &dyn LoginStore,
    ) -> Result<(), Rejected>;
}
