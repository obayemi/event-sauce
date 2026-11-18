//! Time-based one-time passwords ([RFC 6238]) as a [`SecondFactor`].
//!
//! [RFC 6238]: https://datatracker.ietf.org/doc/html/rfc6238

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};

use crate::{
    Clock, Fingerprint, HasSecondFactor, LoginKey, LoginStore, LoginTtl, Rejected, SecondFactor,
    SystemClock,
};

pub use totp_rs::Secret as TotpSecret;

const REPLAY_NAMESPACE: &str = "totp";

/// Credentials that hold the shared secret of a TOTP second factor.
///
/// The secret is part of the credentials, so it must be fed to the
/// session fingerprint: enrolling or replacing a second factor then ends the
/// sessions opened before. [`HasTotp::feed_totp_fingerprint`] does that, and
/// [`Credentials::fingerprint`](crate::Credentials::fingerprint) should call it.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{Credentials, Fingerprint, HasSecondFactor, HasTotp, TotpSecret};
/// use event_sauce_core::AggregateClaim;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
/// struct Account {
///     totp: Option<TotpSecret>,
/// }
///
/// impl Credentials for Account {
///     fn claims(&self) -> Vec<AggregateClaim> {
///         Vec::new()
///     }
///
///     fn fingerprint(&self, fingerprint: &mut Fingerprint) {
///         self.feed_totp_fingerprint(fingerprint);
///     }
/// }
///
/// impl HasSecondFactor for Account {
///     fn requires_second_factor(&self) -> bool {
///         self.totp.is_some()
///     }
/// }
///
/// impl HasTotp for Account {
///     fn totp_secret(&self) -> Option<&TotpSecret> {
///         self.totp.as_ref()
///     }
/// }
///
/// let account = Account { totp: Some(TotpSecret::generate()) };
/// assert!(account.requires_second_factor());
/// ```
pub trait HasTotp: HasSecondFactor {
    /// The shared secret, or `None` when no TOTP second factor is enrolled.
    fn totp_secret(&self) -> Option<&TotpSecret>;

    /// Feeds the secret to a session fingerprint.
    fn feed_totp_fingerprint(&self, fingerprint: &mut Fingerprint) {
        fingerprint.field(self.totp_secret().map_or(&[][..], TotpSecret::as_bytes));
    }
}

/// The TOTP second factor: six digits, thirty-second steps, SHA-1, with one
/// step of tolerance on each side, as authenticator apps expect.
///
/// A code is accepted once. Verifying records the matched time step in the
/// [`LoginStore`], keyed by a digest of the secret, so replaying a code
/// within its validity window fails without writing to the user's event
/// stream. A rejected code records nothing.
#[derive(Debug, Clone)]
pub struct Totp {
    clock: Arc<dyn Clock>,
}

impl Totp {
    /// A TOTP factor reading the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self {
            clock: Arc::new(SystemClock),
        }
    }

    /// Reads the time from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    fn replay_key(secret: &TotpSecret, step: u64) -> LoginKey {
        let mut fingerprint = Fingerprint::new();
        fingerprint
            .str_field("event-sauce-auth totp")
            .field(secret.as_bytes());
        let digest = hex::encode(fingerprint.finish());
        LoginKey::new(REPLAY_NAMESPACE, format_args!("{digest}:{step}"))
    }
}

impl Default for Totp {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<C: HasTotp> SecondFactor<C> for Totp {
    type Challenge = ();
    type Response = SecretString;

    fn challenge(&self, _stored: &C) {}

    async fn verify(
        &self,
        stored: &C,
        _challenge: &(),
        response: &SecretString,
        store: &dyn LoginStore,
    ) -> Result<(), Rejected> {
        let secret = stored.totp_secret().ok_or(Rejected::Mismatch)?;
        let totp = totp_rs::Builder::new()
            .with_secret(secret.clone())
            .build()
            .map_err(|_| Rejected::Mismatch)?;
        let now = u64::try_from(self.clock.now().timestamp()).map_err(|_| Rejected::Mismatch)?;
        let step = totp
            .check(response.expose_secret(), now)
            .ok_or(Rejected::Mismatch)?;
        let window = Duration::from_secs((2 * u64::from(totp.skew()) + 1) * totp.step());
        let ttl = LoginTtl::new(window).map_err(|_| Rejected::Mismatch)?;
        let first_use = store
            .mark_once(&Self::replay_key(secret, step), ttl)
            .await?;
        if first_use {
            Ok(())
        } else {
            Err(Rejected::Mismatch)
        }
    }
}

#[cfg(test)]
mod tests;
