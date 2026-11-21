//! Time-based one-time passwords ([RFC 6238]) as a login step: the
//! [`HasTotp`] capability, the [`Totp`] verifier, the [`NeedTotp`] state and
//! the [`TotpIfEnrolled`] route.
//!
//! [RFC 6238]: https://datatracker.ietf.org/doc/html/rfc6238

use std::{fmt, sync::Arc, time::Duration};

use secrecy::{ExposeSecret, SecretString};

use crate::{
    Clock, Credentials, Fingerprint, LoginKey, LoginStore, LoginTtl, Rejected, SystemClock,
};

mod steps;

pub use steps::{NeedTotp, TotpIfEnrolled, TotpOrDone};
pub use totp_rs::Secret as TotpSecret;

const REPLAY_NAMESPACE: &str = "totp";

/// Credentials that may hold the shared secret of a TOTP second factor.
///
/// The secret is part of the credentials, so it must be fed to the
/// session fingerprint: enrolling or replacing a second factor then ends the
/// sessions opened before. [`HasTotp::feed_totp_fingerprint`] does that, and
/// [`Credentials::fingerprint`](crate::Credentials::fingerprint) should call it.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{Credentials, Fingerprint, HasTotp, TotpSecret};
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
/// impl HasTotp for Account {
///     fn totp_secret(&self) -> Option<&TotpSecret> {
///         self.totp.as_ref()
///     }
/// }
///
/// let account = Account { totp: Some(TotpSecret::generate()) };
/// assert!(account.totp_secret().is_some());
/// ```
pub trait HasTotp: Credentials {
    /// The shared secret, or `None` when no TOTP second factor is enrolled.
    fn totp_secret(&self) -> Option<&TotpSecret>;

    /// Feeds the secret to a session fingerprint.
    fn feed_totp_fingerprint(&self, fingerprint: &mut Fingerprint) {
        fingerprint.field(self.totp_secret().map_or(&[][..], TotpSecret::as_bytes));
    }
}

/// The TOTP verifier: six digits, thirty-second steps, SHA-1, with one step
/// of tolerance on each side, as authenticator apps expect.
///
/// A code is accepted once. Checking records the matched time step in the
/// replay [`LoginStore`], keyed by a digest of the secret, so replaying a
/// code within its validity window fails without writing to the user's
/// event stream. A rejected code records nothing.
///
/// It is the [`Step::Verifier`](crate::Step::Verifier) of [`NeedTotp`], and an
/// application reuses [`Totp::check`] in steps of its own.
#[derive(Clone)]
pub struct Totp {
    replays: Arc<dyn LoginStore>,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for Totp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Totp").finish_non_exhaustive()
    }
}

impl Totp {
    /// A TOTP verifier recording used steps in `replays`, reading the system
    /// clock.
    #[must_use]
    pub fn new(replays: Arc<dyn LoginStore>) -> Self {
        Self {
            replays,
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

    /// Checks `code` against `secret`, accepting each time step once.
    ///
    /// # Errors
    ///
    /// Returns [`Rejected::Mismatch`] when the code does not match or its
    /// step was already used, and [`Rejected::LoginStore`] when the replay
    /// store fails.
    pub async fn check(&self, secret: &TotpSecret, code: &SecretString) -> Result<(), Rejected> {
        let totp = totp_rs::Builder::new()
            .with_secret(secret.clone())
            .build()
            .map_err(|_| Rejected::Mismatch)?;
        let now = u64::try_from(self.clock.now().timestamp()).map_err(|_| Rejected::Mismatch)?;
        let step = totp
            .check(code.expose_secret(), now)
            .ok_or(Rejected::Mismatch)?;
        let window = Duration::from_secs((2 * u64::from(totp.skew()) + 1) * totp.step());
        let ttl = LoginTtl::new(window).map_err(|_| Rejected::Mismatch)?;
        let first_use = self
            .replays
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
#[path = "tests/totp_tests.rs"]
mod tests;
