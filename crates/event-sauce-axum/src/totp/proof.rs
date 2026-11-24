use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use event_sauce_auth::{HasTotp, Totp, TotpSecret, UserAggregate};
use event_sauce_core::AggregateRoot;
use secrecy::SecretString;

use crate::client_ip::ClientAddr;
use crate::diagnostics;
use crate::login::AuthSession;
use crate::throttle::{LoginThrottles, Throttled};

/// What a code of the enrolled key proved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proof {
    /// The code is current and unused: the change may run.
    Proved,
    /// No key is enrolled: there is nothing to prove, and nothing was
    /// counted.
    NotEnrolled,
    /// The code is wrong, used or late, and counted as a failure.
    Refused,
}

/// The outcome of enrolling a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enrolment {
    /// The code matched the key, which was recorded.
    Enrolled,
    /// The key is malformed or the code does not match it: nothing was
    /// recorded.
    CodeRefused,
}

/// The TOTP checks of a signed-in user changing its own settings.
///
/// A change that ends the user's other sessions renews the acting one, so
/// the browser that made it stays signed in; a renewal that fails is logged,
/// and the session then ends on its next request.
///
/// ```no_run
/// use event_sauce_auth::{HasTotp, UserAggregate};
/// use event_sauce_axum::{AuthSession, ClientAddr, Proof, Throttled, TotpProof};
///
/// async fn turn_off<U>(
///     proof: &TotpProof,
///     mut auth: AuthSession<U>,
///     client: ClientAddr,
///     code: &str,
/// ) -> Result<Proof, Throttled>
/// where
///     U: UserAggregate,
///     U::Credentials: HasTotp,
/// {
///     let user = auth.user().cloned().expect("a signed-in user");
///     proof
///         .change(&mut auth, &user, &client, code, async || Ok::<(), Throttled>(()))
///         .await
/// }
/// ```
#[derive(Clone)]
pub struct TotpProof {
    totp: Totp,
    throttles: Arc<LoginThrottles>,
}

impl fmt::Debug for TotpProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TotpProof").finish_non_exhaustive()
    }
}

impl TotpProof {
    /// A proof that checks codes with `totp` and counts wrong ones in
    /// `throttles`. Share the [`Totp`] of the login challenge: a code is
    /// accepted once across both.
    #[must_use]
    pub fn new(totp: Totp, throttles: Arc<LoginThrottles>) -> Self {
        Self { totp, throttles }
    }

    /// Checks `code` against the enrolled key of `user`, throttled per user
    /// and client. A user with no key has nothing to prove and nothing is
    /// counted.
    ///
    /// # Errors
    ///
    /// [`Throttled`] while the user or the client waits out earlier wrong
    /// codes.
    pub async fn prove<U>(
        &self,
        user: &AggregateRoot<U>,
        client: &ClientAddr,
        code: &str,
    ) -> Result<Proof, Throttled>
    where
        U: UserAggregate,
        U::Credentials: HasTotp,
    {
        let Some(secret) = user.identity().credentials().and_then(HasTotp::totp_secret) else {
            return Ok(Proof::NotEnrolled);
        };
        let attempt = self
            .throttles
            .second_factor(user.entity_id(), client, Instant::now())?;
        if self.accepts(secret, code).await {
            attempt.succeeded();
            Ok(Proof::Proved)
        } else {
            Ok(Proof::Refused)
        }
    }

    /// Enrols the base32 `secret` once `code` matches it: runs `record`
    /// with the key, then renews the session.
    ///
    /// Unthrottled: the secret is the one the user was just shown, so
    /// guessing a code of it gains nothing.
    ///
    /// # Errors
    ///
    /// The error of `record`.
    pub async fn enrol<U, E>(
        &self,
        auth: &mut AuthSession<U>,
        secret: &str,
        code: &str,
        record: impl AsyncFnOnce(TotpSecret) -> Result<(), E>,
    ) -> Result<Enrolment, E>
    where
        U: UserAggregate,
    {
        let Ok(secret) = TotpSecret::try_from_base32(secret) else {
            return Ok(Enrolment::CodeRefused);
        };
        if !self.accepts(&secret, code).await {
            return Ok(Enrolment::CodeRefused);
        }
        record(secret).await?;
        renew(auth).await;
        Ok(Enrolment::Enrolled)
    }

    /// Proves `code`, then runs `change` and renews the session: turning the
    /// key off, letting a federated sign-in skip it, anything that wants a
    /// current code. A proof that is not [`Proof::Proved`] runs nothing.
    ///
    /// # Errors
    ///
    /// The [`Throttled`] of [`prove`](Self::prove), or the error of
    /// `change`.
    pub async fn change<U, E>(
        &self,
        auth: &mut AuthSession<U>,
        user: &AggregateRoot<U>,
        client: &ClientAddr,
        code: &str,
        change: impl AsyncFnOnce() -> Result<(), E>,
    ) -> Result<Proof, E>
    where
        U: UserAggregate,
        U::Credentials: HasTotp,
        E: From<Throttled>,
    {
        let proof = self.prove(user, client, code).await?;
        if proof == Proof::Proved {
            change().await?;
            renew(auth).await;
        }
        Ok(proof)
    }

    async fn accepts(&self, secret: &TotpSecret, code: &str) -> bool {
        self.totp
            .check(secret, &SecretString::from(code))
            .await
            .is_ok()
    }
}

async fn renew<U: UserAggregate>(auth: &mut AuthSession<U>) {
    if let Err(error) = auth.renew().await {
        diagnostics::internal(&error);
    }
}
