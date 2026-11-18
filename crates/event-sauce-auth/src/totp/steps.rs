//! The ready-made TOTP step and route.

use async_trait::async_trait;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use crate::{
    Advance, Done, Entered, HasTotp, Login, LoginState, Rejected, Route, Step, Totp, UserAggregate,
};

/// The state of a login that awaits a TOTP code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NeedTotp;

impl LoginState for NeedTotp {
    const TAG: &'static str = "event_sauce.totp";
}

#[async_trait]
impl<U> Step<U> for NeedTotp
where
    U: UserAggregate,
    U::Credentials: HasTotp,
{
    type Proof = SecretString;
    type Verifier = Totp;
    type Next = Login<U, Done>;

    async fn verify(
        advance: Advance<U, Self>,
        verifier: &Totp,
        code: SecretString,
    ) -> Result<Login<U, Done>, Rejected> {
        let secret = advance
            .login()
            .user()
            .identity()
            .credentials()
            .and_then(HasTotp::totp_secret)
            .ok_or(Rejected::Mismatch)?;
        verifier.check(secret, &code).await?;
        Ok(advance.to(Done))
    }
}

/// Where [`TotpIfEnrolled`] sends a login.
#[derive(Debug)]
pub enum TotpOrDone<U: UserAggregate> {
    /// The user has no TOTP secret: the login is done.
    Done(Login<U, Done>),
    /// The user has a TOTP secret: the login awaits a code.
    Totp(Login<U, NeedTotp>),
}

/// The route that asks for a TOTP code from the users who enrolled one.
///
/// It fails closed: a user with any TOTP secret goes to [`NeedTotp`], and only
/// a user with none at all goes to [`Done`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TotpIfEnrolled;

impl<U> Route<U> for TotpIfEnrolled
where
    U: UserAggregate,
    U::Credentials: HasTotp,
{
    type Next = TotpOrDone<U>;

    fn route(&self, entered: Advance<U, Entered>) -> TotpOrDone<U> {
        let enrolled = entered
            .login()
            .user()
            .identity()
            .credentials()
            .is_some_and(|credentials| credentials.totp_secret().is_some());
        if enrolled {
            TotpOrDone::Totp(entered.to(NeedTotp))
        } else {
            TotpOrDone::Done(entered.to(Done))
        }
    }
}
