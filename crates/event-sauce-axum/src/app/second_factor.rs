//! [`SecondFactorStage`]: what an application does between the first factor
//! and the session.

use std::sync::Arc;

use event_sauce_auth::{
    ClaimAuthenticator, HasSecondFactor, LoginStore, NoSecondFactor, SecondFactor, TwoFactor,
    UserAggregate,
};

/// What an application does between the first factor and the session.
///
/// It is [`NoSecondFactor`] until [`UserAppBuilder::second_factor`](crate::UserAppBuilder::second_factor) says
/// otherwise. Only [`NoSecondFactor`] and [`WithSecondFactor`] implement it.
pub trait SecondFactorStage<U: UserAggregate>: sealed::Sealed {
    /// The stage the authenticator ends up with.
    type Stage;

    /// Gives `authenticator` its stage, with pending logins kept in `logins`.
    fn apply<M, S>(
        self,
        authenticator: ClaimAuthenticator<U, M, S>,
        logins: Arc<dyn LoginStore>,
    ) -> ClaimAuthenticator<U, M, S, Self::Stage>;
}

mod sealed {
    pub trait Sealed {}
}

impl sealed::Sealed for NoSecondFactor {}

impl<U: UserAggregate> SecondFactorStage<U> for NoSecondFactor {
    type Stage = NoSecondFactor;

    fn apply<M, S>(
        self,
        authenticator: ClaimAuthenticator<U, M, S>,
        _logins: Arc<dyn LoginStore>,
    ) -> ClaimAuthenticator<U, M, S> {
        authenticator
    }
}

/// A second factor the application demands, held by the login store.
#[derive(Debug)]
pub struct WithSecondFactor<F>(pub(super) F);

impl<F> sealed::Sealed for WithSecondFactor<F> {}

impl<U, F> SecondFactorStage<U> for WithSecondFactor<F>
where
    U: UserAggregate,
    U::Credentials: HasSecondFactor,
    F: SecondFactor<U::Credentials>,
{
    type Stage = TwoFactor<F>;

    fn apply<M, S>(
        self,
        authenticator: ClaimAuthenticator<U, M, S>,
        logins: Arc<dyn LoginStore>,
    ) -> ClaimAuthenticator<U, M, S, TwoFactor<F>> {
        authenticator.with_second_factor(TwoFactor::new(self.0, logins))
    }
}
