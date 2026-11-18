//! What happens between the first factor and the session: [`LoginStage`],
//! [`NoSecondFactor`] and [`TwoFactor`].

use std::sync::Arc;

use async_trait::async_trait;
use event_sauce_core::EntityId;

use crate::{
    Clock, Credentials, HasSecondFactor, Identity, LoginStore, LoginTtl, PendingLogin,
    PendingLoginError, PendingLoginId, PendingLogins, Rejected, SecondFactor, SystemClock,
};

/// How long a pending login lives unless the stage says otherwise: five
/// minutes.
pub const DEFAULT_PENDING_TTL: LoginTtl = LoginTtl::FIVE_MINUTES;

/// Decides whether a login that passed its first factor is complete.
///
/// [`NoSecondFactor`] completes every login. [`TwoFactor`] holds back the
/// logins whose credentials require a second factor.
#[async_trait]
pub trait LoginStage<C: Credentials>: Send + Sync {
    /// Starts a pending login for `user` when `identity` requires one.
    ///
    /// Returns `None` when the login is complete.
    ///
    /// # Errors
    ///
    /// Returns [`PendingLoginError`] when the pending login cannot be stored.
    async fn begin(
        &self,
        user: EntityId,
        identity: &Identity<C>,
    ) -> Result<Option<PendingLoginId>, PendingLoginError>;
}

/// A login stage that never asks for a second factor.
///
/// An application whose credentials implement [`HasSecondFactor`] must not
/// use it: it would let users with a second factor in with the first alone.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSecondFactor;

#[async_trait]
impl<C: Credentials> LoginStage<C> for NoSecondFactor {
    async fn begin(
        &self,
        _user: EntityId,
        _identity: &Identity<C>,
    ) -> Result<Option<PendingLoginId>, PendingLoginError> {
        Ok(None)
    }
}

/// A login stage that demands a [`SecondFactor`] from the users whose
/// credentials require one.
///
/// Pending logins are kept in a [`LoginStore`] for five minutes unless
/// [`TwoFactor::with_pending_ttl`] says otherwise.
#[derive(Debug)]
pub struct TwoFactor<F> {
    factor: F,
    pending: PendingLogins,
    clock: Arc<dyn Clock>,
}

impl<F> TwoFactor<F> {
    /// A stage checking `factor`, with pending logins kept in `store`.
    #[must_use]
    pub fn new(factor: F, store: Arc<dyn LoginStore>) -> Self {
        Self {
            factor,
            pending: PendingLogins::new(store, DEFAULT_PENDING_TTL),
            clock: Arc::new(SystemClock),
        }
    }

    /// Keeps pending logins for `ttl` instead of five minutes.
    #[must_use]
    pub fn with_pending_ttl(mut self, ttl: LoginTtl) -> Self {
        self.pending = self.pending.with_ttl(ttl);
        self
    }

    /// Reads the time from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub(crate) async fn take<C>(
        &self,
        id: PendingLoginId,
    ) -> Result<Option<PendingLogin<F::Challenge>>, PendingLoginError>
    where
        C: HasSecondFactor,
        F: SecondFactor<C>,
    {
        self.pending.take(id).await
    }

    pub(crate) async fn verify<C>(
        &self,
        identity: &Identity<C>,
        pending: &PendingLogin<F::Challenge>,
        response: &F::Response,
    ) -> Result<(), Rejected>
    where
        C: HasSecondFactor,
        F: SecondFactor<C>,
    {
        let credentials = identity.credentials().ok_or(Rejected::Mismatch)?;
        self.factor
            .verify(
                credentials,
                &pending.challenge,
                response,
                self.pending.store(),
            )
            .await
    }
}

#[async_trait]
impl<C, F> LoginStage<C> for TwoFactor<F>
where
    C: HasSecondFactor,
    F: SecondFactor<C>,
{
    async fn begin(
        &self,
        user: EntityId,
        identity: &Identity<C>,
    ) -> Result<Option<PendingLoginId>, PendingLoginError> {
        let Some(credentials) = identity
            .credentials()
            .filter(|credentials| credentials.requires_second_factor())
        else {
            return Ok(None);
        };
        let pending = PendingLogin {
            user,
            challenge: self.factor.challenge(credentials),
            created_at: self.clock.now(),
        };
        self.pending.start(&pending).await.map(Some)
    }
}

#[cfg(test)]
mod tests;
