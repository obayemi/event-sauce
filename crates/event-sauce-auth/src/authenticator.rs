//! Authentication: [`Authenticator`], [`ClaimAuthenticator`], [`LoginOutcome`]
//! and [`AuthError`].

use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use event_sauce_core::{Aggregate, AggregateRoot, EventSourcedRepository, EventStore, Repository};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    HasSecondFactor, HashError, LoginMethod, LoginStage, NoSecondFactor, PendingLoginError,
    PendingLoginId, Rejected, SecondFactor, TwoFactor, UserAggregate,
};

/// What a successful first step of a login yields.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoginOutcome<U: Aggregate> {
    /// Fully authenticated: the caller may open a session.
    Authenticated(AggregateRoot<U>),
    /// First factor accepted, a second one is required.
    ///
    /// No session exists yet. The caller sends the client to prove the
    /// second factor and passes the id to [`ClaimAuthenticator::complete`].
    SecondFactorRequired(PendingLoginId),
}

/// Why an attempt did not authenticate.
#[derive(Debug, thiserror::Error)]
pub enum AuthError<A> {
    /// The attempt does not prove the identity it names.
    #[error("invalid credentials")]
    InvalidCredentials,
    /// The user exists but may not log in.
    #[error("account inactive")]
    Inactive,
    /// No user matches the attempt.
    ///
    /// The attempt is handed back so the app can apply its own policy: link
    /// a federated subject to a user by email, create a user, or report
    /// [`AuthError::InvalidCredentials`] for a password attempt.
    #[error("no user for this login")]
    Unknown {
        /// The attempt no user matched.
        attempt: A,
    },
    /// The user store failed.
    #[error(transparent)]
    Store(#[from] event_sauce_core::Error),
    /// Hashing failed.
    #[error(transparent)]
    Hash(#[from] HashError),
    /// The login store failed while the login was being held or checked.
    #[error(transparent)]
    Pending(#[from] PendingLoginError),
}

impl<A> From<Rejected> for AuthError<A> {
    fn from(rejected: Rejected) -> Self {
        match rejected {
            Rejected::Mismatch => Self::InvalidCredentials,
            Rejected::Hash(error) => Self::Hash(error),
            Rejected::LoginStore(error) => Self::Pending(error.into()),
        }
    }
}

/// Why a pending login did not complete.
#[derive(Debug, thiserror::Error)]
pub enum CompleteError {
    /// No such login is waiting: it is unknown, expired or already used.
    #[error("no login is in progress")]
    NotInProgress,
    /// The user may no longer log in.
    #[error("account inactive")]
    Inactive,
    /// The response does not prove the second factor.
    ///
    /// The pending login is used up: the user must start over.
    #[error("invalid second factor response")]
    InvalidResponse,
    /// The user store failed.
    #[error(transparent)]
    Store(#[from] event_sauce_core::Error),
    /// Hashing failed.
    #[error(transparent)]
    Hash(#[from] HashError),
    /// The login store failed.
    #[error(transparent)]
    Pending(#[from] PendingLoginError),
}

impl From<Rejected> for CompleteError {
    fn from(rejected: Rejected) -> Self {
        match rejected {
            Rejected::Mismatch => Self::InvalidResponse,
            Rejected::Hash(error) => Self::Hash(error),
            Rejected::LoginStore(error) => Self::Pending(error.into()),
        }
    }
}

/// Turns a login attempt into an authenticated user.
#[async_trait]
pub trait Authenticator<U: UserAggregate>: Send + Sync {
    /// What the client submits.
    type Attempt: Send + Sync;

    /// Authenticates `attempt`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the attempt does not authenticate a user.
    async fn authenticate(
        &self,
        attempt: Self::Attempt,
    ) -> Result<LoginOutcome<U>, AuthError<Self::Attempt>>;
}

/// The generic [`Authenticator`]: it finds the user by claim, loads it and
/// lets a [`LoginMethod`] verify the attempt.
///
/// Every attempt goes through the same steps:
///
/// 1. the method locates the user;
/// 2. when nobody matches, the method's `on_unknown` hook runs and the
///    attempt comes back in [`AuthError::Unknown`];
/// 3. the user is loaded, which decrypts it and replays from its snapshot;
/// 4. an inactive user is refused, whatever the method;
/// 5. the method verifies the attempt against the identity;
/// 6. when the credentials require a second factor, the login is held and
///    [`LoginOutcome::SecondFactorRequired`] comes back, otherwise the user
///    is authenticated.
///
/// Step 6 only happens once [`ClaimAuthenticator::with_second_factor`] has
/// been called. [`ClaimAuthenticator::complete`] finishes a held login.
#[derive(Debug)]
pub struct ClaimAuthenticator<U, M, S, T = NoSecondFactor> {
    repository: EventSourcedRepository<S, U>,
    store: Arc<S>,
    method: M,
    stage: T,
    user: PhantomData<fn() -> U>,
}

impl<U, M, S> ClaimAuthenticator<U, M, S>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
    M: LoginMethod<U::Credentials>,
{
    /// An authenticator for the users of `store`, verifying attempts with
    /// `method`.
    #[must_use]
    pub fn new(store: Arc<S>, method: M) -> Self {
        Self {
            repository: EventSourcedRepository::new(store.clone()),
            store,
            method,
            stage: NoSecondFactor,
            user: PhantomData,
        }
    }
}

impl<U, M, S> ClaimAuthenticator<U, M, S> {
    /// Holds the logins whose credentials require a second factor, which
    /// `stage` checks.
    ///
    /// Only credentials that implement [`HasSecondFactor`] can take one.
    #[must_use]
    pub fn with_second_factor<F>(
        self,
        stage: TwoFactor<F>,
    ) -> ClaimAuthenticator<U, M, S, TwoFactor<F>>
    where
        U: UserAggregate,
        U::Credentials: HasSecondFactor,
        F: SecondFactor<U::Credentials>,
    {
        ClaimAuthenticator {
            repository: self.repository,
            store: self.store,
            method: self.method,
            stage,
            user: PhantomData,
        }
    }
}

impl<U, M, S, F> ClaimAuthenticator<U, M, S, TwoFactor<F>>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned,
    U::Credentials: HasSecondFactor,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
    F: SecondFactor<U::Credentials>,
{
    /// Completes the login held under `pending` with the client's `response`
    /// to the second factor.
    ///
    /// The pending login is used up as soon as it is read, so every id gets
    /// one attempt and a wrong response ends the login. The user is reloaded
    /// and must still be active.
    ///
    /// # Errors
    ///
    /// Returns [`CompleteError`] when the login is not in progress, the user
    /// is inactive, or the response does not prove the second factor.
    pub async fn complete(
        &self,
        pending: PendingLoginId,
        response: &F::Response,
    ) -> Result<AggregateRoot<U>, CompleteError> {
        let held = self
            .stage
            .take::<U::Credentials>(pending)
            .await?
            .ok_or(CompleteError::NotInProgress)?;
        let user = self.repository.load_by_id(held.user).await?;
        if !user.is_active() {
            return Err(CompleteError::Inactive);
        }
        self.stage.verify(user.identity(), &held, response).await?;
        Ok(user)
    }
}

#[async_trait]
impl<U, M, S, T> Authenticator<U> for ClaimAuthenticator<U, M, S, T>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
    M: LoginMethod<U::Credentials>,
    T: LoginStage<U::Credentials>,
{
    type Attempt = M::Attempt;

    async fn authenticate(
        &self,
        attempt: Self::Attempt,
    ) -> Result<LoginOutcome<U>, AuthError<Self::Attempt>> {
        let Some(id) = self.method.locate(&*self.store, &attempt).await? else {
            self.method.on_unknown(&attempt).await;
            return Err(AuthError::Unknown { attempt });
        };
        let user = self.repository.load_by_id(id).await?;
        if !user.is_active() {
            return Err(AuthError::Inactive);
        }
        self.method.verify(user.identity(), &attempt).await?;
        match self.stage.begin(user.entity_id(), user.identity()).await? {
            Some(pending) => Ok(LoginOutcome::SecondFactorRequired(pending)),
            None => Ok(LoginOutcome::Authenticated(user)),
        }
    }
}

#[cfg(test)]
mod tests;
