//! Authentication: [`Authenticator`], [`ClaimAuthenticator`], [`LoginOutcome`]
//! and [`AuthError`].

use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use event_sauce_core::{Aggregate, AggregateRoot, EventSourcedRepository, EventStore, Repository};
use serde::{de::DeserializeOwned, Serialize};

use crate::{HashError, LoginMethod, Rejected, UserAggregate};

/// What a successful first step of a login yields.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoginOutcome<U: Aggregate> {
    /// Fully authenticated: the caller may open a session.
    Authenticated(AggregateRoot<U>),
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
}

impl<A> From<Rejected> for AuthError<A> {
    fn from(rejected: Rejected) -> Self {
        match rejected {
            Rejected::Mismatch => Self::InvalidCredentials,
            Rejected::Hash(error) => Self::Hash(error),
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
/// 5. the method verifies the attempt against the identity.
#[derive(Debug)]
pub struct ClaimAuthenticator<U, M, S> {
    repository: EventSourcedRepository<S, U>,
    store: Arc<S>,
    method: M,
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
            user: PhantomData,
        }
    }
}

#[async_trait]
impl<U, M, S> Authenticator<U> for ClaimAuthenticator<U, M, S>
where
    S: EventStore + 'static,
    U: UserAggregate + Serialize + DeserializeOwned,
    U::DeletedState: Serialize + DeserializeOwned,
    U::Event: Serialize + DeserializeOwned,
    M: LoginMethod<U::Credentials>,
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
        Ok(LoginOutcome::Authenticated(user))
    }
}
