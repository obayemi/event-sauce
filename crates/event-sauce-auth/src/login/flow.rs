//! [`Login`], a login in progress, and [`Advance`], the right to move it.

use std::{fmt, sync::Arc};

use chrono::{DateTime, Utc};
use event_sauce_core::AggregateRoot;

use crate::{
    login::users::{Found, Users},
    Authenticated, Complete, FinishError, LoginState, Rejected, Step, UserAggregate,
};

/// A login in progress for one user, in the state `S`.
///
/// It holds the loaded user, the session fingerprint pinned when the first
/// factor passed, the start time and the state's data. There is no public
/// constructor, and it is neither `Clone` nor serialisable: a login moves
/// forward by value, once.
pub struct Login<U: UserAggregate, S> {
    user: Arc<AggregateRoot<U>>,
    fingerprint: [u8; 32],
    started_at: DateTime<Utc>,
    users: Arc<dyn Users<U>>,
    state: S,
}

impl<U: UserAggregate, S> Login<U, S> {
    pub(crate) fn new(
        user: Arc<AggregateRoot<U>>,
        started_at: DateTime<Utc>,
        users: Arc<dyn Users<U>>,
        state: S,
    ) -> Self {
        Self {
            fingerprint: user.identity().session_fingerprint(),
            user,
            started_at,
            users,
            state,
        }
    }

    pub(crate) fn resumed(
        user: Arc<AggregateRoot<U>>,
        fingerprint: [u8; 32],
        started_at: DateTime<Utc>,
        users: Arc<dyn Users<U>>,
        state: S,
    ) -> Self {
        Self {
            user,
            fingerprint,
            started_at,
            users,
            state,
        }
    }

    pub(crate) fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    /// The user this login is for.
    #[must_use]
    pub fn user(&self) -> &AggregateRoot<U> {
        &self.user
    }

    /// The data of the state the login is in.
    #[must_use]
    pub fn state(&self) -> &S {
        &self.state
    }

    /// When the first factor passed.
    #[must_use]
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
}

impl<U: UserAggregate, S: Step<U>> Login<U, S> {
    /// Checks `proof` with `verifier` and moves the login to the state the
    /// step picks.
    ///
    /// The login is consumed: a wrong proof ends it.
    ///
    /// # Errors
    ///
    /// Returns the step's [`Rejected`] when the proof does not hold.
    pub async fn prove(self, verifier: &S::Verifier, proof: S::Proof) -> Result<S::Next, Rejected> {
        S::verify(Advance(self), verifier, proof).await
    }
}

impl<U: UserAggregate, S: Complete> Login<U, S> {
    /// Ends the login.
    ///
    /// The user is read again, from the held copy when the stored version
    /// did not move, and checked: still there, still active, with the same
    /// session fingerprint as when the first factor passed.
    ///
    /// # Errors
    ///
    /// Returns [`FinishError::Inactive`] when the user is gone or inactive,
    /// [`FinishError::CredentialsChanged`] when its credentials changed, and
    /// [`FinishError::Store`] when the store fails.
    pub async fn finish(self) -> Result<Authenticated<U>, FinishError> {
        let Found::Active(user) = self.users.current(&self.user).await? else {
            return Err(FinishError::Inactive);
        };
        if user.identity().session_fingerprint() != self.fingerprint {
            return Err(FinishError::CredentialsChanged);
        }
        Ok(Authenticated::new(user, self.fingerprint))
    }
}

impl<U: UserAggregate, S: fmt::Debug> fmt::Debug for Login<U, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Login")
            .field("user", &self.user.entity_id())
            .field("started_at", &self.started_at)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// The right to move one login out of the state `S`.
///
/// Only the library builds one: [`Login::prove`] hands it to the step of
/// `S`, and [`Logins::begin`](crate::Logins::begin) to the route after the
/// first factor. A login therefore leaves a state only through that state's
/// step.
pub struct Advance<U: UserAggregate, S>(Login<U, S>);

impl<U: UserAggregate, S> Advance<U, S> {
    pub(crate) fn new(login: Login<U, S>) -> Self {
        Self(login)
    }

    /// The login about to move.
    #[must_use]
    pub fn login(&self) -> &Login<U, S> {
        &self.0
    }

    /// Moves the login to `state`.
    #[must_use]
    pub fn to<T: LoginState>(self, state: T) -> Login<U, T> {
        let Login {
            user,
            fingerprint,
            started_at,
            users,
            ..
        } = self.0;
        Login::resumed(user, fingerprint, started_at, users, state)
    }
}

impl<U: UserAggregate, S: fmt::Debug> fmt::Debug for Advance<U, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Advance").field(&self.0).finish()
    }
}

#[cfg(test)]
mod tests;
