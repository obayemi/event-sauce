//! The ways a login can fail to start, resume, finish or sign up.

use event_sauce_core::Error;

use crate::{HashError, LoginStoreError, PendingLoginError, Rejected};

/// Why an attempt did not start a login.
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
    Store(#[from] Error),
    /// Hashing failed.
    #[error(transparent)]
    Hash(#[from] HashError),
    /// The login store failed while the attempt was being checked.
    #[error(transparent)]
    LoginStore(#[from] LoginStoreError),
    /// The attempt could not be checked at all.
    #[error("the attempt could not be checked")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl<A> From<Rejected> for AuthError<A> {
    fn from(rejected: Rejected) -> Self {
        match rejected {
            Rejected::Mismatch => Self::InvalidCredentials,
            Rejected::Hash(error) => Self::Hash(error),
            Rejected::LoginStore(error) => Self::LoginStore(error),
            Rejected::Unavailable(error) => Self::Unavailable(error),
        }
    }
}

/// Why a login did not finish.
#[derive(Debug, thiserror::Error)]
pub enum FinishError {
    /// The user is gone or may no longer log in.
    #[error("account inactive")]
    Inactive,
    /// The user's credentials changed since the first factor passed.
    #[error("credentials changed during the login")]
    CredentialsChanged,
    /// The user store failed.
    #[error(transparent)]
    Store(#[from] Error),
}

/// Why a pending login did not resume.
#[derive(Debug, thiserror::Error)]
pub enum ResumeError {
    /// No login is waiting in that state: the id is unknown, expired,
    /// already used, or the login was saved in another state.
    #[error("no login is in progress")]
    NotInProgress,
    /// The user is gone or may no longer log in.
    #[error("account inactive")]
    Inactive,
    /// The user's credentials changed since the first factor passed.
    #[error("credentials changed during the login")]
    CredentialsChanged,
    /// The user store failed.
    #[error(transparent)]
    Store(#[from] Error),
    /// The login store failed or held something that is not a pending login.
    #[error(transparent)]
    Pending(#[from] PendingLoginError),
}

/// Why a new user could not be signed up.
#[derive(Debug, thiserror::Error)]
pub enum SignUpError {
    /// The aggregate is not new: it was loaded from the store, holds no
    /// event, or its id already exists.
    #[error("the user is not new")]
    NotNew,
    /// The user store failed. A claim conflict surfaces here, through
    /// [`Error::as_claim_conflict`].
    #[error(transparent)]
    Store(#[from] Error),
}

#[cfg(test)]
mod tests;
