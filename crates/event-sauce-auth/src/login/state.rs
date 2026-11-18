//! The states a login moves through and the steps between them.

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{Advance, LoginTtl, Rejected, UserAggregate};

/// A state a login can be in.
///
/// A state is plain data, serialised with the login while the client is
/// away.
pub trait LoginState: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Unique among the application's states: a saved login only resumes in
    /// the state whose tag it carries.
    const TAG: &'static str;

    /// How long a login saved in this state waits for the client.
    const TTL: LoginTtl = LoginTtl::FIVE_MINUTES;
}

/// A state that ends the login process: [`Login::finish`](crate::Login::finish)
/// exists only for these.
pub trait Complete: LoginState {}

/// The only way out of the state `Self`.
///
/// A step receives an [`Advance`], the right to move one login out of `Self`,
/// and a proof checked by its verifier. It returns where the login goes
/// next.
#[async_trait]
pub trait Step<U: UserAggregate>: LoginState {
    /// What the client sends: a code, an enum of alternatives, nothing.
    type Proof: Send;

    /// The service that checks the proof: a TOTP checker, a mailer, a store of
    /// device links.
    type Verifier: Send + Sync;

    /// Where the login may go: one `Login<U, T>`, or an application enum of
    /// them, picked per user.
    type Next: Send;

    /// Checks `proof` and moves the login on.
    ///
    /// # Errors
    ///
    /// Returns [`Rejected`] when the proof does not hold, or could not be
    /// checked.
    async fn verify(
        advance: Advance<U, Self>,
        verifier: &Self::Verifier,
        proof: Self::Proof,
    ) -> Result<Self::Next, Rejected>;
}

/// The ready-made [`Complete`] state: nothing is left to prove.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Done;

impl LoginState for Done {
    const TAG: &'static str = "event_sauce.done";
}

impl Complete for Done {}

/// Where a login stands after the first factor, before its route picked a
/// state.
///
/// It is not a [`LoginState`], so a login in it can be neither saved nor
/// finished.
#[derive(Debug)]
pub struct Entered;

#[cfg(test)]
mod tests;
