//! The first step of a login: a [`LoginMethod`](crate::LoginMethod) paired
//! with a [`Route`].

use crate::{
    Advance, Credentials, Done, Entered, Federated, Login, LoginId, Password, PasswordOrFederated,
    UserAggregate,
};

/// Picks, per user, where a login goes once the first factor passed.
pub trait Route<U: UserAggregate>: Send + Sync {
    /// The states the login may go to: one `Login<U, T>`, or an application
    /// enum of them.
    type Next: Send;

    /// Moves the login out of [`Entered`].
    fn route(&self, entered: Advance<U, Entered>) -> Self::Next;
}

/// A login method and the route its logins take.
///
/// [`Logins::begin`](crate::Logins::begin) runs the method, then the route.
#[derive(Debug, Clone)]
pub struct FirstFactor<M, R> {
    method: M,
    route: R,
}

impl<M, R> FirstFactor<M, R> {
    /// A first factor checking attempts with `method`, then routing the
    /// login with `route`.
    #[must_use]
    pub fn new(method: M, route: R) -> Self {
        Self { method, route }
    }

    pub(crate) fn method(&self) -> &M {
        &self.method
    }

    pub(crate) fn route(&self) -> &R {
        &self.route
    }
}

/// Credentials that hold nothing beyond the first factor.
///
/// This is an assertion by the credentials type: [`AlwaysDone`] only routes
/// users whose credentials make it, so a type that can also hold a second
/// factor must not.
pub trait SingleFactor: Credentials {}

impl<L: LoginId> SingleFactor for Password<L> {}

impl SingleFactor for Federated {}

impl<L: LoginId> SingleFactor for PasswordOrFederated<L> {}

/// The route of logins with a single factor: straight to [`Done`].
#[derive(Debug, Clone, Copy, Default)]
pub struct AlwaysDone;

impl<U> Route<U> for AlwaysDone
where
    U: UserAggregate,
    U::Credentials: SingleFactor,
{
    type Next = Login<U, Done>;

    fn route(&self, entered: Advance<U, Entered>) -> Login<U, Done> {
        entered.to(Done)
    }
}

#[cfg(test)]
mod tests;
