//! The [`UserAggregate`] trait.

use event_sauce_core::Aggregate;

use crate::{Credentials, Identity};

/// An aggregate that represents a user who can log in.
pub trait UserAggregate: Aggregate {
    /// The credentials this user can hold.
    type Credentials: Credentials;

    /// How this user identifies at login.
    fn identity(&self) -> &Identity<Self::Credentials>;

    /// Whether this user may log in; inactive users are refused by every
    /// login method. Defaults to always active.
    fn is_active(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
