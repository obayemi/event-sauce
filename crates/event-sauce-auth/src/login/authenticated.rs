//! [`Authenticated`], the proof that a whole login process ended.

use std::{fmt, sync::Arc};

use event_sauce_core::AggregateRoot;

use crate::UserAggregate;

/// A user who went through a whole login process.
///
/// There is no public constructor, no `Default` and no `Deserialize`:
/// [`Login::finish`](crate::Login::finish) on a login in a
/// [`Complete`](crate::Complete) state is the only way to get one. A session
/// is opened from it.
pub struct Authenticated<U: UserAggregate> {
    user: Arc<AggregateRoot<U>>,
    fingerprint: [u8; 32],
}

impl<U: UserAggregate> Authenticated<U> {
    pub(crate) fn new(user: Arc<AggregateRoot<U>>, fingerprint: [u8; 32]) -> Self {
        Self { user, fingerprint }
    }

    /// The user who logged in.
    #[must_use]
    pub fn user(&self) -> &Arc<AggregateRoot<U>> {
        &self.user
    }

    /// The user who logged in, by value.
    #[must_use]
    pub fn into_user(self) -> Arc<AggregateRoot<U>> {
        self.user
    }

    /// The fingerprint a session is opened with: it ends the session when
    /// the credentials change.
    #[must_use]
    pub fn session_fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}

impl<U: UserAggregate> fmt::Debug for Authenticated<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Authenticated")
            .field("user", &self.user.entity_id())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
