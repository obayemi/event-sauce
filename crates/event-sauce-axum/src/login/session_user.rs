//! [`SessionUser`]: the user type `axum-login` keeps in a session.

use std::fmt;
use std::sync::Arc;

use axum_login::AuthUser;
use event_sauce_auth::{Authenticated, UserAggregate};
use event_sauce_core::{AggregateRoot, EntityId};

/// A user aggregate as `axum-login` sees it.
///
/// It shares the loaded aggregate behind an [`Arc`], so a request never
/// loads a user twice, and it carries the identity's session fingerprint as
/// its session auth hash. Any credential change alters the fingerprint, so
/// every session opened before the change stops resolving to a user.
///
/// It is `pub(crate)` for the reason given on
/// [`Backend`](crate::login::backend::Backend). It is built from an
/// [`Authenticated`] user by a login, or loaded by the backend to rehydrate a
/// session.
pub(crate) struct SessionUser<U: UserAggregate> {
    user: Arc<AggregateRoot<U>>,
    fingerprint: [u8; 32],
}

impl<U: UserAggregate> SessionUser<U> {
    pub(crate) fn user(&self) -> &Arc<AggregateRoot<U>> {
        &self.user
    }

    pub(crate) fn from_loaded(user: Arc<AggregateRoot<U>>) -> Self {
        let fingerprint = user.identity().session_fingerprint();
        Self { user, fingerprint }
    }
}

impl<U: UserAggregate> From<Authenticated<U>> for SessionUser<U> {
    fn from(authenticated: Authenticated<U>) -> Self {
        Self {
            fingerprint: authenticated.session_fingerprint(),
            user: authenticated.into_user(),
        }
    }
}

impl<U: UserAggregate> Clone for SessionUser<U> {
    fn clone(&self) -> Self {
        Self {
            user: Arc::clone(&self.user),
            fingerprint: self.fingerprint,
        }
    }
}

impl<U: UserAggregate> fmt::Debug for SessionUser<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionUser")
            .field("id", &self.user.entity_id())
            .finish_non_exhaustive()
    }
}

impl<U: UserAggregate> AuthUser for SessionUser<U> {
    type Id = EntityId;

    fn id(&self) -> EntityId {
        self.user.entity_id()
    }

    fn session_auth_hash(&self) -> &[u8] {
        &self.fingerprint
    }
}

#[cfg(test)]
#[path = "tests/session_user_tests.rs"]
mod tests;
