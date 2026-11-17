//! [`SessionUser`]: the user type `axum-login` keeps in a session.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use axum_login::AuthUser;
use event_sauce_auth::UserAggregate;
use event_sauce_core::{AggregateRoot, EntityId};

/// A user aggregate as `axum-login` sees it.
///
/// It shares the loaded aggregate behind an [`Arc`], so a request never
/// loads a user twice, and it carries the identity's session fingerprint as
/// its session auth hash. Any credential change alters the fingerprint, so
/// every session opened before the change stops resolving to a user.
///
/// Build one from the aggregate a login produced, then hand it to
/// `AuthSession::login`:
///
/// ```
/// use event_sauce_axum::SessionUser;
/// use event_sauce_auth::UserAggregate;
/// use event_sauce_core::AggregateRoot;
///
/// fn session_user<U: UserAggregate>(root: AggregateRoot<U>) -> SessionUser<U> {
///     SessionUser::from(root)
/// }
/// ```
pub struct SessionUser<U: UserAggregate> {
    user: Arc<AggregateRoot<U>>,
    fingerprint: [u8; 32],
}

impl<U: UserAggregate> SessionUser<U> {
    /// The shared aggregate.
    #[must_use]
    pub fn user(&self) -> &Arc<AggregateRoot<U>> {
        &self.user
    }

    /// The shared aggregate, consuming the session user.
    #[must_use]
    pub fn into_user(self) -> Arc<AggregateRoot<U>> {
        self.user
    }
}

impl<U: UserAggregate> From<Arc<AggregateRoot<U>>> for SessionUser<U> {
    fn from(user: Arc<AggregateRoot<U>>) -> Self {
        let fingerprint = user.identity().session_fingerprint();
        Self { user, fingerprint }
    }
}

impl<U: UserAggregate> From<AggregateRoot<U>> for SessionUser<U> {
    fn from(user: AggregateRoot<U>) -> Self {
        Self::from(Arc::new(user))
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

impl<U: UserAggregate> Deref for SessionUser<U> {
    type Target = AggregateRoot<U>;

    fn deref(&self) -> &AggregateRoot<U> {
        &self.user
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
mod tests;
