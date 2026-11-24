//! Logins kept between two requests.

use std::{fmt, str::FromStr, sync::Arc};

use chrono::{DateTime, Utc};
use event_sauce_core::EntityId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    login::users::Users, Login, LoginKey, LoginState, LoginStore, LoginStoreError, ResumeError,
    UserAggregate,
};

const NAMESPACE: &str = "pending";

/// The unguessable handle of a login waiting for the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PendingLoginId(Uuid);

impl PendingLoginId {
    /// A fresh random id.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for PendingLoginId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for PendingLoginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for PendingLoginId {
    type Err = uuid::Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.parse().map(Self)
    }
}

/// Why a pending login could not be stored or read back.
#[derive(Debug, thiserror::Error)]
pub enum PendingLoginError {
    /// The login store failed.
    #[error(transparent)]
    Store(#[from] LoginStoreError),
    /// A stored entry is not a pending login, or a state does not encode.
    #[error("stored pending login is malformed: {0}")]
    Encoding(#[from] serde_json::Error),
}

/// The wire format of a pending login, borrowed to write and owned to read.
#[derive(Serialize, Deserialize)]
struct Record<Tag, State> {
    tag: Tag,
    user: EntityId,
    fingerprint: [u8; 32],
    started_at: DateTime<Utc>,
    state: State,
}

/// Logins kept in a [`LoginStore`] while the client is away.
///
/// A saved login is single use, expires after the time to live of its state,
/// and resumes only in the state it was saved in.
pub struct PendingLogins<U: UserAggregate> {
    store: Arc<dyn LoginStore>,
    users: Arc<dyn Users<U>>,
}

impl<U: UserAggregate> Clone for PendingLogins<U> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            users: self.users.clone(),
        }
    }
}

impl<U: UserAggregate> fmt::Debug for PendingLogins<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingLogins").finish_non_exhaustive()
    }
}

impl<U: UserAggregate> PendingLogins<U> {
    pub(crate) fn new(store: Arc<dyn LoginStore>, users: Arc<dyn Users<U>>) -> Self {
        Self { store, users }
    }

    /// Stores `login` for the time to live of its state and returns the id
    /// the client keeps.
    ///
    /// # Errors
    ///
    /// Fails when the state cannot be encoded or the login store fails.
    pub async fn save<S: LoginState>(
        &self,
        login: Login<U, S>,
    ) -> Result<PendingLoginId, PendingLoginError> {
        let id = PendingLoginId::new();
        let bytes = serde_json::to_vec(&Record {
            tag: S::TAG,
            user: login.user().entity_id(),
            fingerprint: login.fingerprint(),
            started_at: login.started_at(),
            state: login.state(),
        })?;
        self.store
            .put(&LoginKey::new(NAMESPACE, id), &bytes, S::TTL)
            .await?;
        Ok(id)
    }

    /// Takes the login `id` back, single use, if it was saved in the state
    /// `S`.
    ///
    /// 1. The entry is taken: unknown, expired or already taken is
    ///    [`ResumeError::NotInProgress`].
    /// 2. Its tag must be `S::TAG`. A login saved in another state is
    ///    [`ResumeError::NotInProgress`] too, and is used up: a client cannot
    ///    replay it at another step.
    /// 3. The user is loaded again: gone or inactive is
    ///    [`ResumeError::Inactive`].
    /// 4. The session fingerprint must match the one pinned at the first
    ///    factor, otherwise [`ResumeError::CredentialsChanged`].
    ///
    /// # Errors
    ///
    /// Returns [`ResumeError`] as above, and when a store fails.
    pub async fn resume<S: LoginState>(
        &self,
        id: PendingLoginId,
    ) -> Result<Login<U, S>, ResumeError> {
        let key = LoginKey::new(NAMESPACE, id);
        let bytes = self
            .store
            .take(&key)
            .await
            .map_err(PendingLoginError::from)?
            .ok_or(ResumeError::NotInProgress)?;
        let stored: Record<String, serde_json::Value> =
            serde_json::from_slice(&bytes).map_err(PendingLoginError::from)?;
        if stored.tag != S::TAG {
            return Err(ResumeError::NotInProgress);
        }
        let state = serde_json::from_value(stored.state).map_err(PendingLoginError::from)?;
        let user = self
            .users
            .find(stored.user)
            .await?
            .confirm(stored.fingerprint)?;
        Ok(Login::resumed(
            user,
            stored.fingerprint,
            stored.started_at,
            self.users.clone(),
            state,
        ))
    }
}

#[cfg(test)]
#[path = "tests/pending_tests.rs"]
mod tests;
