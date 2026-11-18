//! Logins held between the first and the second factor.

use std::{fmt, str::FromStr, sync::Arc};

use chrono::{DateTime, Utc};
use event_sauce_core::EntityId;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

use crate::{LoginKey, LoginStore, LoginStoreError, LoginTtl};

const NAMESPACE: &str = "pending";

/// The unguessable handle of a login waiting for its second factor.
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

/// A login that passed its first factor and awaits the second.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingLogin<Ch> {
    /// The user the first factor authenticated.
    pub user: EntityId,
    /// The challenge the second factor issued.
    pub challenge: Ch,
    /// When the first factor passed.
    pub created_at: DateTime<Utc>,
}

/// Why a pending login could not be stored or read back.
#[derive(Debug, thiserror::Error)]
pub enum PendingLoginError {
    /// The login store failed.
    #[error(transparent)]
    Store(#[from] LoginStoreError),
    /// A stored entry is not a pending login.
    #[error("stored pending login is malformed: {0}")]
    Encoding(#[from] serde_json::Error),
}

/// Typed access to the pending logins kept in a [`LoginStore`].
///
/// Every entry is single use and expires after the configured time to live.
#[derive(Clone)]
pub struct PendingLogins {
    store: Arc<dyn LoginStore>,
    ttl: LoginTtl,
}

impl fmt::Debug for PendingLogins {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingLogins")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl PendingLogins {
    /// Pending logins kept in `store`, each for `ttl`.
    #[must_use]
    pub fn new(store: Arc<dyn LoginStore>, ttl: LoginTtl) -> Self {
        Self { store, ttl }
    }

    /// The same pending logins, kept for `ttl` instead.
    #[must_use]
    pub fn with_ttl(mut self, ttl: LoginTtl) -> Self {
        self.ttl = ttl;
        self
    }

    /// The underlying login store.
    #[must_use]
    pub fn store(&self) -> &dyn LoginStore {
        &*self.store
    }

    /// Stores `pending` and returns the id that resumes it.
    ///
    /// # Errors
    ///
    /// Fails when the login cannot be encoded or the store fails.
    pub async fn start<Ch: Serialize + Sync>(
        &self,
        pending: &PendingLogin<Ch>,
    ) -> Result<PendingLoginId, PendingLoginError> {
        let id = PendingLoginId::new();
        let bytes = serde_json::to_vec(pending)?;
        self.store
            .put(&LoginKey::new(NAMESPACE, id), &bytes, self.ttl)
            .await?;
        Ok(id)
    }

    /// Reads and deletes the pending login `id`.
    ///
    /// Returns `None` when it is unknown, expired or already taken.
    ///
    /// # Errors
    ///
    /// Fails when the store fails or holds something that is not a pending
    /// login.
    pub async fn take<Ch: DeserializeOwned>(
        &self,
        id: PendingLoginId,
    ) -> Result<Option<PendingLogin<Ch>>, PendingLoginError> {
        let Some(bytes) = self.store.take(&LoginKey::new(NAMESPACE, id)).await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }
}

#[cfg(test)]
mod tests;
