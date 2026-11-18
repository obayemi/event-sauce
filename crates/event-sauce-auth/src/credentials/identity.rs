//! The [`Identity`] of a user aggregate.

use serde::{Deserialize, Serialize};

use crate::{Credentials, Fingerprint};

/// Why an identity transition was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    /// The identity already holds credentials.
    #[error("identity is already registered")]
    AlreadyRegistered,
}

/// How a user identifies and authenticates at login.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{Credentials, Fingerprint, Identity, IdentityError};
/// use event_sauce_core::AggregateClaim;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
/// struct ApiKey(String);
///
/// impl Credentials for ApiKey {
///     fn claims(&self) -> Vec<AggregateClaim> {
///         vec![AggregateClaim::new("app.api_key", self.0.clone().into())]
///     }
///
///     fn fingerprint(&self, fingerprint: &mut Fingerprint) {
///         fingerprint.str_field(&self.0);
///     }
/// }
///
/// let mut identity = Identity::Anonymous;
/// identity.register(ApiKey("k1".into()))?;
/// assert_eq!(identity.credentials(), Some(&ApiKey("k1".into())));
/// assert_eq!(
///     identity.register(ApiKey("k2".into())),
///     Err(IdentityError::AlreadyRegistered)
/// );
/// # Ok::<(), IdentityError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "C: Credentials", deserialize = "C: Credentials"))]
pub enum Identity<C: Credentials> {
    /// Holds a session, owns data, cannot log in with anything.
    Anonymous,
    /// Can log in with what `C` holds.
    Registered(C),
}

impl<C: Credentials> Identity<C> {
    /// Moves an anonymous identity to registered.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::AlreadyRegistered`] when the identity
    /// already holds credentials; the identity is left untouched.
    pub fn register(&mut self, credentials: C) -> Result<(), IdentityError> {
        match self {
            Self::Anonymous => {
                *self = Self::Registered(credentials);
                Ok(())
            }
            Self::Registered(_) => Err(IdentityError::AlreadyRegistered),
        }
    }

    /// The credentials, or `None` while anonymous.
    #[must_use]
    pub fn credentials(&self) -> Option<&C> {
        match self {
            Self::Anonymous => None,
            Self::Registered(credentials) => Some(credentials),
        }
    }

    /// SHA-256 over what grants access.
    ///
    /// Any credential change, and registering itself, changes the result,
    /// so every session opened before the change stops being valid.
    #[must_use]
    pub fn session_fingerprint(&self) -> [u8; 32] {
        let mut fingerprint = Fingerprint::new();
        if let Self::Registered(credentials) = self {
            fingerprint.str_field("registered");
            credentials.fingerprint(&mut fingerprint);
        } else {
            fingerprint.str_field("anonymous");
        }
        fingerprint.finish()
    }
}

#[cfg(test)]
mod tests;
