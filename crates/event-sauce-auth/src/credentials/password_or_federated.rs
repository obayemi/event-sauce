//! The [`PasswordOrFederated`] credentials.

use event_sauce_core::AggregateClaim;
use serde::{Deserialize, Serialize};

use crate::{
    Credentials, Federated, FederatedId, HasFederated, HasPassword, LoginId, LoginMethodError,
    Password, Provider, Sha256,
};

/// Credentials that log in with a password, with federated links, or with
/// both.
///
/// The three variants make a registered user with no way to log in
/// unrepresentable.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{
///     Email, FederatedId, HasPassword, LoginId, LoginMethodError, Password, PasswordHash,
///     PasswordOrFederated, Provider,
/// };
///
/// let phc = "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";
/// let password = Password::new(Email::parse("ada@example.com")?, PasswordHash::new(phc)?);
/// let mut credentials = PasswordOrFederated::Password(password);
/// assert_eq!(
///     credentials.remove_password(),
///     Err(LoginMethodError::LastLoginMethod)
/// );
/// credentials.link(FederatedId::new(Provider::GOOGLE, "g-1"))?;
/// credentials.remove_password()?;
/// assert!(credentials.password().is_none());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "L: LoginId", deserialize = "L: LoginId"))]
pub enum PasswordOrFederated<L: LoginId> {
    /// Logs in with a password only.
    Password(Password<L>),
    /// Logs in through federated links only.
    Federated(Federated),
    /// Logs in with either.
    Both(Password<L>, Federated),
}

impl<L: LoginId> PasswordOrFederated<L> {
    /// Adds a password or replaces the current one; links are kept.
    pub fn set_password(&mut self, password: Password<L>) {
        match self {
            Self::Password(current) | Self::Both(current, _) => *current = password,
            Self::Federated(links) => *self = Self::Both(password, links.clone()),
        }
    }

    /// Removes the password; links are kept.
    ///
    /// # Errors
    ///
    /// Returns [`LoginMethodError::LastLoginMethod`] when no federated
    /// link remains to log in with.
    pub fn remove_password(&mut self) -> Result<(), LoginMethodError> {
        match self {
            Self::Password(_) => Err(LoginMethodError::LastLoginMethod),
            Self::Federated(_) => Ok(()),
            Self::Both(_, links) => {
                *self = Self::Federated(links.clone());
                Ok(())
            }
        }
    }

    /// Links a federated identity; a password-only user keeps the password.
    ///
    /// # Errors
    ///
    /// Returns [`LoginMethodError::AlreadyLinked`] when `id` is already
    /// linked.
    pub fn link(&mut self, id: FederatedId) -> Result<(), LoginMethodError> {
        match self {
            Self::Federated(links) | Self::Both(_, links) => links.link(id),
            Self::Password(password) => {
                *self = Self::Both(password.clone(), Federated::new(id));
                Ok(())
            }
        }
    }

    /// Unlinks exactly the identity `id`.
    ///
    /// # Errors
    ///
    /// Returns [`LoginMethodError::NotLinked`] when `id` is not linked, and
    /// [`LoginMethodError::LastLoginMethod`] when that would leave no way to
    /// log in.
    pub fn unlink(&mut self, id: &FederatedId) -> Result<(), LoginMethodError> {
        self.unlink_with(|links| links.unlink(id))
    }

    /// Unlinks every identity at `provider`.
    ///
    /// # Errors
    ///
    /// Returns [`LoginMethodError::NotLinked`] when nothing is linked at
    /// `provider`, and [`LoginMethodError::LastLoginMethod`] when that
    /// would leave no way to log in.
    pub fn unlink_provider(&mut self, provider: &Provider) -> Result<(), LoginMethodError> {
        self.unlink_with(|links| links.unlink_provider(provider))
    }

    fn unlink_with(
        &mut self,
        unlink: impl FnOnce(&mut Federated) -> Result<(), LoginMethodError>,
    ) -> Result<(), LoginMethodError> {
        match self {
            Self::Password(_) => Err(LoginMethodError::NotLinked),
            Self::Federated(links) => unlink(links),
            Self::Both(password, links) => match unlink(links) {
                Err(LoginMethodError::LastLoginMethod) => {
                    *self = Self::Password(password.clone());
                    Ok(())
                }
                result => result,
            },
        }
    }
}

impl<L: LoginId> Credentials for PasswordOrFederated<L> {
    fn claims(&self) -> Vec<AggregateClaim> {
        let password = self.password().map(Credentials::claims);
        let federated = self.federated().map(Credentials::claims);
        password.into_iter().chain(federated).flatten().collect()
    }

    fn fingerprint(&self, digest: &mut Sha256) {
        if let Some(password) = self.password() {
            password.fingerprint(digest);
        }
        if let Some(federated) = self.federated() {
            federated.fingerprint(digest);
        }
    }
}

impl<L: LoginId> HasPassword for PasswordOrFederated<L> {
    type Login = L;

    fn password(&self) -> Option<&Password<L>> {
        match self {
            Self::Password(password) | Self::Both(password, _) => Some(password),
            Self::Federated(_) => None,
        }
    }
}

impl<L: LoginId> HasFederated for PasswordOrFederated<L> {
    fn federated(&self) -> Option<&Federated> {
        match self {
            Self::Federated(links) | Self::Both(_, links) => Some(links),
            Self::Password(_) => None,
        }
    }
}

#[cfg(test)]
mod tests;
