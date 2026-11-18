//! Federated login: [`Provider`], [`FederatedId`] and [`Federated`].

use std::{borrow::Cow, fmt};

use event_sauce_core::AggregateClaim;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{credentials::feed, Credentials, HasFederated, LoginMethodError, NonEmpty, Sha256};

/// The identity provider a federated login goes through.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::Provider;
///
/// assert_eq!(Provider::GOOGLE.as_str(), "google");
/// assert_eq!(Provider::from("okta".to_owned()).as_str(), "okta");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Provider(Cow<'static, str>);

impl Provider {
    /// Google.
    pub const GOOGLE: Self = Self::new("google");
    /// GitHub.
    pub const GITHUB: Self = Self::new("github");
    /// Apple.
    pub const APPLE: Self = Self::new("apple");
    /// Microsoft.
    pub const MICROSOFT: Self = Self::new("microsoft");

    /// A provider with a fixed name.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }

    /// The provider name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for Provider {
    fn from(name: String) -> Self {
        Self(Cow::Owned(name))
    }
}

impl fmt::Display for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A user's identity at a provider: who the provider says they are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FederatedId {
    provider: Provider,
    subject: String,
}

impl FederatedId {
    /// The claim type federated identities are made unique under.
    pub const CLAIM_TYPE: &'static str = "auth.federated";

    /// The identity `subject` at `provider`.
    #[must_use]
    pub fn new(provider: Provider, subject: impl Into<String>) -> Self {
        Self {
            provider,
            subject: subject.into(),
        }
    }

    /// The provider.
    #[must_use]
    pub fn provider(&self) -> &Provider {
        &self.provider
    }

    /// The provider's stable identifier for the user.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// The claim that makes this identity unique across users.
    #[must_use]
    pub fn claim(&self) -> AggregateClaim {
        AggregateClaim::new(
            Self::CLAIM_TYPE,
            json!([self.provider.as_str(), self.subject]),
        )
    }
}

/// The federated identities linked to a user: at least one, never repeated.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{Federated, FederatedId, LoginMethodError, Provider};
///
/// let mut federated = Federated::new(FederatedId::new(Provider::GOOGLE, "g-1"));
/// federated.link(FederatedId::new(Provider::GITHUB, "h-1"))?;
/// federated.unlink(&FederatedId::new(Provider::GOOGLE, "g-1"))?;
/// assert_eq!(
///     federated.unlink_provider(&Provider::GITHUB),
///     Err(LoginMethodError::LastLoginMethod)
/// );
/// # Ok::<(), LoginMethodError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Federated(NonEmpty<FederatedId>);

impl Federated {
    /// A user linked to `first` only.
    #[must_use]
    pub fn new(first: FederatedId) -> Self {
        Self(NonEmpty::new(first))
    }

    /// The linked identities, in the order they were linked.
    #[must_use]
    pub fn links(&self) -> &[FederatedId] {
        self.0.as_slice()
    }

    /// Links another identity.
    ///
    /// # Errors
    ///
    /// Returns [`LoginMethodError::AlreadyLinked`] when `id` is already
    /// linked.
    pub fn link(&mut self, id: FederatedId) -> Result<(), LoginMethodError> {
        if self.0.push(id) {
            Ok(())
        } else {
            Err(LoginMethodError::AlreadyLinked)
        }
    }

    /// Removes exactly the link `id`.
    ///
    /// # Errors
    ///
    /// Returns [`LoginMethodError::NotLinked`] when `id` is not linked, and
    /// [`LoginMethodError::LastLoginMethod`] when it is the only link.
    pub fn unlink(&mut self, id: &FederatedId) -> Result<(), LoginMethodError> {
        self.remove_where(|link| link == id)
    }

    /// Removes every identity linked at `provider`.
    ///
    /// # Errors
    ///
    /// Returns [`LoginMethodError::NotLinked`] when nothing is linked at
    /// `provider`, and [`LoginMethodError::LastLoginMethod`] when that
    /// would remove every link.
    pub fn unlink_provider(&mut self, provider: &Provider) -> Result<(), LoginMethodError> {
        self.remove_where(|link| link.provider() == provider)
    }

    fn remove_where(
        &mut self,
        matches: impl Fn(&FederatedId) -> bool,
    ) -> Result<(), LoginMethodError> {
        if !self.0.iter().any(&matches) {
            return Err(LoginMethodError::NotLinked);
        }
        self.0 = self
            .0
            .retained(|link| !matches(link))
            .ok_or(LoginMethodError::LastLoginMethod)?;
        Ok(())
    }
}

impl Credentials for Federated {
    fn claims(&self) -> Vec<AggregateClaim> {
        self.0.iter().map(FederatedId::claim).collect()
    }

    fn fingerprint(&self, digest: &mut Sha256) {
        feed(digest, b"federated");
        for id in &self.0 {
            feed(digest, id.provider.as_str().as_bytes());
            feed(digest, id.subject.as_bytes());
        }
    }
}

impl HasFederated for Federated {
    fn federated(&self) -> Option<&Federated> {
        Some(self)
    }
}

#[cfg(test)]
mod tests;
