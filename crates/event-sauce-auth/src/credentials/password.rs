//! The [`Password`] credentials.

use event_sauce_core::AggregateClaim;
use serde::{Deserialize, Serialize};

use crate::{Credentials, Fingerprint, HasPassword, LoginId, PasswordHash};

/// A login identifier and the hash of its password.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{Email, LoginId, Password, PasswordHash};
///
/// let phc = "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHRzYWx0$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaGhhc2g";
/// let mut password = Password::new(Email::parse("ada@example.com")?, PasswordHash::new(phc)?);
/// password.change_login(Email::parse("ada@example.org")?);
/// assert_eq!(password.login().as_str(), "ada@example.org");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "L: LoginId", deserialize = "L: LoginId"))]
pub struct Password<L: LoginId> {
    login: L,
    hash: PasswordHash,
}

impl<L: LoginId> Password<L> {
    /// The claim type logins are made unique under.
    pub const CLAIM_TYPE: &'static str = "auth.login";

    /// A password for `login`.
    #[must_use]
    pub fn new(login: L, hash: PasswordHash) -> Self {
        Self { login, hash }
    }

    /// The login identifier.
    #[must_use]
    pub fn login(&self) -> &L {
        &self.login
    }

    /// The password hash.
    #[must_use]
    pub fn hash(&self) -> &PasswordHash {
        &self.hash
    }

    /// Replaces the password hash.
    pub fn set_hash(&mut self, hash: PasswordHash) {
        self.hash = hash;
    }

    /// Replaces the login identifier; its claim follows on commit.
    pub fn change_login(&mut self, login: L) {
        self.login = login;
    }

    /// The claim that makes `login` unique across users.
    #[must_use]
    pub fn claim_for(login: &L) -> AggregateClaim {
        AggregateClaim::new(Self::CLAIM_TYPE, login.as_ref().into())
    }
}

impl<L: LoginId> Credentials for Password<L> {
    fn claims(&self) -> Vec<AggregateClaim> {
        vec![Self::claim_for(&self.login)]
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        fingerprint
            .str_field("password")
            .str_field(self.login.as_ref())
            .str_field(self.hash.as_str());
    }
}

impl<L: LoginId> HasPassword for Password<L> {
    type Login = L;

    fn password(&self) -> Option<&Password<L>> {
        Some(self)
    }
}

#[cfg(test)]
mod tests;
