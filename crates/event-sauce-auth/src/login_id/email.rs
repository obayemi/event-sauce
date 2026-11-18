//! The [`Email`] login identifier.

use std::fmt;

use email_address::EmailAddress;
use serde::{Deserialize, Serialize};

use crate::LoginId;

/// Why a string is not an [`Email`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{0}` is not a valid email address")]
pub struct EmailError(String);

/// An email address used to log in, trimmed, lowercased and syntax checked.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{Email, LoginId};
///
/// let email = Email::parse(" Ada@Example.com ")?;
/// assert_eq!(email.as_str(), "ada@example.com");
/// assert!(Email::parse("not an email").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Email(String);

impl Email {
    /// The normalised address.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl LoginId for Email {
    type ParseError = EmailError;

    fn parse(raw: &str) -> Result<Self, Self::ParseError> {
        let normalised = raw.trim().to_lowercase();
        if EmailAddress::is_valid(&normalised) {
            Ok(Self(normalised))
        } else {
            Err(EmailError(raw.trim().to_owned()))
        }
    }
}

impl AsRef<str> for Email {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Email {
    type Error = EmailError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<Email> for String {
    fn from(email: Email) -> Self {
        email.0
    }
}

impl fmt::Display for Email {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests;
