//! The [`Username`] login identifier.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::LoginId;

/// Why a string is not a [`Username`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UsernameError {
    /// Fewer than [`Username::MIN_LEN`] characters.
    #[error("username must have at least {} characters", Username::MIN_LEN)]
    TooShort,
    /// More than [`Username::MAX_LEN`] characters.
    #[error("username must have at most {} characters", Username::MAX_LEN)]
    TooLong,
    /// A character outside `a-z`, `0-9`, `_`, `-` and `.`.
    #[error("username cannot contain `{0}`")]
    InvalidCharacter(char),
    /// First or last character is not a letter or a digit.
    #[error("username must start and end with a letter or a digit")]
    InvalidBoundary,
}

/// A username used to log in.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::{LoginId, Username};
///
/// assert_eq!(Username::parse(" Ada_99 ")?.as_str(), "ada_99");
/// assert!(Username::parse("\u{430}dmin").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Username(String);

impl Username {
    /// Shortest accepted username, in characters.
    pub const MIN_LEN: usize = 3;
    /// Longest accepted username, in characters.
    pub const MAX_LEN: usize = 32;

    /// The normalised username.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl LoginId for Username {
    type ParseError = UsernameError;

    fn parse(raw: &str) -> Result<Self, Self::ParseError> {
        let normalised = raw.trim().to_ascii_lowercase();
        let length = normalised.chars().count();
        if length < Self::MIN_LEN {
            return Err(UsernameError::TooShort);
        }
        if length > Self::MAX_LEN {
            return Err(UsernameError::TooLong);
        }
        if let Some(invalid) = normalised.chars().find(|c| !is_allowed(*c)) {
            return Err(UsernameError::InvalidCharacter(invalid));
        }
        if !normalised.starts_with(|c: char| c.is_ascii_alphanumeric())
            || !normalised.ends_with(|c: char| c.is_ascii_alphanumeric())
        {
            return Err(UsernameError::InvalidBoundary);
        }
        Ok(Self(normalised))
    }
}

fn is_allowed(c: char) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '-' | '.')
}

impl AsRef<str> for Username {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Username {
    type Error = UsernameError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<Username> for String {
    fn from(username: Username) -> Self {
        username.0
    }
}

impl fmt::Display for Username {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests;
