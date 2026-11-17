//! The [`LoginId`] trait and its ready-made implementations.

use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

mod email;
mod username;

pub use email::{Email, EmailError};
pub use username::{Username, UsernameError};

/// The identifier a password login names, such as an email or a username.
///
/// Implementations normalise raw user input, so that two spellings of the
/// same identifier are equal and claim the same uniqueness slot.
pub trait LoginId:
    Clone + Eq + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// The reason raw input is not a valid identifier.
    type ParseError: std::error::Error + Send + Sync + 'static;

    /// Parses raw user input, normalising it (trim, case-fold, ...).
    ///
    /// # Errors
    ///
    /// Returns `Self::ParseError` when the input is not a valid identifier.
    fn parse(raw: &str) -> Result<Self, Self::ParseError>;
}
