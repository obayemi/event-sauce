//! The [`Credentials`] trait.

use std::fmt::Debug;

use event_sauce_core::AggregateClaim;
use serde::{de::DeserializeOwned, Serialize};

pub use sha2::{Digest, Sha256};

/// What a registered user stores to be able to log in.
///
/// This is the only trait a credentials type must implement. The type is
/// a plain value the application names, held by [`Identity::Registered`].
///
/// [`Identity::Registered`]: crate::Identity::Registered
pub trait Credentials:
    Clone + Eq + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Uniqueness claims for the identifiers these credentials answer to.
    ///
    /// They make the identifiers unique and resolvable at login.
    fn claims(&self) -> Vec<AggregateClaim>;

    /// Feeds every secret and identifier that grants access.
    ///
    /// Any change to what is fed changes the session fingerprint, so
    /// sessions opened before the change stop being valid.
    fn fingerprint(&self, digest: &mut Sha256);
}
