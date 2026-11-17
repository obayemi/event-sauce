//! The [`Credentials`] trait and the credential types built on it.

use std::fmt::Debug;

use event_sauce_core::AggregateClaim;
use serde::{de::DeserializeOwned, Serialize};

pub use sha2::{Digest, Sha256};

mod capabilities;
mod error;
mod federated;
mod identity;
mod non_empty;
mod password;
mod password_hash;
mod password_or_federated;

pub use capabilities::{HasFederated, HasPassword};
pub use error::LoginMethodError;
pub use federated::{Federated, FederatedId, Provider};
pub use identity::{Identity, IdentityError};
pub use non_empty::{EmptyError, NonEmpty};
pub use password::Password;
pub use password_hash::{PasswordHash, PasswordHashError};
pub use password_or_federated::PasswordOrFederated;

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

/// Feeds `bytes` behind a length prefix, so that two different sequences of
/// fields never feed the same byte stream.
pub(crate) fn feed(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

/// The fingerprint `credentials` alone would produce.
#[cfg(test)]
pub(crate) fn fingerprint_of(credentials: &impl Credentials) -> [u8; 32] {
    let mut digest = Sha256::new();
    credentials.fingerprint(&mut digest);
    digest.finalize().into()
}
