//! The [`Fingerprint`] that [`Credentials`](crate::Credentials) feed.

use std::fmt;

use sha2::{Digest, Sha256};

/// The session fingerprint under construction.
///
/// Every field is fed behind its length, so two different sequences of
/// fields never feed the same byte stream: `("ab", "c")` and `("a", "bc")`
/// fingerprint differently.
///
/// # Examples
///
/// ```
/// use event_sauce_auth::Fingerprint;
///
/// fn feed(fingerprint: &mut Fingerprint, login: &str, secret: &[u8]) {
///     fingerprint.str_field(login).field(secret);
/// }
/// ```
pub struct Fingerprint(Sha256);

impl Fingerprint {
    pub(crate) fn new() -> Self {
        Self(Sha256::new())
    }

    /// Feeds `bytes` behind a length prefix.
    pub fn field(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.update((bytes.len() as u64).to_be_bytes());
        self.0.update(bytes);
        self
    }

    /// Feeds the UTF-8 bytes of `value` behind a length prefix.
    pub fn str_field(&mut self, value: &str) -> &mut Self {
        self.field(value.as_bytes())
    }

    pub(crate) fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

impl fmt::Debug for Fingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Fingerprint")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "tests/fingerprint_tests.rs"]
mod tests;
