//! The provider side of an OAuth 2.0 sign-in with PKCE: the secrets a flow
//! carries from the consent page back to its callback, and the
//! [`OAuthProvider`] an application implements over the HTTP client of its
//! choice.
//!
//! The crate runs no HTTP: a provider sends the browser to the consent page
//! ([`authorize`](OAuthProvider::authorize)), then turns the code the browser
//! comes back with into the account the provider vouches for
//! ([`identify`](OAuthProvider::identify)). What the application does with
//! that account, link it, create it or refuse it, stays its own.

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::FederatedId;

#[cfg(test)]
#[path = "tests/oauth_tests.rs"]
mod tests;

macro_rules! secret {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wraps the raw value.
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// The raw value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl PartialEq for $name {
            fn eq(&self, other: &Self) -> bool {
                self.0.as_bytes().ct_eq(other.0.as_bytes()).into()
            }
        }

        impl Eq for $name {}

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name)).finish_non_exhaustive()
            }
        }
    };
}

secret!(
    /// The anti-forgery state the provider sends back to the callback,
    /// compared in constant time. `Debug` never prints it.
    OAuthState
);
secret!(
    /// The PKCE verifier proving the callback belongs to the flow that sent
    /// the challenge. `Debug` never prints it.
    PkceVerifier
);
secret!(
    /// The one-time code the provider hands the callback. `Debug` never
    /// prints it.
    AuthorizationCode
);

/// Where to send the browser to start a sign-in, with the secrets the
/// callback checks it against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    /// The provider's consent page, carrying `state` and the PKCE challenge.
    pub url: String,
    /// The anti-forgery state the provider sends back to the callback.
    pub state: OAuthState,
    /// The PKCE verifier matching the challenge in `url`.
    pub pkce_verifier: PkceVerifier,
}

/// What a provider vouches for: who the user is at the provider, and the
/// application's own data about the account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederatedAccount<P> {
    /// The identity at the provider.
    pub id: FederatedId,
    /// What the application keeps of the account besides its identity: the
    /// verified email, a display name, whatever its policies read.
    pub profile: P,
}

/// A sign-in provider, as a flow drives it.
///
/// ```
/// use event_sauce_auth::{
///     Authorization, AuthorizationCode, FederatedAccount, FederatedId, OAuthProvider, OAuthState,
///     PkceVerifier, Provider,
/// };
///
/// struct Fixed;
///
/// #[async_trait::async_trait]
/// impl OAuthProvider for Fixed {
///     type Profile = String;
///     type Error = std::convert::Infallible;
///
///     fn authorize(&self) -> Authorization {
///         Authorization {
///             url: "https://accounts.example/consent".to_owned(),
///             state: OAuthState::new("state"),
///             pkce_verifier: PkceVerifier::new("verifier"),
///         }
///     }
///
///     async fn identify(
///         &self,
///         _code: AuthorizationCode,
///         _verifier: PkceVerifier,
///     ) -> Result<FederatedAccount<String>, Self::Error> {
///         Ok(FederatedAccount {
///             id: FederatedId::new(Provider::GOOGLE, "sub-1"),
///             profile: "ada@example.com".to_owned(),
///         })
///     }
/// }
///
/// assert_eq!(Fixed.authorize().url, "https://accounts.example/consent");
/// ```
#[async_trait]
pub trait OAuthProvider: Send + Sync {
    /// What the application keeps of an account besides its identity.
    type Profile: Send + Sync;
    /// Why a code yielded no account.
    type Error: std::error::Error + Send + Sync + 'static;

    /// A fresh consent-page redirect with its state and PKCE verifier.
    fn authorize(&self) -> Authorization;

    /// The account behind the authorization `code`, proving the flow with
    /// its `verifier`.
    ///
    /// # Errors
    ///
    /// When the provider refuses the code or vouches for nothing usable.
    async fn identify(
        &self,
        code: AuthorizationCode,
        verifier: PkceVerifier,
    ) -> Result<FederatedAccount<Self::Profile>, Self::Error>;
}
