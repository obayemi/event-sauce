//! # event-sauce-auth
//!
//! Pluggable credentials and login identity for event-sauce user aggregates.
//!
//! The library does not choose how users log in. A user aggregate holds an
//! [`Identity`] generic over a [`Credentials`] type that the application
//! names, and the rules on identity transitions live here, pure and unit
//! testable.
//!
//! - [`Identity`] is either anonymous or registered with credentials.
//! - [`Credentials`] is what a registered user stores to be able to log in.
//! - [`LoginId`] is the normalised identifier a credential answers to, with
//!   [`Email`] and [`Username`] as ready-made implementations.
//! - [`Password`], [`Federated`] and [`PasswordOrFederated`] are the ready-made
//!   credentials, with [`PasswordHash`], [`FederatedId`] and [`Provider`] as
//!   their value types and [`NonEmpty`] guarding the links.
//! - [`HasPassword`] and [`HasFederated`] are the capabilities login methods
//!   require of the credentials an application picked.
//! - [`UserAggregate`] tells the login machinery how to read an identity off
//!   an aggregate.
//! - [`PasswordHasher`] hashes and checks passwords, with [`Argon2Hasher`] behind
//!   the `argon2` feature.
//! - [`LoginMethod`] locates a user and verifies an attempt, with
//!   [`PasswordLogin`], [`FederatedLogin`] and [`PasswordOrFederatedLogin`]
//!   ready-made.
//! - [`LoginStore`] keeps the short-lived state of logins in progress, with
//!   `PostgresLoginStore` (feature `postgres`) and `RedisLoginStore` (feature
//!   `redis`) as the only implementations: state that is not shared between
//!   instances would break a login in progress.
//! - [`SecondFactor`] proves a login a second time, with `Totp` (feature `totp`)
//!   ready-made, and [`TwoFactor`] holds back the logins that require one
//!   until [`ClaimAuthenticator::complete`] finishes them.
//! - [`UserCache`] keeps loaded users, checked against the stored version on every
//!   read, with `MokaUserCache` (feature `moka`) and `RedisUserCache` (feature
//!   `redis`) ready-made. [`CachedUserLoader`] resolves the current user through it.
//! - `ResetToken` and `ResetTokenHash` (feature `reset`) issue and check the
//!   single-use tokens of a password reset, storing only the hash.
//! - [`Authenticator`] turns an attempt into a [`LoginOutcome`], and
//!   [`ClaimAuthenticator`] is the generic implementation over any event store.
//!
//! # Examples
//!
//! ```
//! use event_sauce_auth::{Email, LoginId};
//!
//! let email = Email::parse("  Ada@Example.COM ")?;
//! assert_eq!(email.as_str(), "ada@example.com");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]

mod authenticator;
mod capabilities;
mod clock;
mod credentials;
mod email;
mod error;
mod federated;
mod identity;
mod login_id;
mod login_method;
mod login_store;
mod non_empty;
mod password;
mod password_hash;
mod password_hasher;
mod password_or_federated;
#[cfg(feature = "reset")]
mod reset;
mod second_factor;
#[cfg(test)]
mod test_support;
#[cfg(any(feature = "postgres", feature = "redis"))]
mod ttl;
mod user;
mod user_cache;
mod username;

pub use authenticator::{
    AuthError, Authenticator, ClaimAuthenticator, CompleteError, LoginOutcome,
};
pub use capabilities::{HasFederated, HasPassword};
pub use clock::{Clock, FixedClock, SystemClock};
pub use credentials::{Credentials, Digest, Sha256};
pub use email::{Email, EmailError};
pub use error::LoginMethodError;
pub use federated::{Federated, FederatedId, Provider};
pub use identity::{Identity, IdentityError};
pub use login_id::LoginId;
pub use login_method::{
    ClaimLocator, FederatedLogin, LocateError, LoginMethod, PasswordAttempt, PasswordLogin,
    PasswordOrFederatedAttempt, PasswordOrFederatedLogin, Rejected, VerifiedFederated,
};
#[cfg(feature = "redis")]
pub use login_store::RedisLoginStore;
#[cfg(feature = "postgres")]
pub use login_store::{InvalidSchemaName, PostgresLoginStore};
pub use login_store::{LoginKey, LoginStore, LoginStoreError};
pub use non_empty::{EmptyError, NonEmpty};
pub use password::Password;
pub use password_hash::{PasswordHash, PasswordHashError};
#[cfg(feature = "argon2")]
pub use password_hasher::Argon2Hasher;
pub use password_hasher::{HashError, PasswordHasher};
pub use password_or_federated::PasswordOrFederated;
#[cfg(feature = "reset")]
pub use reset::{ResetError, ResetToken, ResetTokenError, ResetTokenHash};
pub use second_factor::{
    HasSecondFactor, LoginStage, NoSecondFactor, PendingLogin, PendingLoginError, PendingLoginId,
    PendingLogins, SecondFactor, TwoFactor,
};
#[cfg(feature = "totp")]
pub use second_factor::{HasTotp, Totp, TotpSecret};
pub use user::UserAggregate;
#[cfg(feature = "moka")]
pub use user_cache::MokaUserCache;
#[cfg(feature = "redis")]
pub use user_cache::RedisUserCache;
pub use user_cache::{CacheError, CachedUserLoader, UserCache};
pub use username::{Username, UsernameError};
