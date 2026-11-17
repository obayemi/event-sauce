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

#[cfg(feature = "argon2")]
mod argon2_hasher;
mod authenticator;
mod capabilities;
mod clock;
mod credentials;
mod email;
mod error;
mod federated;
mod federated_login;
mod identity;
mod login_id;
mod login_method;
mod login_store;
mod non_empty;
mod password;
mod password_hash;
mod password_hasher;
mod password_login;
mod password_or_federated;
mod password_or_federated_login;
mod pending_login;
#[cfg(feature = "reset")]
mod reset;
mod second_factor;
#[cfg(test)]
mod test_support;
#[cfg(feature = "totp")]
mod totp;
#[cfg(any(feature = "postgres", feature = "redis"))]
mod ttl;
mod two_factor;
mod user;
mod user_cache;
mod username;

#[cfg(feature = "argon2")]
pub use argon2_hasher::Argon2Hasher;
pub use authenticator::{
    AuthError, Authenticator, ClaimAuthenticator, CompleteError, LoginOutcome,
};
pub use capabilities::{HasFederated, HasPassword};
pub use clock::{Clock, FixedClock, SystemClock};
pub use credentials::{Credentials, Digest, Sha256};
pub use email::{Email, EmailError};
pub use error::LoginMethodError;
pub use federated::{Federated, FederatedId, Provider};
pub use federated_login::{FederatedLogin, VerifiedFederated};
pub use identity::{Identity, IdentityError};
pub use login_id::LoginId;
pub use login_method::{ClaimLocator, LocateError, LoginMethod, Rejected};
#[cfg(feature = "redis")]
pub use login_store::RedisLoginStore;
#[cfg(feature = "postgres")]
pub use login_store::{InvalidSchemaName, PostgresLoginStore};
pub use login_store::{LoginKey, LoginStore, LoginStoreError};
pub use non_empty::{EmptyError, NonEmpty};
pub use password::Password;
pub use password_hash::{PasswordHash, PasswordHashError};
pub use password_hasher::{HashError, PasswordHasher};
pub use password_login::{PasswordAttempt, PasswordLogin};
pub use password_or_federated::PasswordOrFederated;
pub use password_or_federated_login::{PasswordOrFederatedAttempt, PasswordOrFederatedLogin};
pub use pending_login::{PendingLogin, PendingLoginError, PendingLoginId, PendingLogins};
#[cfg(feature = "reset")]
pub use reset::{ResetError, ResetToken, ResetTokenError, ResetTokenHash};
pub use second_factor::{HasSecondFactor, SecondFactor};
#[cfg(feature = "totp")]
pub use totp::{HasTotp, Totp, TotpSecret};
pub use two_factor::{LoginStage, NoSecondFactor, TwoFactor};
pub use user::UserAggregate;
#[cfg(feature = "moka")]
pub use user_cache::MokaUserCache;
#[cfg(feature = "redis")]
pub use user_cache::RedisUserCache;
pub use user_cache::{CacheError, CachedUserLoader, UserCache};
pub use username::{Username, UsernameError};
