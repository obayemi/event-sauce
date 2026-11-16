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
mod credentials;
mod email;
mod error;
mod federated;
mod federated_login;
mod identity;
mod login_id;
mod login_method;
mod login_store;
#[cfg(any(feature = "postgres", feature = "redis"))]
mod login_ttl;
mod non_empty;
mod password;
mod password_hash;
mod password_hasher;
mod password_login;
mod password_or_federated;
mod password_or_federated_login;
#[cfg(feature = "postgres")]
mod postgres_login_store;
#[cfg(feature = "redis")]
mod redis_login_store;
#[cfg(test)]
mod test_support;
mod user;
mod username;

#[cfg(feature = "argon2")]
pub use argon2_hasher::Argon2Hasher;
pub use authenticator::{AuthError, Authenticator, ClaimAuthenticator, LoginOutcome};
pub use capabilities::{HasFederated, HasPassword};
pub use credentials::{Credentials, Digest, Sha256};
pub use email::{Email, EmailError};
pub use error::LoginMethodError;
pub use federated::{Federated, FederatedId, Provider};
pub use federated_login::{FederatedLogin, VerifiedFederated};
pub use identity::{Identity, IdentityError};
pub use login_id::LoginId;
pub use login_method::{ClaimLocator, LocateError, LoginMethod, Rejected};
pub use login_store::{LoginKey, LoginStore, LoginStoreError};
pub use non_empty::{EmptyError, NonEmpty};
pub use password::Password;
pub use password_hash::{PasswordHash, PasswordHashError};
pub use password_hasher::{HashError, PasswordHasher};
pub use password_login::{PasswordAttempt, PasswordLogin};
pub use password_or_federated::PasswordOrFederated;
pub use password_or_federated_login::{PasswordOrFederatedAttempt, PasswordOrFederatedLogin};
#[cfg(feature = "postgres")]
pub use postgres_login_store::{InvalidSchemaName, PostgresLoginStore};
#[cfg(feature = "redis")]
pub use redis_login_store::RedisLoginStore;
pub use user::UserAggregate;
pub use username::{Username, UsernameError};
