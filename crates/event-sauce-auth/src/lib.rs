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
//!   an aggregate. `#[aggregate(..., user)]` generates it, with `#[user(identity)]`
//!   on the identity field and an optional `#[user(active)]` on a flag field.
//! - [`PasswordHasher`] hashes and checks passwords, with [`Argon2Hasher`] behind
//!   the `argon2` feature.
//! - [`LoginMethod`] locates a user and verifies an attempt, with
//!   [`PasswordLogin`], [`FederatedLogin`] and [`PasswordOrFederatedLogin`]
//!   ready-made. [`ClaimLocator`] finds the user holding a claim, and every event
//!   store is one.
//! - [`LoginStore`] keeps the short-lived state of logins in progress, with
//!   `PostgresLoginStore` (feature `postgres`) and `RedisLoginStore` (feature
//!   `redis`) as the only implementations: state that is not shared between
//!   instances would break a login in progress.
//! - [`SecondFactor`] proves a login a second time, with `Totp` (feature `totp`)
//!   ready-made, and [`TwoFactor`] holds back the logins that require one
//!   until [`ClaimAuthenticator::complete`] finishes them. [`LoginStage`] is
//!   what the authenticator asks at that point, [`NoSecondFactor`] being the
//!   stage that never holds a login. [`PendingLogins`] keeps the held
//!   [`PendingLogin`]s in a [`LoginStore`], each under a [`PendingLoginId`].
//! - [`UserCache`] keeps loaded users, checked against the stored version on every
//!   read, with `MokaUserCache` (feature `moka`) and `RedisUserCache` (feature
//!   `redis`) ready-made. [`CachedUserLoader`] resolves the current user through it.
//! - `ResetToken` and `ResetTokenHash` (feature `reset`) issue and check the
//!   single-use tokens of a password reset, storing only the hash.
//! - [`Authenticator`] turns an attempt into a [`LoginOutcome`], and
//!   [`ClaimAuthenticator`] is the generic implementation over any event store.
//! - [`Clock`] is the time source that second factors and pending logins read, with [`SystemClock`] and the
//!   test double [`FixedClock`].
//!
//! # Features
//!
//! - `argon2` (default): [`Argon2Hasher`].
//! - `postgres` (default): `PostgresLoginStore`.
//! - `redis`: `RedisLoginStore` and `RedisUserCache`.
//! - `moka`: `MokaUserCache`, an in-process user cache.
//! - `reset`: `ResetToken` and `ResetTokenHash` for password resets.
//! - `totp`: `Totp`, `HasTotp` and `TotpSecret` for TOTP second factors.
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
mod clock;
mod credentials;
mod login_id;
mod login_method;
mod login_store;
mod login_ttl;
mod password_hasher;
#[cfg(feature = "reset")]
mod reset;
mod second_factor;
#[cfg(test)]
mod test_support;
mod user;
mod user_cache;

pub use authenticator::{
    AuthError, Authenticator, ClaimAuthenticator, CompleteError, LoginOutcome,
};
pub use clock::{Clock, FixedClock, SystemClock};
pub use credentials::{
    Credentials, Digest, EmptyError, Federated, FederatedId, HasFederated, HasPassword, Identity,
    IdentityError, LoginMethodError, NonEmpty, Password, PasswordHash, PasswordHashError,
    PasswordOrFederated, Provider, Sha256,
};
pub use login_id::{Email, EmailError, LoginId, Username, UsernameError};
pub use login_method::{
    ClaimLocator, FederatedLogin, LocateError, LoginMethod, PasswordAttempt, PasswordLogin,
    PasswordOrFederatedAttempt, PasswordOrFederatedLogin, Rejected, VerifiedFederated,
};
#[cfg(feature = "redis")]
pub use login_store::RedisLoginStore;
#[cfg(feature = "postgres")]
pub use login_store::{InvalidSchemaName, PostgresLoginStore};
pub use login_store::{LoginKey, LoginStore, LoginStoreError};
pub use login_ttl::{LoginTtl, LoginTtlError};
#[cfg(feature = "argon2")]
pub use password_hasher::Argon2Hasher;
pub use password_hasher::{HashError, PasswordHasher};
#[cfg(feature = "reset")]
pub use reset::{ResetError, ResetToken, ResetTokenError, ResetTokenHash};
pub use second_factor::{
    HasSecondFactor, LoginStage, NoSecondFactor, PendingLogin, PendingLoginError, PendingLoginId,
    PendingLogins, SecondFactor, TwoFactor, DEFAULT_PENDING_TTL,
};
#[cfg(feature = "totp")]
pub use second_factor::{HasTotp, Totp, TotpSecret};
pub use user::UserAggregate;
#[cfg(feature = "moka")]
pub use user_cache::MokaUserCache;
#[cfg(feature = "redis")]
pub use user_cache::RedisUserCache;
pub use user_cache::{CacheError, CachedUserLoader, UserCache};
