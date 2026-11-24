//! # event-sauce-auth
//!
//! Pluggable credentials, typestate logins and login identity for
//! event-sauce user aggregates.
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
//! - [`Logins`] starts a login: a [`FirstFactor`] pairs a [`LoginMethod`] with a
//!   [`Route`], and the [`Login`] it yields moves through the states the
//!   application names. Each state's [`Step`] is the only way out of it, and
//!   [`Login::finish`] exists only for a [`Complete`] state, such as [`Done`]. It
//!   yields an [`Authenticated`] user, the only thing a session is opened from, so
//!   no session can open before the whole process ended.
//! - [`LoginStore`] keeps the short-lived state of logins in progress, with
//!   `PostgresLoginStore` (feature `postgres`) and `RedisLoginStore` (feature
//!   `redis`) as the only implementations: state that is not shared between
//!   instances would break a login in progress. [`PendingLogins`] saves a
//!   [`Login`] in it between two requests, under a [`PendingLoginId`].
//! - `Totp`, `NeedTotp` and `TotpIfEnrolled` (feature `totp`) are the
//!   ready-made TOTP step.
//! - [`UserCache`] keeps loaded users, checked against the stored version on every
//!   read, with `MokaUserCache` (feature `moka`) and `RedisUserCache` (feature
//!   `redis`) ready-made. [`CachedUserLoader`] resolves the current user through it.
//! - `ResetToken` and `ResetTokenHash` (feature `reset`) issue and check the
//!   single-use tokens of a password reset, storing only the hash.
//! - [`Clock`] is the time source that logins and TOTP read, with [`SystemClock`]
//!   and the test double [`FixedClock`].
//!
//! # Features
//!
//! - `argon2` (default): [`Argon2Hasher`].
//! - `postgres` (default): `PostgresLoginStore`.
//! - `redis`: `RedisLoginStore` and `RedisUserCache`.
//! - `moka`: `MokaUserCache`, an in-process user cache.
//! - `reset`: `ResetToken` and `ResetTokenHash` for password resets.
//! - `totp`: `Totp`, `NeedTotp`, `TotpIfEnrolled`, `HasTotp`, `TotpSecret` and
//!   `otpauth_uri`.
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

mod clock;
mod credentials;
mod login;
mod login_id;
mod login_method;
mod login_store;
mod login_ttl;
mod password_hasher;
#[cfg(feature = "reset")]
mod reset;
#[cfg(test)]
mod test_support;
#[cfg(feature = "totp")]
mod totp;
mod user;
mod user_cache;

pub use clock::{Clock, FixedClock, SystemClock};
pub use credentials::{
    Credentials, EmptyError, Federated, FederatedId, Fingerprint, HasFederated, HasPassword,
    Identity, IdentityError, LoginMethodError, NonEmpty, Password, PasswordHash, PasswordHashError,
    PasswordOrFederated, Provider,
};
pub use login::{
    Advance, AlwaysDone, AuthError, Authenticated, Complete, Done, Entered, FinishError,
    FirstFactor, Login, LoginState, Logins, PendingLoginError, PendingLoginId, PendingLogins,
    ResumeError, Route, SignUpError, SingleFactor, Step,
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
#[cfg(feature = "totp")]
pub use totp::{otpauth_uri, HasTotp, NeedTotp, Totp, TotpIfEnrolled, TotpOrDone, TotpSecret};
pub use user::UserAggregate;
#[cfg(feature = "moka")]
pub use user_cache::MokaUserCache;
#[cfg(feature = "redis")]
pub use user_cache::RedisUserCache;
pub use user_cache::{CacheError, CachedUserLoader, UserCache};
