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

mod capabilities;
mod credentials;
mod email;
mod error;
mod federated;
mod identity;
mod login_id;
mod non_empty;
mod password;
mod password_hash;
mod password_or_federated;
mod user;
mod username;

pub use capabilities::{HasFederated, HasPassword};
pub use credentials::{Credentials, Digest, Sha256};
pub use email::{Email, EmailError};
pub use error::LoginMethodError;
pub use federated::{Federated, FederatedId, Provider};
pub use identity::{Identity, IdentityError};
pub use login_id::LoginId;
pub use non_empty::{EmptyError, NonEmpty};
pub use password::Password;
pub use password_hash::{PasswordHash, PasswordHashError};
pub use password_or_federated::PasswordOrFederated;
pub use user::UserAggregate;
pub use username::{Username, UsernameError};
