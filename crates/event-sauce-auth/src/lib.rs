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

mod credentials;
mod email;
mod identity;
mod login_id;
mod user;
mod username;

pub use credentials::{Credentials, Digest, Sha256};
pub use email::{Email, EmailError};
pub use identity::{Identity, IdentityError};
pub use login_id::LoginId;
pub use user::UserAggregate;
pub use username::{Username, UsernameError};
