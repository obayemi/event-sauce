//! Typestate logins: a login moves through states the application names, and
//! only a login that reached a [`Complete`] state yields an
//! [`Authenticated`] user.
//!
//! - [`Logins::begin`] runs the first factor, a [`FirstFactor`], and its
//!   [`Route`] picks the first [`Login`] state.
//! - [`Login::prove`] moves a login out of a state through that state's
//!   [`Step`], and [`Login::finish`] ends a login in a [`Complete`] state.
//! - [`PendingLogins`] keeps a login between two requests.

mod authenticated;
mod error;
mod first_factor;
mod flow;
mod logins;
mod pending;
mod state;
pub(crate) mod users;

pub use authenticated::Authenticated;
pub use error::{AuthError, FinishError, ResumeError, SignUpError};
pub use first_factor::{AlwaysDone, FirstFactor, Route, SingleFactor};
pub use flow::{Advance, Login};
pub use logins::Logins;
pub use pending::{PendingLoginError, PendingLoginId, PendingLogins};
pub use state::{Complete, Done, Entered, LoginState, Step};
