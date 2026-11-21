//! Login for axum applications: the sealed `axum-login` backend, the session
//! a handler opens from a finished login, the pending login cookie and the
//! extractors of the current user.

mod auth_layer;
mod auth_session;
mod backend;
mod current_user;
mod pending;
mod session_user;
#[cfg(test)]
pub(crate) mod test_support;

pub use auth_layer::AuthLayer;
pub use auth_session::{AuthSession, SessionError};
pub use current_user::{AuthRejection, CurrentUser, MaybeUser, Registered};
pub use pending::PendingLogin;
pub(crate) use session_user::SessionUser;
