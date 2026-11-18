//! Login for axum applications: an `axum-login` backend that opens a session
//! from a finished login, the session user, the pending login cookie and the
//! extractors of the current user.

mod backend;
mod current_user;
mod pending;
mod session_user;
#[cfg(test)]
pub(crate) mod test_support;

pub use backend::{BackendError, EventSauceBackend, UserBackend};
pub use current_user::{AuthRejection, CurrentUser, MaybeUser, Registered};
pub use pending::PendingLogin;
pub use session_user::SessionUser;
