//! Login for axum applications: an `axum-login` backend over an
//! [`Authenticator`](event_sauce_auth::Authenticator), the session user, the
//! pending second-factor login and the extractors of the current user.

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
