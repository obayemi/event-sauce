//! Login for axum applications: the sealed `axum-login` backend, the session
//! a handler opens from a finished login, the pending login cookie and the
//! extractors of the current user.

mod auth_layer;
mod auth_session;
mod authorized;
mod backend;
mod current_user;
mod login_error;
mod login_step;
mod pending;
mod rejection;
mod session_user;
#[cfg(test)]
pub(crate) mod test_support;

pub use auth_layer::AuthLayer;
pub use auth_session::{AuthSession, SessionError};
pub use authorized::{Access, AccessRejection, Authorized, DeniedStatus};
pub use current_user::{CurrentUser, MaybeUser, Registered};
pub use login_error::LoginError;
pub use login_step::LoginStep;
pub use pending::PendingLogin;
pub use rejection::AuthRejection;
pub(crate) use session_user::SessionUser;
