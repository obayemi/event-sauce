//! The [`LoginMethodError`] of credential rules.

/// Why a change to the login methods of a user was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LoginMethodError {
    /// The federated identity is already linked.
    #[error("federated identity is already linked")]
    AlreadyLinked,
    /// No federated identity of that provider is linked.
    #[error("no federated identity is linked for that provider")]
    NotLinked,
    /// The change would leave the user with no way to log in.
    #[error("this is the last way to log in")]
    LastLoginMethod,
}
