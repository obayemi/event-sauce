//! The capability traits login methods rely on.

use crate::{Credentials, Federated, LoginId, Password};

/// Credentials that can hold a password login.
///
/// A password login method needs `C: HasPassword`, so configuring one for
/// credentials without a password fails to compile.
pub trait HasPassword: Credentials {
    /// The identifier the password login names.
    type Login: LoginId;

    /// The password, or `None` when this user currently has none.
    fn password(&self) -> Option<&Password<Self::Login>>;
}

/// Credentials that can hold federated links.
///
/// A federated login method needs `C: HasFederated`, so configuring one
/// for credentials without links fails to compile.
pub trait HasFederated: Credentials {
    /// The linked providers, or `None` when this user currently has none.
    fn federated(&self) -> Option<&Federated>;
}
