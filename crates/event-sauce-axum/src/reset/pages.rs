use crate::pages::Pages;

/// Why the new-password form is shown again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NewPasswordError {
    /// The password could not be hashed. The link is not used up.
    HashFailed,
}

/// What an application answers the steps of a password reset with.
pub trait ResetPages: Pages {
    /// The form asking for a link.
    fn request_form(&self) -> Self::Response;

    /// The answer to a request for a link, the same whatever the login.
    fn request_sent(&self) -> Self::Response;

    /// The form for the new password, with the reason it is shown again, if
    /// any.
    fn new_password_form(&self, error: Option<NewPasswordError>) -> Self::Response;

    /// The error of a link that is no good, the same for every way it can be
    /// wrong, so it tells nothing about the account (a `404`).
    fn invalid_link(&self) -> Self::Error;

    /// The password is reset and the session open: where the browser goes.
    fn signed_in(&self) -> Self::Response;

    /// The password is reset but the user has to sign in: a second factor
    /// is asked, or the session could not open.
    fn sign_in_again(&self) -> Self::Response;
}
