use crate::pages::Pages;
use crate::return_to::ReturnTo;

/// Why a callback did not go on, for the application's page.
///
/// `E` is the error of the provider and `F` the refusal of the application.
#[derive(Debug)]
pub enum CallbackError<E, F> {
    /// The state is not the one the session's flow was started with: forged,
    /// replayed, or outside any flow. The flow ends.
    State,
    /// The flow lost its PKCE verifier, so the session expired meanwhile.
    Expired,
    /// The provider came back without a code: the user cancelled at its
    /// consent page, or the provider failed.
    Cancelled,
    /// The provider could not tell who the code stands for.
    Provider(E),
    /// The sign-in was refused: `Some` when the application refused an
    /// unknown account, `None` when the user is inactive or the login could
    /// not go on.
    Refused(Option<F>),
    /// The session could not open.
    Session,
}

/// What an application answers the callback of an OAuth flow with.
///
/// Every target is the one the flow was started with, read back from the
/// session: the application picks its own default when there was none.
pub trait OAuthPages<E, F>: Pages {
    /// The callback did not go on. `return_to` is the target of the flow when
    /// the callback could still read it, never after a [`State`] refusal.
    ///
    /// [`State`]: CallbackError::State
    fn failed(&self, error: CallbackError<E, F>, return_to: Option<&ReturnTo>) -> Self::Response;

    /// A link or a claim was started for a user who is not the one signed in
    /// on this session, and nothing was changed.
    fn not_session_user(&self) -> Self::Response;

    /// A link needs a second factor code it did not carry: the page that asks
    /// for it before another try.
    fn link_needs_code(&self) -> Self::Response;

    /// A link or a claim ended, whatever its outcome: where the browser goes
    /// back to.
    fn returned(&self, return_to: Option<ReturnTo>) -> Self::Response;

    /// The sign-in passed its first factor and the login waits for its
    /// second: the code step.
    fn code_step(&self, return_to: &ReturnTo) -> Self::Response;

    /// The session is open: where the browser goes.
    fn done(&self, return_to: ReturnTo) -> Self::Response;
}
