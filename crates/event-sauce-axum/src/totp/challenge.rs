use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use axum::response::{IntoResponse, Response};
use axum_login::tower_sessions::{session, Session};
use event_sauce_auth::{
    Authenticated, HasTotp, NeedTotp, PendingLoginId, PendingLogins, Totp, TotpOrDone,
    UserAggregate,
};
use event_sauce_core::EntityId;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use crate::client_ip::ClientAddr;
use crate::diagnostics;
use crate::login::{AuthSession, LoginError, SessionError};
use crate::pages::Pages;
use crate::return_to::ReturnTo;
use crate::throttle::LoginThrottles;

#[cfg(test)]
#[path = "tests/challenge_tests.rs"]
mod tests;

const WAITING: &str = "event_sauce.totp.waiting";

#[derive(Serialize, Deserialize)]
struct Waiting {
    login: PendingLoginId,
    user: EntityId,
}

async fn waiting(session: &Session) -> Option<Waiting> {
    session.get(WAITING).await.ok().flatten()
}

/// Where a login that passed its first factor went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Opened {
    /// The session is open: the browser goes to the target.
    SignedIn(ReturnTo),
    /// The login waits for its code: the browser goes to the code step, and
    /// to the target once the code proved it.
    AwaitingCode(ReturnTo),
}

/// Why a login that passed its first factor could not go on.
///
/// It answers like its source does: a [`LoginError`] with its `401` or
/// `500`, a [`SessionError`] with a `500`.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The session store failed.
    #[error(transparent)]
    Session(#[from] SessionError),
    /// The login was refused, or a store failed.
    #[error(transparent)]
    Login(#[from] LoginError),
}

impl From<session::Error> for OpenError {
    fn from(error: session::Error) -> Self {
        Self::Session(error.into())
    }
}

impl IntoResponse for OpenError {
    fn into_response(self) -> Response {
        match self {
            Self::Session(error) => error.into_response(),
            Self::Login(error) => error.into_response(),
        }
    }
}

/// Why the code form is shown again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeRefusal {
    /// The code did not prove the login: wrong, used or late, or no login
    /// was waiting for it, or the login could not go on. A login is used up
    /// by its first code, whatever the code, so the user signs in again.
    Invalid,
    /// The code proved the login but its session could not open.
    Session,
}

/// What an application answers the code step of a login with.
pub trait TotpLoginPages: Pages {
    /// The code form, with the refusal that brought it back, if any.
    fn code_form(&self, refusal: Option<CodeRefusal>, return_to: &ReturnTo) -> Self::Response;

    /// No login waits for a code in this session: it expired, was used up,
    /// or never began.
    fn no_login_waiting(&self) -> Self::Response;

    /// The session is open: where the browser goes.
    fn signed_in(&self, return_to: ReturnTo) -> Self::Response;
}

/// The code page of a login: the form while a login waits for its code in
/// the session, [`no_login_waiting`](TotpLoginPages::no_login_waiting)
/// otherwise.
pub async fn code_page<P: TotpLoginPages>(
    pages: &P,
    session: &Session,
    return_to: &ReturnTo,
) -> P::Response {
    match waiting(session).await {
        Some(_) => pages.code_form(None, return_to),
        None => pages.no_login_waiting(),
    }
}

/// The second step of the logins whose route asked for a TOTP code.
///
/// [`open`](Self::open) takes the login the first factor yielded: a login
/// that is done opens its session, one that waits for a code is parked in the
/// [`PendingLogins`] and its handle goes into the session record. The code
/// step then ends it with [`prove`](Self::prove), throttled per user and
/// client. An HTML login flow keeps its in-between state in the session
/// record, so the browser holds the one session cookie and nothing else.
///
/// ```no_run
/// use event_sauce_auth::{HasTotp, TotpOrDone, UserAggregate};
/// use event_sauce_axum::{AuthSession, OpenError, Opened, ReturnTo, TotpChallenge};
/// use tower_sessions::Session;
///
/// async fn after_the_password<U>(
///     challenge: &TotpChallenge<U>,
///     mut auth: AuthSession<U>,
///     session: Session,
///     begun: TotpOrDone<U>,
/// ) -> Result<&'static str, OpenError>
/// where
///     U: UserAggregate,
///     U::Credentials: HasTotp,
/// {
///     Ok(match challenge.open(&mut auth, &session, begun, ReturnTo::root()).await? {
///         Opened::SignedIn(_) => "signed in",
///         Opened::AwaitingCode(_) => "send the code",
///     })
/// }
/// ```
pub struct TotpChallenge<U: UserAggregate> {
    pending: PendingLogins<U>,
    totp: Totp,
    throttles: Arc<LoginThrottles>,
}

impl<U: UserAggregate> Clone for TotpChallenge<U> {
    fn clone(&self) -> Self {
        Self {
            pending: self.pending.clone(),
            totp: self.totp.clone(),
            throttles: self.throttles.clone(),
        }
    }
}

impl<U: UserAggregate> fmt::Debug for TotpChallenge<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TotpChallenge").finish_non_exhaustive()
    }
}

impl<U> TotpChallenge<U>
where
    U: UserAggregate,
    U::Credentials: HasTotp,
{
    /// A challenge that parks logins in `pending`, checks codes with `totp`
    /// and counts wrong ones in `throttles`.
    #[must_use]
    pub fn new(pending: PendingLogins<U>, totp: Totp, throttles: Arc<LoginThrottles>) -> Self {
        Self {
            pending,
            totp,
            throttles,
        }
    }

    /// Opens the session of the login `begun` when it is done, or parks it
    /// when it waits for its code.
    ///
    /// # Errors
    ///
    /// [`OpenError::Login`] when the user is gone, inactive, changed its
    /// credentials since the first factor, or a login store failed;
    /// [`OpenError::Session`] when the session store failed.
    pub async fn open(
        &self,
        auth: &mut AuthSession<U>,
        session: &Session,
        begun: TotpOrDone<U>,
        return_to: ReturnTo,
    ) -> Result<Opened, OpenError> {
        match begun {
            TotpOrDone::Done(login) => {
                auth.login(login.finish().await.map_err(LoginError::from)?)
                    .await?;
                Ok(Opened::SignedIn(return_to))
            }
            TotpOrDone::Totp(login) => {
                let user = login.user().entity_id();
                let login = self.pending.save(login).await.map_err(LoginError::from)?;
                session.insert(WAITING, Waiting { login, user }).await?;
                Ok(Opened::AwaitingCode(return_to))
            }
        }
    }

    /// Ends the waiting login of the session with `code`.
    ///
    /// The user and the client are throttled before the login is taken, so
    /// a login whose code step must wait stays waiting. A code that does not
    /// prove the login uses it up, and counts as a failure of the user and of
    /// the client; only a proved code takes the failure back.
    ///
    /// # Errors
    ///
    /// The [`Throttled`](crate::Throttled) of the user or the client while
    /// either waits. Every other refusal, and every outage, is the code form
    /// again, the outage logged.
    pub async fn prove<P: TotpLoginPages>(
        &self,
        pages: &P,
        auth: &mut AuthSession<U>,
        session: &Session,
        client: &ClientAddr,
        code: SecretString,
        return_to: ReturnTo,
    ) -> Result<P::Response, P::Error> {
        let Some(waiting) = waiting(session).await else {
            return Ok(pages.code_form(Some(CodeRefusal::Invalid), &return_to));
        };
        let attempt = self
            .throttles
            .second_factor(waiting.user, client, Instant::now())?;
        if let Err(error) = session.remove_value(WAITING).await {
            diagnostics::internal(&error);
        }
        let authenticated = match self.finish(waiting.login, code).await {
            Ok(authenticated) => authenticated,
            Err(error) => {
                if !error.is_refused() {
                    diagnostics::internal(&error);
                }
                return Ok(pages.code_form(Some(CodeRefusal::Invalid), &return_to));
            }
        };
        attempt.succeeded();
        Ok(match auth.login(authenticated).await {
            Ok(()) => pages.signed_in(return_to),
            Err(error) => {
                diagnostics::internal(&error);
                pages.code_form(Some(CodeRefusal::Session), &return_to)
            }
        })
    }

    async fn finish(
        &self,
        login: PendingLoginId,
        code: SecretString,
    ) -> Result<Authenticated<U>, LoginError> {
        let waiting = self.pending.resume::<NeedTotp>(login).await?;
        let done = waiting.prove(&self.totp, code).await?;
        Ok(done.finish().await?)
    }
}
