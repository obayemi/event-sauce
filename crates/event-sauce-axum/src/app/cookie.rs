//! How [`App::layer`](super::App::layer) protects the session cookie.

use std::fmt;

use tower_sessions::{
    cookie::Key,
    service::{CookieController, PlaintextCookie, PrivateCookie, SignedCookie},
    SessionManagerLayer, SessionStore,
};

/// How an [`App`](super::App) protects the cookie of the session layer it
/// is handed, when that layer's cookie is held by `C`.
///
/// The builder chooses it: [`KeepCookie`] by default,
/// [`SignCookie`] with [`UserAppBuilder::sign_cookie`](super::UserAppBuilder::sign_cookie)
/// and [`EncryptCookie`] with
/// [`UserAppBuilder::encrypt_cookie`](super::UserAppBuilder::encrypt_cookie).
/// The keyed ones take only a plaintext session layer, so a cookie cannot be
/// protected twice.
pub trait SessionCookie<C: CookieController> {
    /// What holds the cookie once it is protected.
    type Controller: CookieController + Sync;

    /// `sessions`, with its cookie protected.
    fn protect<Sess: SessionStore>(
        &self,
        sessions: SessionManagerLayer<Sess, C>,
    ) -> SessionManagerLayer<Sess, Self::Controller>;
}

/// Keeps the cookie the session layer sets: plaintext from
/// `SessionManagerLayer::new`, signed after `with_signed`, encrypted after
/// `with_private`.
#[derive(Debug, Clone, Copy, Default)]
pub struct KeepCookie;

impl<C: CookieController + Sync> SessionCookie<C> for KeepCookie {
    type Controller = C;

    fn protect<Sess: SessionStore>(
        &self,
        sessions: SessionManagerLayer<Sess, C>,
    ) -> SessionManagerLayer<Sess, C> {
        sessions
    }
}

/// Signs the session cookie with a key: a client still reads the session id,
/// but cannot present one the key did not sign.
#[derive(Clone)]
pub struct SignCookie(pub(super) Key);

impl SessionCookie<PlaintextCookie> for SignCookie {
    type Controller = SignedCookie;

    fn protect<Sess: SessionStore>(
        &self,
        sessions: SessionManagerLayer<Sess>,
    ) -> SessionManagerLayer<Sess, SignedCookie> {
        sessions.with_signed(self.0.clone())
    }
}

impl fmt::Debug for SignCookie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignCookie").finish_non_exhaustive()
    }
}

/// Encrypts the session cookie with a key: a client can neither read the
/// session id nor present one the key did not encrypt.
#[derive(Clone)]
pub struct EncryptCookie(pub(super) Key);

impl SessionCookie<PlaintextCookie> for EncryptCookie {
    type Controller = PrivateCookie;

    fn protect<Sess: SessionStore>(
        &self,
        sessions: SessionManagerLayer<Sess>,
    ) -> SessionManagerLayer<Sess, PrivateCookie> {
        sessions.with_private(self.0.clone())
    }
}

impl fmt::Debug for EncryptCookie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptCookie").finish_non_exhaustive()
    }
}
