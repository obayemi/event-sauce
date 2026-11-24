use event_sauce_auth::{PasswordHasher, ResetToken, ResetTokenHash, Route, UserAggregate};
use event_sauce_core::EntityId;
use secrecy::SecretString;

use super::signin::Redeemed;
use super::{Finished, NewPasswordError, PasswordReset, ResetAccounts, ResetMailer, ResetPages};
use crate::diagnostics;
use crate::login::{AuthSession, LoginError};

impl<U, A, M, H, R> PasswordReset<U, A, M, H, R>
where
    U: UserAggregate,
    A: ResetAccounts,
    M: ResetMailer,
    H: PasswordHasher,
    R: Route<U>,
    R::Next: Finished<U>,
{
    /// Resets the password of `user` to `password` with the link `token`,
    /// then signs the user in.
    ///
    /// The application validates the form (the two passwords match, the
    /// length) before it calls this, so a form shown again never uses the
    /// link up. The reset is consumed and the password set in one step by
    /// [`ResetAccounts::redeem`]. The session then opens along the route the
    /// flow was built with: a user it asks a second factor of gets
    /// [`sign_in_again`](ResetPages::sign_in_again) and no session, as does a
    /// session that could not open.
    ///
    /// # Errors
    ///
    /// [`invalid_link`](ResetPages::invalid_link), for every way a link can
    /// be wrong, including a token that is not one.
    pub async fn redeem<P: ResetPages>(
        &self,
        pages: &P,
        auth: &mut AuthSession<U>,
        user: EntityId,
        token: &str,
        password: SecretString,
    ) -> Result<P::Response, P::Error> {
        let Ok(token) = ResetToken::parse(token) else {
            return Err(pages.invalid_link());
        };
        let hash = match self.hasher.hash(&password).await {
            Ok(hash) => hash,
            Err(error) => {
                diagnostics::internal(&error);
                return Ok(pages.new_password_form(Some(NewPasswordError::HashFailed)));
            }
        };
        let redeemed = self
            .accounts
            .redeem(user, ResetTokenHash::of(&token), hash, self.clock.now())
            .await;
        if let Err(error) = redeemed {
            diagnostics::conflict(&error);
            return Err(pages.invalid_link());
        }
        Ok(match self.open_session(auth, user).await {
            Ok(true) => pages.signed_in(),
            Ok(false) => pages.sign_in_again(),
            Err(error) => {
                if !error.is_refused() {
                    diagnostics::internal(&error);
                }
                pages.sign_in_again()
            }
        })
    }

    async fn open_session(
        &self,
        auth: &mut AuthSession<U>,
        user: EntityId,
    ) -> Result<bool, LoginError> {
        let begun = self.logins.begin(&self.first, Redeemed(user)).await?;
        let Some(login) = begun.done() else {
            return Ok(false);
        };
        auth.login(login.finish().await?).await?;
        Ok(true)
    }
}
