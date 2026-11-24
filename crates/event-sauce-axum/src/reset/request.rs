use std::sync::Arc;
use std::time::Instant;

use event_sauce_auth::{Clock, PasswordHasher, ResetToken, Route, UserAggregate};

use super::{Finished, PasswordReset, PublicOrigin, ResetAccounts, ResetMailer, ResetPages};
use crate::client_ip::ClientAddr;
use crate::diagnostics;

impl<U, A, M, H, R> PasswordReset<U, A, M, H, R>
where
    U: UserAggregate,
    A: ResetAccounts,
    M: ResetMailer,
    H: PasswordHasher,
    R: Route<U>,
    R::Next: Finished<U>,
{
    /// Asks for a link for `login`.
    ///
    /// The client is throttled first, whatever the login. The answer is
    /// [`request_sent`](ResetPages::request_sent), at once and whether or
    /// not an account holds the login: the lookup, the record and the email
    /// run in a spawned task, which only logs its failures. `draft` is what
    /// the email needs from the request, moved into that task.
    ///
    /// It spawns the task, so it must run inside a tokio runtime.
    ///
    /// # Errors
    ///
    /// The [`Throttled`](crate::Throttled) of the client while it waits.
    pub fn request<P: ResetPages>(
        &self,
        pages: &P,
        client: &ClientAddr,
        login: String,
        draft: M::Draft,
    ) -> Result<P::Response, P::Error> {
        self.throttles.reset_request(client, Instant::now())?;
        tokio::spawn(send_link(
            self.accounts.clone(),
            self.mailer.clone(),
            self.origin.clone(),
            self.clock.clone(),
            login,
            draft,
        ));
        Ok(pages.request_sent())
    }
}

async fn send_link<A: ResetAccounts, M: ResetMailer>(
    accounts: Arc<A>,
    mailer: Arc<M>,
    origin: PublicOrigin,
    clock: Arc<dyn Clock>,
    login: String,
    draft: M::Draft,
) {
    let user = match accounts.holder(&login).await {
        Ok(Some(user)) => user,
        Ok(None) => return,
        Err(error) => return diagnostics::internal(&error),
    };
    let (token, hash) = ResetToken::issue();
    let to = match accounts.record(user, hash, clock.now()).await {
        Ok(Some(to)) => to,
        Ok(None) => return,
        Err(error) => return diagnostics::internal(&error),
    };
    let link = origin.link(&accounts.link_path(user, &token));
    if let Err(error) = mailer.send(draft, &to, &link).await {
        diagnostics::internal(&*error);
    }
}
