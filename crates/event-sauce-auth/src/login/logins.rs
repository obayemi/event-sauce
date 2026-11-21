//! [`Logins`], where logins start.

use std::{fmt, sync::Arc};

use event_sauce_core::{AggregateRoot, EventStore};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    login::users::{Found, StoreUsers, Users},
    Advance, AuthError, Clock, Done, Entered, FirstFactor, Login, LoginMethod, LoginStore,
    PendingLogins, Route, SignUpError, SystemClock, UserAggregate, UserCache,
};

/// Starts logins for the users of one event store.
///
/// It is cheap to clone and does not name the event store: the store sits
/// behind a trait object, so a [`Login`] does not name it either.
///
/// # Examples
///
/// A password login that asks the users who enrolled a TOTP code for it. A
/// user without a secret finishes right away, the others are saved while the
/// client fetches the code, then resumed:
///
/// ```no_run
/// # #[cfg(all(feature = "argon2", feature = "totp"))]
/// # mod wiring {
/// use std::sync::Arc;
///
/// use event_sauce_auth::{
///     Argon2Hasher, Authenticated, FirstFactor, HasPassword, HasTotp, LoginStore, Logins,
///     NeedTotp, PasswordAttempt, PasswordLogin, Totp, TotpIfEnrolled, TotpOrDone, UserAggregate,
/// };
/// use event_sauce_core::EventStore;
/// use secrecy::SecretString;
/// use serde::{de::DeserializeOwned, Serialize};
///
/// async fn log_in<U, S>(
///     store: Arc<S>,
///     login_store: Arc<dyn LoginStore>,
///     attempt: PasswordAttempt<<U::Credentials as HasPassword>::Login>,
///     code: SecretString,
/// ) -> Result<Authenticated<U>, Box<dyn std::error::Error>>
/// where
///     S: EventStore + 'static,
///     U: UserAggregate,
///     U::Credentials: HasPassword + HasTotp,
///     U::DeletedState: Serialize + DeserializeOwned,
/// {
///     let logins = Logins::<U>::new(store);
///     let pending = logins.pending(login_store.clone());
///     let factor = FirstFactor::new(PasswordLogin::new(Argon2Hasher::default()), TotpIfEnrolled);
///     match logins.begin(&factor, attempt).await? {
///         TotpOrDone::Done(login) => Ok(login.finish().await?),
///         TotpOrDone::Totp(login) => {
///             let id = pending.save(login).await?;
///             let login = pending.resume::<NeedTotp>(id).await?;
///             let done = login.prove(&Totp::new(login_store), code).await?;
///             Ok(done.finish().await?)
///         }
///     }
/// }
/// # }
/// ```
pub struct Logins<U: UserAggregate> {
    users: Arc<dyn Users<U>>,
    clock: Arc<dyn Clock>,
}

impl<U: UserAggregate> Clone for Logins<U> {
    fn clone(&self) -> Self {
        Self {
            users: self.users.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<U: UserAggregate> fmt::Debug for Logins<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Logins").finish_non_exhaustive()
    }
}

impl<U: UserAggregate> Logins<U> {
    /// Logins for the users of `store`.
    #[must_use]
    pub fn new<S: EventStore + 'static>(store: Arc<S>) -> Self
    where
        U::DeletedState: Serialize + DeserializeOwned,
    {
        Self::over(Arc::new(StoreUsers::new(store)))
    }

    pub(crate) fn over(users: Arc<dyn Users<U>>) -> Self {
        Self {
            users,
            clock: Arc::new(SystemClock),
        }
    }

    /// Loads users through `cache`, the version-checked cache the session
    /// backend shares.
    #[must_use]
    pub fn with_shared_cache(mut self, cache: Arc<dyn UserCache<U>>) -> Self {
        self.users = self.users.with_cache(cache);
        self
    }

    /// Reads the start time of logins from `clock` instead of the system
    /// clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Runs the first factor, then its route.
    ///
    /// 1. The method locates the user. When nobody matches, its `on_unknown`
    ///    hook runs and the attempt comes back in [`AuthError::Unknown`].
    /// 2. The user is loaded and must be active, whatever the method.
    /// 3. The method verifies the attempt against the identity.
    /// 4. The session fingerprint and the start time are pinned, and the
    ///    route picks where the login goes. Nothing here is a session yet.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when the attempt does not start a login.
    pub async fn begin<M, R>(
        &self,
        first: &FirstFactor<M, R>,
        attempt: M::Attempt,
    ) -> Result<R::Next, AuthError<M::Attempt>>
    where
        M: LoginMethod<U::Credentials>,
        R: Route<U>,
    {
        let method = first.method();
        let found = match method.locate(self.users.locator(), &attempt).await? {
            Some(id) => self.users.find(id).await?,
            None => Found::Missing,
        };
        let user = match found {
            Found::Active(user) => user,
            Found::Inactive => return Err(AuthError::Inactive),
            Found::Missing => {
                method.on_unknown(&attempt).await;
                return Err(AuthError::Unknown { attempt });
            }
        };
        method.verify(user.identity(), &attempt).await?;
        let login = Login::new(user, self.clock.now(), self.users.clone(), Entered);
        Ok(first.route().route(Advance(login)))
    }

    /// Commits a brand-new user and starts its login in [`Done`].
    ///
    /// The commit expects the initial version, so it fails on an id that
    /// exists: this can never log into an account somebody already holds.
    ///
    /// # Errors
    ///
    /// Returns [`SignUpError::NotNew`] when the aggregate is not new, and
    /// [`SignUpError::Store`] when the commit fails, a claim conflict
    /// included.
    pub async fn sign_up(&self, user: AggregateRoot<U>) -> Result<Login<U, Done>, SignUpError> {
        let user = self.users.create(user).await?;
        Ok(Login::new(user, self.clock.now(), self.users.clone(), Done))
    }

    /// Pending logins kept in `store`, between two requests.
    #[must_use]
    pub fn pending(&self, store: Arc<dyn LoginStore>) -> PendingLogins<U> {
        PendingLogins::new(store, self.users.clone())
    }
}

#[cfg(test)]
mod tests;
