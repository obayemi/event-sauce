use std::time::{Duration, Instant};

use event_sauce_core::EntityId;

use super::{Attempt, Backoff, Throttle, Throttled};
use crate::client_ip::ClientAddr;

#[cfg(test)]
#[path = "tests/login_tests.rs"]
mod tests;

const SECOND: Duration = Duration::from_secs(1);
const MINUTE: Duration = Duration::from_mins(1);
const QUARTER_HOUR: Duration = Duration::from_mins(15);
const HOUR: Duration = Duration::from_hours(1);

/// The throttles of the sign-in endpoints.
///
/// Each throttle has a [`Backoff`] default, replaced with the `with_`
/// methods. A password attempt counts against its account as seen from its
/// client, and a second-factor attempt against its user, and both against
/// their client, so an attacker failing on an account never makes its owner
/// wait.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use event_sauce_axum::{ClientAddr, LoginThrottles};
///
/// let throttles = LoginThrottles::default();
/// let client = ClientAddr(Some("192.0.2.1".parse().unwrap()));
///
/// let attempt = throttles
///     .password("ada@example.com", &client, Instant::now())
///     .expect("the first attempts are free");
/// attempt.succeeded();
/// ```
#[derive(Debug)]
pub struct LoginThrottles {
    password: Throttle,
    second_factor: Throttle,
    client: Throttle,
    reset_requests: Throttle,
}

impl LoginThrottles {
    /// Wrong passwords of one account from one client: counted per client,
    /// so an attacker failing on an account never makes its owner wait.
    pub const PASSWORD: Backoff = Backoff {
        free: 5,
        base: SECOND,
        cap: MINUTE,
        forget_after: HOUR,
    };

    /// Wrong second-factor codes of one user. Only someone past the password
    /// gets there, so the cap is longer.
    pub const SECOND_FACTOR: Backoff = Backoff {
        free: 5,
        base: SECOND,
        cap: QUARTER_HOUR,
        forget_after: HOUR,
    };

    /// Every failed password or code from one client address. Generous, as a
    /// whole band behind one router shares it.
    pub const CLIENT: Backoff = Backoff {
        free: 5 * 4,
        base: SECOND,
        cap: QUARTER_HOUR,
        forget_after: HOUR,
    };

    /// Every reset link one client address asks for, whatever the account.
    pub const RESET_REQUESTS: Backoff = Backoff {
        free: 5,
        base: MINUTE,
        cap: HOUR,
        forget_after: HOUR,
    };

    /// Throttles passwords with `backoff` instead of [`Self::PASSWORD`].
    #[must_use]
    pub fn with_password(mut self, backoff: Backoff) -> Self {
        self.password = Throttle::new(backoff);
        self
    }

    /// Throttles second-factor codes with `backoff` instead of
    /// [`Self::SECOND_FACTOR`].
    #[must_use]
    pub fn with_second_factor(mut self, backoff: Backoff) -> Self {
        self.second_factor = Throttle::new(backoff);
        self
    }

    /// Throttles client addresses with `backoff` instead of [`Self::CLIENT`].
    #[must_use]
    pub fn with_client(mut self, backoff: Backoff) -> Self {
        self.client = Throttle::new(backoff);
        self
    }

    /// Throttles reset requests with `backoff` instead of
    /// [`Self::RESET_REQUESTS`].
    #[must_use]
    pub fn with_reset_requests(mut self, backoff: Backoff) -> Self {
        self.reset_requests = Throttle::new(backoff);
        self
    }

    /// Starts a password attempt on `account` from `client` at `now`.
    ///
    /// `account` is the key of the account as the application normalises it,
    /// so that every spelling of one account shares its count: the throttle
    /// takes it as is.
    ///
    /// # Errors
    ///
    /// [`Throttled`] with the longer wait while the account, as seen from
    /// this client, or the client waits; nothing is counted then.
    pub fn password(
        &self,
        account: &str,
        client: &ClientAddr,
        now: Instant,
    ) -> Result<Attempt<'_>, Throttled> {
        self.attempt(
            &self.password,
            format!("{account} {}", client.key()),
            client,
            now,
        )
    }

    /// Starts a second-factor attempt of `user` from `client` at `now`.
    ///
    /// # Errors
    ///
    /// [`Throttled`] with the longer wait while the user or the client waits;
    /// nothing is counted then.
    pub fn second_factor(
        &self,
        user: EntityId,
        client: &ClientAddr,
        now: Instant,
    ) -> Result<Attempt<'_>, Throttled> {
        self.attempt(&self.second_factor, user.to_string(), client, now)
    }

    /// Counts a reset request from `client` at `now`, unless it must wait.
    ///
    /// # Errors
    ///
    /// [`Throttled`] while the client waits.
    pub fn reset_request(&self, client: &ClientAddr, now: Instant) -> Result<(), Throttled> {
        self.reset_requests
            .charge(&client.key(), now)
            .map(drop)
            .map_err(Throttled::new)
    }

    /// Counts the attempt as a failure of `key` in `account` and of `client`
    /// from its start; only [`Attempt::succeeded`] takes it back.
    fn attempt<'a>(
        &'a self,
        account: &'a Throttle,
        key: String,
        client: &ClientAddr,
        now: Instant,
    ) -> Result<Attempt<'a>, Throttled> {
        let client_key = client.key();
        let client_charge = match self.client.charge(&client_key, now) {
            Ok(charge) => charge,
            Err(client_wait) => {
                let account_wait = account.check(&key, now).err();
                return Err(Throttled::new(
                    account_wait.map_or(client_wait, |wait| wait.max(client_wait)),
                ));
            }
        };
        if let Err(account_wait) = account.charge(&key, now) {
            self.client.refund(&client_key, client_charge);
            return Err(Throttled::new(account_wait));
        }
        Ok(Attempt {
            account,
            account_key: key,
            clients: &self.client,
            client_key,
            client_charge,
        })
    }
}

impl Default for LoginThrottles {
    fn default() -> Self {
        Self {
            password: Throttle::new(Self::PASSWORD),
            second_factor: Throttle::new(Self::SECOND_FACTOR),
            client: Throttle::new(Self::CLIENT),
            reset_requests: Throttle::new(Self::RESET_REQUESTS),
        }
    }
}
