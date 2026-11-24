use async_trait::async_trait;
use axum::BoxError;
use chrono::{DateTime, Utc};
use event_sauce_auth::{PasswordHash, ResetToken, ResetTokenHash};
use event_sauce_core::EntityId;

/// A reset waiting for its link, as the application stored it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingReset {
    /// The hash of the token the link carries.
    pub hash: ResetTokenHash,
    /// The last instant the link works.
    pub expires_at: DateTime<Utc>,
}

/// The accounts of the application, as a reset reads and changes them.
///
/// The reset stays in the application's own aggregate: its cooldown, its
/// validity and the rule that a user may hold one at a time are the
/// application's, and a link another issuer made (an administrator) is
/// redeemed through the same flow.
#[async_trait]
pub trait ResetAccounts: Send + Sync + 'static {
    /// Why an account could not be read or changed. The flow logs it and
    /// never shows it.
    type Error: std::error::Error + Send + Sync + 'static;

    /// The user a typed login names, if any.
    ///
    /// # Errors
    ///
    /// When the lookup fails.
    async fn holder(&self, login: &str) -> Result<Option<EntityId>, Self::Error>;

    /// Records a reset of `user` with `hash`, as the user itself, and
    /// answers the address to mail the link to, or `None` when the
    /// application refuses: a reset too soon after the last, an inactive
    /// account, no address.
    ///
    /// # Errors
    ///
    /// When the record could not be written.
    async fn record(
        &self,
        user: EntityId,
        hash: ResetTokenHash,
        now: DateTime<Utc>,
    ) -> Result<Option<String>, Self::Error>;

    /// The reset `user` waits for, to check a link before its form. It never
    /// consumes it.
    ///
    /// # Errors
    ///
    /// When the reset could not be read.
    async fn pending(&self, user: EntityId) -> Result<Option<PendingReset>, Self::Error>;

    /// Consumes the reset of `user` presented with `hash` and sets
    /// `password`, atomically.
    ///
    /// # Errors
    ///
    /// Any refusal: another token, an expired or used reset, a user gone. The
    /// flow answers all of them with the same invalid link.
    async fn redeem(
        &self,
        user: EntityId,
        hash: ResetTokenHash,
        password: PasswordHash,
        now: DateTime<Utc>,
    ) -> Result<(), Self::Error>;

    /// The path of the link that resets `user` with `token`, from the root
    /// of the origin: the application's route, shared with whoever else
    /// issues links.
    fn link_path(&self, user: EntityId, token: &ResetToken) -> String;
}

/// Sends the link of a reset.
#[async_trait]
pub trait ResetMailer: Send + Sync + 'static {
    /// What the request prepared in its own context, moved into the
    /// background task that has none: the translated texts of the email.
    type Draft: Send + 'static;

    /// Mails `link` to `to`, as `draft` says.
    ///
    /// # Errors
    ///
    /// When the mail could not be sent. The flow logs it and never shows it.
    async fn send(&self, draft: Self::Draft, to: &str, link: &str) -> Result<(), BoxError>;
}
