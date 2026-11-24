use chrono::{DateTime, TimeDelta, Utc};
use event_sauce_auth::OAuthState;
use event_sauce_core::EntityId;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "tests/link_tests.rs"]
mod tests;

/// Whether a link to a signed-in user carries the proof of a current second
/// factor code, which the application's rules may ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkProof {
    /// A code was proved for this link, recently enough.
    Proved,
    /// No code was proved, or it was proved too long ago.
    Unproved,
}

#[derive(Serialize, Deserialize)]
pub(super) struct PendingLink {
    user: EntityId,
    state: OAuthState,
    proved_until: Option<DateTime<Utc>>,
}

impl PendingLink {
    pub(super) fn new(
        user: EntityId,
        state: OAuthState,
        proof: LinkProof,
        now: DateTime<Utc>,
        ttl: TimeDelta,
    ) -> Self {
        Self {
            user,
            state,
            proved_until: (proof == LinkProof::Proved).then_some(now + ttl),
        }
    }

    pub(super) fn user(&self) -> EntityId {
        self.user
    }

    pub(super) fn answers(&self, returned: &OAuthState) -> bool {
        &self.state == returned
    }

    pub(super) fn proof_at(&self, now: DateTime<Utc>) -> LinkProof {
        if self.proved_until.is_some_and(|until| now <= until) {
            LinkProof::Proved
        } else {
            LinkProof::Unproved
        }
    }
}
