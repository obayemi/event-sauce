use super::{Charge, Throttle};

/// An attempt on an account, counted as a failure of the account and of its
/// client from its start: a refusal, or an outage, leaves it counted, and only
/// a success takes it back.
#[must_use = "an attempt counts as a failure unless it succeeds"]
pub struct Attempt<'a> {
    pub(super) account: &'a Throttle,
    pub(super) account_key: String,
    pub(super) clients: &'a Throttle,
    pub(super) client_key: String,
    pub(super) client_charge: Charge,
}

impl Attempt<'_> {
    /// The attempt proved itself: the account's failures are forgotten, and
    /// the client's count of this attempt. The client's other failures stay:
    /// one account of its own would otherwise clear them.
    pub fn succeeded(self) {
        self.account.clear(&self.account_key);
        self.clients.refund(&self.client_key, self.client_charge);
    }
}
