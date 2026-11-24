//! Growing delays for sign-in endpoints, never a lockout.
//!
//! A [`Throttle`] counts the failures of each key (an account, a client
//! address): the first few are free, then each one makes the key wait twice
//! as long as the previous one, up to a cap ([`Backoff`]). A key that waits is
//! refused without being counted, so the delay never outgrows its cap: an
//! attacker can slow an account down, never lock its owner out. An
//! [`Attempt`] counts as a failure of its account and of its client from its
//! start, until it succeeds, so attempts sent together gain nothing over
//! attempts sent in turn. A key with no failure for a while is forgotten, and
//! a table never holds more keys than its capacity. Every method takes the
//! instant it runs at, so tests never sleep.
//!
//! [`LoginThrottles`] bundles the throttles of the sign-in endpoints with
//! the defaults of a typical deployment, and [`Throttled`] answers a refused
//! request with `429` and a `Retry-After`. The client address the throttles
//! count comes from [`ClientAddr`](crate::ClientAddr).
//!
//! The state is in-process: each instance throttles on its own, which is
//! enough while the app runs a single replica. Behind several replicas an
//! attacker gets the free failures and the delays of each one.

mod attempt;
mod backoff;
mod login;
mod table;
mod throttled;

pub use attempt::Attempt;
pub use backoff::Backoff;
pub use login::LoginThrottles;
pub use table::{Charge, Throttle};
pub use throttled::Throttled;
