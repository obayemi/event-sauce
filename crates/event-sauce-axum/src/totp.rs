//! The TOTP code of a sign-in, and the code that proves a signed-in user
//! before it changes its second factor.
//!
//! [`TotpChallenge`] is the second step of a login whose route asked for a
//! code ([`TotpIfEnrolled`](event_sauce_auth::TotpIfEnrolled)): it opens the
//! session of a login that is done, parks one that waits for its code, and
//! ends the waiting login with the code, throttled per user and client.
//! The waiting login is kept in the session record, never in a cookie of its
//! own: the browser keeps the one cookie it has, and the record is gone with
//! the session.
//!
//! [`TotpProof`] checks the code of a user who is signed in and changes its
//! own settings: enrolling a key, turning it off, anything the application
//! wants a current code for. A change renews the acting session, so the
//! user's other sessions end.
//!
//! The engines never render: the application answers each step through
//! [`TotpLoginPages`], and reads the [`Proof`] and the [`Enrolment`] of the
//! settings steps itself.

mod challenge;
mod proof;

pub use challenge::{code_page, CodeRefusal, OpenError, Opened, TotpChallenge, TotpLoginPages};
pub(crate) use proof::renew;
pub use proof::{Enrolment, Proof, TotpProof};
