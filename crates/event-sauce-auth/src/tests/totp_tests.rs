use chrono::{DateTime, Utc};
use event_sauce_core::AggregateClaim;
use serde::{Deserialize, Serialize};

use super::*;
use crate::{test_support::FakeLoginStore, Credentials, Fingerprint, FixedClock, LoginTtl};

const T0: i64 = 1_700_000_010;
const STEP: i64 = 30;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Enrolled(Option<TotpSecret>);

impl Credentials for Enrolled {
    fn claims(&self) -> Vec<AggregateClaim> {
        Vec::new()
    }

    fn fingerprint(&self, fingerprint: &mut Fingerprint) {
        self.feed_totp_fingerprint(fingerprint);
    }
}

impl HasTotp for Enrolled {
    fn totp_secret(&self) -> Option<&TotpSecret> {
        self.0.as_ref()
    }
}

fn secret(seed: u8) -> TotpSecret {
    TotpSecret::from([seed; 20])
}

fn enrolled(seed: u8) -> Enrolled {
    Enrolled(Some(secret(seed)))
}

fn at(seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(seconds, 0).unwrap()
}

fn totp_at(seconds: i64, store: &Arc<FakeLoginStore>) -> Totp {
    Totp::new(store.clone()).with_clock(Arc::new(FixedClock(at(seconds))))
}

fn code(seed: u8, seconds: i64) -> SecretString {
    let reference = totp_rs::Builder::new()
        .with_secret(secret(seed))
        .build()
        .unwrap();
    SecretString::from(
        reference
            .generate(u64::try_from(seconds).unwrap())
            .to_string(),
    )
}

async fn verify(
    totp: &Totp,
    credentials: &Enrolled,
    response: &SecretString,
) -> Result<(), Rejected> {
    let secret = credentials.totp_secret().ok_or(Rejected::Mismatch)?;
    totp.check(secret, response).await
}

#[tokio::test]
async fn the_code_of_the_current_step_is_accepted() {
    let store = Arc::new(FakeLoginStore::default());

    let verified = verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0)).await;

    assert!(verified.is_ok());
}

#[tokio::test]
async fn the_codes_of_the_neighbouring_steps_are_accepted() {
    let store = Arc::new(FakeLoginStore::default());

    let previous = verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0 - STEP)).await;
    let next = verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0 + STEP)).await;

    assert!(previous.is_ok() && next.is_ok());
}

#[tokio::test]
async fn a_code_two_steps_away_is_rejected() {
    let store = Arc::new(FakeLoginStore::default());

    let stale = verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0 - 2 * STEP)).await;
    let early = verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0 + 2 * STEP)).await;

    assert!(matches!(stale, Err(Rejected::Mismatch)));
    assert!(matches!(early, Err(Rejected::Mismatch)));
}

#[tokio::test]
async fn a_wrong_code_is_rejected() {
    let store = Arc::new(FakeLoginStore::default());

    let verified = verify(&totp_at(T0, &store), &enrolled(1), &code(2, T0)).await;

    assert!(matches!(verified, Err(Rejected::Mismatch)));
}

#[tokio::test]
async fn something_that_is_not_a_code_is_rejected() {
    let store = Arc::new(FakeLoginStore::default());

    for garbage in ["", "abcdef", "12345", "1234567", " 123456"] {
        let verified = verify(
            &totp_at(T0, &store),
            &enrolled(1),
            &SecretString::from(garbage),
        )
        .await;

        assert!(
            matches!(verified, Err(Rejected::Mismatch)),
            "{garbage:?} was accepted"
        );
    }
}

#[tokio::test]
async fn a_step_is_accepted_only_once() {
    let store = Arc::new(FakeLoginStore::default());
    let totp = totp_at(T0, &store);

    let first = verify(&totp, &enrolled(1), &code(1, T0)).await;
    let replay = verify(&totp, &enrolled(1), &code(1, T0)).await;

    assert!(first.is_ok());
    assert!(matches!(replay, Err(Rejected::Mismatch)));
}

#[tokio::test]
async fn a_replay_is_caught_from_a_later_instant_of_the_same_window() {
    let store = Arc::new(FakeLoginStore::default());

    let first = verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0)).await;
    let replay = verify(&totp_at(T0 + STEP, &store), &enrolled(1), &code(1, T0)).await;

    assert!(first.is_ok());
    assert!(matches!(replay, Err(Rejected::Mismatch)));
}

#[tokio::test]
async fn the_next_step_is_still_accepted_after_one_was_used() {
    let store = Arc::new(FakeLoginStore::default());

    let first = verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0)).await;
    let second = verify(
        &totp_at(T0 + STEP, &store),
        &enrolled(1),
        &code(1, T0 + STEP),
    )
    .await;

    assert!(first.is_ok() && second.is_ok());
}

#[tokio::test]
async fn a_rejected_code_does_not_use_up_the_step() {
    let store = Arc::new(FakeLoginStore::default());
    let totp = totp_at(T0, &store);

    let wrong = verify(&totp, &enrolled(1), &code(2, T0)).await;
    let right = verify(&totp, &enrolled(1), &code(1, T0)).await;

    assert!(wrong.is_err());
    assert!(right.is_ok());
}

#[tokio::test]
async fn replay_protection_is_per_secret() {
    let store = Arc::new(FakeLoginStore::default());
    let totp = totp_at(T0, &store);

    let ada = verify(&totp, &enrolled(1), &code(1, T0)).await;
    let eve = verify(&totp, &enrolled(2), &code(2, T0)).await;

    assert!(ada.is_ok() && eve.is_ok());
}

#[tokio::test]
async fn a_used_step_is_remembered_for_the_whole_window_it_could_be_replayed_in() {
    let store = Arc::new(FakeLoginStore::default());

    verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0))
        .await
        .unwrap();

    assert_eq!(
        store.ttls(),
        [LoginTtl::new(Duration::from_secs(90)).unwrap()]
    );
}

#[tokio::test]
async fn the_replay_mark_does_not_expose_the_secret() {
    let store = Arc::new(FakeLoginStore::default());

    verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0))
        .await
        .unwrap();

    let secret_base32 = secret(1).to_base32();
    assert!(store
        .keys()
        .iter()
        .all(|key| key.starts_with("totp:") && !key.contains(&secret_base32)));
}

#[tokio::test]
async fn a_failing_store_is_reported_not_swallowed() {
    let store = Arc::new(FakeLoginStore::broken());

    let verified = verify(&totp_at(T0, &store), &enrolled(1), &code(1, T0)).await;

    assert!(matches!(verified, Err(Rejected::LoginStore(_))));
}

#[tokio::test]
async fn the_system_clock_is_the_default() {
    let store = Arc::new(FakeLoginStore::default());
    let now = Utc::now().timestamp();

    let verified = verify(&Totp::new(store.clone()), &enrolled(1), &code(1, now)).await;

    assert!(verified.is_ok());
}

#[test]
fn enrolling_changes_the_fingerprint() {
    let before = crate::credentials::fingerprint_of(&Enrolled(None));
    let after = crate::credentials::fingerprint_of(&enrolled(1));

    assert_ne!(before, after);
}

#[test]
fn replacing_the_secret_changes_the_fingerprint() {
    assert_ne!(
        crate::credentials::fingerprint_of(&enrolled(1)),
        crate::credentials::fingerprint_of(&enrolled(2))
    );
}

#[test]
fn the_same_secret_gives_the_same_fingerprint() {
    assert_eq!(
        crate::credentials::fingerprint_of(&enrolled(1)),
        crate::credentials::fingerprint_of(&enrolled(1))
    );
}

#[test]
fn the_replay_key_is_the_lowercase_hex_digest_of_the_secret_and_the_step() {
    let key = Totp::replay_key(&secret(1), 7);

    assert_eq!(
        key.as_str(),
        "totp:d2b3c25e318be0c985ecdd042683796b39789bf8b07f398f52e57f97e544a331:7"
    );
}

#[test]
fn the_otpauth_uri_carries_issuer_account_and_secret() {
    let secret = TotpSecret::try_from_base32("JBSWY3DPEHPK3PXP").unwrap();

    let uri = otpauth_uri("Saxy-P", "ada@example.com", &secret);

    assert_eq!(
        uri,
        "otpauth://totp/Saxy-P:ada%40example.com?secret=JBSWY3DPEHPK3PXP&issuer=Saxy-P"
    );
}
