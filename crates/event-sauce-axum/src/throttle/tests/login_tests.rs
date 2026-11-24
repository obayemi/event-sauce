use super::*;

fn client_of(raw: &str) -> ClientAddr {
    ClientAddr(Some(raw.parse().unwrap()))
}

fn user() -> EntityId {
    EntityId::nil()
}

/// Fails `count` attempts of `account` from `client` in turn, each once the
/// wait of the previous one is over, and answers when the last one failed.
fn fail_password_in_turn(
    throttles: &LoginThrottles,
    account: &str,
    client: &ClientAddr,
    count: u32,
    mut at: Instant,
) -> Instant {
    for _ in 0..count {
        loop {
            match throttles.password(account, client, at) {
                Ok(_refused) => break,
                Err(refusal) => at += refusal.wait(),
            }
        }
    }
    at
}

/// Fails `count` attempts from `client`, each on an account of its own, so
/// that only the client's count grows.
fn fail_client_in_turn(
    throttles: &LoginThrottles,
    client: &ClientAddr,
    count: u32,
    mut at: Instant,
) -> Instant {
    for n in 0..count {
        at = fail_password_in_turn(throttles, &format!("u{n}"), client, 1, at);
    }
    at
}

fn fail_code_in_turn(
    throttles: &LoginThrottles,
    user: EntityId,
    client: &ClientAddr,
    count: u32,
    mut at: Instant,
) -> Instant {
    for _ in 0..count {
        loop {
            match throttles.second_factor(user, client, at) {
                Ok(_refused) => break,
                Err(refusal) => at += refusal.wait(),
            }
        }
    }
    at
}

#[test]
fn the_defaults_are_the_backoffs_saxy_p_ran_with() {
    let second = Duration::from_secs(1);
    assert_eq!(
        LoginThrottles::PASSWORD,
        Backoff {
            free: 5,
            base: second,
            cap: 60 * second,
            forget_after: 3600 * second
        }
    );
    assert_eq!(
        LoginThrottles::SECOND_FACTOR,
        Backoff {
            free: 5,
            base: second,
            cap: 900 * second,
            forget_after: 3600 * second
        }
    );
    assert_eq!(
        LoginThrottles::CLIENT,
        Backoff {
            free: 20,
            base: second,
            cap: 900 * second,
            forget_after: 3600 * second
        }
    );
    assert_eq!(
        LoginThrottles::RESET_REQUESTS,
        Backoff {
            free: 5,
            base: 60 * second,
            cap: 3600 * second,
            forget_after: 3600 * second
        }
    );
}

/// Wrong passwords from one client make that client wait on the account,
/// never another client: an attacker cannot keep the owner out.
#[test]
fn an_accounts_password_failures_make_only_their_client_wait() {
    let throttles = LoginThrottles::default();
    let attacker = client_of("192.0.2.1");
    let owner = client_of("198.51.100.7");
    let now = fail_password_in_turn(
        &throttles,
        "ada@example.com",
        &attacker,
        LoginThrottles::PASSWORD.free + 1,
        Instant::now(),
    );

    assert!(throttles
        .password("ada@example.com", &attacker, now)
        .is_err());
    assert!(throttles.password("ada@example.com", &owner, now).is_ok());
}

/// A success forgets the failures of its account, never those of its client:
/// one account of its own would otherwise clear them.
#[test]
fn a_success_keeps_the_failures_of_its_client() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let mut now = Instant::now();
    for attempt in 0..LoginThrottles::CLIENT.free {
        now = fail_password_in_turn(&throttles, &format!("u{attempt}"), &client, 1, now);
    }
    throttles
        .password("ada@example.com", &client, now)
        .expect("the client does not wait yet")
        .succeeded();

    now = fail_password_in_turn(&throttles, "ada@example.com", &client, 1, now);

    assert!(throttles.password("ada@example.com", &client, now).is_err());
}

/// A refusal attempt says how many whole seconds are left, rounded up,
/// for the longest of the account's and the client's waits.
#[test]
fn a_throttled_attempt_says_when_to_retry() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let now = fail_code_in_turn(
        &throttles,
        user(),
        &client,
        LoginThrottles::SECOND_FACTOR.free + 3,
        Instant::now(),
    );

    let refusal = throttles
        .second_factor(user(), &client, now + Duration::from_millis(500))
        .err()
        .expect("the account waits");

    assert_eq!(refusal.retry_after_secs(), 4);
}

#[test]
fn a_throttled_reset_request_says_when_to_retry() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let now = Instant::now();
    for _ in 0..=LoginThrottles::RESET_REQUESTS.free {
        throttles.reset_request(&client, now).unwrap();
    }

    let refusal = throttles.reset_request(&client, now).unwrap_err();

    assert_eq!(refusal.retry_after_secs(), 60);
}

/// An attempt counts against its account from its start: a burst of attempts
/// sent together cannot all slip in before the first refusal is counted, so
/// guessing a code in parallel gains nothing over guessing it in turn.
#[test]
fn attempts_sent_together_cannot_outrun_the_count_of_their_account() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let now = Instant::now();

    let started: Vec<_> = (0..100)
        .map(|_| throttles.second_factor(user(), &client, now))
        .collect();

    let admitted = started.iter().filter(|attempt| attempt.is_ok()).count();
    assert_eq!(admitted, LoginThrottles::SECOND_FACTOR.free as usize + 1);
}

/// An attempt counts against its client address from its start too: one
/// address sending attempts on many accounts together gets no more of them
/// than its own count allows.
#[test]
fn attempts_sent_together_cannot_outrun_the_count_of_their_client() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let now = Instant::now();

    let started: Vec<_> = (0..100)
        .map(|n| throttles.password(&format!("u{n}"), &client, now))
        .collect();

    let admitted = started.iter().filter(|attempt| attempt.is_ok()).count();
    assert_eq!(admitted, LoginThrottles::CLIENT.free as usize + 1);
}

/// An attempt refused because its account waits leaves its client as it
/// found it: a refusal of a waiting key is never counted, so it never makes
/// the client wait either.
#[test]
fn an_attempt_on_a_waiting_account_leaves_its_client_as_it_was() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let mut now = Instant::now();
    for n in 0..=LoginThrottles::CLIENT.free {
        now = fail_password_in_turn(&throttles, &format!("u{n}"), &client, 1, now);
    }
    now += LoginThrottles::CLIENT.base;
    let elsewhere = client_of("198.51.100.7");
    fail_code_in_turn(
        &throttles,
        user(),
        &elsewhere,
        LoginThrottles::SECOND_FACTOR.free + 1,
        now,
    );

    assert!(
        throttles.second_factor(user(), &client, now).is_err(),
        "the account waits"
    );
    assert!(
        throttles.password("ada@example.com", &client, now).is_ok(),
        "the client does not"
    );
}

/// A success leaves its client as it found it: an address busy with earlier
/// failures, its wait over, does not wait again because one of its attempts
/// succeeded, so a password step never makes its code step wait.
#[test]
fn a_success_leaves_its_client_as_it_was() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let mut now = Instant::now();
    for n in 0..LoginThrottles::CLIENT.free + 10 {
        now = fail_password_in_turn(&throttles, &format!("u{n}"), &client, 1, now);
    }
    now += LoginThrottles::CLIENT.cap;
    throttles
        .password("ada@example.com", &client, now)
        .expect("the client's wait is over")
        .succeeded();

    assert!(throttles.second_factor(user(), &client, now).is_ok());
}

#[test]
fn a_refusal_reports_the_longer_wait_of_the_account_and_the_client() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let elsewhere = client_of("198.51.100.7");
    let now = fail_code_in_turn(
        &throttles,
        user(),
        &elsewhere,
        LoginThrottles::SECOND_FACTOR.free + 4,
        Instant::now(),
    );
    let now = fail_client_in_turn(&throttles, &client, LoginThrottles::CLIENT.free + 1, now);

    let refusal = throttles
        .second_factor(user(), &client, now)
        .err()
        .expect("both wait");

    assert_eq!(refusal.wait(), Duration::from_secs(8));
}

#[test]
fn a_refusal_reports_the_client_wait_when_it_is_the_longer() {
    let throttles = LoginThrottles::default();
    let client = client_of("192.0.2.1");
    let now = fail_client_in_turn(
        &throttles,
        &client,
        LoginThrottles::CLIENT.free + 3,
        Instant::now(),
    );
    fail_code_in_turn(
        &throttles,
        user(),
        &client_of("198.51.100.7"),
        LoginThrottles::SECOND_FACTOR.free + 1,
        now,
    );

    let refusal = throttles
        .second_factor(user(), &client, now)
        .err()
        .expect("both wait");

    assert_eq!(refusal.wait(), Duration::from_secs(4));
}

#[test]
fn a_throttle_can_be_replaced_with_another_backoff() {
    let strict = Backoff {
        free: 0,
        base: Duration::from_secs(30),
        cap: Duration::from_secs(30),
        forget_after: Duration::from_secs(60),
    };
    let throttles = LoginThrottles::default().with_password(strict);
    let client = client_of("192.0.2.1");
    let now = Instant::now();
    drop(throttles.password("a", &client, now).unwrap());

    let refusal = throttles.password("a", &client, now).err().unwrap();

    assert_eq!(refusal.wait(), Duration::from_secs(30));
}

#[test]
fn every_throttle_can_be_replaced() {
    let never = Backoff {
        free: u32::MAX - 1,
        ..LoginThrottles::PASSWORD
    };
    let throttles = LoginThrottles::default()
        .with_second_factor(never)
        .with_client(never)
        .with_reset_requests(never);
    let client = client_of("192.0.2.1");
    let now = Instant::now();

    for _ in 0..50 {
        throttles.reset_request(&client, now).unwrap();
        drop(throttles.second_factor(user(), &client, now).unwrap());
    }
}
