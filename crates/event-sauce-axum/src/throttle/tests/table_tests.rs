use super::*;

const SECOND: Duration = Duration::from_secs(1);

fn backoff() -> Backoff {
    Backoff {
        free: 2,
        base: SECOND,
        cap: 8 * SECOND,
        forget_after: 60 * SECOND,
    }
}

fn throttle() -> Throttle {
    Throttle::new(backoff()).with_capacity(100)
}

#[test]
fn the_free_failures_owe_no_delay() {
    let throttle = throttle();
    let now = Instant::now();
    throttle.fail("a", now);
    throttle.fail("a", now);

    assert_eq!(throttle.check("a", now), Ok(()));
}

#[test]
fn a_key_waits_out_its_delay_then_passes_again() {
    let throttle = throttle();
    let now = Instant::now();
    for _ in 0..4 {
        throttle.fail("a", now);
    }

    assert_eq!(throttle.check("a", now), Err(2 * SECOND));
    assert_eq!(throttle.check("a", now + SECOND), Err(SECOND));
    assert_eq!(throttle.check("a", now + 2 * SECOND), Ok(()));
}

#[test]
fn a_delay_never_outgrows_its_cap_so_nobody_is_locked_out() {
    let throttle = throttle();
    let now = Instant::now();
    for _ in 0..1_000 {
        throttle.fail("a", now);
    }

    assert_eq!(throttle.check("a", now + 8 * SECOND), Ok(()));
}

#[test]
fn keys_are_throttled_apart() {
    let throttle = throttle();
    let now = Instant::now();
    for _ in 0..3 {
        throttle.fail("a", now);
    }

    assert!(throttle.check("a", now).is_err());
    assert_eq!(throttle.check("b", now), Ok(()));
}

#[test]
fn a_success_forgets_the_failures() {
    let throttle = throttle();
    let now = Instant::now();
    for _ in 0..3 {
        throttle.fail("a", now);
    }
    throttle.clear("a");

    assert_eq!(throttle.check("a", now), Ok(()));
    throttle.fail("a", now);
    assert_eq!(throttle.check("a", now), Ok(()), "free failures again");
}

#[test]
fn failures_are_forgotten_after_a_quiet_while() {
    let throttle = throttle();
    let now = Instant::now();
    for _ in 0..3 {
        throttle.fail("a", now);
    }
    let later = now + 60 * SECOND;
    throttle.fail("a", later);

    assert_eq!(throttle.check("a", later), Ok(()), "counted from one again");
}

#[test]
fn a_full_table_drops_forgotten_keys_first_then_the_oldest() {
    let throttle = Throttle::new(backoff()).with_capacity(2);
    let now = Instant::now();
    for _ in 0..3 {
        throttle.fail("old", now);
    }
    for _ in 0..3 {
        throttle.fail("recent", now + 30 * SECOND);
    }

    throttle.fail("new", now + 61 * SECOND);
    assert_eq!(throttle.len(), 2);
    assert!(throttle.check("recent", now + 30 * SECOND).is_err());

    throttle.fail("newest", now + 62 * SECOND);
    assert_eq!(throttle.len(), 2);
    assert_eq!(
        throttle.check("recent", now + 30 * SECOND),
        Ok(()),
        "the oldest went"
    );
}

#[test]
fn a_table_remembers_ten_thousand_keys_unless_told_otherwise() {
    let throttle = Throttle::new(backoff());
    let now = Instant::now();
    for key in 0..10_001 {
        throttle.fail(&key.to_string(), now + Duration::from_millis(key));
    }

    assert_eq!(throttle.len(), 10_000);
}

#[test]
fn a_charge_counts_a_failure_and_a_refund_takes_it_back() {
    let throttle = throttle();
    let now = Instant::now();
    let charge = throttle.charge("a", now).unwrap();
    assert_eq!(throttle.len(), 1);

    throttle.refund("a", charge);

    assert_eq!(throttle.len(), 0);
}

#[test]
fn a_refund_leaves_a_key_as_the_charge_found_it() {
    let throttle = throttle();
    let now = Instant::now();
    throttle.fail("a", now);
    let charge = throttle.charge("a", now).unwrap();
    throttle.fail("a", now);

    throttle.refund("a", charge);

    assert_eq!(throttle.check("a", now), Ok(()), "two failures are free");
    throttle.fail("a", now);
    assert_eq!(
        throttle.check("a", now),
        Err(SECOND),
        "the two that count and this one owe one second"
    );
}

#[test]
fn a_charge_on_a_waiting_key_is_refused_and_counts_nothing() {
    let throttle = throttle();
    let now = Instant::now();
    for _ in 0..3 {
        throttle.fail("a", now);
    }

    assert_eq!(throttle.charge("a", now).err(), Some(SECOND));
    assert_eq!(throttle.check("a", now + SECOND), Ok(()));
}
