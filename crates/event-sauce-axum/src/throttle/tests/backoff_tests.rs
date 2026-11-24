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

#[test]
fn each_failure_past_the_free_ones_doubles_the_delay() {
    let backoff = backoff();
    let delays: Vec<_> = (1..=7).map(|failures| backoff.delay(failures)).collect();

    assert_eq!(
        delays,
        [
            Duration::ZERO,
            Duration::ZERO,
            SECOND,
            2 * SECOND,
            4 * SECOND,
            8 * SECOND,
            8 * SECOND,
        ]
    );
}

#[test]
fn a_huge_count_of_failures_stays_at_the_cap() {
    assert_eq!(backoff().delay(u32::MAX), 8 * SECOND);
}

#[test]
fn a_backoff_with_every_failure_free_never_waits() {
    let backoff = Backoff {
        free: u32::MAX,
        ..backoff()
    };

    assert_eq!(backoff.delay(u32::MAX), Duration::ZERO);
}
