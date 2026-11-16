use super::*;

#[test]
fn the_system_clock_reads_the_current_time() {
    let before = Utc::now();
    let now = SystemClock.now();
    let after = Utc::now();

    assert!(before <= now && now <= after);
}

#[test]
fn a_fixed_clock_always_reads_the_same_instant() {
    let instant = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let clock = FixedClock(instant);

    assert_eq!(clock.now(), instant);
    assert_eq!(clock.now(), instant);
}
