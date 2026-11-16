use super::*;

#[test]
fn whole_milliseconds_pass_through() {
    assert_eq!(millis(Duration::from_millis(1500)).unwrap(), 1500);
    assert_eq!(millis(Duration::from_secs(60)).unwrap(), 60_000);
}

#[test]
fn sub_millisecond_durations_round_up_to_one_millisecond() {
    assert_eq!(millis(Duration::from_micros(1)).unwrap(), 1);
    assert_eq!(millis(Duration::from_nanos(1_000_001)).unwrap(), 2);
}

#[test]
fn a_zero_ttl_is_refused() {
    assert!(matches!(
        millis(Duration::ZERO),
        Err(LoginStoreError::InvalidTtl(ttl)) if ttl.is_zero()
    ));
}

#[test]
fn the_longest_ttl_is_accepted_and_anything_above_is_refused() {
    assert!(millis(MAX_TTL).is_ok());
    assert!(matches!(
        millis(MAX_TTL + Duration::from_millis(1)),
        Err(LoginStoreError::InvalidTtl(_))
    ));
}
