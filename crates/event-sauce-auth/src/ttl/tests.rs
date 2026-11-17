use super::*;

#[test]
fn whole_milliseconds_pass_through() {
    assert_eq!(millis(Duration::from_millis(1500)), Some(1500));
    assert_eq!(millis(Duration::from_secs(60)), Some(60_000));
}

#[test]
fn sub_millisecond_durations_round_up_to_one_millisecond() {
    assert_eq!(millis(Duration::from_micros(1)), Some(1));
    assert_eq!(millis(Duration::from_nanos(1_000_001)), Some(2));
}

#[test]
fn a_zero_ttl_is_refused() {
    assert_eq!(millis(Duration::ZERO), None);
}

#[test]
fn the_longest_ttl_is_accepted_and_anything_above_is_refused() {
    assert!(millis(MAX_TTL).is_some());
    assert_eq!(millis(MAX_TTL + Duration::from_millis(1)), None);
}

#[cfg(feature = "redis")]
#[test]
fn a_redis_expiry_counts_the_same_milliseconds() {
    assert!(matches!(redis_expiry(1500), redis::SetExpiry::PX(1500)));
}

#[cfg(feature = "redis")]
#[test]
fn the_longest_ttl_makes_a_redis_expiry() {
    let longest = millis(MAX_TTL).expect("the longest ttl is accepted");

    assert!(matches!(
        redis_expiry(longest),
        redis::SetExpiry::PX(ms) if i64::try_from(ms) == Ok(longest)
    ));
}
