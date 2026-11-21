use std::num::NonZeroU32;

use super::*;

fn ttl(duration: Duration) -> LoginTtl {
    LoginTtl::new(duration).expect("a valid ttl")
}

#[test]
fn whole_milliseconds_pass_through() {
    assert_eq!(
        ttl(Duration::from_millis(1500)).as_duration(),
        Duration::from_millis(1500)
    );
    assert_eq!(
        ttl(Duration::from_secs(60)).as_duration(),
        Duration::from_secs(60)
    );
}

#[test]
fn sub_millisecond_durations_round_up_to_one_millisecond() {
    assert_eq!(
        ttl(Duration::from_micros(1)).as_duration(),
        Duration::from_millis(1)
    );
    assert_eq!(
        ttl(Duration::from_nanos(1_000_001)).as_duration(),
        Duration::from_millis(2)
    );
}

#[test]
fn a_zero_ttl_is_refused() {
    assert_eq!(LoginTtl::new(Duration::ZERO), Err(LoginTtlError::Zero));
}

#[test]
fn the_longest_ttl_is_accepted_and_anything_above_is_refused() {
    let longest = Duration::from_hours(366 * 24);

    assert_eq!(ttl(longest), LoginTtl::MAX);
    assert_eq!(
        LoginTtl::new(longest + Duration::from_millis(1)),
        Err(LoginTtlError::TooLong(longest + Duration::from_millis(1)))
    );
}

#[test]
fn rounding_up_never_pushes_a_ttl_past_the_longest() {
    let just_over = Duration::from_hours(366 * 24) + Duration::from_nanos(1);

    assert!(matches!(
        LoginTtl::new(just_over),
        Err(LoginTtlError::TooLong(_))
    ));
}

#[test]
fn the_common_ttls_are_the_durations_they_name() {
    assert_eq!(LoginTtl::ONE_MINUTE.as_duration(), Duration::from_secs(60));
    assert_eq!(
        LoginTtl::FIVE_MINUTES.as_duration(),
        Duration::from_secs(300)
    );
    assert_eq!(LoginTtl::ONE_HOUR.as_duration(), Duration::from_hours(1));
    assert_eq!(LoginTtl::ONE_DAY.as_duration(), Duration::from_hours(24));
}

#[test]
fn a_non_zero_count_of_milliseconds_is_always_a_ttl() {
    const ONE: NonZeroU32 = NonZeroU32::MIN;

    assert_eq!(
        LoginTtl::from_millis(ONE).as_duration(),
        Duration::from_millis(1)
    );
    assert_eq!(
        LoginTtl::from_millis(NonZeroU32::MAX).as_duration(),
        Duration::from_millis(u64::from(u32::MAX))
    );
}

#[test]
fn a_ttl_converts_from_and_into_a_duration() {
    let converted = LoginTtl::try_from(Duration::from_secs(90)).expect("a valid ttl");

    assert_eq!(Duration::from(converted), Duration::from_secs(90));
    assert_eq!(LoginTtl::try_from(Duration::ZERO), Err(LoginTtlError::Zero));
}

#[test]
fn ttls_order_by_length() {
    assert!(LoginTtl::ONE_MINUTE < LoginTtl::ONE_HOUR);
}

#[cfg(feature = "postgres")]
#[test]
fn a_ttl_counts_its_milliseconds_for_sql() {
    assert_eq!(ttl(Duration::from_millis(1500)).sql_millis(), 1500);
    assert_eq!(LoginTtl::MAX.sql_millis(), 366 * 24 * 3_600_000);
}

#[cfg(feature = "redis")]
#[test]
fn a_redis_expiry_counts_the_same_milliseconds() {
    assert!(matches!(
        ttl(Duration::from_millis(1500)).redis_expiry(),
        redis::SetExpiry::PX(1500)
    ));
}

#[test]
fn errors_say_what_is_wrong() {
    assert_eq!(LoginTtlError::Zero.to_string(), "time to live is zero");
    assert!(LoginTtlError::TooLong(Duration::from_hours(9000))
        .to_string()
        .contains("longer than"));
}
