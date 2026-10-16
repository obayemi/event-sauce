use super::*;

fn assert_multiplier(policy: &RetryPolicy, expected: f64) {
    assert!(
        (policy.multiplier() - expected).abs() < f64::EPSILON,
        "expected multiplier {expected}, got {}",
        policy.multiplier()
    );
}

#[test]
fn default_has_the_documented_values() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.max_attempts().get(), 3);
    assert_eq!(policy.initial_backoff(), Duration::from_millis(10));
    assert_multiplier(&policy, 2.0);
    assert_eq!(policy.max_backoff(), Duration::from_secs(1));
}

#[test]
fn none_allows_a_single_attempt_with_zero_backoff() {
    let policy = RetryPolicy::none();
    assert_eq!(policy.max_attempts().get(), 1);
    for attempt in 0..5 {
        assert_eq!(policy.backoff(attempt), Duration::ZERO);
    }
}

#[test]
fn backoff_of_zero_is_always_zero() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.backoff(0), Duration::ZERO);
}

#[test]
fn backoff_starts_at_the_initial_delay() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.backoff(1), Duration::from_millis(10));
}

#[test]
fn backoff_doubles_with_the_default_multiplier() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.backoff(2), Duration::from_millis(20));
    assert_eq!(policy.backoff(3), Duration::from_millis(40));
    assert_eq!(policy.backoff(4), Duration::from_millis(80));
}

#[test]
fn backoff_is_capped_at_max_backoff() {
    let policy = RetryPolicy::default().with_max_backoff(Duration::from_millis(25));
    assert_eq!(policy.backoff(1), Duration::from_millis(10));
    assert_eq!(policy.backoff(2), Duration::from_millis(20));
    assert_eq!(policy.backoff(3), Duration::from_millis(25));
    assert_eq!(policy.backoff(10), Duration::from_millis(25));
}

#[test]
fn backoff_respects_a_custom_multiplier() {
    let policy = RetryPolicy::default()
        .with_initial_backoff(Duration::from_millis(100))
        .with_multiplier(1.5)
        .with_max_backoff(Duration::from_secs(10));
    assert_eq!(policy.backoff(1), Duration::from_millis(100));
    assert_eq!(policy.backoff(2), Duration::from_millis(150));
    assert_eq!(policy.backoff(3), Duration::from_millis(225));
}

#[test]
fn backoff_with_a_multiplier_at_or_below_one_never_exceeds_the_initial_delay() {
    let policy = RetryPolicy::default()
        .with_initial_backoff(Duration::from_millis(50))
        .with_multiplier(1.0)
        .with_max_backoff(Duration::from_secs(10));
    for attempt in 1..10 {
        assert_eq!(policy.backoff(attempt), Duration::from_millis(50));
    }
}

#[test]
fn backoff_with_a_huge_attempt_number_terminates_and_stays_capped() {
    let policy = RetryPolicy::default().with_max_backoff(Duration::from_secs(1));
    assert_eq!(policy.backoff(u32::MAX), Duration::from_secs(1));
}

#[test]
fn backoff_applies_the_jitter_function() {
    let policy = RetryPolicy::default().with_jitter(|d| d / 2);
    assert_eq!(policy.backoff(1), Duration::from_millis(5));
    assert_eq!(policy.backoff(2), Duration::from_millis(10));
}

#[test]
fn backoff_with_zero_initial_delay_and_a_multiplier_stays_zero() {
    let policy = RetryPolicy::default().with_initial_backoff(Duration::ZERO);
    assert_eq!(policy.backoff(5), Duration::ZERO);
}

#[test]
fn builder_methods_are_independent_and_chainable() {
    let policy = RetryPolicy::default()
        .with_max_attempts(NonZeroU32::new(7).unwrap())
        .with_initial_backoff(Duration::from_millis(1))
        .with_multiplier(3.0)
        .with_max_backoff(Duration::from_secs(2));

    assert_eq!(policy.max_attempts().get(), 7);
    assert_eq!(policy.initial_backoff(), Duration::from_millis(1));
    assert_multiplier(&policy, 3.0);
    assert_eq!(policy.max_backoff(), Duration::from_secs(2));
}

#[test]
fn retry_policy_is_clonable_and_debuggable() {
    let policy = RetryPolicy::default();
    let cloned = policy.clone();
    assert_eq!(cloned.max_attempts(), policy.max_attempts());
    let debug = format!("{policy:?}");
    assert!(debug.contains("RetryPolicy"));
}

#[test]
fn scale_treats_non_finite_or_non_positive_factors_as_zero() {
    assert_eq!(scale(Duration::from_millis(10), f64::NAN), Duration::ZERO);
    assert_eq!(
        scale(Duration::from_millis(10), f64::INFINITY),
        Duration::ZERO
    );
    assert_eq!(scale(Duration::from_millis(10), 0.0), Duration::ZERO);
    assert_eq!(scale(Duration::from_millis(10), -1.0), Duration::ZERO);
}
