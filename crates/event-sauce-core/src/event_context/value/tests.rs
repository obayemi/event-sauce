use super::*;
use chrono::TimeZone;
use serde_json::json;
use std::borrow::Cow;
use uuid::Uuid;

#[test]
fn bool_is_a_json_bool() {
    assert_eq!(true.to_json(), json!(true));
}

#[test]
fn signed_integers_are_json_numbers() {
    assert_eq!(1_i8.to_json(), json!(1));
    assert_eq!((-2_i16).to_json(), json!(-2));
    assert_eq!(3_i32.to_json(), json!(3));
    assert_eq!(i64::MIN.to_json(), json!(i64::MIN));
    assert_eq!((-5_isize).to_json(), json!(-5));
}

#[test]
fn unsigned_integers_are_json_numbers() {
    assert_eq!(1_u8.to_json(), json!(1));
    assert_eq!(2_u16.to_json(), json!(2));
    assert_eq!(3_u32.to_json(), json!(3));
    assert_eq!(u64::MAX.to_json(), json!(u64::MAX));
    assert_eq!(5_usize.to_json(), json!(5));
}

#[test]
fn wide_integers_that_fit_are_numbers() {
    assert_eq!(7_i128.to_json(), json!(7));
    assert_eq!((-7_i128).to_json(), json!(-7));
    assert_eq!(7_u128.to_json(), json!(7));
}

#[test]
fn wide_integers_that_do_not_fit_are_decimal_strings() {
    assert_eq!(u128::MAX.to_json(), json!(u128::MAX.to_string()));
    assert_eq!(i128::MIN.to_json(), json!(i128::MIN.to_string()));
    assert_eq!(i128::MAX.to_json(), json!(i128::MAX.to_string()));
}

#[test]
fn floats_are_numbers_and_non_finite_are_null() {
    assert_eq!(1.5_f32.to_json(), json!(1.5));
    assert_eq!(2.5_f64.to_json(), json!(2.5));
    assert_eq!(f64::NAN.to_json(), Value::Null);
    assert_eq!(f32::INFINITY.to_json(), Value::Null);
}

#[test]
fn text_types_are_strings() {
    assert_eq!('x'.to_json(), json!("x"));
    assert_eq!("abc".to_json(), json!("abc"));
    assert_eq!(String::from("abc").to_json(), json!("abc"));
    assert_eq!(Cow::Borrowed("abc").to_json(), json!("abc"));
    assert_eq!(Cow::<str>::Owned("abc".into()).to_json(), json!("abc"));
}

#[test]
fn uuid_is_a_hyphenated_string() {
    let id = Uuid::nil();
    assert_eq!(id.to_json(), json!("00000000-0000-0000-0000-000000000000"));
}

#[test]
fn datetime_is_rfc3339() {
    let at = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
    assert_eq!(at.to_json(), json!("2026-01-02T03:04:05+00:00"));
}

#[test]
fn json_value_is_cloned() {
    let value = json!({"a": [1, 2]});
    assert_eq!(value.to_json(), value);
}

#[test]
fn option_is_null_or_inner() {
    assert_eq!(None::<u8>.to_json(), Value::Null);
    assert_eq!(Some(4_u8).to_json(), json!(4));
}

#[test]
fn collections_are_arrays() {
    assert_eq!(vec![1_u8, 2].to_json(), json!([1, 2]));
    assert_eq!([1_u8, 2][..].to_json(), json!([1, 2]));
    assert_eq!([1_u8, 2, 3].to_json(), json!([1, 2, 3]));
    assert_eq!(vec![vec![1_u8], vec![]].to_json(), json!([[1], []]));
}

#[test]
fn references_delegate() {
    let text = String::from("abc");
    assert_eq!((&&text).to_json(), json!("abc"));
    assert_eq!(vec![&1_u8, &2].to_json(), json!([1, 2]));
}

#[test]
fn display_renders_through_to_string() {
    assert_eq!(
        display(Uuid::nil()).to_json(),
        json!(Uuid::nil().to_string())
    );
    assert_eq!(display(12).to_json(), json!("12"));
}
