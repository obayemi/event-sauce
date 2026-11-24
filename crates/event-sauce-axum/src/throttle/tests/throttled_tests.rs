use axum::response::IntoResponse;
use http::header::RETRY_AFTER;
use http::StatusCode;

use super::*;

fn retry_after(wait: Duration) -> String {
    let response = Throttled::new(wait).into_response();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    response.headers()[RETRY_AFTER].to_str().unwrap().to_owned()
}

#[test]
fn the_retry_after_is_the_wait_in_whole_seconds_rounded_up() {
    assert_eq!(retry_after(Duration::from_millis(1)), "1");
    assert_eq!(retry_after(Duration::from_secs(4)), "4");
    assert_eq!(retry_after(Duration::from_millis(3500)), "4");
    assert_eq!(retry_after(Duration::from_mins(15)), "900");
}

#[test]
fn a_throttled_request_keeps_its_wait() {
    let throttled = Throttled::new(Duration::from_millis(3500));

    assert_eq!(throttled.wait(), Duration::from_millis(3500));
    assert_eq!(throttled.retry_after_secs(), 4);
}

#[tokio::test]
async fn the_response_carries_no_detail() {
    let response = Throttled::new(Duration::from_secs(1)).into_response();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    assert!(body.is_empty());
}

#[test]
fn a_throttled_request_says_how_long_to_wait() {
    let message = Throttled::new(Duration::from_secs(4)).to_string();

    assert!(message.contains('4'), "{message}");
}
