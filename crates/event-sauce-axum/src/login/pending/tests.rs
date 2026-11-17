use std::time::Duration;

use event_sauce_auth::PendingLoginId;
use http::{header::COOKIE, HeaderMap, HeaderValue};

use super::PendingLogin;

fn headers(cookie: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.append(COOKIE, HeaderValue::from_str(cookie).unwrap());
    headers
}

#[test]
fn the_cookie_round_trips_the_pending_login_id() {
    let id = PendingLoginId::new();
    let header = PendingLogin::set_cookie(id, PendingLogin::DEFAULT_TTL);
    let sent = header.split(';').next().unwrap();

    let pending = PendingLogin::from_headers(&headers(sent)).unwrap();

    assert_eq!(pending.id(), id);
}

#[test]
fn the_cookie_is_short_lived_and_locked_down() {
    let set = PendingLogin::set_cookie(PendingLoginId::new(), Duration::from_secs(90));

    assert!(set.starts_with(&format!("{}=", PendingLogin::COOKIE)));
    assert!(set.contains("Max-Age=90"));
    assert!(set.contains("HttpOnly"));
    assert!(set.contains("Secure"));
    assert!(set.contains("SameSite=Lax"));
    assert!(set.contains("Path=/"));
}

#[test]
fn clearing_expires_the_cookie_immediately() {
    let cleared = PendingLogin::clear_cookie();

    assert!(cleared.starts_with(&format!("{}=;", PendingLogin::COOKIE)));
    assert!(cleared.contains("Max-Age=0"));
}

#[test]
fn the_pending_cookie_is_found_among_other_cookies() {
    let id = PendingLoginId::new();
    let cookie = format!("theme=dark; {}={id}; id=abc", PendingLogin::COOKIE);

    let pending = PendingLogin::from_headers(&headers(&cookie)).unwrap();

    assert_eq!(pending.id(), id);
}

#[test]
fn the_pending_cookie_is_found_across_cookie_headers() {
    let id = PendingLoginId::new();
    let mut map = headers("theme=dark");
    map.append(
        COOKIE,
        HeaderValue::from_str(&format!("{}={id}", PendingLogin::COOKIE)).unwrap(),
    );

    assert_eq!(PendingLogin::from_headers(&map).unwrap().id(), id);
}

#[test]
fn no_cookie_means_no_pending_login() {
    assert!(PendingLogin::from_headers(&HeaderMap::new()).is_none());
    assert!(PendingLogin::from_headers(&headers("theme=dark")).is_none());
}

#[test]
fn a_malformed_id_means_no_pending_login() {
    let cookie = format!("{}=not-a-uuid", PendingLogin::COOKIE);

    assert!(PendingLogin::from_headers(&headers(&cookie)).is_none());
}
