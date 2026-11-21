use event_sauce_auth::{Done, LoginState, LoginTtl, PendingLoginId};
use http::{header::COOKIE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

use super::PendingLogin;

#[derive(Serialize, Deserialize)]
struct Brief;

impl LoginState for Brief {
    const TAG: &'static str = "test.brief";
    const TTL: LoginTtl = LoginTtl::ONE_HOUR;
}

fn headers(cookie: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.append(COOKIE, HeaderValue::from_str(cookie).unwrap());
    headers
}

#[test]
fn the_cookie_round_trips_the_pending_login_id() {
    let id = PendingLoginId::new();
    let header = PendingLogin::set_cookie::<Done>(id);
    let sent = header.split(';').next().unwrap();

    let pending = PendingLogin::from_headers(&headers(sent)).unwrap();

    assert_eq!(pending.id(), id);
}

#[test]
fn the_cookie_is_locked_down() {
    let set = PendingLogin::set_cookie::<Done>(PendingLoginId::new());

    assert!(set.starts_with(&format!("{}=", PendingLogin::COOKIE)));
    assert!(set.contains("HttpOnly"));
    assert!(set.contains("Secure"));
    assert!(set.contains("SameSite=Lax"));
    assert!(set.contains("Path=/"));
}

#[test]
fn the_cookie_lives_as_long_as_the_state_it_waits_in() {
    let five_minutes = PendingLogin::set_cookie::<Done>(PendingLoginId::new());
    let one_hour = PendingLogin::set_cookie::<Brief>(PendingLoginId::new());

    assert!(five_minutes.contains("Max-Age=300"));
    assert!(one_hour.contains("Max-Age=3600"));
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
