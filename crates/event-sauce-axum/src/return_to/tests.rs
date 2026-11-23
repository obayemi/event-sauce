use axum::response::IntoResponse;
use http::{header::LOCATION, StatusCode};
use proptest::prelude::*;
use serde::Deserialize;

use super::ReturnTo;

#[derive(Deserialize)]
struct Form {
    #[serde(default)]
    return_to: ReturnTo,
}

fn local(raw: &str) -> Option<String> {
    ReturnTo::parse(raw).map(|target| target.as_str().to_owned())
}

#[test]
fn local_paths_are_accepted() {
    for path in [
        "/",
        "/groups",
        "/?view=my-events",
        "/activities/42?tab=rsvp",
        "/a?b=c#d",
        "/a%5Cb",
        "/a%2F%2Fb",
        "/caf\u{e9}",
    ] {
        assert_eq!(local(path).as_deref(), Some(path), "{path:?}");
    }
}

#[test]
fn external_and_relative_targets_are_rejected() {
    for raw in [
        "",
        "//evil.com",
        "//evil.com/steal",
        "http://evil.com",
        "https://evil.com/path",
        "javascript:alert(1)",
        "evil.com",
        "\\\\evil.com",
        "groups",
    ] {
        assert_eq!(local(raw), None, "{raw:?}");
    }
}

#[test]
fn targets_a_browser_reads_as_external_are_rejected() {
    for raw in [
        "/\\evil.com",
        "/\\/evil.com",
        "\\/evil.com",
        "/\t/evil.com",
        "/\n/evil.com",
        "/\r/evil.com",
        "//\tevil.com",
    ] {
        assert_eq!(local(raw), None, "{raw:?}");
    }
}

#[test]
fn control_characters_are_rejected() {
    for raw in [
        "/groups\0",
        "/groups\u{7f}",
        "/a\x1b[31m",
        "/a\u{85}",
        "/a\u{9f}",
    ] {
        assert_eq!(local(raw), None, "{raw:?}");
    }
}

#[test]
fn the_characters_next_to_the_control_ranges_are_accepted() {
    for raw in ["/ ", "/a\u{a0}", "/a\u{a1}"] {
        assert_eq!(local(raw).as_deref(), Some(raw), "{raw:?}");
    }
}

#[test]
fn the_root_is_the_default() {
    assert_eq!(ReturnTo::default(), ReturnTo::root());
    assert_eq!(ReturnTo::root().as_str(), "/");
}

#[test]
fn a_static_target_is_kept() {
    const LOGIN: ReturnTo = ReturnTo::from_static("/login");

    assert_eq!(LOGIN.as_str(), "/login");
}

#[test]
#[should_panic(expected = "not a local path")]
fn a_static_target_that_is_not_local_panics() {
    let _ = ReturnTo::from_static("//host");
}

#[test]
fn a_target_displays_as_its_path() {
    assert_eq!(ReturnTo::from_static("/a?b=c").to_string(), "/a?b=c");
}

#[test]
fn a_local_target_is_deserialized_from_a_query_string() {
    let form: Form = serde_urlencoded::from_str("return_to=%2Fgroups%3Fa%3D1").unwrap();

    assert_eq!(form.return_to.as_str(), "/groups?a=1");
}

#[test]
fn a_target_that_is_not_local_is_deserialized_as_the_root() {
    for query in [
        "return_to=%2F%2Fevil.com",
        "return_to=http%3A%2F%2Fevil.com",
        "return_to=%2F%5Cevil.com",
        "return_to=%2F%09%2Fevil.com",
        "return_to=",
    ] {
        let form: Form = serde_urlencoded::from_str(query).unwrap();

        assert_eq!(form.return_to, ReturnTo::root(), "{query}");
    }
}

#[test]
fn an_absent_target_is_the_root() {
    let form: Form = serde_urlencoded::from_str("").unwrap();

    assert_eq!(form.return_to, ReturnTo::root());
}

#[test]
fn a_target_answers_see_other_to_itself() {
    let response = ReturnTo::from_static("/groups?a=1").into_response();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()[LOCATION], "/groups?a=1");
}

#[test]
fn redirect_answers_the_same_as_the_target() {
    let target = ReturnTo::from_static("/groups");

    assert_eq!(
        target.redirect().into_response().headers()[LOCATION],
        target.into_response().headers()[LOCATION]
    );
}

proptest! {
    #[test]
    fn an_accepted_target_cannot_leave_the_site(raw in any::<String>()) {
        if let Some(target) = ReturnTo::parse(&raw) {
            let target = target.as_str();
            prop_assert!(target.starts_with('/'));
            prop_assert!(!target.starts_with("//"));
            prop_assert!(!target.chars().any(|c| c == '\\' || c.is_control()));
            prop_assert_eq!(target, raw.as_str());
        }
    }

    #[test]
    fn a_path_without_backslash_or_control_is_accepted(tail in "[^\\\\\\p{Cc}]*") {
        let raw = format!("/a{tail}");

        prop_assert_eq!(local(&raw), Some(raw));
    }

    #[test]
    fn a_target_with_a_scheme_or_host_is_rejected(
        scheme in "[a-z][a-z0-9+.-]{0,8}",
        host in "[a-z.]{1,12}",
    ) {
        let absolute = format!("{scheme}://{host}/x");
        let relative = format!("//{host}/x");

        prop_assert_eq!(local(&absolute), None);
        prop_assert_eq!(local(&relative), None);
    }

    #[test]
    fn any_deserialized_target_is_a_parsed_one(raw in any::<String>()) {
        let json = serde_json::to_string(&raw).unwrap();
        let target: ReturnTo = serde_json::from_str(&json).unwrap();

        prop_assert_eq!(target, ReturnTo::parse(&raw).unwrap_or_default());
    }
}
