use super::*;

fn origin(raw: &str) -> String {
    PublicOrigin::parse(raw).unwrap().to_string()
}

#[test]
fn an_origin_keeps_its_scheme_and_host() {
    assert_eq!(origin("https://saxy.example"), "https://saxy.example");
    assert_eq!(origin("http://localhost:3000"), "http://localhost:3000");
}

#[test]
fn a_bare_trailing_slash_and_surrounding_spaces_are_dropped() {
    assert_eq!(origin("https://saxy.example/"), "https://saxy.example");
    assert_eq!(origin("  https://saxy.example\n"), "https://saxy.example");
}

#[test]
fn a_port_the_scheme_implies_is_dropped() {
    assert_eq!(origin("https://saxy.example:443"), "https://saxy.example");
    assert_eq!(origin("http://saxy.example:80"), "http://saxy.example");
    assert_eq!(
        origin("https://saxy.example:8443"),
        "https://saxy.example:8443"
    );
    assert_eq!(origin("http://saxy.example:443"), "http://saxy.example:443");
}

#[test]
fn an_ipv6_host_keeps_its_brackets() {
    assert_eq!(origin("http://[::1]:8080"), "http://[::1]:8080");
}

#[test]
fn a_scheme_is_never_guessed() {
    assert_eq!(
        PublicOrigin::parse("saxy.example"),
        Err(InvalidOrigin::Scheme)
    );
    assert_eq!(
        PublicOrigin::parse("ftp://saxy.example"),
        Err(InvalidOrigin::Scheme)
    );
}

#[test]
fn anything_but_a_bare_origin_is_refused() {
    assert_eq!(
        PublicOrigin::parse("https://user:secret@saxy.example"),
        Err(InvalidOrigin::UserInfo)
    );
    assert_eq!(
        PublicOrigin::parse("https://saxy.example/app"),
        Err(InvalidOrigin::Path)
    );
    assert_eq!(
        PublicOrigin::parse("https://saxy.example?a=1"),
        Err(InvalidOrigin::Query)
    );
    assert_eq!(
        PublicOrigin::parse("https://saxy.example#top"),
        Err(InvalidOrigin::Fragment)
    );
}

#[test]
fn stray_characters_make_it_malformed() {
    for raw in [
        "",
        "https://",
        "\"https://saxy.example\"",
        "https://saxy .example",
    ] {
        assert_eq!(
            PublicOrigin::parse(raw),
            Err(InvalidOrigin::Malformed),
            "{raw:?}"
        );
    }
}

#[test]
fn a_link_is_the_origin_and_the_path() {
    let origin = PublicOrigin::parse("https://saxy.example").unwrap();

    assert_eq!(
        origin.link("/reset/1/abc"),
        "https://saxy.example/reset/1/abc"
    );
    assert_eq!(
        origin.link("reset/1/abc"),
        "https://saxy.example/reset/1/abc"
    );
}
