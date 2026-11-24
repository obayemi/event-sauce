use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRef, FromRequestParts};
use axum::http::Request;

use super::*;

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.append(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    headers
}

fn ip(raw: &str) -> IpAddr {
    raw.parse().unwrap()
}

#[test]
fn by_default_the_peer_counts_and_headers_are_ignored() {
    let spoofed = headers(&[("x-forwarded-for", "1.2.3.4"), ("x-real-ip", "1.2.3.4")]);

    assert_eq!(
        ClientIpSource::default().resolve(&spoofed, Some(ip("10.0.0.9"))),
        Some(ip("10.0.0.9"))
    );
}

#[test]
fn forwarded_for_is_read_from_the_entry_the_proxy_appended() {
    let source = ClientIpSource::Header(HeaderName::from_static("x-forwarded-for"));
    let forwarded = headers(&[("x-forwarded-for", "6.6.6.6, 203.0.113.7")]);

    assert_eq!(
        source.resolve(&forwarded, Some(ip("10.0.0.1"))),
        Some(ip("203.0.113.7"))
    );
}

#[test]
fn the_last_forwarded_for_header_holds_the_proxy_entry() {
    let source = ClientIpSource::Header(HeaderName::from_static("x-forwarded-for"));
    let forwarded = headers(&[
        ("x-forwarded-for", "6.6.6.6"),
        ("x-forwarded-for", "203.0.113.7"),
    ]);

    assert_eq!(source.resolve(&forwarded, None), Some(ip("203.0.113.7")));
}

#[test]
fn another_header_is_read_whole() {
    let source = ClientIpSource::Header(HeaderName::from_static("x-real-ip"));

    assert_eq!(
        source.resolve(&headers(&[("x-real-ip", " 203.0.113.7 ")]), None),
        Some(ip("203.0.113.7"))
    );
}

#[test]
fn a_configured_header_missing_or_garbled_gives_no_address() {
    let source = ClientIpSource::Header(HeaderName::from_static("x-real-ip"));

    assert_eq!(
        source.resolve(&HeaderMap::new(), Some(ip("10.0.0.1"))),
        None
    );
    assert_eq!(
        source.resolve(&headers(&[("x-real-ip", "nope")]), None),
        None
    );
}

fn key(raw: Option<&str>) -> String {
    ClientAddr(raw.map(ip)).key()
}

#[test]
fn an_ipv6_client_is_counted_by_its_slash_64() {
    assert_eq!(
        key(Some("2001:db8:1:2:aaaa::1")),
        key(Some("2001:db8:1:2:bbbb::2"))
    );
    assert_eq!(key(Some("2001:db8:1:2::1")), "2001:db8:1:2::/64");
    assert_eq!(key(Some("::ffff:192.0.2.1")), "192.0.2.1");
    assert_eq!(key(Some("192.0.2.1")), "192.0.2.1");
    assert_eq!(key(None), "unknown");
}

#[derive(Clone)]
struct State {
    source: ClientIpSource,
}

impl FromRef<State> for ClientIpSource {
    fn from_ref(state: &State) -> Self {
        state.source.clone()
    }
}

async fn extract(
    source: ClientIpSource,
    peer: Option<&str>,
    forwarded: Option<&str>,
) -> ClientAddr {
    let mut request = Request::builder().uri("/");
    if let Some(forwarded) = forwarded {
        request = request.header("x-forwarded-for", forwarded);
    }
    let mut request = request.body(()).unwrap();
    if let Some(peer) = peer {
        let addr = SocketAddr::new(ip(peer), 4000);
        request.extensions_mut().insert(ConnectInfo(addr));
    }
    let (mut parts, ()) = request.into_parts();
    ClientAddr::from_request_parts(&mut parts, &State { source })
        .await
        .unwrap()
}

#[tokio::test]
async fn the_extractor_reads_the_connect_info_peer() {
    let addr = extract(ClientIpSource::Peer, Some("192.0.2.1"), Some("1.2.3.4")).await;

    assert_eq!(addr, ClientAddr(Some(ip("192.0.2.1"))));
}

#[tokio::test]
async fn the_extractor_reads_the_header_the_state_names() {
    let source = ClientIpSource::Header(HeaderName::from_static("x-forwarded-for"));

    let addr = extract(source, Some("10.0.0.1"), Some("6.6.6.6, 203.0.113.7")).await;

    assert_eq!(addr, ClientAddr(Some(ip("203.0.113.7"))));
}

#[tokio::test]
async fn the_extractor_has_no_address_without_connect_info() {
    let addr = extract(ClientIpSource::Peer, None, None).await;

    assert_eq!(addr, ClientAddr(None));
}
