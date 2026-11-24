//! The address a request comes from, as the sign-in throttles count it.
//!
//! By default it is the socket peer: a header is anyone's to write, so it is
//! read only when the deployment says a proxy in front of the app sets it.
//! `X-Forwarded-For` is then read from its last entry, the one the proxy
//! appended; any other header is read whole, as the proxy replaces it.

use std::future::Future;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use axum::extract::{ConnectInfo, FromRef, FromRequestParts};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName};

#[cfg(test)]
#[path = "tests/client_ip_tests.rs"]
mod tests;

/// Where the client address is read from.
///
/// The extractor [`ClientAddr`] reads it from the application state through
/// `FromRef`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ClientIpSource {
    /// The socket peer: the app faces clients directly.
    #[default]
    Peer,
    /// A header the proxy in front of the app sets.
    Header(HeaderName),
}

impl ClientIpSource {
    /// The client address of a request with `headers` from `peer`, if known.
    #[must_use]
    pub fn resolve(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> Option<IpAddr> {
        match self {
            Self::Peer => peer,
            Self::Header(name) => {
                let value = headers.get_all(name).iter().next_back()?.to_str().ok()?;
                let entry = if name == "x-forwarded-for" {
                    value.rsplit(',').next()?
                } else {
                    value
                };
                entry.trim().parse().ok()
            }
        }
    }
}

/// The client address of the request, as [`ClientIpSource`] resolves it.
///
/// The server must be served with
/// `into_make_service_with_connect_info::<SocketAddr>()` for the socket peer to
/// be known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAddr(pub Option<IpAddr>);

impl ClientAddr {
    /// The key the throttles count this address under: a whole IPv6 `/64`,
    /// which one client usually holds entirely, or `unknown` when the address
    /// is not known.
    #[must_use]
    pub fn key(&self) -> String {
        match self.0 {
            Some(IpAddr::V6(v6)) if v6.to_ipv4_mapped().is_none() => {
                let [a, b, c, d, ..] = v6.segments();
                Ipv6Addr::new(a, b, c, d, 0, 0, 0, 0).to_string() + "/64"
            }
            Some(IpAddr::V6(v6)) => v6.to_canonical().to_string(),
            Some(ip) => ip.to_string(),
            None => "unknown".to_owned(),
        }
    }
}

impl<St> FromRequestParts<St> for ClientAddr
where
    ClientIpSource: FromRef<St>,
    St: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        state: &St,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let peer = parts
            .extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip());
        std::future::ready(Ok(Self(
            ClientIpSource::from_ref(state).resolve(&parts.headers, peer),
        )))
    }
}
