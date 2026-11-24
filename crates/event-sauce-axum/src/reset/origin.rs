use std::fmt;

use http::Uri;

/// Why a string is not a public origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidOrigin {
    /// It is not a URL with a scheme and a host.
    #[error("the origin is not a URL with a host")]
    Malformed,
    /// Its scheme is neither `http` nor `https`, or it has none: the scheme
    /// is part of the origin and never guessed.
    #[error("the origin must start with http:// or https://")]
    Scheme,
    /// It carries a user name or a password.
    #[error("the origin must not carry user information")]
    UserInfo,
    /// It carries a path: the application is served from the root of its
    /// origin.
    #[error("the origin must not carry a path")]
    Path,
    /// It carries a query.
    #[error("the origin must not carry a query")]
    Query,
    /// It carries a fragment.
    #[error("the origin must not carry a fragment")]
    Fragment,
}

/// The absolute origin a link points at: `scheme://host[:port]`, configured
/// and never read from the `Host` of a request.
///
/// # Examples
///
/// ```
/// use event_sauce_axum::PublicOrigin;
///
/// let origin = PublicOrigin::parse("https://saxy.example/")?;
/// assert_eq!(origin.as_str(), "https://saxy.example");
/// assert_eq!(origin.link("/reset/1/abc"), "https://saxy.example/reset/1/abc");
/// # Ok::<(), event_sauce_axum::InvalidOrigin>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicOrigin(String);

impl PublicOrigin {
    /// Reads a configured origin: `http` or `https`, a host, an optional port
    /// and an optional bare trailing `/`. A port the scheme implies is
    /// dropped.
    ///
    /// # Errors
    ///
    /// [`InvalidOrigin`] for anything else: a missing scheme, user
    /// information, a path, a query, a fragment, stray characters.
    pub fn parse(raw: &str) -> Result<Self, InvalidOrigin> {
        let raw = raw.trim();
        let uri: Uri = raw.parse().map_err(|_| InvalidOrigin::Malformed)?;
        if raw.contains('#') {
            return Err(InvalidOrigin::Fragment);
        }
        let (scheme, default_port) = match uri.scheme_str() {
            Some("https") => ("https", 443),
            Some("http") => ("http", 80),
            _ => return Err(InvalidOrigin::Scheme),
        };
        let authority = uri.authority().ok_or(InvalidOrigin::Malformed)?;
        if authority.as_str().contains('@') {
            return Err(InvalidOrigin::UserInfo);
        }
        if uri.query().is_some() {
            return Err(InvalidOrigin::Query);
        }
        if uri.path() != "/" {
            return Err(InvalidOrigin::Path);
        }
        let host = authority.host();
        Ok(Self(match authority.port_u16() {
            Some(port) if port != default_port => format!("{scheme}://{host}:{port}"),
            _ => format!("{scheme}://{host}"),
        }))
    }

    /// The origin, `scheme://host[:port]`, without a trailing `/`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The link to `path`, a path from the root of the origin.
    #[must_use]
    pub fn link(&self, path: &str) -> String {
        format!("{}/{}", self.0, path.trim_start_matches('/'))
    }
}

impl fmt::Display for PublicOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
#[path = "tests/origin_tests.rs"]
mod tests;
