//! [`ReturnTo`]: a same-origin redirect target.

use std::borrow::Cow;
use std::fmt;

use axum::response::{IntoResponse, Redirect, Response};
use serde::{Deserialize, Deserializer};

#[cfg(test)]
#[path = "tests/return_to_tests.rs"]
mod tests;

const fn is_control(bytes: &[u8], at: usize) -> bool {
    let byte = bytes[at];
    byte < 0x20
        || byte == 0x7f
        || (byte == 0xc2 && at + 1 < bytes.len() && bytes[at + 1] >= 0x80 && bytes[at + 1] < 0xa0)
}

const fn is_local(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    if bytes.is_empty() || bytes[0] != b'/' || (bytes.len() > 1 && bytes[1] == b'/') {
        return false;
    }
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'\\' || is_control(bytes, at) {
            return false;
        }
        at += 1;
    }
    true
}

/// A same-origin path, safe to redirect to.
///
/// A target is a path that starts with `/` but not `//`, and holds no `\` and
/// no control character. A scheme or a host cannot start with `/`, so a
/// target never leaves the site. Browsers read the target of a `Location`
/// header with the WHATWG URL rules, which turn `\` into `/` and drop tabs
/// and newlines: `/\host` and `/<TAB>/host` both become `//host`, so they are
/// refused outright. That also keeps a target safe to put in a header.
/// Percent-encoded bytes are kept as they are.
///
/// Parse a raw value with [`parse`](Self::parse), which is strict. A form or a
/// query that carries a target deserializes it leniently: a value that is not
/// local, like an absent one under `#[serde(default)]`, is the
/// [root](Self::root), so a forged value never fails a login form and never
/// leaves the site.
///
/// ```
/// use event_sauce_axum::ReturnTo;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct LoginForm {
///     #[serde(default)]
///     return_to: ReturnTo,
/// }
///
/// let form: LoginForm = serde_urlencoded::from_str("return_to=%2F%2Fevil.com")?;
/// assert_eq!(form.return_to, ReturnTo::root());
///
/// assert_eq!(ReturnTo::parse("/notes?page=2").unwrap().as_str(), "/notes?page=2");
/// assert!(ReturnTo::parse("https://evil.com").is_none());
/// # Ok::<(), serde_urlencoded::de::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReturnTo(Cow<'static, str>);

impl ReturnTo {
    /// A target known at compile time, for a login or registration page.
    ///
    /// # Panics
    ///
    /// Panics, at compile time in a `const`, when `path` is not a local path.
    ///
    /// ```
    /// use event_sauce_axum::ReturnTo;
    ///
    /// const LOGIN: ReturnTo = ReturnTo::from_static("/login");
    /// assert_eq!(LOGIN.as_str(), "/login");
    /// ```
    #[must_use]
    pub const fn from_static(path: &'static str) -> Self {
        assert!(is_local(path), "not a local path");
        Self(Cow::Borrowed(path))
    }

    /// The target `raw` names, or `None` when it is not a local path.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        is_local(raw).then(|| Self(Cow::Owned(raw.to_owned())))
    }

    /// The root of the site, `/`.
    #[must_use]
    pub const fn root() -> Self {
        Self(Cow::Borrowed("/"))
    }

    /// The path, query included.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A `303 See Other` to the target.
    pub fn redirect(&self) -> Redirect {
        Redirect::to(self.as_str())
    }
}

impl Default for ReturnTo {
    fn default() -> Self {
        Self::root()
    }
}

impl fmt::Display for ReturnTo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ReturnTo {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(Self::parse(&raw).unwrap_or_default())
    }
}

impl IntoResponse for ReturnTo {
    fn into_response(self) -> Response {
        self.redirect().into_response()
    }
}
