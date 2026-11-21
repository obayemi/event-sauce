use std::fmt;

use http::HeaderValue;

const MAX_LEN: usize = 128;

/// A well-formed request id: 1 to 128 characters of `[A-Za-z0-9._-]`.
///
/// # Examples
///
/// ```
/// use event_sauce_axum::RequestId;
///
/// assert!(RequestId::parse("req-123").is_some());
/// assert!(RequestId::parse("not valid!").is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestId(String);

impl RequestId {
    /// Accepts `raw` only when it is a well-formed id.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let well_formed = (1..=MAX_LEN).contains(&raw.len())
            && raw
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        well_formed.then(|| Self(raw.to_owned()))
    }

    /// A fresh id (UUID v4).
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub(crate) fn to_header_value(&self) -> HeaderValue {
        HeaderValue::from_str(&self.0).expect("request ids are visible ASCII")
    }

    /// The id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests;
