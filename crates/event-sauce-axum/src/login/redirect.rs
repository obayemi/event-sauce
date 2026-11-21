//! [`LoginRedirect`]: send a browser that is not logged in to the login page.

use axum::extract::OriginalUri;
use axum::middleware::AddExtension;
use http::request::Parts;
use http::HeaderValue;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use tower::Layer;

use crate::{AuthRejection, ReturnTo};

#[cfg(test)]
#[path = "tests/redirect_tests.rs"]
mod tests;

const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

/// A layer that sends a browser that is not logged in to the login page.
///
/// Apply it to the router of the HTML pages. The extractors of the login
/// session, [`CurrentUser`](crate::CurrentUser), [`Registered`](crate::Registered),
/// [`Authorized`](crate::Authorized) and [`Acting`](crate::Acting), then answer a request nobody is
/// logged in for with a `303 See Other` to `{login}?{param}={original path
/// and query}`, percent-encoded, instead of `401`. A router without the layer,
/// a JSON API next to the pages for instance, keeps answering `401` and `403`
/// on the same [`AuthLayer`](crate::AuthLayer).
///
/// | Situation | Answer |
/// |---|---|
/// | nobody is logged in | `303` to the login page |
/// | an anonymous user where a registered one is required | `303` to [`register_at`](Self::register_at), or `403` when unset |
/// | the [`AuthLayer`](crate::AuthLayer) is not installed | `500`, never a redirect |
/// | [`MaybeUser`](crate::MaybeUser) | never rejects for a missing user |
///
/// The page the request came from is axum's [`OriginalUri`], because
/// `Router::nest` strips the mount prefix from the request's own URI. Only its
/// path and query are kept, never a scheme or a host. After the login, the
/// handler redirects to the [`ReturnTo`] the form carries, which keeps the
/// cookie of the [`LoginStep`](crate::LoginStep):
///
/// ```
/// use axum::{routing::get, Router};
/// use event_sauce_axum::{LoginRedirect, ReturnTo};
///
/// async fn notes() -> &'static str {
///     "notes"
/// }
///
/// let pages: Router = Router::new()
///     .route("/notes", get(notes))
///     .layer(LoginRedirect::to(ReturnTo::from_static("/login"))
///         .register_at(ReturnTo::from_static("/register")));
/// let app: Router = Router::new().nest("/app", pages);
/// ```
#[derive(Debug, Clone)]
pub struct LoginRedirect {
    login: ReturnTo,
    param: &'static str,
    register: Option<ReturnTo>,
}

impl LoginRedirect {
    /// A layer that redirects to the login page `login`, with the page the
    /// request came from in the `return_to` query parameter.
    #[must_use]
    pub fn to(login: ReturnTo) -> Self {
        Self {
            login,
            param: "return_to",
            register: None,
        }
    }

    /// Names the query parameter that carries the page the request came from,
    /// `return_to` by default.
    ///
    /// # Panics
    ///
    /// Panics when `name` is empty or holds a byte other than an unreserved
    /// one (`A-Z a-z 0-9 - . _ ~`), because the name is written unencoded in
    /// the `Location` header. The name is configuration, so it fails once,
    /// when the router is built.
    #[must_use]
    pub fn param(self, name: &'static str) -> Self {
        assert!(
            !name.is_empty() && name.bytes().all(is_unreserved),
            "a query parameter name holds only unreserved bytes"
        );
        Self {
            param: name,
            ..self
        }
    }

    /// Sends an anonymous user, where a registered one is required, to `page`
    /// with the same query parameter, instead of answering `403`.
    #[must_use]
    pub fn register_at(self, page: ReturnTo) -> Self {
        Self {
            register: Some(page),
            ..self
        }
    }

    fn location(&self, page: &ReturnTo, parts: &Parts) -> HeaderValue {
        let from = parts
            .extensions
            .get::<OriginalUri>()
            .map_or(&parts.uri, |OriginalUri(original)| original)
            .path_and_query()
            .map_or("/", |path_and_query| path_and_query.as_str());
        let separator = if page.as_str().contains('?') {
            '&'
        } else {
            '?'
        };
        let location = format!(
            "{page}{separator}{}={}",
            self.param,
            utf8_percent_encode(from, UNRESERVED)
        );
        HeaderValue::try_from(location)
            .expect("an unreserved parameter name and an encoded target make a valid header")
    }

    pub(crate) fn refuse(&self, parts: &Parts, rejection: AuthRejection) -> AuthRejection {
        let page = match rejection {
            AuthRejection::Unauthenticated => Some(&self.login),
            AuthRejection::Forbidden => self.register.as_ref(),
            AuthRejection::MissingLayer | AuthRejection::Redirect { .. } => None,
        };
        page.map_or(rejection, |page| AuthRejection::Redirect {
            location: self.location(page, parts),
        })
    }
}

impl<S> Layer<S> for LoginRedirect {
    type Service = AddExtension<S, Self>;

    fn layer(&self, inner: S) -> Self::Service {
        axum::Extension(self.clone()).layer(inner)
    }
}

pub(crate) fn refuse(parts: &Parts, rejection: AuthRejection) -> AuthRejection {
    match parts.extensions.get::<LoginRedirect>() {
        Some(redirect) => redirect.refuse(parts, rejection),
        None => rejection,
    }
}
