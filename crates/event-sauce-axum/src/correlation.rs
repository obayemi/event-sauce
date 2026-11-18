use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use event_sauce_core::{event_context, WithEventContext, WithEventContextExt};
use http::{HeaderName, HeaderValue, Request, Response};
use tower::{Layer, Service};
#[cfg(feature = "tracing")]
use tracing::{info_span, instrument::Instrumented, Instrument};

use crate::RequestId;

/// The header carrying the request id when none is configured.
pub const DEFAULT_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[cfg(feature = "tracing")]
type Scoped<F> = Instrumented<WithEventContext<F>>;
#[cfg(not(feature = "tracing"))]
type Scoped<F> = WithEventContext<F>;

/// Layer that correlates every request with the events it produces.
///
/// For each request the layer:
///
/// - reuses the well-formed id found in the correlation header, or mints a
///   UUID v4 when the header is absent or malformed;
/// - runs the inner service inside a root
///   [`EventContext`](event_sauce_core::EventContext) whose correlation id is
///   that id, so every event committed while serving the request is stored
///   with it;
/// - opens a `request` tracing span carrying the id (feature `tracing`, on by
///   default);
/// - sets the header on the response to that id.
///
/// The event context follows the request future across `.await` points and
/// threads, but a task spawned by a handler starts without it. Hand the
/// spawned future the current context to keep the correlation:
///
/// ```
/// use event_sauce_core::{EventContext, WithEventContextExt};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let work = async { /* commits events */ };
/// tokio::spawn(work.with_event_context(EventContext::current()))
///     .await
///     .unwrap();
/// # }
/// ```
///
/// # Examples
///
/// ```
/// use axum::Router;
/// use event_sauce_axum::{CorrelationLayer, HeaderName};
///
/// let app: Router = Router::new().layer(CorrelationLayer::new());
/// let name = HeaderName::from_static("x-correlation-id");
/// let custom: Router = Router::new().layer(CorrelationLayer::header(name));
/// ```
#[derive(Debug, Clone)]
pub struct CorrelationLayer {
    header: HeaderName,
}

impl CorrelationLayer {
    /// A layer reading and echoing the [`DEFAULT_HEADER`] (`x-request-id`).
    #[must_use]
    pub fn new() -> Self {
        Self::header(DEFAULT_HEADER)
    }

    /// A layer reading and echoing the header `name` instead.
    #[must_use]
    pub const fn header(name: HeaderName) -> Self {
        Self { header: name }
    }
}

impl Default for CorrelationLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for CorrelationLayer {
    type Service = Correlation<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Correlation {
            inner,
            header: self.header.clone(),
        }
    }
}

/// The service produced by [`CorrelationLayer`].
#[derive(Debug, Clone)]
pub struct Correlation<S> {
    inner: S,
    header: HeaderName,
}

impl<S, B, R> Service<Request<B>> for Correlation<S>
where
    S: Service<Request<B>, Response = Response<R>>,
{
    type Response = Response<R>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let id = req
            .headers()
            .get(&self.header)
            .and_then(|value| value.to_str().ok())
            .and_then(RequestId::parse)
            .unwrap_or_else(RequestId::generate);
        let future = self
            .inner
            .call(req)
            .with_event_context(event_context!(root, correlation_id = id.as_str()));
        #[cfg(feature = "tracing")]
        let future = future.instrument(info_span!("request", request_id = %id));
        ResponseFuture {
            inner: future,
            echo: (self.header.clone(), id.to_header_value()),
        }
    }
}

/// The future returned by [`Correlation`].
#[derive(Debug)]
pub struct ResponseFuture<F> {
    inner: Scoped<F>,
    echo: (HeaderName, HeaderValue),
}

impl<F, R, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response<R>, E>>,
{
    type Output = Result<Response<R>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(cx).map_ok(|mut response| {
            let (name, value) = this.echo.clone();
            response.headers_mut().insert(name, value);
            response
        })
    }
}

#[cfg(test)]
mod tests;
