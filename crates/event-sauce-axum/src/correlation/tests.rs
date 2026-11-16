use std::convert::Infallible;

use http::{Request, Response};
use tower::{service_fn, Layer, ServiceExt};

use super::*;

type Echo = Result<Response<()>, Infallible>;

async fn call(layer: CorrelationLayer, header: Option<(&str, &str)>) -> Response<()> {
    let mut request = Request::builder().uri("/");
    if let Some((name, value)) = header {
        request = request.header(name, value);
    }
    layer
        .layer(service_fn(|_: Request<()>| async {
            Echo::Ok(Response::new(()))
        }))
        .oneshot(request.body(()).unwrap())
        .await
        .unwrap()
}

fn echoed<'a>(response: &'a Response<()>, name: &str) -> &'a str {
    response.headers()[name].to_str().unwrap()
}

#[tokio::test]
async fn a_well_formed_incoming_id_is_echoed() {
    let response = call(CorrelationLayer::new(), Some(("x-request-id", "req-123"))).await;
    assert_eq!(echoed(&response, "x-request-id"), "req-123");
}

#[tokio::test]
async fn a_missing_id_is_replaced_by_a_uuid() {
    let response = call(CorrelationLayer::new(), None).await;
    assert!(uuid::Uuid::parse_str(echoed(&response, "x-request-id")).is_ok());
}

#[tokio::test]
async fn a_malformed_id_is_replaced_by_a_uuid() {
    let response = call(
        CorrelationLayer::new(),
        Some(("x-request-id", "not valid!")),
    )
    .await;
    assert!(uuid::Uuid::parse_str(echoed(&response, "x-request-id")).is_ok());
}

#[tokio::test]
async fn a_custom_header_is_read_and_echoed_instead_of_the_default() {
    let layer = CorrelationLayer::header("x-correlation-id");
    let response = call(layer, Some(("x-correlation-id", "corr-9"))).await;
    assert_eq!(echoed(&response, "x-correlation-id"), "corr-9");
    assert!(response.headers().get("x-request-id").is_none());
}

#[tokio::test]
async fn the_default_header_is_ignored_when_a_custom_one_is_configured() {
    let layer = CorrelationLayer::header("x-correlation-id");
    let response = call(layer, Some(("x-request-id", "req-1"))).await;
    assert_ne!(echoed(&response, "x-correlation-id"), "req-1");
}

#[tokio::test]
async fn the_correlation_header_overrides_one_set_by_the_inner_service() {
    let service = CorrelationLayer::new().layer(service_fn(|_: Request<()>| async {
        let mut response = Response::new(());
        response
            .headers_mut()
            .insert("x-request-id", "inner".parse().unwrap());
        Echo::Ok(response)
    }));
    let request = Request::builder()
        .header("x-request-id", "req-1")
        .body(())
        .unwrap();
    let response = service.oneshot(request).await.unwrap();
    assert_eq!(echoed(&response, "x-request-id"), "req-1");
}

#[tokio::test]
async fn an_inner_error_passes_through_untouched() {
    let service = CorrelationLayer::new().layer(service_fn(|_: Request<()>| async {
        Err::<Response<()>, _>("boom")
    }));
    let error = service
        .oneshot(Request::new(()))
        .await
        .expect_err("inner error propagates");
    assert_eq!(error, "boom");
}
