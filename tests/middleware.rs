mod common;

use axum::{
    Router,
    body::Body,
    extract::Request,
    middleware::{Next, from_fn},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use axum_ratelimit::{IdentityError, RateLimitConfig, RateLimitLayer, store::MemoryStore};
use common::{app, request, text};
use http::StatusCode;
use std::{
    cell::Cell,
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tower::{Layer, ServiceBuilder, ServiceExt, service_fn};

#[tokio::test]
async fn allows_quota_then_rejects_without_entering_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let app = Router::new()
        .route(
            "/",
            get(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    "ok"
                }
            }),
        )
        .layer(RateLimitLayer::new(
            RateLimitConfig::default().max_requests(2),
            MemoryStore::new(),
        ));
    for expected in [
        StatusCode::OK,
        StatusCode::OK,
        StatusCode::TOO_MANY_REQUESTS,
    ] {
        let response = app
            .clone()
            .oneshot(request("127.0.0.1:1234"))
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert!(!response.headers().contains_key("ratelimit"));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn peer_ip_ignores_ports_and_forwarded_headers() {
    let app = app(RateLimitLayer::new(
        RateLimitConfig::default().max_requests(1),
        MemoryStore::new(),
    ));
    assert_eq!(
        app.clone()
            .oneshot(request("127.0.0.1:1"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let mut spoofed = request("127.0.0.1:9999");
    spoofed
        .headers_mut()
        .insert("x-forwarded-for", "8.8.8.8".parse().unwrap());
    spoofed
        .headers_mut()
        .insert("forwarded", "for=1.1.1.1".parse().unwrap());
    assert_eq!(
        app.clone().oneshot(spoofed).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        app.oneshot(request("127.0.0.2:1")).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn ipv4_mapped_ipv6_and_ipv4_share_a_bucket() {
    let app = app(RateLimitLayer::new(
        RateLimitConfig::default().max_requests(1),
        MemoryStore::new(),
    ));
    assert_eq!(
        app.clone()
            .oneshot(request("[::ffff:127.0.0.1]:5"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.oneshot(request("127.0.0.1:6")).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn missing_connect_info_is_not_an_unknown_bucket_or_fail_open() {
    let app = app(RateLimitLayer::new(
        RateLimitConfig::default().max_requests(1),
        MemoryStore::new(),
    ));
    let missing = Request::new(Body::empty());
    assert_eq!(
        app.clone().oneshot(missing).await.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        app.oneshot(request("127.0.0.1:1")).await.unwrap().status(),
        StatusCode::OK
    );
}

#[derive(Clone)]
struct UserId(String);

async fn authenticate(mut request: Request, next: Next) -> Response {
    // This test uses a fixed trusted identity; a real auth layer validates credentials.
    request.extensions_mut().insert(UserId("usr_1".into()));
    next.run(request).await
}

#[tokio::test]
async fn auth_extensions_are_visible_when_service_builder_orders_auth_first() {
    let prefix = "org_1:".to_owned();
    let layer = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(1),
        MemoryStore::new(),
    )
    .try_id(move |parts| {
        parts
            .extensions
            .get::<UserId>()
            .map(|id| format!("{prefix}{}", id.0))
            .ok_or(IdentityError::MissingIdentity)
    });
    let app = Router::new().route("/", get(|| async { "ok" })).layer(
        ServiceBuilder::new()
            .layer(from_fn(authenticate))
            .layer(layer),
    );
    assert_eq!(
        app.clone()
            .oneshot(Request::new(Body::empty()))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.oneshot(Request::new(Body::empty()))
            .await
            .unwrap()
            .status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn missing_custom_identity_does_not_check_store() {
    let store = common::FixedStore::new(axum_ratelimit::store::RateLimitDecision {
        allowed: true,
        metadata: None,
    });
    let layer = RateLimitLayer::new(RateLimitConfig::default(), store.clone())
        .try_id(|_| Err(IdentityError::MissingIdentity));
    assert_eq!(
        app(layer)
            .oneshot(request("127.0.0.1:1"))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(store.calls(), (0, 0));
}

#[tokio::test]
async fn custom_rejection_supports_captures_and_receives_request_parts() {
    let message = Arc::new(String::from("custom response"));
    let layer = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(0),
        MemoryStore::new(),
    )
    .id(|_| "usr_1".into())
    .exceeded(move |id, config, parts| {
        assert_eq!(id, "usr_1");
        assert_eq!(config.max_requests, 0);
        assert_eq!(parts.uri.path(), "/");
        (StatusCode::FORBIDDEN, message.to_string())
    });
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(text(response).await, "custom response");
}

#[tokio::test]
async fn storage_errors_are_503_and_do_not_expose_backend_detail() {
    let response = app(RateLimitLayer::new(
        RateLimitConfig::default(),
        common::ErrorStore,
    ))
    .oneshot(request("127.0.0.1:1"))
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(text(response).await, "Rate limit storage unavailable");
}

#[tokio::test]
async fn cloned_layers_and_separate_routers_share_the_same_store() {
    let layer = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(1),
        MemoryStore::new(),
    );
    assert_eq!(
        app(layer.clone())
            .oneshot(request("127.0.0.1:1"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app(layer)
            .oneshot(request("127.0.0.1:2"))
            .await
            .unwrap()
            .status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn separately_configured_layers_share_only_equal_rules() {
    let store = Arc::new(MemoryStore::new());
    let config = RateLimitConfig::default().max_requests(1);
    for (namespace, status) in [
        ("a", StatusCode::OK),
        ("b", StatusCode::OK),
        ("a", StatusCode::TOO_MANY_REQUESTS),
    ] {
        let layer = RateLimitLayer::new(config.clone().namespace(namespace), store.clone());
        assert_eq!(
            app(layer)
                .oneshot(request("127.0.0.1:1"))
                .await
                .unwrap()
                .status(),
            status
        );
    }
}

#[tokio::test]
async fn request_body_and_extensions_survive_admission() {
    let layer =
        RateLimitLayer::new(RateLimitConfig::default(), MemoryStore::new()).id(|_| "id".into());
    let app = Router::new()
        .route("/", post(|body: String| async move { body }))
        .layer(layer);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/")
                .body(Body::from("payload"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(text(response).await, "payload");
}

#[tokio::test]
async fn a_send_but_not_sync_request_body_is_supported() {
    let inner = service_fn(|request: http::Request<Cell<u8>>| async move {
        assert_eq!(request.body().get(), 7);
        Ok::<_, Infallible>("ok".into_response())
    });
    let service = RateLimitLayer::new(RateLimitConfig::default(), MemoryStore::new())
        .id(|_| "id".into())
        .layer(inner);
    let response = service
        .oneshot(http::Request::new(Cell::new(7)))
        .await
        .unwrap();
    assert_eq!(text(response).await, "ok");
}

#[tokio::test]
async fn route_layer_does_not_charge_unmatched_routes() {
    let layer = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(1),
        MemoryStore::new(),
    )
    .id(|_| "id".into());
    let app = Router::new()
        .route("/", get(|| async { "ok" }))
        .route_layer(layer);
    let missing = Request::builder()
        .uri("/missing")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(missing).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.oneshot(Request::new(Body::empty()))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn middleware_admits_exactly_once_under_concurrency() {
    let app = app(RateLimitLayer::new(
        RateLimitConfig::default().max_requests(7),
        MemoryStore::new(),
    )
    .response_headers(true));
    let futures = (0..64).map(|_| app.clone().oneshot(request("127.0.0.1:1")));
    let responses = futures_util::future::join_all(futures).await;
    let mut allowed = 0;
    for response in responses {
        let response = response.unwrap();
        match response.status() {
            StatusCode::OK => allowed += 1,
            StatusCode::TOO_MANY_REQUESTS => {}
            status => panic!("unexpected status: {status}"),
        }
    }
    assert_eq!(allowed, 7);
}
