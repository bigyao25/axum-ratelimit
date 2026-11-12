mod common;

use axum::{Router, response::IntoResponse, routing::get};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    observability::RateLimitEvent,
    store::{MemoryStore, RateLimitDecision, RateLimitMetadata},
};
use common::{FixedStore, app, request};
use http::{HeaderValue, StatusCode};
use std::time::Duration;
use tower::ServiceExt;

fn decision(allowed: bool, remaining: usize, retry_after: Option<Duration>) -> RateLimitDecision {
    RateLimitDecision {
        allowed,
        metadata: Some(RateLimitMetadata {
            remaining,
            retry_after,
        }),
    }
}

#[tokio::test]
async fn remaining_describes_the_same_atomic_admission() {
    let layer = RateLimitLayer::new(
        RateLimitConfig::default()
            .namespace("api")
            .max_requests(2)
            .window_secs(60),
        MemoryStore::new(),
    )
    .response_headers(true);
    let app = app(layer);
    for (status, remaining) in [
        (StatusCode::OK, 1),
        (StatusCode::OK, 0),
        (StatusCode::TOO_MANY_REQUESTS, 0),
    ] {
        let response = app.clone().oneshot(request("127.0.0.1:1")).await.unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["ratelimit-policy"], "\"api\";q=2;w=60");
        assert_eq!(
            response.headers()["ratelimit"],
            format!("\"api\";r={remaining}")
        );
        assert_eq!(
            response.headers().contains_key("retry-after"),
            status == StatusCode::TOO_MANY_REQUESTS
        );
    }
}

#[tokio::test]
async fn boolean_or_detailed_path_is_selected_once_with_or_without_observer() {
    for headers in [false, true] {
        for observed in [false, true] {
            let store = FixedStore::new(decision(true, 9, None));
            let mut layer = RateLimitLayer::new(RateLimitConfig::default(), store.clone())
                .response_headers(headers);
            if observed {
                layer = layer.observe(|_: &RateLimitEvent<'_>| {});
            }
            let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
            assert_eq!(store.calls(), if headers { (0, 1) } else { (1, 0) });
            assert_eq!(response.headers().contains_key("ratelimit"), headers);
        }
    }
}

#[tokio::test]
async fn retry_after_rounds_up_fractional_seconds() {
    for (delay, expected) in [
        (Duration::from_nanos(1), "1"),
        (Duration::from_secs(1), "1"),
        (Duration::from_millis(1001), "2"),
    ] {
        let store = FixedStore::new(decision(false, 0, Some(delay)));
        let layer = RateLimitLayer::new(RateLimitConfig::default(), store).response_headers(true);
        let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
        assert_eq!(response.headers()["retry-after"], expected);
    }
}

#[tokio::test]
async fn unknown_metadata_never_fabricates_headers() {
    let store = FixedStore::new(RateLimitDecision {
        allowed: false,
        metadata: None,
    });
    let layer = RateLimitLayer::new(RateLimitConfig::default(), store).response_headers(true);
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    for name in ["ratelimit-policy", "ratelimit", "retry-after"] {
        assert!(!response.headers().contains_key(name));
    }
}

#[tokio::test]
async fn zero_quota_has_no_finite_retry_time() {
    let layer = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(0),
        MemoryStore::new(),
    )
    .response_headers(true);
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["ratelimit"], "\"default\";r=0");
    assert!(!response.headers().contains_key("retry-after"));
}

#[tokio::test]
async fn fractional_window_omits_w_without_changing_admission() {
    let config = RateLimitConfig::default().window(Duration::from_millis(1250));
    let layer = RateLimitLayer::new(config, FixedStore::new(decision(true, 9, None)))
        .response_headers(true);
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.headers()["ratelimit-policy"], "\"default\";q=10");
}

#[tokio::test]
async fn namespace_quotes_and_backslashes_are_escaped() {
    let config = RateLimitConfig::default().namespace("a\"b\\c");
    let layer = RateLimitLayer::new(config, FixedStore::new(decision(true, 9, None)))
        .response_headers(true);
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(
        response.headers()["ratelimit-policy"],
        "\"a\\\"b\\\\c\";q=10;w=100"
    );
}

#[tokio::test]
async fn invalid_policy_name_omits_draft_fields_but_keeps_retry_after() {
    for namespace in ["bad\r\nname", "caf\u{e9}", "bad\u{7f}"] {
        let config = RateLimitConfig::default().namespace(namespace);
        let store = FixedStore::new(decision(false, 0, Some(Duration::from_secs(2))));
        let response = app(RateLimitLayer::new(config, store).response_headers(true))
            .oneshot(request("127.0.0.1:1"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(!response.headers().contains_key("ratelimit-policy"));
        assert!(!response.headers().contains_key("ratelimit"));
        assert_eq!(response.headers()["retry-after"], "2");
    }
}

#[tokio::test]
#[cfg(target_pointer_width = "64")]
async fn structured_fields_numeric_limit_does_not_change_admission() {
    let config = RateLimitConfig::default().max_requests(1_000_000_000_000_000);
    let store = FixedStore::new(decision(false, 0, Some(Duration::from_secs(1))));
    let response = app(RateLimitLayer::new(config, store).response_headers(true))
        .oneshot(request("127.0.0.1:1"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(!response.headers().contains_key("ratelimit-policy"));
    assert!(!response.headers().contains_key("ratelimit"));
    assert_eq!(response.headers()["retry-after"], "1");
}

#[tokio::test]
async fn existing_headers_are_preserved_individually() {
    for existing in ["ratelimit-policy", "ratelimit"] {
        let layer = RateLimitLayer::new(
            RateLimitConfig::default(),
            FixedStore::new(decision(true, 9, None)),
        )
        .response_headers(true);
        let app = Router::new()
            .route(
                "/",
                get(move || async move {
                    let mut response = "ok".into_response();
                    response
                        .headers_mut()
                        .insert(existing, HeaderValue::from_static("custom"));
                    response
                }),
            )
            .layer(layer);
        let response = app.oneshot(request("127.0.0.1:1")).await.unwrap();
        assert_eq!(response.headers()[existing], "custom");
        assert!(response.headers().contains_key("ratelimit-policy"));
        assert!(response.headers().contains_key("ratelimit"));
    }
}

#[tokio::test]
async fn rejection_callback_headers_win_over_automatic_headers() {
    let store = FixedStore::new(decision(false, 0, Some(Duration::from_secs(1))));
    let layer = RateLimitLayer::new(RateLimitConfig::default(), store)
        .response_headers(true)
        .exceeded(|_, _, _| {
            let mut response = (StatusCode::TOO_MANY_REQUESTS, "custom").into_response();
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("99"));
            response
                .headers_mut()
                .insert("ratelimit", HeaderValue::from_static("custom"));
            response
        });
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.headers()["retry-after"], "99");
    assert_eq!(response.headers()["ratelimit"], "custom");
    assert!(response.headers().contains_key("ratelimit-policy"));
}

#[tokio::test]
async fn custom_non_429_rejection_does_not_get_retry_after() {
    let store = FixedStore::new(decision(false, 0, Some(Duration::from_secs(1))));
    let layer = RateLimitLayer::new(RateLimitConfig::default(), store)
        .response_headers(true)
        .exceeded(|_, _, _| StatusCode::FORBIDDEN);
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(!response.headers().contains_key("retry-after"));
}

#[tokio::test]
async fn downstream_429_is_not_a_limiter_rejection() {
    let store = FixedStore::new(decision(true, 9, Some(Duration::from_secs(1))));
    let app = Router::new()
        .route("/", get(|| async { StatusCode::TOO_MANY_REQUESTS }))
        .layer(RateLimitLayer::new(RateLimitConfig::default(), store).response_headers(true));
    let response = app.oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(!response.headers().contains_key("retry-after"));
}

#[tokio::test]
async fn store_errors_do_not_leak_quota_or_retry_metadata() {
    let layer =
        RateLimitLayer::new(RateLimitConfig::default(), common::ErrorStore).response_headers(true);
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    for name in ["ratelimit", "ratelimit-policy", "retry-after"] {
        assert!(!response.headers().contains_key(name));
    }
}
