mod common;

use axum::{Router, routing::get};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    observability::{
        ObservedRateLimitDecision, RateLimitErrorKind, RateLimitEvent, RateLimitOutcome,
    },
    store::{MemoryStore, RateLimitDecision, RateLimitStore, StoreError},
};
use common::{app, request};
use futures_util::future::BoxFuture;
use http::StatusCode;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tower::ServiceExt;

#[tokio::test]
async fn observer_reports_admission_not_downstream_status() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let collected = events.clone();
    let layer = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(1),
        MemoryStore::new(),
    )
    .observe(move |event: &RateLimitEvent<'_>| {
        collected.lock().unwrap().push(event.outcome);
    });
    let app = Router::new()
        .route("/", get(|| async { StatusCode::TOO_MANY_REQUESTS }))
        .layer(layer);
    for _ in 0..2 {
        assert_eq!(
            app.clone()
                .oneshot(request("127.0.0.1:1"))
                .await
                .unwrap()
                .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }
    assert_eq!(
        *events.lock().unwrap(),
        [RateLimitOutcome::Allowed, RateLimitOutcome::Limited]
    );
}

#[tokio::test]
async fn observer_runs_before_handler_and_after_storage_locks_are_released() {
    let observed = Arc::new(AtomicBool::new(false));
    let flag = observed.clone();
    let store = Arc::new(MemoryStore::new());
    let callback_store = store.clone();
    let layer = RateLimitLayer::new(RateLimitConfig::default(), store).observe(
        move |_: &RateLimitEvent<'_>| {
            callback_store.prune_expired();
            flag.store(true, Ordering::SeqCst);
        },
    );
    let app = Router::new()
        .route(
            "/",
            get(move || {
                let flag = observed.clone();
                async move {
                    assert!(flag.load(Ordering::SeqCst));
                    "ok"
                }
            }),
        )
        .layer(layer);
    assert_eq!(
        app.oneshot(request("127.0.0.1:1")).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn store_without_metadata_is_allowed_not_fail_open() {
    struct BasicStore;
    impl RateLimitStore for BasicStore {
        fn is_limited<'a>(
            &'a self,
            _: &'a str,
            _: &'a RateLimitConfig,
        ) -> BoxFuture<'a, Result<bool, StoreError>> {
            Box::pin(async { Ok(false) })
        }
    }
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    let layer = RateLimitLayer::new(RateLimitConfig::default(), BasicStore)
        .response_headers(true)
        .observe(move |event: &RateLimitEvent<'_>| {
            captured
                .lock()
                .unwrap()
                .push((event.outcome, event.error_kind, event.metadata));
        });
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("ratelimit"));
    assert_eq!(
        *events.lock().unwrap(),
        [(RateLimitOutcome::Allowed, None, None)]
    );
}

struct FailOpen(Arc<AtomicUsize>);

impl RateLimitStore for FailOpen {
    fn is_limited<'a>(
        &'a self,
        _: &'a str,
        _: &'a RateLimitConfig,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async { Ok(false) })
    }
    fn check_observed<'a>(
        &'a self,
        _: &'a str,
        _: &'a RateLimitConfig,
        _: bool,
    ) -> BoxFuture<'a, Result<ObservedRateLimitDecision, StoreError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ObservedRateLimitDecision {
                decision: RateLimitDecision {
                    allowed: true,
                    metadata: None,
                },
                fail_open: true,
                error_kind: Some(RateLimitErrorKind::Timeout),
            })
        })
    }
}

#[tokio::test]
async fn explicit_fail_open_survives_arc_and_box_forwarding() {
    for headers in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let store: Arc<Box<dyn RateLimitStore>> = Arc::new(Box::new(FailOpen(calls.clone())));
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = events.clone();
        let layer = RateLimitLayer::new(RateLimitConfig::default(), store)
            .response_headers(headers)
            .observe(move |event: &RateLimitEvent<'_>| {
                captured
                    .lock()
                    .unwrap()
                    .push((event.outcome, event.error_kind, event.metadata));
            });
        let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key("ratelimit"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *events.lock().unwrap(),
            [(
                RateLimitOutcome::FailOpen,
                Some(RateLimitErrorKind::Timeout),
                None
            )]
        );
    }
}

#[tokio::test]
async fn storage_error_events_expose_categories_not_sensitive_messages() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capture = seen.clone();
    let layer = RateLimitLayer::new(RateLimitConfig::default(), common::ErrorStore)
        .id(|_| "secret-client-id".into())
        .observe(move |event: &RateLimitEvent<'_>| {
            capture
                .lock()
                .unwrap()
                .push((event.outcome, event.error_kind, format!("{event:?}")));
        });
    let response = app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let events = seen.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, RateLimitOutcome::StoreError);
    assert_eq!(events[0].1, Some(RateLimitErrorKind::Timeout));
    assert!(!events[0].2.contains("secret-client-id"));
    assert!(!events[0].2.contains("private backend"));
}

#[tokio::test]
async fn metadata_is_only_present_when_requested_and_no_extra_check_occurs() {
    for headers in [false, true] {
        let events = Arc::new(AtomicUsize::new(0));
        let capture = events.clone();
        let store = common::FixedStore::new(RateLimitDecision {
            allowed: true,
            metadata: None,
        });
        let layer = RateLimitLayer::new(RateLimitConfig::default(), store.clone())
            .response_headers(headers)
            .observe(move |event: &RateLimitEvent<'_>| {
                assert_eq!(event.namespace, "default");
                assert_eq!(event.max_requests, 10);
                capture.fetch_add(1, Ordering::SeqCst);
            });
        app(layer).oneshot(request("127.0.0.1:1")).await.unwrap();
        assert_eq!(events.load(Ordering::SeqCst), 1);
        assert_eq!(store.calls(), if headers { (0, 1) } else { (1, 0) });
    }
}

struct PanicPayload {
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
}
impl Drop for PanicPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_on_drop, "payload destructor panic");
    }
}

#[tokio::test]
#[cfg(panic = "unwind")]
async fn observer_panic_disables_shared_callback_without_leaking_its_payload() {
    for panic_on_drop in [false, true] {
        let callbacks = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let callback_count = callbacks.clone();
        let payload_drops = drops.clone();
        let layer = RateLimitLayer::new(RateLimitConfig::default(), MemoryStore::new()).observe(
            move |_: &RateLimitEvent<'_>| {
                callback_count.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(PanicPayload {
                    drops: payload_drops.clone(),
                    panic_on_drop,
                });
            },
        );
        for _ in 0..2 {
            assert_eq!(
                app(layer.clone())
                    .oneshot(request("127.0.0.1:1"))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
        assert_eq!(callbacks.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn io_error_classification_is_coarse_and_does_not_format_messages() {
    use std::io::ErrorKind;
    for (kind, expected) in [
        (ErrorKind::TimedOut, RateLimitErrorKind::Timeout),
        (ErrorKind::ConnectionRefused, RateLimitErrorKind::Connection),
        (ErrorKind::UnexpectedEof, RateLimitErrorKind::Connection),
        (ErrorKind::PermissionDenied, RateLimitErrorKind::Other),
    ] {
        assert_eq!(
            StoreError::new(std::io::Error::new(kind, "private")).kind(),
            expected
        );
    }
}
