mod common;

use axum::{
    body::Body,
    response::{IntoResponse, Response},
};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    store::{MemoryStore, RateLimitStore, StoreError},
};
use futures_util::future::BoxFuture;
use http::{Request, StatusCode};
use std::{
    convert::Infallible,
    future::{Ready, ready},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tower::{Layer, Service, ServiceExt, service_fn};

#[derive(Default)]
struct Stats {
    calls: AtomicUsize,
    reservations: AtomicUsize,
}

struct ReadyService {
    ready: bool,
    stats: Arc<Stats>,
}

impl Clone for ReadyService {
    fn clone(&self) -> Self {
        Self {
            ready: false,
            stats: self.stats.clone(),
        }
    }
}

impl Drop for ReadyService {
    fn drop(&mut self) {
        if self.ready {
            self.stats.reservations.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Service<Request<Body>> for ReadyService {
    type Response = Response;
    type Error = Infallible;
    type Future = Ready<Result<Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        if !self.ready {
            self.ready = true;
            self.stats.reservations.fetch_add(1, Ordering::SeqCst);
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request<Body>) -> Self::Future {
        assert!(self.ready, "an unready cloned service was called");
        self.ready = false;
        self.stats.reservations.fetch_sub(1, Ordering::SeqCst);
        self.stats.calls.fetch_add(1, Ordering::SeqCst);
        ready(Ok("ok".into_response()))
    }
}

fn ready_service(stats: &Arc<Stats>) -> ReadyService {
    ReadyService {
        ready: false,
        stats: stats.clone(),
    }
}

#[tokio::test]
async fn calls_the_ready_instance_not_its_unready_clone() {
    let stats = Arc::new(Stats::default());
    let service = RateLimitLayer::new(RateLimitConfig::default(), MemoryStore::new())
        .layer(ready_service(&stats));
    let response = service
        .oneshot(common::request("127.0.0.1:1"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(stats.calls.load(Ordering::SeqCst), 1);
    assert_eq!(stats.reservations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rejecting_releases_the_downstream_readiness_reservation() {
    let stats = Arc::new(Stats::default());
    let service = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(0),
        MemoryStore::new(),
    )
    .layer(ready_service(&stats));
    assert_eq!(
        service
            .oneshot(common::request("127.0.0.1:1"))
            .await
            .unwrap()
            .status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(stats.calls.load(Ordering::SeqCst), 0);
    assert_eq!(stats.reservations.load(Ordering::SeqCst), 0);
}

struct PendingStore;

impl RateLimitStore for PendingStore {
    fn is_limited<'a>(
        &'a self,
        _key: &'a str,
        _config: &'a RateLimitConfig,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn cancellation_during_storage_releases_reservation_and_never_enters_handler() {
    let stats = Arc::new(Stats::default());
    let service =
        RateLimitLayer::new(RateLimitConfig::default(), PendingStore).layer(ready_service(&stats));
    let mut future = Box::pin(service.oneshot(common::request("127.0.0.1:1")));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    assert_eq!(stats.reservations.load(Ordering::SeqCst), 1);
    assert_eq!(stats.calls.load(Ordering::SeqCst), 0);
    drop(future);
    assert_eq!(stats.reservations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_an_unpolled_call_never_consumes_quota() {
    let stats = Arc::new(Stats::default());
    let store = Arc::new(MemoryStore::new());
    let config = RateLimitConfig::default().max_requests(1);
    let mut service =
        RateLimitLayer::new(config.clone(), store.clone()).layer(ready_service(&stats));
    let future = service
        .ready()
        .await
        .unwrap()
        .call(common::request("127.0.0.1:1"));
    drop(future);
    assert_eq!(stats.reservations.load(Ordering::SeqCst), 0);
    assert!(!store.is_limited("127.0.0.1", &config).await.unwrap());
}

#[tokio::test]
async fn downstream_errors_propagate_unchanged_and_admission_is_not_refunded() {
    let inner = service_fn(|_: Request<Body>| async {
        Err::<Response, _>(std::io::Error::other("downstream"))
    });
    let service = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(1),
        MemoryStore::new(),
    )
    .layer(inner);
    let error = service
        .clone()
        .oneshot(common::request("127.0.0.1:1"))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "downstream");
    let response = service
        .oneshot(common::request("127.0.0.1:1"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[derive(Clone)]
struct NotReady;

impl Service<Request<Body>> for NotReady {
    type Response = Response;
    type Error = Infallible;
    type Future = Ready<Result<Response, Infallible>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Pending
    }
    fn call(&mut self, _: Request<Body>) -> Self::Future {
        panic!("unready service called")
    }
}

#[tokio::test]
async fn backpressure_does_not_start_the_store_check() {
    let store = common::FixedStore::new(axum_ratelimit::store::RateLimitDecision {
        allowed: true,
        metadata: None,
    });
    let service = RateLimitLayer::new(RateLimitConfig::default(), store.clone()).layer(NotReady);
    let mut future = Box::pin(service.oneshot(common::request("127.0.0.1:1")));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    assert_eq!(store.calls(), (0, 0));
}

struct UnreadBody(Arc<AtomicUsize>);

impl http_body::Body for UnreadBody {
    type Data = bytes::Bytes;
    type Error = Infallible;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(None)
    }
}

#[tokio::test]
async fn rejected_streaming_bodies_are_not_polled_or_buffered() {
    let polls = Arc::new(AtomicUsize::new(0));
    let inner =
        service_fn(|_: Request<Body>| async { Ok::<_, Infallible>("unused".into_response()) });
    let service = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(0),
        MemoryStore::new(),
    )
    .id(|_| "id".into())
    .layer(inner);
    let response = service
        .oneshot(Request::new(Body::new(UnreadBody(polls.clone()))))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
}
