#![allow(dead_code)]

use axum::{Router, body::Body, extract::ConnectInfo, response::Response, routing::get};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    store::{RateLimitDecision, RateLimitStore, StoreError},
};
use futures_util::future::BoxFuture;
use http::Request;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub fn request(peer: &str) -> Request<Body> {
    let mut request = Request::builder().uri("/").body(Body::empty()).unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(peer.parse::<SocketAddr>().unwrap()));
    request
}

pub fn app(layer: RateLimitLayer) -> Router {
    Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(layer)
}

pub async fn text(response: Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[derive(Clone)]
pub struct FixedStore {
    pub decision: RateLimitDecision,
    pub bool_calls: Arc<AtomicUsize>,
    pub detailed_calls: Arc<AtomicUsize>,
}

impl FixedStore {
    pub fn new(decision: RateLimitDecision) -> Self {
        Self {
            decision,
            bool_calls: Arc::new(AtomicUsize::new(0)),
            detailed_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn calls(&self) -> (usize, usize) {
        (
            self.bool_calls.load(Ordering::SeqCst),
            self.detailed_calls.load(Ordering::SeqCst),
        )
    }
}

impl RateLimitStore for FixedStore {
    fn is_limited<'a>(
        &'a self,
        _key: &'a str,
        _config: &'a RateLimitConfig,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            self.bool_calls.fetch_add(1, Ordering::SeqCst);
            Ok(!self.decision.allowed)
        })
    }

    fn check<'a>(
        &'a self,
        _key: &'a str,
        _config: &'a RateLimitConfig,
    ) -> BoxFuture<'a, Result<RateLimitDecision, StoreError>> {
        Box::pin(async move {
            self.detailed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.decision)
        })
    }
}

pub struct ErrorStore;

impl RateLimitStore for ErrorStore {
    fn is_limited<'a>(
        &'a self,
        _key: &'a str,
        _config: &'a RateLimitConfig,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async {
            Err(StoreError::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "private backend detail that must not reach clients",
            )))
        })
    }
}
