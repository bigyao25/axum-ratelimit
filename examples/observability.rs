use axum::{Router, routing::get};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    observability::{RateLimitEvent, RateLimitOutcome},
    store::MemoryStore,
};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Counters {
    allowed: AtomicU64,
    limited: AtomicU64,
    errors: AtomicU64,
    fail_open: AtomicU64,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let counts = Arc::new(Counters::default());
    let observer = counts.clone();
    let limiter = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(10).window_secs(60),
        MemoryStore::new(),
    )
    .observe(move |event: &RateLimitEvent<'_>| {
        let target = match event.outcome {
            RateLimitOutcome::Allowed => &observer.allowed,
            RateLimitOutcome::Limited => &observer.limited,
            RateLimitOutcome::StoreError => &observer.errors,
            RateLimitOutcome::FailOpen => &observer.fail_open,
            _ => return,
        };
        target.fetch_add(1, Ordering::Relaxed);
    });
    // Export happens outside the synchronous callback and is owned by the app.
    let reporter = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            println!(
                "allowed={} limited={} errors={} fail_open={}",
                counts.allowed.load(Ordering::Relaxed),
                counts.limited.load(Ordering::Relaxed),
                counts.errors.load(Ordering::Relaxed),
                counts.fail_open.load(Ordering::Relaxed)
            );
        }
    });
    let app = Router::new()
        .route("/", get(|| async { "Observed" }))
        .layer(limiter);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    let result = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await;
    reporter.abort();
    result
}
