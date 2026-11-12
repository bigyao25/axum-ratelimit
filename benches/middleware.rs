use axum::{body::Body, response::IntoResponse};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer, observability::RateLimitEvent, store::MemoryStore,
};
use http::Request;
use std::{convert::Infallible, hint::black_box, time::Instant};
use tower::{Layer, ServiceExt, service_fn};

fn main() {
    let iterations = std::env::var("BENCH_ITERATIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100_000);
    assert!(iterations > 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for headers in [false, true] {
            for observed in [false, true] {
                let config = RateLimitConfig::default().max_requests(iterations + 1).window_secs(3600);
                let mut layer = RateLimitLayer::new(config, MemoryStore::new()).id(|_| "id".into()).response_headers(headers);
                if observed { layer = layer.observe(|_: &RateLimitEvent<'_>| {}); }
                let inner = service_fn(|_: Request<Body>| async { Ok::<_, Infallible>("ok".into_response()) });
                let service = layer.layer(inner);
                let started = Instant::now();
                for _ in 0..iterations { black_box(service.clone().oneshot(Request::new(Body::empty())).await.unwrap()); }
                println!("{{\"case\":\"middleware\",\"headers\":{},\"observer\":{},\"iterations\":{},\"ns_per_request\":{:.2}}}",
                    headers, observed, iterations, started.elapsed().as_nanos() as f64 / iterations as f64);
            }
        }
    });
}
