use axum_ratelimit::{
    RateLimitConfig,
    store::{MemoryStore, RateLimitStore},
};
use std::{hint::black_box, time::Instant};

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
        for detailed in [false, true] {
            let store = MemoryStore::new();
            let config = RateLimitConfig::default()
                .max_requests(iterations + 1)
                .window_secs(3600);
            let started = Instant::now();
            for _ in 0..iterations {
                if detailed {
                    black_box(store.check("client", &config).await.unwrap());
                } else {
                    black_box(store.is_limited("client", &config).await.unwrap());
                }
            }
            println!(
                "{{\"case\":\"memory-{}\",\"iterations\":{},\"ns_per_check\":{:.2}}}",
                if detailed { "metadata" } else { "boolean" },
                iterations,
                started.elapsed().as_nanos() as f64 / iterations as f64
            );
        }
    });
}
