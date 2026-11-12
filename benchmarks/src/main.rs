//! Benchmark-only server. NEVER expose this server to untrusted clients.
//! Client identities are deliberately controllable to generate reproducible loads.
use axum::{Router, routing::get};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    store::{MemoryStore, RedisStore},
};
use http::{Request, request::Parts};
use std::{env, time::Duration};
use tower_governor::{
    GovernorError, GovernorLayer, governor::GovernorConfigBuilder, key_extractor::KeyExtractor,
};

#[derive(Clone)]
struct BenchmarkIdentity;

fn identity(parts: &Parts) -> String {
    parts
        .headers
        .get("x-benchmark-client")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("0")
        .to_owned()
}

impl KeyExtractor for BenchmarkIdentity {
    type Key = String;
    fn extract<T>(&self, req: &Request<T>) -> Result<Self::Key, GovernorError> {
        Ok(req
            .headers()
            .get("x-benchmark-client")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("0")
            .to_owned())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let variant = env::var("BENCH_VARIANT").unwrap_or_else(|_| "head".into());
    let backend = env::var("BENCH_BACKEND").unwrap_or_else(|_| "memory".into());
    let mode = env::var("BENCH_MODE").unwrap_or_else(|_| "allow".into());
    let (quota, window) = match mode.as_str() {
        "allow" => (10_000_000, 1),
        "deny" => (1, 600),
        "mixed" => (100, 1),
        _ => return Err("BENCH_MODE must be allow, deny or mixed".into()),
    };
    let config = RateLimitConfig::default()
        .namespace("benchmark")
        .max_requests(quota)
        .window_secs(window);
    let routes = Router::new().route("/", get(|| async { "ok" }));
    let routes = match (variant.as_str(), backend.as_str()) {
        ("baseline", "memory" | "redis") => routes,
        ("head", "memory") => {
            routes.layer(RateLimitLayer::new(config, MemoryStore::new()).id(identity))
        }
        ("head", "redis") => {
            let url = env::var("REDIS_URL")?;
            let prefix = env::var("BENCH_PREFIX")?;
            let options = axum_ratelimit::store::RedisStoreOptions {
                timeout: Duration::from_secs(1),
                failure_policy: axum_ratelimit::store::FailurePolicy::FailClosed,
            };
            let store = RedisStore::with_options(&url, options)
                .await?
                .with_prefix(&prefix);
            routes.layer(RateLimitLayer::new(config, store).id(identity))
        }
        ("governor", "memory") => {
            // GCRA is NOT equivalent to an exact sliding window. Mixed-path
            // admission rates must be reported, not treated as equivalent quotas.
            let period = match mode.as_str() {
                "allow" => Duration::from_nanos(1),
                "deny" => Duration::from_secs(600),
                _ => Duration::from_millis(10),
            };
            let config = GovernorConfigBuilder::default()
                .key_extractor(BenchmarkIdentity)
                .period(period)
                .burst_size(quota as u32)
                .finish()
                .ok_or("invalid governor quota")?;
            routes.layer(GovernorLayer::new(config))
        }
        _ => return Err("unsupported variant/backend combination".into()),
    };
    // Health checks intentionally do not consume quota.
    let app = routes.route("/health", get(|| async { "ready" }));
    let address = env::var("BENCH_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    eprintln!("variant={variant} backend={backend} mode={mode} bind={address}");
    axum::serve(listener, app).await?;
    Ok(())
}
