//! Benchmark-only Axum server with controlled identities and shared HTTP behavior.
use axum::{Router, middleware, response::IntoResponse, routing::get};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    store::{FailurePolicy, MemoryStore, RedisStore, RedisStoreOptions},
};
use http::{Request, StatusCode, request::Parts};
use std::{env, sync::Arc, time::Duration};
use tower_governor::{
    GovernorError, GovernorLayer, governor::GovernorConfigBuilder, key_extractor::KeyExtractor,
};

#[derive(Clone)]
struct BenchmarkIdentity;

/// The upstream token bucket has no keyed HTTP layer; this adapter is measured too.
struct TokenBuckets {
    buckets: dashmap::DashMap<String, ratelimit::Ratelimiter>,
    quota: u64,
    window: Duration,
}

impl TokenBuckets {
    fn new(quota: u64, window: Duration) -> Result<Self, Box<dyn std::error::Error>> {
        if quota == 0 {
            return Err("ratelimit treats zero as unlimited, not deny-all".into());
        }
        let _ = ratelimit::Ratelimiter::builder(quota)
            .period(window)
            .max_tokens(quota)
            .initial_available(quota)
            .build()?;
        Ok(Self {
            buckets: dashmap::DashMap::new(),
            quota,
            window,
        })
    }

    fn admit(&self, key: String) -> bool {
        self.buckets
            .entry(key)
            .or_insert_with(|| {
                ratelimit::Ratelimiter::builder(self.quota)
                    .period(self.window)
                    .max_tokens(self.quota)
                    .initial_available(self.quota)
                    .build()
                    .expect("token bucket settings validated at startup")
            })
            .try_wait()
            .is_ok()
    }
}

fn identity(parts: &Parts) -> String {
    parts
        .headers
        .get("x-bench-client")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("0")
        .to_owned()
}

impl KeyExtractor for BenchmarkIdentity {
    type Key = String;
    fn extract<T>(&self, request: &Request<T>) -> Result<String, GovernorError> {
        Ok(request
            .headers()
            .get("x-bench-client")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("0")
            .to_owned())
    }
}

fn number(name: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workers = number("WORKERS", 1)?;
    if workers == 0 {
        return Err("WORKERS must be positive".into());
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?
        .block_on(serve())
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let variant = env::var("VARIANT").unwrap_or_else(|_| "head".into());
    let backend = env::var("BACKEND").unwrap_or_else(|_| "memory".into());
    let quota = number("LIMIT", 100)?;
    let window = number("WINDOW", 60)?;
    let config = RateLimitConfig::default()
        .namespace("benchmark")
        .max_requests(quota)
        .window_secs(window as u64);
    config.validate()?;
    let routes = Router::new().route("/", get(|| async { "ok" }));
    let mut app = match (variant.as_str(), backend.as_str()) {
        ("baseline", "memory" | "redis") => routes,
        ("head", "memory") => routes.layer(
            RateLimitLayer::new(config, MemoryStore::new())
                .id(identity)
                .exceeded(|_, _, _| (StatusCode::TOO_MANY_REQUESTS, "limited")),
        ),
        ("head", "redis") => {
            let store = RedisStore::with_options(
                "redis://redis:6379/",
                RedisStoreOptions {
                    timeout: Duration::from_secs(1),
                    failure_policy: FailurePolicy::FailClosed,
                },
            )
            .await?
            .with_prefix("benchmark:");
            routes.layer(
                RateLimitLayer::new(config, store)
                    .id(identity)
                    .exceeded(|_, _, _| (StatusCode::TOO_MANY_REQUESTS, "limited")),
            )
        }
        ("governor", "memory") => {
            if quota == 0 {
                return Err("GCRA has no equivalent zero quota".into());
            }
            let burst = u32::try_from(quota)?;
            let config = GovernorConfigBuilder::default()
                .key_extractor(BenchmarkIdentity)
                .period(Duration::from_secs_f64(window as f64 / quota as f64))
                .burst_size(burst)
                .finish()
                .ok_or("invalid governor quota")?;
            routes.layer(GovernorLayer::new(config))
        }
        ("ratelimit", "memory") => {
            let buckets = Arc::new(TokenBuckets::new(
                quota as u64,
                Duration::from_secs(window as u64),
            )?);
            routes.layer(middleware::from_fn(
                move |request: axum::extract::Request, next: middleware::Next| {
                    let buckets = buckets.clone();
                    async move {
                        let (parts, body) = request.into_parts();
                        if buckets.admit(identity(&parts)) {
                            next.run(Request::from_parts(parts, body)).await
                        } else {
                            (StatusCode::TOO_MANY_REQUESTS, "limited").into_response()
                        }
                    }
                },
            ))
        }
        _ => return Err("unsupported variant/backend combination".into()),
    };
    if env::var("DIAGNOSTICS").as_deref() == Ok("1") {
        use std::sync::atomic::{AtomicU64, Ordering};
        let wakeup = Arc::new(AtomicU64::new(0));
        let monitored = wakeup.clone();
        tokio::spawn(async move {
            loop {
                let due = tokio::time::Instant::now() + Duration::from_millis(5);
                tokio::time::sleep_until(due).await;
                monitored.store(
                    tokio::time::Instant::now()
                        .saturating_duration_since(due)
                        .as_micros() as u64,
                    Ordering::Relaxed,
                );
            }
        });
        app = app.layer(middleware::from_fn(
            move |request: axum::extract::Request, next: middleware::Next| {
                let wakeup = wakeup.clone();
                async move {
                    let start = std::time::Instant::now();
                    let mut response = next.run(request).await;
                    response.headers_mut().insert(
                        "x-bench-service-us",
                        start.elapsed().as_micros().to_string().parse().unwrap(),
                    );
                    response.headers_mut().insert(
                        "x-bench-runtime-us",
                        wakeup.load(Ordering::Relaxed).to_string().parse().unwrap(),
                    );
                    response
                }
            },
        ));
    }
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    eprintln!("variant={variant} backend={backend} limit={quota} window={window}");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_adapter_preserves_burst_and_identity_isolation() {
        let buckets = TokenBuckets::new(2, Duration::from_secs(3600)).unwrap();
        assert!(buckets.admit("a".into()));
        assert!(buckets.admit("a".into()));
        assert!(!buckets.admit("a".into()));
        assert!(buckets.admit("b".into()));
        assert!(buckets.admit("b".into()));
        assert!(!buckets.admit("b".into()));
    }

    #[test]
    fn token_bucket_adapter_rejects_non_equivalent_zero_quota() {
        assert!(TokenBuckets::new(0, Duration::from_secs(60)).is_err());
    }
}
