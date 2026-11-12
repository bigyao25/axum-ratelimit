use axum::{Router, routing::get};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    store::{FailurePolicy, RedisStore, RedisStoreOptions},
};
use std::{net::SocketAddr, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("REDIS_URL")?;
    let store = RedisStore::with_options(
        &url,
        RedisStoreOptions {
            timeout: Duration::from_millis(250),
            failure_policy: FailurePolicy::FailClosed,
        },
    )
    .await?
    .with_prefix("myapp:ratelimit:");
    let limiter = RateLimitLayer::new(
        RateLimitConfig::default()
            .namespace("api")
            .max_requests(10)
            .window_secs(60),
        store,
    )
    .response_headers(true);
    let app = Router::new()
        .route("/", get(|| async { "Shared Redis quota" }))
        .layer(limiter);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
