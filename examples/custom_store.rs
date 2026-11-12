use axum::{Router, routing::get};
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    store::{RateLimitStore, StoreError},
};
use futures_util::future::BoxFuture;
use std::net::SocketAddr;

// A minimal custom backend. Real backends must atomically
// check AND record admissions. Return metadata only from that same operation.
struct DenyAll;
impl RateLimitStore for DenyAll {
    fn is_limited<'a>(
        &'a self,
        _: &'a str,
        config: &'a RateLimitConfig,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            config.validate().map_err(StoreError::new)?;
            Ok(true)
        })
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let limiter = RateLimitLayer::new(RateLimitConfig::default(), DenyAll).response_headers(true);
    let app = Router::new()
        .route("/", get(|| async { "Not reached" }))
        .layer(limiter);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}
