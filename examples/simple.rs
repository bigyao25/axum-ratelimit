use axum::{Router, routing::get};
use axum_ratelimit::{RateLimitConfig, RateLimitLayer, store::MemoryStore};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let limiter = RateLimitLayer::new(
        RateLimitConfig::default().max_requests(3).window_secs(10),
        MemoryStore::new(),
    )
    .response_headers(true);
    let app = Router::new()
        .route("/", get(|| async { "Hello world!" }))
        .layer(limiter);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}
