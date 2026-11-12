//! Exact sliding-window rate limiting for Axum 0.8, built on Tower.
//!
//! The in-memory backend is process-local. The optional (default-enabled) Redis
//! backend shares atomic quotas across processes. Requests admitted into the
//! half-open window `(now - window, now]` are recorded; rejections are not.
//!
//! ```rust,no_run
//! use axum::{routing::get, Router};
//! use axum_ratelimit::{RateLimitConfig, RateLimitLayer, store::MemoryStore};
//! use std::net::SocketAddr;
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     let layer = RateLimitLayer::new(
//!         RateLimitConfig::default().max_requests(100).window_secs(60),
//!         MemoryStore::new(),
//!     ).response_headers(true);
//!     let app = Router::new().route("/", get(|| async { "Hello" })).layer(layer);
//!     let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
//!     axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await
//! }
//! ```
//!
//! The default extractor requires `ConnectInfo<SocketAddr>` and never trusts
//! forwarding headers. Missing connection information is HTTP 500, not an
//! anonymous shared bucket. Configure [`RateLimitLayer::id`] or
//! [`RateLimitLayer::try_id`] to read trusted authentication extensions.
//!
//! Storage errors are HTTP 503. Quota exhaustion invokes a configurable HTTP
//! response (429 by default). Only downstream service errors propagate as Tower
//! errors. This is an Axum-first HTTP layer, not an unqualified gRPC adapter.

pub mod config;
pub mod extractor;
mod headers;
mod middleware;
pub mod observability;
pub mod store;

pub use config::{ConfigError, RateLimitConfig};
pub use extractor::{IdentityError, KeyExtractor, PeerIpKeyExtractor};
pub use middleware::{RateLimitLayer, RateLimitService};

#[cfg(doctest)]
mod readme_examples {
    #[doc = include_str!("../README.md")]
    struct English;

    #[doc = include_str!("../README-cn.md")]
    struct Chinese;
}
