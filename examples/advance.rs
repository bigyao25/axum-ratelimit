//! Demonstrates trusted request extensions and middleware ordering.
//! X-Demo-User is NOT authentication; replace the demo layer with real auth.
use axum::{
    Router,
    extract::Request,
    middleware::{Next, from_fn},
    response::{IntoResponse, Response},
    routing::get,
};
use axum_ratelimit::{IdentityError, RateLimitConfig, RateLimitLayer, store::MemoryStore};
use http::StatusCode;
use tower::ServiceBuilder;

#[derive(Clone)]
struct UserId(u64);

async fn demo_identity(mut request: Request, next: Next) -> Response {
    let id = request
        .headers()
        .get("x-demo-user")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|id| *id > 0);
    let Some(id) = id else {
        return (StatusCode::UNAUTHORIZED, "Provide a positive X-Demo-User").into_response();
    };
    request.extensions_mut().insert(UserId(id));
    next.run(request).await
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let limiter = RateLimitLayer::new(
        RateLimitConfig::default()
            .namespace("private-api")
            .max_requests(3)
            .window_secs(10),
        MemoryStore::new(),
    )
    .try_id(|parts| {
        parts
            .extensions
            .get::<UserId>()
            .map(|id| format!("user:{}", id.0))
            .ok_or(IdentityError::MissingIdentity)
    })
    .exceeded(|_, _, _| {
        (
            StatusCode::TOO_MANY_REQUESTS,
            "Quota exhausted; retry later",
        )
    })
    .response_headers(true);
    let app = Router::new()
        .route("/", get(|| async { "Authenticated route" }))
        .layer(
            ServiceBuilder::new()
                .layer(from_fn(demo_identity))
                .layer(limiter),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, app).await
}
