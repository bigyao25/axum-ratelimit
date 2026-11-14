# axum-ratelimit

An accurate, high-performance and easy-to-customize rate limiting middleware for **Axum 0.8**, with in-memory and Redis stores.

[![Crates.io](https://img.shields.io/crates/v/axum-ratelimit.svg)](https://crates.io/crates/axum-ratelimit)
[![Documentation](https://docs.rs/axum-ratelimit/badge.svg)](https://docs.rs/axum-ratelimit)
[![CI](https://img.shields.io/github/actions/workflow/status/bigyao25/axum-ratelimit/CI.yml?branch=main)](https://github.com/bigyao25/axum-ratelimit/actions/workflows/CI.yml)
[![Security](https://img.shields.io/github/actions/workflow/status/bigyao25/axum-ratelimit/security.yml?branch=main&label=security)](https://github.com/bigyao25/axum-ratelimit/actions/workflows/security.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[中文文档](README-cn.md)

## Features

- **Accurate sliding window**: A more precise sliding window algorithm that strictly keeps every time window within the quota. See [How It Works](#how-it-works).
- **High performance**: Fine-grained sharded locking with no global admission lock; Redis decides in a single round trip with a warm script cache. End-to-end load tests show throughput on par with, or better than, leading alternatives.
- **Atomic decisions**: Both memory and Redis stores check and record atomically, staying exact across multiple Redis-backed instances.
- **Standard response headers**: Optional IETF draft `RateLimit-Policy` / `RateLimit` plus `Retry-After`, so clients can back off adaptively.
- **Easy to start, easy to customize**: A few lines to integrate; custom client identity, exceeded responses, namespaces and Redis failure policy.
- **Extensible storage**: Built-in memory and Redis stores; implement `RateLimitStore` for your own backend.
- **Observability**: Optional, backend-agnostic event hooks for rate-limit decisions, storage failures, and check latency. See [the example](examples/observability.rs).
- **Well tested and benchmarked**: Boundary, concurrency, multi-instance and Redis 7/8 integration tests; micro-benchmarks plus Docker-based end-to-end load tests. See [benchmarks](benchmarks/README.md).

Requires Rust 1.89 or newer.

## Quick Start

```toml
[dependencies]
axum-ratelimit = "0.1"
axum = "0.8"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net"] }
```

Redis support is enabled by default. For a memory-only build, disable default features:

```toml
[dependencies]
axum-ratelimit = { version = "0.1", default-features = false }
```

## Usage

### Basic Usage with In-Memory Store

```rust,no_run
use axum::{Router, routing::get};
use axum_ratelimit::{RateLimitConfig, RateLimitLayer, store::MemoryStore};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Allow 3 requests per 10-second window.
    let config = RateLimitConfig::default().max_requests(3).window_secs(10);
    let limiter = RateLimitLayer::new(config, MemoryStore::new());

    let app = Router::new()
        .route("/", get(|| async { "Hello world!" }))
        .layer(limiter);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;

    // Provide the peer address to the default client identifier.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}
```

The default identifier is the connection's peer IP without its source port; IPv4-mapped IPv6 addresses are canonicalized. Use `into_make_service_with_connect_info::<SocketAddr>()` as above. Missing connection information returns HTTP 500 without consuming quota. For `Router::oneshot` tests, insert `ConnectInfo<SocketAddr>` into request extensions or configure a custom identifier.

Clones of a layer share its store across requests and runtime threads. To share counters across independently configured layers or routers, create one `Arc<MemoryStore>` and pass clones of that `Arc` to each layer. Separate processes need Redis to share counters. Full example: [examples/simple.rs](examples/simple.rs).

### Advanced Configuration

By default, clients are identified by peer IP. This example uses `.id(...)` to read `X-Client-Id` instead, falling back to `anonymous` when the header is missing or not valid text, and customizes the exceeded response:

```rust
use axum::http::StatusCode;
use axum_ratelimit::{RateLimitConfig, RateLimitLayer, store::MemoryStore};

let config = RateLimitConfig::default()
    .namespace("api")
    .max_requests(3)
    .window_secs(10);

let limiter = RateLimitLayer::new(config, MemoryStore::new())
    .id(|parts| {
        parts.headers.get("X-Client-Id")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("anonymous")
            .to_owned()
    })
    .exceeded(|_id, _config, _parts| {
        (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded")
    });
```

Validate such headers in production so clients cannot bypass limits with forged identifiers. A shared `anonymous` identifier also means a shared quota for all requests without a usable header.

For authenticated routes, read an identity placed in request extensions by authentication middleware. Use `.try_id(...)` when extraction can fail:

```rust
use axum_ratelimit::{IdentityError, RateLimitConfig, RateLimitLayer, store::MemoryStore};

#[derive(Clone)]
struct UserId(u64);

let limiter = RateLimitLayer::new(RateLimitConfig::default(), MemoryStore::new())
    .try_id(|parts| {
        parts.extensions.get::<UserId>()
            .map(|id| format!("user:{}", id.0))
            .ok_or(IdentityError::MissingIdentity)
    });
```

Run authentication before rate limiting. With `ServiceBuilder`, add the authentication layer first and the rate-limit layer second. Missing application identity returns HTTP 400 without touching the store; authentication middleware should reject invalid credentials with HTTP 401/403 itself. Identity and exceeded callbacks may capture values and must be `Send + Sync`.

Full example: [examples/advance.rs](examples/advance.rs). Its `X-Demo-User` middleware demonstrates ordering and extensions; replace it with real authentication in production.

### Quota Response Headers

Response headers are disabled by default. Enable them on the layer:

```rust
use axum_ratelimit::{RateLimitConfig, RateLimitLayer, store::MemoryStore};

let config = RateLimitConfig::default().namespace("api").max_requests(100).window_secs(60);
let limiter = RateLimitLayer::new(config, MemoryStore::new()).response_headers(true);
```

Normal response:

```http
RateLimit-Policy: "api";q=100;w=60
RateLimit: "api";r=42
```

Quota exhausted:

```http
Retry-After: 12
RateLimit-Policy: "api";q=100;w=60
RateLimit: "api";r=0
```

The quota fields use `draft-ietf-httpapi-ratelimit-headers-11`. `Retry-After` is rounded up to seconds and only added to this layer's own HTTP 429 response when a retry delay is known. A downstream 429 or a custom non-429 rejection does not receive an automatic retry field. Existing fields are preserved individually.

The remaining quota is a snapshot after this check, not a reservation. A zero quota has no finite retry delay. Fractional windows omit the optional `w` parameter; invalid policy names or out-of-range Structured Field integers omit the draft quota fields. Custom stores without metadata and Redis fail-open responses do not invent quota fields. Headers and observation never cause a second admission check.

### Redis Store

The `redis` feature is enabled by default. With default features disabled, enable it explicitly:

```toml
[dependencies]
axum-ratelimit = { version = "0.1", default-features = false, features = ["redis"] }
```

Create the store once before serving requests and pass it to `RateLimitLayer::new`:

```rust,no_run
# #[cfg(feature = "redis")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use axum_ratelimit::{RateLimitConfig, RateLimitLayer, store::RedisStore};

let store = RedisStore::new("redis://127.0.0.1:6379/0").await?
    .with_prefix("myapp:ratelimit:");
let limiter = RateLimitLayer::new(RateLimitConfig::default(), store);
# Ok(())
# }
```

Full example: [examples/redis.rs](examples/redis.rs).

`RedisStore::new` uses a one-second timeout and `FailurePolicy::FailOpen`, admitting requests when a runtime Redis check fails. To change the timeout or return HTTP 503 on Redis failures, use `with_options`:

```rust,no_run
# #[cfg(feature = "redis")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use axum_ratelimit::store::{FailurePolicy, RedisStore, RedisStoreOptions};
use std::time::Duration;

let options = RedisStoreOptions {
    timeout: Duration::from_millis(250),
    failure_policy: FailurePolicy::FailClosed,
};
let store = RedisStore::with_options("redis://127.0.0.1:6379/0", options).await?;
# let _ = store;
# Ok(())
# }
```

Notes:

- Initialization errors are always returned, regardless of failure policy.
- Clones share one asynchronous, multiplexed connection that reconnects automatically; the timeout includes reconnect waits.
- When a check times out or is cancelled, the request may already have been counted.
- Redis keys contain the client identifier, so use non-secret internal IDs rather than raw API keys.
- Fail-open admits requests without reliable quota metadata. Choose the failure policy to match the route's availability and quota requirements.

## Configuration

### RateLimitConfig

| Method | Description | Default |
| --- | --- | --- |
| `namespace(name)` | Rule name used to isolate counters | `"default"` |
| `max_requests(usize)` | Maximum requests per window; `0` denies all | 10 |
| `window_secs(u64)` | Window length in seconds; must be positive | 100 seconds |
| `window(Duration)` | Window length with sub-second precision | Same window field |

### RateLimitLayer

| Method | Description | Default |
| --- | --- | --- |
| `id(fn)` | Infallible client identification function | Peer IP through `ConnectInfo<SocketAddr>` |
| `try_id(fn)` / `key_extractor(extractor)` | Fallible identification function or `KeyExtractor` implementation | Same peer-IP extractor |
| `exceeded(fn)` | Limit-exceeded response function | HTTP 429 |
| `response_headers(bool)` | Include quota response fields when metadata is available | Disabled |
| `observe(observer)` | Receive completed check events | Disabled |

- **Behind a proxy**: The default identifier ignores `Forwarded` and `X-Forwarded-For`. Validate your trusted proxy chain and provide the verified identity through a custom extractor or request extensions; otherwise clients share the proxy IP's quota.
- **Counter isolation**: Within one store or Redis prefix, equal namespace, window, quota and client ID share a counter. Different windows or quotas are isolated automatically; routes with equal settings that need independent counters require different namespaces.
- **Validation**: `RateLimitLayer::new` panics on invalid configuration. Use `config.validate()` or `RateLimitLayer::try_new(config, store)` to handle errors. The quota and the window rounded up to microseconds must not exceed `2^53 - 1`.
- **Sub-second windows**: Use `.window(Duration::from_millis(500))` or set the public `window_secs: Duration` field.
- **Layer placement**: `Router::route_layer` limits matched routes, leaving unmatched 404s uncharged. OPTIONS requests are not automatically exempt. Place authentication, CORS and timeout middleware deliberately. The service delegates downstream readiness and forwards downstream errors unchanged; it expects Axum HTTP responses.

### Storage Backends

| | MemoryStore | RedisStore (requires `redis` feature) |
| --- | --- | --- |
| Use case | Single-instance applications | Multi-instance / distributed applications |
| Pros | Very fast, no external dependencies | Counters shared across instances, async I/O |
| Limits | Not shared across processes, cleared on restart | Requires a Redis server |

**MemoryStore cleanup**: While requests arrive, inactive counters are swept at most once per minute. No background task is started; to reclaim memory while idle, call `store.prune_expired()` from an application timer. Its cost scales with the number of counters.

**RedisStore precision**: Window precision is one microsecond, with smaller values rounded up; TTL precision is one millisecond. Persistence, eviction, server clock behavior and failover can affect quota guarantees.

### Custom Store

Implement `RateLimitStore::is_limited`. This minimal backend rejects every request:

```rust
use axum_ratelimit::{RateLimitConfig, store::{RateLimitStore, StoreError}};
use futures_util::future::BoxFuture;

struct DenyAll;

impl RateLimitStore for DenyAll {
    fn is_limited<'a>(
        &'a self,
        _key: &'a str,
        config: &'a RateLimitConfig,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            config.validate().map_err(StoreError::new)?;
            Ok(true)
        })
    }
}
```

- `Ok(true)` invokes the exceeded handler (HTTP 429 by default); `Ok(false)` admits the request; an error produces HTTP 503.
- Checking and recording must be atomic, and network I/O must be asynchronous.
- `Arc`, `Box` and trait objects are supported.
- The default `check` calls `is_limited` once and returns no metadata. Override `check` to return admission and quota metadata from the same atomic operation. Override `check_observed` if your backend needs to report fail-open degradation explicitly.

Full example: [examples/custom_store.rs](examples/custom_store.rs).

### Observability

Attach an observer with `.observe(...)` to receive `Allowed`, `Limited`, `StoreError` and `FailOpen` outcomes, check duration, rule settings and coarse error categories. Events contain no client identity or raw backend error.

```rust
use axum_ratelimit::{
    RateLimitConfig, RateLimitLayer,
    observability::{RateLimitEvent, RateLimitOutcome},
    store::MemoryStore,
};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

let limited = Arc::new(AtomicU64::new(0));
let counter = limited.clone();
let limiter = RateLimitLayer::new(RateLimitConfig::default(), MemoryStore::new())
    .observe(move |event: &RateLimitEvent<'_>| {
        if event.outcome == RateLimitOutcome::Limited {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
```

Callbacks run synchronously after storage locks are released and before the handler. Keep them fast and non-blocking; use atomics or an application-owned bounded queue for asynchronous export. The library starts no exporter or background task. An unwinding panic disables the shared observer; in-flight callbacks may still finish. Keep namespaces low-cardinality and free of sensitive values. Metadata is included only when already requested for response headers.

Full example: [examples/observability.rs](examples/observability.rs).

## How It Works

### Sliding Window Log

Each client has a list of admitted request timestamps. When a request arrives:

1. Extract its identifier and combine it with namespace, window and quota into a counter key.
2. Drop timestamps outside the window `(now - window, now]`.
3. If the remaining count is below the quota, record the current timestamp and admit the request.
4. Otherwise, invoke the exceeded handler, returning HTTP 429 by default.

Rejected requests are never recorded, so they do not extend the window or renew the Redis TTL. Admission is charged before the downstream handler; handler failures, later cancellation or client disconnection do not refund quota.

### Why It Is More Accurate

| Algorithm | Typical issue | This crate |
| --- | --- | --- |
| Fixed window | Can admit nearly 2× the quota around window boundaries | The window slides with every request; no boundary bursts |
| Bucketed / approximate sliding window | Aggregates or weights time buckets, introducing error | Records every timestamp; counts are exact |
| Token bucket / GCRA | Controls average rate and burst, not "at most N in any window" | Directly guarantees at most N requests in any window |

The trade-off is storing up to `max_requests` timestamps per active client, which is small for typical API quotas. There is no global hard cap on the number of identities; choose stable identifiers and account for identity cardinality when sizing storage. Exactness depends on the backend retaining its data and clock/availability assumptions; fail-open intentionally relaxes enforcement during outages.

### Concurrency and Consistency

- **MemoryStore**: DashMap uses sharded locks. The clock is sampled and admission decided while holding the entry lock. Requests for the same key serialize; different keys can contend when they share a shard.
- **RedisStore**: A Lua script atomically prunes, counts and records, using Redis server time and guarding against rollback relative to recorded timestamps. Instances sharing Redis do not rely on their application clocks for admission. A warm script cache normally needs one round trip; `NOSCRIPT` may require loading and retrying the script. Redis failover or lost state can affect enforcement.

## Examples

Run one example at a time; each listens on `127.0.0.1:3000`:

```bash
cargo run --example simple
cargo run --example advance
REDIS_URL=redis://127.0.0.1:6379/ cargo run --example redis
cargo run --example observability
cargo run --example custom_store
```

Verify the limiter:

```bash
# In the simple example, the fourth request within 10 seconds returns 429.
for i in {1..5}; do curl -s -w " %{http_code}\n" http://127.0.0.1:3000/; done

# The advance example requires a positive numeric X-Demo-User.
for i in {1..5}; do curl -s -w " %{http_code}\n" -H "X-Demo-User: 42" http://127.0.0.1:3000/; done
```

The custom-store example always rejects requests; the observability example exports counters to stdout from an application-owned task.

## Tests and Benchmarks

```bash
# Unit, integration and example tests, without running microbenchmarks.
cargo test --locked --lib --tests --examples
cargo test --locked --lib --tests --examples --no-default-features

# Doc tests for both feature configurations.
cargo test --locked --doc
cargo test --locked --doc --no-default-features

# Real Redis tests, including reconnection and transport deadlines.
REDIS_URL=redis://127.0.0.1:6379/ cargo test --locked --lib --tests --all-features -- --include-ignored

# Explicit performance runs.
cargo bench --locked --bench memory --no-default-features
cargo bench --locked --bench middleware --no-default-features
```

Redis-dependent tests are ignored by default. They use unique test prefixes and never flush the database; use a disposable Redis instance. Microbenchmarks measure per-check cost, not HTTP throughput. See [benchmarks/README.md](benchmarks/README.md) for end-to-end load tests and the manual benchmark workflow.

## Cargo Features

- `redis`: Enables the Redis storage backend (enabled by default).

## License

This project is licensed under the [MIT License](LICENSE).

## Provenance

Ported from [bigyao25/actix-web-ratelimit](https://github.com/bigyao25/actix-web-ratelimit) at commit `a90c5b1befbb3b5d603582b3eb448159ac5d2aac`, crate 0.2.3. The storage implementations, Lua script and license retain that baseline.
