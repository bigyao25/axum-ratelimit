# axum-ratelimit

一个精确、高性能且易于定制的 **Axum 0.8** 限流中间件，支持内存和 Redis 存储。

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

[English Documentation](README.md)

## 特性

- **精确的滑动窗口**：采用更精准的滑动窗口算法，严格保证任意时间窗口内不超过限额。参见[运行机制](#运行机制)。
- **高性能**：采用细粒度分片锁，无全局判定锁；Redis 在脚本缓存命中时单次往返即完成判定。端到端压测中吞吐表现优异。
- **原子判定**：内存与 Redis 存储均原子执行判定和记录，Redis 多实例部署同样精确。
- **标准响应头**：可选输出 IETF 草案 `RateLimit-Policy` / `RateLimit` 及 `Retry-After`，便于客户端退避重试。
- **开箱即用，按需定制**：几行代码即可接入；支持自定义客户端标识、超限响应、命名空间和 Redis 故障策略。
- **可扩展存储**：内置内存和 Redis 存储，实现 `RateLimitStore` 即可接入自定义后端。
- **可观测性**：可选、平台无关的事件接口，覆盖限流判定、存储故障和检查耗时。参见[示例](examples/observability.rs)。
- **测试与基准完备**：覆盖边界、并发、Tower readiness、真实 HTTP、多实例及 Redis 7/8 集成测试；提供微基准和基于 Docker 的端到端压测。参见 [benchmarks](benchmarks/README.md)。

需要 Rust 1.89 或更新版本。

## 快速开始

首次发布到 crates.io 之前，通过本地源码依赖接入，路径按实际位置调整：

```toml
[dependencies]
axum-ratelimit = { path = "../axum-ratelimit" }
axum = "0.8"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "net"] }
```

Redis 支持默认启用。仅使用内存存储时，可关闭默认特性：

```toml
[dependencies]
axum-ratelimit = { path = "../axum-ratelimit", default-features = false }
```

`0.1.0` 发布后，可以将路径依赖替换为 `axum-ratelimit = "0.1"`。

## 使用方法

### 基础用法（内存存储）

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

默认按连接对端 IP 限流，忽略源端口，并将 IPv4-mapped IPv6 地址归一化。需要像上例一样使用 `into_make_service_with_connect_info::<SocketAddr>()`；缺少连接信息时返回 HTTP 500，不消耗额度。通过 `Router::oneshot` 测试时，应向请求 Extensions 插入 `ConnectInfo<SocketAddr>`，或配置自定义标识函数。

同一 Layer 的克隆在请求和运行时线程之间共享 Store。分别配置的多个 Layer 或 Router 需要共享计数时，应创建一个 `Arc<MemoryStore>`，将它的克隆分别传入各层。跨进程共享额度需要 Redis。完整代码见 [examples/simple.rs](examples/simple.rs)。

### 高级配置

默认按连接对端 IP 限流。下例通过 `.id(...)` 改用 `X-Client-Id` 请求头作为客户端标识，缺失或非有效文本时使用 `anonymous`，并自定义超限响应：

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

生产环境应先校验此类请求头，避免客户端伪造标识绕过限流。使用统一的 `anonymous` 也意味着所有缺少有效请求头的请求共享额度。

对于需要认证的路由，建议读取认证中间件写入 Extensions 的可信身份。身份提取可能失败时，使用 `.try_id(...)`：

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

认证应在限流之前执行。使用 `ServiceBuilder` 时，先添加认证层，再添加限流层。缺少应用身份时返回 HTTP 400，不访问 Store；无效凭据应由认证中间件自行返回 HTTP 401/403。标识提取与超限回调支持捕获变量，且必须满足 `Send + Sync`。

完整代码见 [examples/advance.rs](examples/advance.rs)。其中的 `X-Demo-User` 中间件用于演示顺序和 Extensions，生产环境需替换为真实认证。

### 额度响应头

响应头默认关闭，在 Layer 上开启：

```rust
use axum_ratelimit::{RateLimitConfig, RateLimitLayer, store::MemoryStore};

let config = RateLimitConfig::default().namespace("api").max_requests(100).window_secs(60);
let limiter = RateLimitLayer::new(config, MemoryStore::new()).response_headers(true);
```

正常响应：

```http
RateLimit-Policy: "api";q=100;w=60
RateLimit: "api";r=42
```

额度耗尽时：

```http
Retry-After: 12
RateLimit-Policy: "api";q=100;w=60
RateLimit: "api";r=0
```

额度字段采用 `draft-ietf-httpapi-ratelimit-headers-11` 格式。`Retry-After` 向上取整到秒，仅在本层自身返回 HTTP 429 且恢复时间已知时添加；不会给下游返回的 429 或自定义非 429 超限响应自动添加。已有字段逐项保留。

剩余额度是本次检查之后的快照，不是为后续请求预留的额度。零限额没有有限恢复时间。亚秒窗口省略可选的 `w` 参数；策略名不合法或整数超出 Structured Fields 范围时省略草案额度字段。自定义 Store 未提供元数据，或 Redis 故障后按 FailOpen 放行时，不会虚构额度字段。响应头和观察回调都不会引发第二次计数。

### Redis 存储

`redis` 特性默认启用。关闭默认特性时，可显式启用：

```toml
[dependencies]
axum-ratelimit = { path = "../axum-ratelimit", default-features = false, features = ["redis"] }
```

在开始处理请求前创建一次 Store，并传入 `RateLimitLayer::new`：

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

完整代码见 [examples/redis.rs](examples/redis.rs)。

`RedisStore::new` 默认使用 1 秒超时和 `FailurePolicy::FailOpen`，运行时 Redis 检查失败时放行。如需调整超时，或在 Redis 故障时返回 HTTP 503，使用 `with_options`：

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

注意事项：

- 初始化失败始终返回错误，不受故障策略影响。
- 克隆的 Store 共享同一异步多路复用连接，断线后自动重连；超时时间包含重连等待。
- 检查超时或取消时，本次请求可能已被计入额度。
- Redis 键包含客户端标识，应使用非敏感的内部 ID，不要直接使用原始 API Key。
- FailOpen 放行时没有可靠额度元数据。应根据路由对可用性和额度约束的要求选择故障策略。

## 配置选项

### RateLimitConfig

| 方法 | 描述 | 默认值 |
| --- | --- | --- |
| `namespace(name)` | 规则名称，用于隔离计数 | `"default"` |
| `max_requests(usize)` | 窗口内最大请求数；`0` 表示全部拒绝 | 10 |
| `window_secs(u64)` | 窗口长度（秒），必须大于零 | 100 秒 |
| `window(Duration)` | 支持亚秒精度的窗口长度 | 同一窗口字段 |

### RateLimitLayer

| 方法 | 描述 | 默认值 |
| --- | --- | --- |
| `id(fn)` | 不会失败的客户端标识函数 | 通过 `ConnectInfo<SocketAddr>` 读取对端 IP |
| `try_id(fn)` / `key_extractor(extractor)` | 可失败的标识函数或 `KeyExtractor` 实现 | 同一对端 IP 提取器 |
| `exceeded(fn)` | 超限响应函数 | HTTP 429 |
| `response_headers(bool)` | 有元数据时输出额度响应字段 | 关闭 |
| `observe(observer)` | 接收已完成检查的事件 | 关闭 |

- **代理场景**：默认标识忽略 `Forwarded` 和 `X-Forwarded-For`。应验证可信代理链，通过自定义提取器或 Extensions 提供校验后的身份，否则客户端会共享代理 IP 的额度。
- **计数隔离**：同一 Store 或 Redis 前缀内，命名空间、窗口、限额和客户端标识均相同的规则共享计数。窗口或限额不同的规则自动隔离；配置相同但需独立计数的路由，应使用不同命名空间。
- **配置校验**：`RateLimitLayer::new` 在配置无效时 panic。如需自行处理，使用 `config.validate()` 或 `RateLimitLayer::try_new(config, store)`。限额和向上取整到微秒的窗口均不得超过 `2^53 - 1`。
- **亚秒窗口**：使用 `.window(Duration::from_millis(500))`，或直接设置公开字段 `window_secs: Duration`。
- **层的放置**：`Router::route_layer` 仅限制匹配路由，不匹配的 404 不计数。OPTIONS 请求不会自动豁免。需明确认证、CORS 和超时层的顺序。Service 向下游传递 readiness，原样传播下游错误，响应类型使用 Axum HTTP Response。

### 存储后端

| | MemoryStore | RedisStore（需 `redis` 特性） |
| --- | --- | --- |
| 适用场景 | 单实例应用 | 多实例 / 分布式应用 |
| 优点 | 极快，无外部依赖 | 多实例共享计数，异步访问 |
| 限制 | 不跨进程共享，重启后清空 | 需要 Redis 服务 |

**MemoryStore 清理**：有请求到达时，最多每分钟扫描一次并回收不活跃的计数。Store 不会自动启动后台任务；如需在空闲时回收，可在应用定时器中调用 `store.prune_expired()`，其开销与计数键数量成正比。

**RedisStore 精度**：窗口精度为微秒，更小的值向上取整；TTL 精度为毫秒。持久化、驱逐、服务端时钟和故障切换可能影响额度保证。

### 自定义 Store

实现 `RateLimitStore::is_limited` 即可。以下最小后端拒绝所有请求：

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

- `Ok(true)` 调用超限响应函数，默认 HTTP 429；`Ok(false)` 放行；返回错误时响应 HTTP 503。
- 判定与记录必须原子执行，网络 I/O 必须异步。
- 支持通过 `Arc`、`Box` 及 trait object 使用。
- 默认 `check` 调用一次 `is_limited`，不提供元数据。如需返回额度信息，应重写 `check`，在同一次原子操作中完成放行和元数据获取。如需显式报告 FailOpen 降级状态，可重写 `check_observed`。

完整代码见 [examples/custom_store.rs](examples/custom_store.rs)。

### 可观测性

通过 `.observe(...)` 接收 `Allowed`、`Limited`、`StoreError`、`FailOpen` 事件，以及检查耗时、规则设置和粗粒度错误类别。事件不包含客户端身份或原始后端错误。

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

回调在存储锁释放后、业务处理前同步执行，应保持快速且不阻塞；异步导出可使用原子计数或应用自行管理的有界队列。库不启动导出器或后台任务。可展开 panic 会禁用共享观察器，正在执行的回调可能继续完成。命名空间应保持低基数且不包含敏感信息。只有响应头已经请求元数据时，事件才会携带这些元数据。

完整代码见 [examples/observability.rs](examples/observability.rs)。

## 运行机制

### 滑动窗口日志

每个客户端维护一份已放行请求的时间戳列表。请求到达时：

1. 提取客户端标识，与命名空间、窗口、限额组合成计数键。
2. 移除窗口 `(now - window, now]` 之外的时间戳。
3. 剩余数量未达限额时，记录当前时间戳并放行。
4. 否则调用超限响应函数，默认返回 HTTP 429。

被拒绝的请求不会被记录，因此不会延长窗口，也不会续期 Redis TTL。放行发生在下游处理之前；业务失败、后续取消或客户端断开不会退还额度。

### 为什么更精确

| 算法 | 典型问题 | 本库 |
| --- | --- | --- |
| 固定窗口 | 两个窗口交界处可在短时间内放行近 2 倍限额 | 窗口随每个请求连续滑动，无边界突刺 |
| 分桶 / 近似滑动窗口 | 按时间桶聚合或加权估算，存在误差 | 逐条记录时间戳，计数精确 |
| 令牌桶 / GCRA | 控制的是平均速率与突发量，而非“任意窗口内最多 N 次” | 直接保证任意窗口内不超过 N 次 |

代价是每个活跃客户端最多保存 `max_requests` 个时间戳，对常见的 API 限额而言开销很小。身份数量没有全局硬上限，应选择稳定标识，并在容量规划中考虑身份基数。精确性依赖后端保留数据以及时钟和可用性条件；FailOpen 会在故障期间主动放宽限制。

### 并发与一致性

- **MemoryStore**：DashMap 使用分片锁，持有条目锁时读取时钟并完成判定。同一键的请求串行执行，不同键落入同一分片时也可能竞争。
- **RedisStore**：Lua 脚本原子完成清理、计数和记录，使用 Redis 服务端时间，并相对已记录时间戳防止回拨。共享 Redis 的实例无需依赖各自应用时钟判定。脚本已缓存时通常只需一次往返；`NOSCRIPT` 可能需要加载脚本后重试。Redis 故障切换或状态丢失可能影响约束。

## 示例

每次运行一个示例，均监听 `127.0.0.1:3000`：

```bash
cargo run --example simple
cargo run --example advance
REDIS_URL=redis://127.0.0.1:6379/ cargo run --example redis
cargo run --example observability
cargo run --example custom_store
```

验证限流效果：

```bash
# In the simple example, the fourth request within 10 seconds returns 429.
for i in {1..5}; do curl -s -w " %{http_code}\n" http://127.0.0.1:3000/; done

# The advance example requires a positive numeric X-Demo-User.
for i in {1..5}; do curl -s -w " %{http_code}\n" -H "X-Demo-User: 42" http://127.0.0.1:3000/; done
```

自定义 Store 示例始终拒绝请求；可观测性示例通过应用自行管理的任务向标准输出导出计数。

## 测试与基准

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

依赖真实 Redis 的测试默认忽略，使用独立测试前缀，不会清空数据库；建议使用临时 Redis 实例。微基准衡量单次检查开销，不代表 HTTP 吞吐。端到端压测和手动性能 workflow 见 [benchmarks/README.md](benchmarks/README.md)。

## Cargo 特性

- `redis`：启用 Redis 存储后端，默认启用。

## 许可证

本项目基于 [MIT 许可证](LICENSE) 发布。

## 基线说明

移植自 [bigyao25/actix-web-ratelimit](https://github.com/bigyao25/actix-web-ratelimit) 的提交 `a90c5b1befbb3b5d603582b3eb448159ac5d2aac`，对应 crate 0.2.3。存储实现、Lua 脚本和许可证保留该基线。
