# Axum-native performance measurements

Run the manual **Axum Performance Benchmarks** workflow after functional CI passes
to collect end-to-end measurements or run in-process microbenchmarks.

## Workloads and variants

- `baseline`: same Axum handler without any limiter; a lower-overhead reference, not another limiter.
- `head`: current local axum-ratelimit source with either memory or Redis.
- `governor`: tower_governor exactly 0.8.0, memory only, separate benchmark workspace.

Each sample creates a fresh server/container. Redis samples also create a disposable
Redis8 container, so no shared database is touched. Docker limits: application2CPUs/512MiB,
Redis1CPU/512MiB. Wrk runs on the host with two load threads/64connections; containers
and load generator still share the runner's hardware. These are not fully isolated cores.
The captured environment, Docker inspect/logs and resource snapshot help identify contention,
OOM and setup failures. Resource snapshots are not peak-memory measurements.

Default cases: (1,allow), (100,allow), (100,deny), (100,mixed), (10000,allow).
Full matrix: 1/100/10000 identities x allow/deny/mixed. Samples are independently restarted,
randomized using seed42 and repeated3times (default10seconds each). Allow uses a very high
quota with a one-second window. Deny uses one request/600seconds and prefills every identity
before timing: it exercises a populated backend, not the special zero-quota shortcut.
Mixed uses100requests/second per identity. GCRA's refill/burst semantics differ from an exact
sliding log, so mixed admission rates are intentionally reported, never treated as equivalent.

Wrk uses a closed-loop workload with status collection enabled for every variant. p50/p95/p99
are in microseconds in JSON and milliseconds in the Markdown report. Counts of200/429 and
unexpected statuses are recorded separately. Transport errors,503/backend failures, unexpected
allow rejections or deny admissions mark a sample failed. They are not throughput wins.
Closed-loop results do not establish open-loop SLO capacity; load-generator saturation and
shared-runner noise may dominate. No broad "faster than" claim is automatically generated.

## Workflow inputs

`compare` runs the comparison matrix; `smoke` runs one-second operational samples;
`micro` explicitly runs in-process memory and Tower service microbenchmarks. Inputs select
full_matrix, duration1..120seconds and repeats1..10. Archives contain raw logs, status counts,
latencies, runtime/environment details, per-sample Docker diagnostics and a failure-first report.
Both backend reports must exist for the overview to be complete. Failed commands propagate a
non-zero workflow result even though reports are uploaded with `always()`.

## Local equivalent

Requires Linux-compatible Docker, wrk, Rust and registry access. macOS measurements are
not equivalent to Linux runners, especially if building a macOS binary for a Linux image:
use a Linux host/CI for this native-binary Docker workflow.

```sh
cargo build --locked --release --manifest-path benchmarks/Cargo.toml
mkdir -p benchmarks/docker-bin
cp benchmarks/target/release/axum-ratelimit-benchmark benchmarks/docker-bin/benchmark-server
docker build -f benchmarks/Dockerfile -t axum-ratelimit-benchmark:local .
python3 benchmarks/harness.py --backend memory --suite smoke
python3 benchmarks/harness.py --backend redis --suite compare --output benchmarks/results/redis
```

The benchmark binary accepts test-controlled `X-Benchmark-Client`; do not expose it to
untrusted clients. Docker publishes it on127.0.0.1 only. The tests in `benchmarks/tests`
exercise parsing/reporting/invalid-results logic without Rust, Redis or performance runs.
Lua protocol reference: https://github.com/wg/wrk/blob/master/SCRIPTING

The isolated benchmark manifest and its lock are not dependencies shipped to crate users.
