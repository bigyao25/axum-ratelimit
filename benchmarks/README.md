# Axum high-load benchmarks

The manual **Axum High-load Benchmarks** workflow compares the current crate with
an unmodified Axum handler, `tower_governor` and `ratelimit`. It follows the same bounded
probe/confirmation method and measurement-quality requirements as the
[Actix benchmark workflow](https://github.com/bigyao25/actix-web-ratelimit/blob/a90c5b1befbb3b5d603582b3eb448159ac5d2aac/.github/workflows/benchmark.yml).
It never publishes a crate, changes repository permissions or posts comments.

## Products and comparable settings

| Variant | Framework | Backends | Policy |
| --- | --- | --- | --- |
| `baseline` | Axum 0.8.9 / Hyper 1.12.0 | Memory, Redis labels | Same handler, no limiter |
| `head` | Same shared binary | Memory, Redis | Exact admitted-request sliding log |
| `governor` | Same shared binary | Memory | tower_governor 0.8.0, GCRA |
| `ratelimit` | Same shared binary | Memory | ratelimit 2.0.0, token bucket with keyed Axum adapter |

All variants use the same locked benchmark workspace, Tokio worker count, `ok`
handler, `X-Bench-Client` identity and HTTP/1.1 connection handling. The binary is
built once per backend runner and tagged for each variant. Limiter defaults and
rejection responses are product behavior; quota headers and observation are off.
GCRA and token bucket are not rewritten into exact sliding windows. They receive
the same nominal quota/window, with continuous refill and their own burst behavior.
The token bucket starts full, has capacity equal to quota, and refills quota tokens
per window. Its benchmark-only Axum middleware uses DashMap 6.2.1 for per-client
buckets; all adapter and identity-lookup costs are included. This measures an
integrated rate-limiting core rather than a native HTTP middleware product.
Zero quota is unsupported for both peers (upstream ratelimit interprets zero as
unlimited). Rejection never sleeps or retries. There is no
Redis competitor in this comparison. No Actix-only adapters are included.

## Full matrix and execution budget

Select `suite=compare`, `full_matrix=true` (the default) on the intended branch.
Each backend stays on one runner for its entire campaign; at most two backend
jobs run in parallel. Products or repetitions are never split across machines.

The full matrix is **1 / 100 / 10,000 measured identities × allow / mixed / reject**.
The identity count is separate from the bounded concurrency ceiling of 512 slots.
`suite=micro` retains the explicit in-process memory/middleware microbenchmarks;
`suite=smoke` is a short operational check.

The optional representative matrix contains five cases: 1/100/10,000 allow,
100 mixed and 100 reject.

1. Calibrate generator headroom, then probe Memory at 45k/60k or Redis at 10k/16k
   offered RPS with 2-second warmup, 5-second measurement and one repetition.
2. Refine adjacent passing/failing candidates using at most three extra rates.
   Target gaps are 5k RPS for Memory and 2k for Redis. Reaching the refinement
   budget does not imply reaching the requested resolution. If all clean baseline
   probes fail, halve the lowest failing baseline rate (1k RPS floor) before
   refining product bounds. These downshifts share the same three-probe budget;
   invalid measurements never become failing bounds.
3. Confirm at most HEAD's passing/failing endpoints, always with baseline and all
   compatible peers: **5-second warmup, 30-second measurement, five independent
   repetitions**. Alternate product order across repetitions.
4. Survey each remaining identity/response case at a conservative shared rate.
   Allow at most one lower-rate fallback; insufficiently exercised or invalid
   peer cases are explicit exclusions, not winners. Missing baseline/HEAD
   coverage fails the campaign.
5. Confirm eligible cases with the same five-repeat requirements. High-cardinality
   rejection surveys can extend within 60 seconds to exercise the intended mix.

For the full matrix, the confirmation upper bound is 200 Memory rounds / 7,000
seconds of load and 100 Redis rounds / 3,500 seconds of load. These exclude probes,
builds, preflight/recovery and container orchestration. The workflow timeout is
240 minutes for full coverage and 180 minutes for representative coverage.
`--plan` previews bounds without building images or starting Docker.

Builds are reused between phases only after verifying commit, tracked source
hashes, build settings, image IDs and native architecture. Formal runs require a
clean committed tree. Bounded scheduler-loss recovery retains failed diagnostics
and lowers offered rates instead of relaxing correctness or qualification criteria.
Recovery and scenario fallback use a 1k RPS floor, so a low calibrated range is
never raised back to 5k RPS.

## Selected backends and targeted follow-ups

The workflow `backends` input selects `both` (default), `memory` or `redis`.
Separate Memory and Redis workflow runs do not cancel each other. All products
and repetitions for one backend remain on the same runner.

For a follow-up, select one backend and pass space-separated `targets`, for example
`1:allow:20000 10000:mixed:15000`. This overrides the full-matrix switch and skips
capacity search, then runs fresh same-host baseline/HEAD surveys, long preflight
and all five confirmations for each selected case. Surveys may fall back once;
generator recovery remains bounded. Reports show the actual tested rate. No
measurements, passing rates or build manifests are imported from earlier runs.
Other matrix cells are explicitly labeled as outside this follow-up's scope.

```bash
python3 benchmarks/campaign.py --backend memory --full-matrix --output benchmarks/results/memory
python3 benchmarks/campaign.py --backend redis --targets 1:allow:20000 10000:mixed:15000 --output benchmarks/results/redis-followup
```

The overview separates execution from qualification: every requested scenario
has a paired confirmation table, including passing/recorded repeat counts for
HEAD and baseline, unqualified levels and missing cases. `qualification.json`
contains the same evidence in machine-readable form. Five clean paired repeats
of at least 30 seconds qualify a level; four passing repeats do not. A successful
workflow can therefore finish with qualification warnings. Earlier failed
attempts stay in their original artifacts and are never merged into a new run.

Set `report_run` to an existing run ID to regenerate its overview without
starting any benchmark containers. Select the original backend scope. Reports
retain the original measurement commit and raw data, and record the new report
generator commit separately. This is useful when a report bug is fixed after a
long campaign. Report-only runs do not cancel active measurement runs.

## Queue diagnostics and client method

The shared benchmark server explicitly enables TCP_NODELAY for every accepted
socket and every product. Axum 0.8.9 leaves this socket option at the OS default;
Nagle plus delayed ACK can add transport latency outside the limiter/handler.
CLI `--server-nagle` retains the former setting for the same-host A/B experiment.
This is server harness configuration, not a change to the rate-limiting crate.
[Axum listener configuration](https://docs.rs/axum/0.8.9/axum/serve/trait.ListenerExt.html)


Select `suite=diagnose` and either or both backends to run controlled single-client
allow experiments at 35k Memory / 20k Redis RPS. The same runner and images run
baseline and HEAD with four profiles: legacy drop/cold pool, drop/warm pool, bounded
admission/warm pool, and the same client with server TCP_NODELAY enabled.
Profile and product order reverse across repeat blocks. Each uses three 30-second repetitions and 2-second
warmup. Diagnostic response headers and runtime probes are enabled for all peers
in this suite, so these measurements are separate from formal uninstrumented
performance comparisons. `diagnosis.json` and the overview summarize each profile.

The measurement arrival epoch starts after histogram/counter setup (recorded as
`setup_ms`), so allocation work does not create a fictitious initial backlog.
The native generator now reuses HTTP connections from warmup while preserving
separate client identities and per-phase counters. One pending admission can wait
for a concurrency slot until 100 ms after its original scheduled arrival. Due
times and total intended arrivals never change: waiting contributes to scheduled
latency, expiry remains a dropped request, and sustained overload still fails the
existing latency/completion/no-drop gates. The 512-slot ceiling is unchanged;
written requests are never replayed. CLI `--admission-policy drop` and `--cold-pool`
retain the former behavior for controlled experiments.

New histograms expose p99.9, p99.99 and exact maximum as well as p99. Bounded slow
request examples and drop events locate admission, executor, connection-pool,
connect, response and server-service delays. Diagnostic-only wakeup probes help
separate runtime scheduling from handler/middleware/Redis waits. Host CPU/pressure
snapshots and container throttling counters remain supporting evidence, not proof
of a specific cause. Historical and new admission/pool/instrumentation profiles
are never pooled into one comparison.

Product scorecards show every recorded repeat, achieved RPS, request p99, server
CPU and response mix at a common offered load. CPU/HEAD ratios require qualified
matched workloads; the separate paired intervals retain statistical/noise gates.
The scorecard provides readable observations even when no statistical winner is
established. Failed repeats and semantically different mixes remain visible.

## Resource isolation and request accounting

Native Rust generator: **2 CPUs / 512 MiB**, server: **1 CPU / 1 GiB**, Redis:
**1 CPU / 512 MiB**, on disjoint CPU sets 0–1, 2 and 3. Four native Docker CPUs,
4 GiB RAM and cgroup v2 are required. Limits and effective CPU sets are checked;
unsupported capabilities or emulated image architectures fail explicitly.

Redis uses the official `redis:8-alpine` image, 256 MiB maxmemory/noeviction and no
persistence. Container inspect data records the actual image identity. Every
round recreates server/Redis state on an internal Compose network with no host
ports. Only this run's containers/networks are removed; shared images/caches and
other workloads are retained. A checkout lock prevents overlapping local runs.

The generator uses bounded open-loop scheduling and persistent pooled HTTP/1.1
connections. It records target, attempted and completed RPS, reserved/queued/active
requests, dispatch lag, sent and scheduled latency, response-status distributions,
transport categories and capacity/scheduler drops. Requests are never replayed
after being written. Warmup uses separate identities.

Measurement windows are longer than a round, so exact-log tests assert a
first-window burst oracle: admitted count equals `min(attempts, quota)` per
identity. This is not a complete rolling-window proof; production regression
tests cover expiration/boundaries. Mixed requires 20–80% rejection, positive-quota
reject requires at least 90%, and every requested identity must be observed.
Baseline always permits requests. GCRA and token-bucket observations retain their own semantics.

## Qualification and reports

A tested load qualifies only with **five clean 30-second confirmations**,
HTTP completion at least 99% of offered rate, scheduled p99 at most 10 ms, no
capacity/scheduler drops, no unexpected HTTP/transport/correctness errors, and
generator CPU below 85% of its quota. The latency budget is a diagnostic threshold,
not a universal application SLO. Clean overload is preserved as load failure;
corrupted or generator-limited measurements never establish a server limit.

A sustained-load winner needs all confirmations to pass where the peer fails
load criteria in every repeat. At jointly qualified levels, latency/CPU cost
verdicts use approximate 95% t intervals on five paired log ratios, beyond the
larger of 5% and matching baseline A/A spread. Mixed/reject rejection fractions
must agree within two percentage points for directional cost verdicts. Missing
calibration, resource capture skew or unmatched settings yields `inconclusive`.
These are workload-scoped observations, not a universal fastest-crate ranking.

The native generator pauses at measurement boundaries while the controller reads
server/Redis cgroups. CPU excludes warmup; capture skew remains explicit. Container
memory peak includes its entire lifetime; RSS and Redis bytes are snapshots and
are not ranked. CPU model, runner identity and resource layout accompany results.
Memory and Redis shards are never pooled or directly ranked across runners.

Each backend artifact contains `assessment.md`, `report.md`, `raw.json`,
`validated.json`, `verdicts.json`, `campaign-plan.json`, result classification,
environment/build manifests, lockfiles, per-phase diagnostics and `run.log`.
`benchmark-overview` indexes the separate backend reports. Missing/failed shards
fail the overview; runner-limited evidence remains explicitly inconclusive.
Artifacts are retained for 14 days, including after failure.

## Local commands

Run from the repository root. Python 3.11+ and Docker Compose are required.
Only short unit/protocol tests are part of ordinary CI; long measurements are
manual. Local Apple Silicon results cannot be pooled with hosted x64 numbers.

```sh
python3 -W error::ResourceWarning -m unittest discover -s benchmarks/tests -v
cargo test --locked --manifest-path benchmarks/native-loadgen/Cargo.toml
python3 benchmarks/campaign.py --backend memory --full-matrix --output benchmarks/results/memory --plan
python3 benchmarks/campaign.py --backend memory --full-matrix --output benchmarks/results/memory
python3 benchmarks/campaign.py --backend redis --full-matrix --output benchmarks/results/redis
# Short operational checks do not qualify performance results.
python3 benchmarks/bench.py smoke --resources ci --variants baseline head governor ratelimit --allow-dirty
python3 benchmarks/campaign.py --backend memory --check --allow-dirty --output benchmarks/results/check
```
