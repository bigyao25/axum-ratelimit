#!/usr/bin/env python3
"""Axum-only benchmark campaign. All measurements originate from real wrk output."""
from __future__ import annotations
import argparse
import contextlib
import http.client
import json
import os
from pathlib import Path
import platform
import random
import shutil
import statistics
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent
MARKER = "AXUM_BENCH_JSON "


def command(args: list[str], *, timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=timeout, check=False)
    if check and result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {args!r}\n{result.stdout}\n{result.stderr}")
    return result


def parse_wrk(text: str) -> dict:
    lines = [line[len(MARKER):] for line in text.splitlines() if line.startswith(MARKER)]
    if len(lines) != 1:
        raise ValueError("Expected exactly one machine-readable wrk result")
    data = json.loads(lines[0])
    required = ("requests", "duration_us", "rps", "p50_us", "p95_us", "p99_us", "ok", "limited", "other",
                "connect_errors", "read_errors", "write_errors", "timeouts")
    for key in required:
        value = data.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= value < float("inf"):
            raise ValueError(f"Invalid or missing metric: {key}")
    if not data["requests"] or not data["duration_us"]:
        raise ValueError("No measured requests")
    if data["ok"] + data["limited"] + data["other"] != data["requests"]:
        raise ValueError("Status totals do not match measured requests")
    if not data["p50_us"] <= data["p95_us"] <= data["p99_us"]:
        raise ValueError("Latency percentiles are out of order")
    return data


def assess(data: dict, variant: str, mode: str) -> list[str]:
    failures = []
    if sum(data[k] for k in ("connect_errors", "read_errors", "write_errors", "timeouts")):
        failures.append("transport errors/timeouts")
    if data["other"]:
        failures.append("unexpected HTTP statuses (including storage failures)")
    if (variant == "baseline" or mode == "allow") and data["limited"]:
        failures.append("unexpected rejections in allow/baseline workload")
    if variant != "baseline" and mode == "deny" and data["ok"]:
        failures.append("deny workload admitted requests after prefill")
    return failures


def cases(full: bool) -> list[tuple[int, str]]:
    if full:
        return [(clients, mode) for clients in (1, 100, 10000) for mode in ("allow", "deny", "mixed")]
    return [(1, "allow"), (100, "allow"), (100, "deny"), (100, "mixed"), (10000, "allow")]


def http_status(port: int, path: str = "/", client: int = 0) -> int:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        conn.request("GET", path, headers={"X-Benchmark-Client": str(client)})
        response = conn.getresponse()
        response.read()
        return response.status
    finally:
        conn.close()


def wait_ready(port: int, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if http_status(port, "/health") == 200:
                return
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(0.1)
    raise TimeoutError("Benchmark server did not become ready")


def summarize(records: list[dict]) -> str:
    lines = ["# Axum rate-limit benchmark", "", "## Assessment", ""]
    failed = [r for r in records if r.get("status") != "ok"]
    lines += [f"{len(records) - len(failed)}/{len(records)} measurements passed operational checks.",
              "Errors are not reported as performance wins. Redis and memory are separate workloads.",
              "GCRA and exact sliding windows have different admission semantics; compare mixed-mode status rates, not just RPS.",
              "Closed-loop wrk, shared runner, status instrumentation enabled. These are not open-loop latency/SLO capacity measurements.", "",
              "| Backend | Variant | Clients | Mode | Repeat | RPS | p95 ms | p99 ms | 200 | 429 | Status |",
              "|---|---|---:|---|---:|---:|---:|---:|---:|---:|---|"]
    for row in records:
        m = row.get("metrics", {})
        lines.append(f"| {row['backend']} | {row['variant']} | {row['clients']} | {row['mode']} | {row['repeat']} | "
                     f"{m.get('rps', 0):.1f} | {m.get('p95_us', 0) / 1000:.3f} | {m.get('p99_us', 0) / 1000:.3f} | "
                     f"{m.get('ok', 0)} | {m.get('limited', 0)} | {row['status']} |")
    groups: dict[tuple, list[float]] = {}
    for row in records:
        if row["status"] == "ok":
            key = tuple(row[k] for k in ("backend", "variant", "clients", "mode"))
            groups.setdefault(key, []).append(row["metrics"]["rps"])
    lines += ["", "## Repeated-run throughput", ""]
    for key, values in sorted(groups.items()):
        lines.append(f"- {key}: median={statistics.median(values):.1f} RPS; min={min(values):.1f}; max={max(values):.1f}; n={len(values)}.")
    if failed:
        lines += ["", "## Failures", "", *[f"- {r['id']}: {r.get('error', 'unknown error')}" for r in failed]]
    return "\n".join(lines) + "\n"


def write_reports(out: Path, records: list[dict]) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "measurements.json").write_text(json.dumps(records, indent=2) + "\n")
    (out / "assessment.md").write_text(summarize(records))


@contextlib.contextmanager
def server(variant: str, backend: str, mode: str, out: Path, cpus: str, memory: str):
    suffix = uuid.uuid4().hex[:12]
    name, redis_name, network = "axum-bench-" + suffix, "axum-redis-" + suffix, "axum-net-" + suffix
    command(["docker", "network", "create", network])
    try:
        if backend == "redis" and variant != "baseline":
            command(["docker", "run", "-d", "--name", redis_name, "--network", network,
                     "--cpus", "1", "--memory", "512m", "redis:8-alpine",
                     "redis-server", "--save", "", "--appendonly", "no"])
            for _ in range(100):
                ping = command(["docker", "exec", redis_name, "redis-cli", "ping"], check=False)
                if ping.returncode == 0 and "PONG" in ping.stdout:
                    break
                time.sleep(0.1)
            else:
                raise TimeoutError("Redis was not ready")
        command(["docker", "run", "-d", "--name", name, "--network", network,
                 "--cpus", cpus, "--memory", memory, "-p", "127.0.0.1::8080",
                 "-e", f"BENCH_VARIANT={variant}", "-e", f"BENCH_BACKEND={backend}",
                 "-e", f"BENCH_MODE={mode}", "-e", f"BENCH_PREFIX=benchmark:{suffix}:",
                 "-e", f"REDIS_URL=redis://{redis_name}:6379/", "axum-ratelimit-benchmark:local"])
        port = int(command(["docker", "port", name, "8080/tcp"]).stdout.strip().rsplit(":", 1)[1])
        wait_ready(port)
        yield port, name
    finally:
        for container_name in (name, redis_name):
            logs = command(["docker", "logs", container_name], check=False)
            (out / f"{container_name}.log").write_text(logs.stdout + logs.stderr)
            inspect = command(["docker", "inspect", container_name], check=False)
            (out / f"{container_name}.inspect.json").write_text(inspect.stdout or "[]")
            command(["docker", "rm", "-f", container_name], check=False)
        command(["docker", "network", "rm", network], check=False)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--backend", choices=("memory", "redis"), required=True)
    p.add_argument("--suite", choices=("compare", "smoke"), default="compare")
    p.add_argument("--full-matrix", action="store_true")
    p.add_argument("--duration", type=int, default=10)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--cpus", default="2")
    p.add_argument("--memory", default="512m")
    p.add_argument("--connections", type=int, default=64)
    p.add_argument("--output", type=Path, default=ROOT / "benchmarks/results/local")
    args = p.parse_args()
    if not 1 <= args.duration <= 120 or not 1 <= args.repeats <= 10 or not 2 <= args.connections <= 4096:
        p.error("duration=1..120, repeats=1..10, connections=2..4096 required")
    for tool in ("docker", "wrk"):
        if not shutil.which(tool):
            p.error(f"Required executable missing: {tool}")
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    metadata = {"platform": platform.platform(), "cpu_count": os.cpu_count(), "cpus": args.cpus,
                "memory": args.memory, "suite": args.suite, "closed_loop": True,
                "commit": os.environ.get("GITHUB_SHA", "local-uncommitted"),
                "wrk": command(["wrk", "--version"], check=False).stdout,
                "docker": command(["docker", "version"], check=False).stdout}
    (out / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    variants = ["baseline", "head", "governor"] if args.backend == "memory" else ["baseline", "head"]
    matrix = [(v, n, m, repeat) for n, m in cases(args.full_matrix) for v in variants for repeat in range(args.repeats)]
    if args.suite == "smoke":
        matrix = [(v, 4, mode, 0) for v in variants for mode in ("allow", "deny")]
    random.Random(42).shuffle(matrix)
    records = []
    for variant, clients, mode, repeat in matrix:
        row = {"backend": args.backend, "variant": variant, "clients": clients, "mode": mode,
               "repeat": repeat, "id": f"{args.backend}-{variant}-{clients}-{mode}-{repeat}", "status": "error"}
        case_dir = out / row["id"]
        case_dir.mkdir(exist_ok=True)
        try:
            with server(variant, args.backend, mode, case_dir, args.cpus, args.memory) as (port, name):
                if mode == "deny" and variant != "baseline":
                    for client in range(clients):
                        if http_status(port, client=client) != 200:
                            raise RuntimeError("Initial quota prefill did not admit exactly one request")
                    if http_status(port, client=0) != 429:
                        raise RuntimeError("Deny preflight failed")
                duration = 1 if args.suite == "smoke" else args.duration
                # No warm-up request in mixed mode; each sample starts from fresh state.
                run = command(["wrk", "-t2", f"-c{args.connections}", f"-d{duration}s", "--latency",
                               "-s", "benchmarks/load.lua", f"http://127.0.0.1:{port}/", "--", str(clients)],
                              timeout=duration + 30, check=False)
                (case_dir / "wrk.log").write_text(run.stdout + run.stderr)
                if run.returncode:
                    raise RuntimeError(f"wrk exited {run.returncode}; see the preserved wrk.log")
                metrics = parse_wrk(run.stdout)
                row["metrics"] = metrics
                faults = assess(metrics, variant, mode)
                stats = command(["docker", "stats", "--no-stream", "--format", "{{json .}}", name], check=False)
                (case_dir / "resource-snapshot.jsonl").write_text(stats.stdout)
                if faults:
                    raise RuntimeError("; ".join(faults))
                row["status"] = "ok"
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            row["error"] = str(error)
            print(f"FAILED {row['id']}: {error}", file=sys.stderr)
        records.append(row)
        write_reports(out, records)
        print(json.dumps(row), flush=True)
    return int(any(row["status"] != "ok" for row in records))


if __name__ == "__main__":
    raise SystemExit(main())
