#!/usr/bin/env python3
"""Fast functional validation, never runs performance benchmarks or publishes."""
from __future__ import annotations
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent


def run(*args: str, env: dict | None = None) -> None:
    print("+", " ".join(args), flush=True)
    subprocess.run(args, cwd=ROOT, check=True, env=env)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--static", action="store_true", help="Only Python/Lua checks; NOT Rust validation")
    parser.add_argument("--redis", action="store_true", help="Run real Redis tests against REDIS_URL")
    args = parser.parse_args()
    run(sys.executable, "-W", "error::ResourceWarning", "-m", "unittest", "discover", "-s", "benchmarks/tests", "-v")
    run(sys.executable, "-m", "unittest", "discover", "-s", "scripts/tests", "-v")
    lua = next((path for exe in ("lua5.1", "lua", "texlua") if (path := shutil.which(exe))), None)
    if lua:
        run(lua, "scripts/test_sliding_window.lua")
    else:
        print("SKIPPED: standalone Lua model tests (no Lua interpreter)", file=sys.stderr)
    if args.static:
        print("Static/tooling checks only. Rust compilation and integration tests were NOT run.")
        return 0
    if not shutil.which("cargo"):
        print("ERROR: cargo unavailable. Rust validation NOT run.", file=sys.stderr)
        return 2
    for lock in ("Cargo.lock", "benchmarks/Cargo.lock", "benchmarks/native-loadgen/Cargo.lock"):
        if not (ROOT / lock).is_file():
            print(f"ERROR: required lockfile {lock} is missing; restore it from version control.", file=sys.stderr)
            return 2
    run("cargo", "fmt", "--all", "--", "--check")
    run("cargo", "fmt", "--manifest-path", "benchmarks/Cargo.toml", "--all", "--", "--check")
    run("cargo", "fmt", "--manifest-path", "benchmarks/native-loadgen/Cargo.toml", "--all", "--", "--check")
    for flags in ([], ["--no-default-features"]):
        run("cargo", "clippy", "--locked", "--all-targets", *flags, "--", "-D", "warnings")
        run("cargo", "test", "--locked", "--lib", "--tests", "--examples", *flags)
        run("cargo", "test", "--locked", "--doc", *flags)
        run("cargo", "doc", "--locked", "--no-deps", *flags,
            env={**os.environ, "RUSTDOCFLAGS": "-D warnings"})
    run("cargo", "check", "--locked", "--benches", "--all-features")
    run("cargo", "test", "--locked", "--manifest-path", "benchmarks/Cargo.toml")
    run("cargo", "test", "--locked", "--manifest-path", "benchmarks/native-loadgen/Cargo.toml")
    if args.redis:
        if "REDIS_URL" not in os.environ:
            print("ERROR: --redis requires explicit REDIS_URL.", file=sys.stderr)
            return 2
        run("cargo", "test", "--locked", "--all-features", "--lib", "--tests", "--", "--include-ignored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
