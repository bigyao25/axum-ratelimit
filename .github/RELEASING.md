# Release procedure

## Repository setup

- Confirm the `axum-ratelimit` crates.io name is available or owned by the publishing account.
- Verify the repository URL and package metadata in [Cargo.toml](../Cargo.toml).
- Create a protected GitHub environment named `crates-io`, configure required reviewers,
  and add a narrowly scoped `CARGO_REGISTRY_TOKEN` secret to that environment.

## Prepare a release

1. Update the version in [Cargo.toml](../Cargo.toml) and the release notes/date in
   [CHANGELOG.md](../CHANGELOG.md). Keep both Cargo.lock files committed and consistent
   with their manifests.
2. Validate locally from the repository root:

   ```sh
   REDIS_URL=redis://127.0.0.1:6379/ python3 scripts/check.py --redis
   ```

   Use a disposable Redis instance. This check does not run performance benchmarks.
3. Commit the release changes, verify a clean working tree, and run `cargo package --locked`.
4. Push to the default branch and wait for both [CI](workflows/CI.yml) and
   [Security](workflows/security.yml) to pass for the release commit. Security runs
   independently; the release workflow does not automatically wait for it.

## Publish

1. As a repository administrator, run [Validate or Publish Release](workflows/release.yml)
   from the default branch, setting `version` to the package version and `dry_run=true`.
   This runs the full reusable CI and `cargo publish --dry-run` without publishing or tagging.
2. Review the result, then run the workflow for the same commit/version with `dry_run=false`.
   Approve the protected environment as configured. Both the original and rerun actors
   must be repository administrators.
3. After publication succeeds, the workflow creates `v<version>` and a GitHub Release.
   Verify the crate on crates.io and its documentation build on docs.rs.
