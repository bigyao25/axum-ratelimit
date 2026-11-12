# Changelog

## 0.1.0 - Unreleased

Initial release.

- Native Tower Layer/Service with ready-instance preservation and opaque body forwarding.
- Framework-independent rule configuration; captured identity/rejection callbacks on Layer.
- Precise memory/Redis admission implementations and the Redis v2 key protocol.
- Optional draft quota headers and backend-neutral observer with panic isolation.
- Functional, concurrency, response-header, real TCP, RESP mock and real Redis test sources.
- CI/security/release workflows; Axum-native Docker/wrk comparisons and microbenches.
- Strict missing-ConnectInfo behavior instead of silently pooling unidentified clients.
