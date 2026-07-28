# Changelog

## 0.30.0

- Added a curated public API: `Publisher<T>`, `Subscriber<T>`, `Apply<T>`, and `prelude`.
- Added `Publisher::builder()` for public policy configuration.
- Added `Packet::to_bytes()` / `Packet::from_bytes()` convenience methods.
- Added public API and recovery examples plus dedicated public API regression tests.
- Reorganized release, security, architecture, protocol, and benchmark documentation under `docs/`.
- Added CI release gates for formatting, all-feature checks, Clippy with warnings denied, tests, rustdoc, and V30 validation.
- Added V30 fast adaptive encoder with bounded two-strategy shortlist.
- Added workload change profiling and recent-winner self tuning.
- Added compression feedback and minimum-payload gating.
- Added partial chunk repair with final hash verification.
- Added recovery planner choosing replay, partial repair, or snapshot.
- Added runtime limits, backpressure decisions, and runtime metrics.
- Added V30 validation, tests, Criterion benchmark, CLI benchmark, protocol/security/test-plan docs.
- Incorporated the V25 fixes discovered during local compilation and validation.

## 0.25.0

- Added V25 byte-state adaptive synchronization engine.
- Added smart delta strategies: sparse, contiguous ranges, XOR, splice, and chunks.
- Added raw/zstd candidate competition across smart delta strategies.
- Added lightweight adaptive compression threshold tuning.
- Added bounded reorder buffering for short out-of-order gaps.
- Added bounded recovery history and replay-vs-snapshot planning.
- Integrated recovery replay planning into async publisher sessions.
- Added deterministic hard torture simulation with late joins, long disconnects, reorder, duplicates, drops, and a global resync storm.
- Added shared-snapshot resync-storm benchmark.
- Added V25 edge-case suite including resize, malformed delta, stale/future packets, corruption, schema mismatch, and 64 MiB boundaries.
- Added V25 property tests and Criterion microbenchmarks.
- Added smart-delta, workload, recovery, and client-scale benchmark matrices.
- Preserved V20 protocol v3 envelope compatibility and existing transport adapters.

## 0.20.0

- Added four-way snapshot/delta/zstd adaptive selection.
- Added stable recovery sequence behavior.
- Added v2/v3 envelope compatibility and stricter protocol validation.
- Added schema migration support and multi-client deterministic fault testing.
