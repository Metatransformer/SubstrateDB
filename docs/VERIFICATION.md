# Verification record

Development verification on 2026-10-21/22. Host: Apple M2 Max, 32 GiB RAM, macOS/aarch64; Rust 1.87 and Node 22.21.1. These results establish exercised behavior, not production certification.

## Executed checks

- Rust formatting and Clippy across all targets with warnings denied.
- 44 Rust tests passed on macOS and again in a Linux/aarch64 release-build container: document/index/CAS rollback, request replay and restart, concurrent atomic read snapshots, retained-event gaps, queue receipts/retries/ordering, cache TTL/types/fencing/rate limits, quotas/cleanup, checksummed restore, cron/DST, vector source revisions/deletion/batches/visibility/ranking/numeric bounds, scoped server authorization and cross-partition vector merge.
- Three-node consensus and public-HTTP failover tests: committed data survives leader shutdown; minority writes fail. Four-node placement test: a node outside a group forwards requests, learner catch-up precedes voter movement, and data remains available after removal of a former member.
- Eight TypeScript SDK tests, including malformed response handling, payload contracts, signed HTTP validation, temporary worker failures, lease-loss ack suppression, request/reply timeout and connection cleanup.
- Standalone process E2E: authenticated HTTP, atomic document/job, worker ack, cache, async hybrid search, scoped WebSocket changes and revocation, signed HTTP dispatch, SIGKILL persistence recovery, Unix-socket permissions and IPC.
- Three-process cluster E2E: scoped service credentials, cross-node channels/presence, single-document snapshot/removal, credential revocation, killed-replica restart and catch-up after sixteen 600 KB commands.
- Native release binaries and Linux Docker image builds. Container smoke checks authenticated persistence across restart and non-root uid 10001.

The catch-up test found a 50 ms default append timeout incompatible with larger durable batches. The runtime now uses 500 ms heartbeats, 1.5–3 second elections, and at most four commands per append RPC; the process-restart regression passes with these defaults. It also exposed a test cleanup issue: signalled child processes retain a null exitCode, so cleanup checks signalCode as well.

## Performance evidence

[Raw search measurement](benchmarks/search-2000-32.json): 2,000 synthetic 32-dimensional vectors, 100 queries, seed 42, ef 128, release build, recall@10 1.0. The run shared its host with build activity. Graph p95 was 15.16 ms; exact p95 was 33.80 ms. See SEARCH.md for all parameters and limitations.

No 1M/10M scale, sustained mixed-load, disk-failure, clock-jump or rolling-upgrade result is claimed. CI is configured for Linux and macOS. The Linux release tests were executed locally in Docker; no remote CI run is implied. See IMPLEMENTATION.md for remaining release gates.
