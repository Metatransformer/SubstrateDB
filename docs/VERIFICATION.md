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

## Embedded dashboard — 2026-10-22

Added a Next.js static dashboard compiled into the Rust binary, offline documentation routes, and a browser regression that starts its own isolated database. Verified:

- Frontend production export and TypeScript checking.
- Rust formatting, Clippy with warnings denied, and all 45 Rust tests (including embedded assets, offline docs, missing paths, and unauthenticated API rejection).
- Chromium UI workflows: invalid/valid authentication, document creation and revision updates, a live WebSocket document watch, job enqueue/list, cache values including `false`, vector submission and search, schedule creation/list, retained events, raw commands, and documentation.
- Namespace switching clears previous screen results; refresh clears credentials; no credentials enter local/session storage. Mobile navigation fits a 390px viewport.

Run `npm --prefix dashboard run build`, `cargo build --locked --bin substrate`, then `npm --prefix dashboard test` after installing Chromium with `npm --prefix dashboard exec -- playwright install chromium`. Browser test data is temporary and removed after the test. The dashboard does not change the runtime's production qualification status.

The Linux/aarch64 release Docker image also built successfully. A temporary container verified UID 10001, the dashboard HTML and referenced JavaScript, the embedded quickstart, and authenticated `scope.stats`; the container and its anonymous volume were removed afterward.
