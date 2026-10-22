# SubstrateDB build project

Approved architecture, Rust and Apache-2.0: 2026-10-21. Objective: complete the agreed open-source runtime; intermediate packages are not release completion.

## Delivery tracks

| Track | Deliverable | Verification gate | Status |
|---|---|---|---|
| Foundation | Rust binary, embedded transactions, namespaces, command protocol, persistence, Apache license | atomic rollback, restart, request replay | Implemented; tested |
| Documents/events | CRUD/CAS, indexes, filtered queries, partition event cursors, atomic document plus job | index consistency, snapshot/event handoff, history gaps | Implemented; qualification notes below |
| Work execution | Durable queues, leases, retries, dead letters, ordering, signed HTTP push, cron/timezones | stale ack rejection, crash reclaim, DST, no silent loss | Implemented; qualification notes below |
| Cache/messaging | TTL structures, counters, pubsub/topics, presence, request/reply | expiry/atomicity, routing and authorization | Implemented; qualification notes below |
| Search | Named dense/sparse vectors, lexical/hybrid, exact/ANN, visibility, filtering | exact-ground-truth recall, revision/deletion correctness | Implemented; qualification notes below |
| Cluster | Persistent consensus, shard groups, membership/recovery and routing | three-node leader failure, acknowledged durability, minority rejection | Implemented; qualification notes below |
| Realtime/security | Scoped credentials, indexed subscriptions, renewal/revoke, bounded fanout | snapshot/update gap, tenant isolation, reconnect | Implemented; qualification notes below |
| Operations | Backup/restore paused, limits, metrics, Docker, CI, TS SDK/examples | restore reconciliation, build/typecheck, end-to-end example | Implemented; qualification notes below |
| Release | Sustained mixed-load/scale and fault evidence, API docs | 1M/10M vector benchmarks on specified hardware; no unsupported claims | Not qualified |

## Execution rules

The planner owns contracts, integration and review. Sol workers receive closed file allowlists and bounded work packages; no worker stages or commits. Audit actual diffs and reproduce tests. When Sol is unavailable, the planner executes the same scoped package directly. Engineering mechanisms and defaults are recorded as selected, with limitations explicit. No hosted control plane is required.

## Foundation design

Start with redb 2.6.3 (compatible with the available Rust 1.87 toolchain) as an embedded transactional persistence candidate. Use immediate durable commits. It is not a claim that this engine meets the eventual vector throughput goals; search indexes and consensus remain separate modules. Keep application commands deterministic: inject operation time and IDs before replication, not inside state-machine application. One storage transaction covers all changes of one command within a namespace/partition. Replication wraps this command boundary.

Use a versioned JSON command envelope for the first native protocol, with explicit operation names and namespace/partition routing. The typed TypeScript SDK hides envelope details. Credentials are never inferred from client filters. No numeric latency or scale result is claimed before measurement.

## Implementation review

The repository now contains the durable Rust runtime, native TS SDK, executable examples, Docker packaging, CI, persistent consensus, explicit per-shard replica placement and membership movement. Verification covers atomic rollback, restart/crash recovery, source-versioned indexing, queue receipt fencing, DST/catch-up, indexed subscriptions, scoped browser/service tokens, cross-node channels/presence, quorum failure and placement changes. The application and integration tests are executable, not placeholder interfaces.

Raft log persistence was changed from a whole-file JSON prototype to incremental immediate-durability redb writes. Worker claims use due-time and ordering indexes. Background checks avoid empty mutation traffic; retained records are swept in bounded pages. Cache leases expose monotonically increasing fencing tokens; handlers still need external enforcement.

### Remaining production release gates

- Benchmark and choose a production ANN representation. The custom persisted single-layer graph is experimental, with only small-scale evidence. Quantization, disk-specialized ANN, 1M single-node and 10M cluster mixed workloads are not qualified.
- Fleet-wide namespace CPU/memory/storage isolation. Current admission controls are per-node and logical storage quotas per-partition.
- Automated consistent cluster backup/restore orchestration and a disaster-recovery campaign. Standalone checksummed atomic backups and paused restore are implemented.
- Long-running fault/clock/upgrade testing, large snapshot transfers, migration compatibility, security review, operational readiness and numerical release thresholds.
- Global lexical/hybrid ranking across partitions. Vector-only scatter is implemented; there is no cross-partition snapshot.

Fixed shard count and preconfigured discovery peers are current operational constraints. Replica membership can move safely within that configured pool; an automatic elastic shard splitter/control plane is not included. Hosted/Vercel integration remains intentionally outside the open-source build.

Completion of the implementation checks is not equivalent to completing these production gates. No mission-critical suitability or million-vector performance claim is made.

## Reproducible evidence

See [VERIFICATION.md](VERIFICATION.md) for exercised checks and [SEARCH.md](SEARCH.md) for raw benchmark context. The final review also constrained Raft batches to the transport body limit, corrected append/election timing for large durable catch-up, moved blocking engine work off async transport tasks, and allowed MVCC readers to proceed concurrently with writers.
