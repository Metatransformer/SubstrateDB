# Operations and guarantees

## Persistence

Standalone commands use immediate-durability redb transactions. One transaction covers document indexes, lexical indexes, job state, events and request deduplication. Queue acceptance is durable; handlers execute externally with at-least-once delivery. Application side effects must use idempotency keys. An expired lease cannot stop code already running elsewhere; protected resources must reject stale fencing tokens if that distinction matters.

Each cluster shard has an incremental redb Raft log and an independent application engine. Votes, committed indexes and appended entries are durable before acknowledgement. Engine application precedes the persisted applied boundary; replay after a crash returns the stored request result. Storage failures stop state-machine application. Snapshots serialize application and membership state and recover interrupted installations. Reads require a quorum barrier; a minority must not return successful durable writes.

An operation timeout can mean either failure or an unobserved commit. Retry the same request ID and content. Stable job IDs add business-level deduplication until those job records are explicitly or automatically removed. Empty claims may see newly available work on retry because negative polls are not retained indefinitely.

## Defaults and limits

| Setting | Default / bound |
|---|---|
| Logical stored bytes per namespace/partition | 1 GiB; adjustable through `scope.configure` |
| Request deduplication retention | 24 hours minimum |
| Event retention | 7 days; configurable down to 1 minute |
| Completed/cancelled/expired job retention | 7 days; configurable down to 1 minute |
| Pending jobs / dead letters | Independent of event retention; retained until transition or explicit action |
| Command body | 1 MiB over HTTP |
| Individual read result | 4 MiB; paginate larger queries |
| Concurrent commands per node | 128; overload rejected |
| WebSocket sessions per node | 1,024 |
| WebSocket incoming message | 1 MiB |
| Live-query result | 1,000 documents; outgoing messages capped at 4 MiB |
| Ephemeral channel buffer | 1,024 messages per node |
| HTTP deliveries | 32 concurrent; 20-second request timeout; 30-second claim lease |
| HTTP completion | Any 2xx except 202; redirects disabled; other outcomes retry |
| Job attempts | 5; configurable 1–1,000 |
| Lease | 30 seconds; configurable 100 ms–1 hour |
| Retry | Exponential delay with deterministic jitter; optional explicit delay |
| Background index/schedule check | 500 ms, only shard leaders mutate |
| Retention sweep | Every 10 seconds, bounded pages with rotating cursors |

`scope.stats` reports logical serialized bytes and limits. This accounting includes derived indexes, retained events and deduplication; it does not equal physical file size. Redb may retain freed pages for reuse. Quota checks roll back growth atomically. Cleanup and limit increases remain possible at capacity. Cache eviction does not remove authoritative data: expired keys are invisible immediately and physically reclaimed by the bounded sweep.

Limits currently apply per logical partition and per node. They are **not** a global namespace-wide memory/CPU quota across a fleet. A large scan or graph update can still delay other commands in its shard. This remains a production qualification item.

## Backup and restore

Stop the standalone process first, then:

```sh
substrate --data ./data/substrate.redb --backup ./backup.json
substrate --data ./recovered/substrate.redb --restore ./backup.json
```

The database file lock prevents an offline command opening a live engine. Backup uses one atomic engine snapshot. Its manifest records the format, application version, boundary and SHA-256 checksum; a checksum mismatch is rejected before replacing live state. The output path must not already exist. Store backups outside the node's failure domain.

Restore pauses every queue found in the backup and every schedule. Reconcile external effects before resuming queues/schedules. Expired leases may be reclaimed after resume. Connections and in-flight HTTP requests are not restored. Keep the signing key if restored revocation records and capabilities must remain meaningful; rotate it to invalidate all old tokens.

Cluster backup orchestration is not automated. Do not overwrite an engine beneath an existing Raft history. For an offline logical recovery, quiesce applications and background work, stop the cluster, back up authoritative shard engines, and restore identical logical images into **fresh** node directories for each shard's initial voters before bootstrapping new Raft groups. Each shard image has its own atomic boundary; there is no cross-shard transaction/snapshot. Existing `raft-log.redb`, applied metadata and old consensus snapshots must never accompany modified logical engine images. This procedure needs fleet-level automation and disaster-recovery qualification before production use.

## Security and networking

The public HTTP listener does not implement TLS termination. Use an ingress/proxy or trusted network. All public data operations require an administrator or scoped service credential; browser tokens grant only realtime operations. Cluster members share the public admin/signing configuration for realtime relays and a separate internal Raft secret. Explicit signing keys and non-development internal secrets must be at least 24 bytes. Do not expose the internal Raft routes to untrusted networks.

Credential checks and periodic browser revalidation consult durable revocations. Normal revalidation runs every 250 ms; quorum unavailability may extend that by the cluster request deadline, then closes the connection. Remote presence expires after 15 seconds without refresh. Ephemeral relay failures may lose messages and do not imply durable acceptance.

## Observability and shutdown

`/health` is process liveness and reports standalone/cluster mode. It is not a quorum-readiness assertion. Authenticated `/metrics` exposes operation/error counters and per-shard leader/log state. Use a read of an application scope to check its availability. Logs report background indexing/scheduling/dispatch failures. SIGINT and SIGTERM request graceful HTTP shutdown; a hard kill relies on durable recovery and expiring worker receipts.

All nodes require synchronized clocks. Leases, cron due times, capability expiry and TTLs use server time. Large clock jumps have not been fault-qualified. Membership changes may time out after taking effect: inspect actual voters before retrying.

## Release qualifications still open

The source is runnable and tested; this is not evidence of mission-critical readiness. Outstanding work includes the agreed 1M/10M-vector mixed-load experiments, a production ANN/storage choice with recall evidence, large snapshot transfers, global workload isolation, operational backup orchestration, upgrade/migration tests and a longer fault campaign. Fixed shard counts and preconfigured discovery peers are deliberate current limits. No automatic elastic shard splitter or cluster autoscaler is supplied.
