# SubstrateDB design notebook

Research date: 2026-10-21. Status: discovery; proposals remain open except for the explicitly confirmed decisions recorded below and in ADRs.

## Product intent

One deployable NoSQL system for application documents, core vector search, real-time changefeeds, messaging, cache-like primitives, background work, and cron schedules. It must support clustering, sharding, and replication. The goal is to eliminate separately operated queue, cache, vector-search, scheduling, and real-time infrastructure as an application grows.

The implementation language remains under evaluation, with Rust preferred if supported by serious production precedent. No storage engine or consensus design has been approved.

## Distribution strategy: open source first, optional hosted integration later

The user identified an Upstash-like integration strategy: install a Vercel integration and provision a unified document DB, queue, cache, vector DB, scheduling, and WebSocket runtime. The subsequent clarification establishes open source first: build a useful independently deployable engine, then consider a hosted product if it proves useful. The Vercel integration is a conditional future distribution direction, not the initial launch path. It does not replace the engine requirements or imply that the database runs inside Vercel Functions.

Verified reference: [Upstash Vercel integration](https://upstash.com/docs/redis/howto/vercelintegration) supports Redis, Vector, Search, and QStash. [Vercel native integration concepts](https://vercel.com/docs/integrations/create-integration/native-integration) describe resource provisioning and project environment-variable injection. [Integration creation](https://vercel.com/docs/integrations/create-integration) describes the native integration onboarding process; marketplace distribution is not automatic merely because an integration is implemented.

Current priority: the open-source engine, usable independently and locally. A possible later Substrate Cloud would add provisioning, tenant lifecycle, credentials, usage/billing, backups, regions, and project/environment integration. A managed offering could run the same engine externally to Vercel while connecting functions through HTTP and browsers through direct WebSockets. Cloud billing and marketplace infrastructure are not prerequisites for the open-source project. The open-source license remains undecided.

Accepted job-delivery requirement: signed HTTP push to application routes in addition to connected pull workers. This allows on-demand functions to execute due jobs without a permanently running consumer, whether Substrate is self-hosted or managed. A delivery response must have defined acknowledgement semantics, timeouts, idempotency, retry and concurrency limits. This remains external application-code execution. Long-running work remains constrained by the destination's execution limits. [QStash delivery/retry reference](https://upstash.com/docs/qstash/features/retry).

Current Vercel docs state that Functions support WebSockets ([reference](https://vercel.com/kb/guide/do-vercel-serverless-functions-support-websocket-connections)); do not base positioning on the outdated assertion that Vercel cannot host them. Substrate's proposed differentiation is integrated durable state, subscriptions, fanout, authorization and recovery, coupled to documents/jobs/vector search.

Accepted future hosted behavior: isolated preview resources, with production schedules and external job deliveries disabled by default and explicit opt-in for test handlers and schedules. This does not authorize building the hosted control plane now. Next engine frontier: vector data model, retrieval modes, search freshness, representative workload/scale; then storage reuse, runtime architecture, packaging and license. Earlier open questions remain pending.

## Confirmed first-round decisions

- Use a native Substrate API and clients. Existing clients are design references for idioms, not compatibility targets.
- Substrate runs cron scheduling and manages queues. Application-owned worker processes connect and execute job handlers; arbitrary business-code execution inside Substrate is out of initial scope. A scripting language is only a possible future extension.
- Use order processing as the initial proving scenario.
- Include direct browser WebSocket connectivity to the sharded runtime to eliminate separately deployed socket servers. Authorization, routing, and reconnect mechanisms remain open.

See the glossary and ADRs for canonical terminology and the reasons for these boundaries.

## Confirmed second-round decisions

- Atomic document save and job submission is required. Its supported transaction/shard boundary remains open.
- Durable documents and jobs must survive one node failure in a properly configured three-node cluster after success is acknowledged. Prefer this guarantee over write availability without a majority.
- Worker retry behavior must be configurable with sensible defaults. Exact retry counts, backoff, leases, and exhaustion handling are not yet accepted decisions.
- Expose distinct live query and durable retained-event subscription APIs.
- Use Supabase as the design reference for browser authentication and Realtime authorization. This does not establish a dependency on Supabase or approval of its RLS implementation.

## Confirmed third-round decisions

- Try application-issued scoped browser tokens as the initial authorization design. Details remain subject to validation.
- Initial atomic document/job operations require a shared explicit logical partition key. Reject unsupported cross-partition transactions. Physical shard layout is still open.
- Missed cron runs require configurable recovery with sensible defaults. One catch-up occurrence is a recommendation, not an accepted fixed default.
- Redis-like application primitives are in scope: TTL caching, counters, hashes, lists, sets, sorted sets, pub/sub, and primitives for rate limiting and leases. Specialized time-series querying and arbitrary scripting remain deferred.
- Performant vector database capability is core product scope, independent of the Redis subset. It must influence the initial architecture and performance evaluation.

## Supabase reference and initial Substrate adaptation

Supabase private Broadcast/Presence channels authorize topic operations using JWTs and policies on realtime.messages. Postgres Changes uses a separate source-row authorization path. Channel permission caching means changing access does not necessarily immediately revoke an established subscription; refreshed credentials trigger reevaluation and token expiry bounds access without refresh. The protocol supports access_token messages on existing sockets.

Sources: [Realtime authorization](https://supabase.com/docs/guides/realtime/authorization), [Realtime protocol](https://supabase.com/docs/guides/realtime/protocol), [database-change subscriptions](https://supabase.com/docs/guides/realtime/subscribing-to-database-changes).

Accepted as the initial approach to try: the application backend decides entitlements and issues a short-lived signed Substrate capability, and Substrate enforces it on direct browser connections. Proposed implementation details: the SDK maintains heartbeats and refreshes credentials in-band; Substrate validates issuer/signature/audience/expiry and enforces allowed resources, operations, and data visibility. Treat browser-supplied filters as query parameters, not authority. Enforce the same authorization on initial snapshots, updates, replay, and reconnect. Channel permissions and document visibility are separate concerns. No Supabase or PostgreSQL dependency is required; Supabase Auth can remain an application identity provider.

Still open: capability format, key rotation, credential renewal, immediate versus expiry-bounded revocation, query authorization constraints, and routing across shards.

Candidate worker defaults, not yet accepted: manual acknowledgement, redelivery after lease expiry, bounded exponential retry with jitter for retryable failures, stable job identity, and retained failed/dead-letter state after exhaustion. Configuration should cover retry limits, delays, execution leases/heartbeats, and explicit non-retryable failures. No-retry mode cannot guarantee completion after a worker failure. External effects still require application-level idempotency.

## Production language precedents

Rust is a credible production database implementation language, not merely an experimental option. TiKV is a Rust distributed transactional key-value database and a graduated CNCF project; it uses RocksDB for storage, demonstrating that choosing Rust does not require implementing every storage component in Rust. Qdrant is another Rust database, specialized for vector search. These are evidence of feasibility, not evidence that Substrate will inherit their reliability.

- [TiKV source and architecture overview](https://github.com/tikv/tikv)
- [Qdrant source](https://github.com/qdrant/qdrant)
- C++ remains common: [DuckDB](https://github.com/duckdb/duckdb) and [ScyllaDB's Seastar framework](https://github.com/scylladb/seastar).
- Go is also used: [CockroachDB](https://github.com/cockroachdb/cockroach) and its [Pebble engine](https://github.com/cockroachdb/pebble).
- Zig is used by [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle), a specialized financial transaction database.

Recommendation: Rust for Substrate's core, subject to the storage and runtime design review. Ownership and type checking catch many memory/concurrency mistakes before runtime ([Rust Book](https://doc.rust-lang.org/book/ch16-00-concurrency.html)); they do not establish distributed consistency, crash safety, deadlock freedom, or bounded latency. Those require explicit invariants and testing.

## Research findings

| System | Relevant capabilities | Boundary to preserve |
| --- | --- | --- |
| MongoDB | Documents, indexes, sharding, replication, transactions, resumable change streams | Change streams require replica sets or sharded clusters; resumption depends on retained history. They are not worker queues with acknowledgements and retries. |
| RethinkDB | Subscriptions to documents, tables, and supported query results, including initial results | Changefeeds lack consumer acknowledgements and guaranteed delivery. Live query results and durable event consumption are different contracts. |
| RabbitMQ | Routing, work queues, publisher confirms, consumer acknowledgements, replicated quorum queues, replayable streams, flow control | Clustering alone does not replicate every queue. Retries can duplicate delivery; external effects require idempotency. |
| ZeroMQ | Messaging library with pub/sub, request/reply, pipeline patterns, and multiple transports | Transport and reconnection do not supply durable storage, replay, or processing acknowledgement. |
| Redis | Cache primitives, TTL, counters, collections, Pub/Sub, Streams, and a broad set of specialized data types | Pub/Sub is at-most-once. Streams offer retained entries and consumer groups. Full Redis equivalence is much broader than caching. |

Primary sources:

- MongoDB: https://www.mongodb.com/docs/manual/ and https://www.mongodb.com/docs/manual/changestreams/
- RethinkDB: https://rethinkdb.com/docs/changefeeds and https://rethinkdb.com/docs/consistency/ and https://rethinkdb.com/docs/architecture
- RabbitMQ: https://www.rabbitmq.com/docs/reliability and https://www.rabbitmq.com/docs/quorum-queues and https://www.rabbitmq.com/docs/clustering and https://www.rabbitmq.com/docs/exchanges and https://www.rabbitmq.com/docs/streams and https://www.rabbitmq.com/docs/flow-control
- ZeroMQ: https://zeromq.org/get-started/ and https://zeromq.org/socket-api/ and https://zguide.zeromq.org/docs/chapter4/
- Redis: https://redis.io/docs/latest/develop/data-types/ and https://redis.io/docs/latest/develop/pubsub/ and https://redis.io/docs/latest/develop/data-types/stream/ and https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/ and https://redis.io/docs/latest/reference/eviction/

## Candidate architecture to investigate

One server distribution and cluster control plane, with document storage, vector indexing/search, durable messaging, scheduling, ephemeral messaging/cache, and connection handling as internal subsystems. This is a proposed interpretation, not a settled implementation plan.

## Accepted vector model and remaining frontier

Accepted in the latest round: document-native named vector fields, metadata-filtered retrieval, hybrid semantic and ranked keyword search, external embedding generation, asynchronous indexing with an explicit wait for search visibility, and the initial benchmark workload described below. Detailed feature parity with other vector products is not implied by the phrase "typical vector DB features."

- Named vectors belong to ordinary documents. Per-field dimension/distance configuration remains proposed detail. Support metadata-filtered nearest-neighbor search; do not treat post-filtering a small global top-k as adequate filtered retrieval.
- External application workers generate embeddings, consistent with external job execution. Substrate stores/indexes/searches supplied vectors. Updates should carry a source revision so a delayed embedding job cannot overwrite a newer document's vector; exact contract open.
- Hybrid semantic and ranked keyword retrieval is accepted. Specific support for caller-supplied sparse vectors, fusion methods, and ranking algorithms remains to be resolved. Text filtering and ranked full-text retrieval are different capabilities.
- Asynchronous indexing with an explicit caller-selectable wait for search visibility is accepted. Index progress must be observable, and deletion/access-control enforcement needs explicit semantics. Exact freshness API, timeout/failure handling, and replica routing are open.
- Accepted representative benchmark envelope: multi-tenant knowledge retrieval at roughly 1M vectors on one node and 10M across a cluster, filtered/unfiltered queries, concurrent updates, and document/queue load. Candidate dimensions are 768/1536, still to be specified. The envelope is a testing target, not demonstrated capacity or a latency guarantee. Measure recall against exact ground truth alongside p95/p99 latency, throughput, RAM, disk, indexing lag, and rebuild/failover behavior on fixed hardware.
- Transaction partition locality does not imply collection-wide vector queries must be limited to one partition. Distributed search routing/merge and tenant locality remain design work.
- Live-query subscriptions do not yet imply continuously maintained approximate nearest-neighbor result subscriptions.

Primary references: [Qdrant filtered vector search](https://qdrant.tech/documentation/search-patterns/vector-search-filtering/), [indexing](https://qdrant.tech/documentation/manage-data/indexing/), [search](https://qdrant.tech/documentation/search/). Qdrant documents filter-aware indexes and query planning, illustrating why vector performance must be assessed with filtering and recall, not unfiltered throughput alone. These are reference patterns, not an engine selection.

Proposed initial feature checklist: dense and sparse vectors, named fields, configurable dimensions and cosine/dot/Euclidean distance for dense fields, filtered top-k, exact search and approximate search with recall/speed controls, ranked lexical retrieval and hybrid fusion, batch upsert/delete, persistence/replication, and search visibility waits. Disk-backed vector/index options and quantization require evaluation for the accepted scale; implementation and release gates are open. Token-level multivectors/late-interaction retrieval, model inference, learned rerankers, and live nearest-neighbor subscriptions remain explicit unresolved scope, not silently included. References: [Qdrant vectors](https://qdrant.tech/documentation/manage-data/vectors/), [hybrid queries](https://qdrant.tech/documentation/search/hybrid-queries/), [quantization](https://qdrant.tech/documentation/manage-data/quantization/).

Accepted: embedded storage/index libraries are allowed inside the one deployable runtime. [RocksDB](https://rocksdb.org/) is an example, not a selected dependency. A native library adds no separately operated service, but brings its own build, licensing, recovery, and performance considerations.

## Confirmed engine and release boundaries

- Initial document queries: nested documents, CRUD, conditional updates, secondary/compound indexes, filtering, sorting, projection, and pagination. Joins and a general aggregation pipeline are deferred. Aggregation is an explicit future requirement, not an exclusion from the product vision; keep query representation extensible without implementing speculative stages now.
- Initial targets: Linux production, Docker distribution, local development on macOS/Linux, TypeScript SDK for server and browser. Keep the protocol language-neutral for later clients.
- Accepted vector baseline: dense/sparse vectors; multiple named vector fields; cosine, dot-product and Euclidean distance for dense fields; metadata filtering; exact and approximate retrieval with accuracy/speed controls; ranked keyword search and hybrid fusion; batch writes/deletes; persistence and replication; observable indexing and visibility waits. Evaluate disk-backed storage/compression against the benchmark targets. Token-level multivector retrieval and in-runtime model inference are deferred; external workers can generate embeddings and run learned rerankers.
- Embedded library reuse is permitted, with no particular storage, consensus, or vector library yet chosen.

## Current architectural synthesis (proposal)

One independently deployable runtime with transport/session handling, authorization, request routing, per-partition durable state transitions, local embedded storage, asynchronous vector/text indexing, durable events/consumer state, scheduler, and job dispatch. Share ownership/commit boundaries for document-plus-job atomicity; avoid a global ordering bottleneck. Browser/server transports may differ while using the same data model and permissions. Hosted provisioning remains outside the initial project.

Resource separation matters inside the process: vector indexing/search, cache traffic, socket fanout and background compaction must not starve durable commits or job control. Exact scheduling, memory accounting, storage layout, and consensus-library choices require design/benchmark work after contracts are settled.

## Accepted ordering, retention, and recovery contracts

- Events are ordered within a logical partition; no cluster-wide total order is promised. Jobs may opt into sequential execution by ordering key. Delivery order and completion order are distinct. Exact retry-blocking and terminal-failure behavior for ordered jobs remain to be specified.
- If a durable subscription cursor predates retained history, return an explicit history-unavailable outcome. Do not silently skip missing events. Live queries may recover from a fresh snapshot; durable consumers explicitly choose recovery. Retention is configurable with documented defaults; specific values remain open.
- Pending jobs remain durable until acknowledged, cancelled, or expired by an explicit configured policy, independent of event-history retention. Apply backpressure or reject new submissions when full rather than silently discarding accepted work. Failed jobs remain inspectable under configurable retention.
- Consistent backup/restore is required for the first production release. Restored queues and schedules start paused until explicitly resumed, allowing reconciliation of external side effects that the restore cannot undo. Backup scope, incremental/PITR support, and restoration of live connection/session state are not yet defined.

## Accepted operating model and live-query boundary

- One binary supports standalone and replicated/sharded cluster operation without a mandatory external coordinator. Standalone durability cannot provide resilience to loss of its only machine; replica configuration determines machine-failure tolerance.
- A cluster supports isolated databases/namespaces with scoped credentials and resource limits. Applications may implement their own tenant identifiers within a namespace. Neither namespace nor application tenant implies one physical shard. Exact isolation and resource accounting mechanisms remain design work.
- Initial live queries cover individual documents and indexed filtered document sets, with initial snapshots and subsequent additions, updates, and removals. Indexed subscriptions are explicitly required. Unsupported/unindexed subscriptions should be rejected clearly; no promise of arbitrary full-scan subscriptions. Sorted top-N, aggregation, and continuous vector-result subscriptions are deferred; ordinary sorted and vector queries remain in scope.
- Schedules default to UTC and accept optional IANA timezones. Missed-run, DST, and overlap policies are configurable. Accepted initial defaults: one catch-up occurrence after downtime, skip nonexistent local times, run once during repeated local times, and prevent overlapping execution of the same schedule. Precise occurrence identities and distributed overlap enforcement remain implementation design work; a lease alone does not stop an external handler already executing.

## Accepted messaging, browser, and regional boundaries

The user's two abbreviated "3 yes" replies are interpreted in sequence as Q33 and Q34; Q35 and Q36 were explicitly accepted.

- Initial messaging: competing workers, fanout, exact/wildcard topics, delayed delivery, retries, dead-letter handling, and request/reply with timeouts. Ephemeral pub/sub and durable delivery remain distinct. This is an explicit subset of messaging capabilities, not total RabbitMQ compatibility.
- Browser credentials initially allow scoped subscriptions, channel publishing, and presence. Document writes and job administration go through trusted application servers. Exact read/search operation access remains to be specified in the authorization matrix.
- Expiry-bounded revocation is the baseline, supplemented by explicit revoke/disconnect for urgent cases. Reconnect must enforce revocation; disconnect alone is insufficient. Propagation and partition behavior require a documented bound, not a promise of instantaneous global invalidation.
- First production clusters target one region across failure zones. Cross-region disaster recovery comes later; active-active multi-region writes are deferred.

The first consolidated architecture proposal is in ARCHITECTURE.md. Remaining user-level decisions: final core language commitment and open-source license. Library selection, storage layout, shard migration, quantitative performance thresholds, and exact configurable defaults require engineering evaluation. They are explicitly unresolved rather than silently accepted. Implementation awaits shared design confirmation.

- Share durable commit machinery where document changes and submitted jobs need atomicity. Define the transaction/partition boundary before promising atomic document-plus-enqueue operations.
- Keep ephemeral traffic and cache eviction policies separate from authoritative records and outstanding jobs. Use bounded buffering, admission control, and resource isolation inside the deployment.
- Separate live query subscriptions, replayable event streams, and competing work consumers in the public model.
- Distinguish cluster membership, shard placement, and replication. A single global log could bottleneck horizontal write scaling; evaluate per-shard consensus after consistency requirements are settled.
- Scheduling creates durable job occurrences; workers execute handlers. Stable occurrence identities, leases, and fencing are candidate mechanisms. These do not provide exactly-once external side effects.
- WebSockets and local IPC are transports. Browser access introduces authorization and subscription filtering requirements beyond transport support.
- A cluster removes external product dependencies but still requires multiple machines for machine-failure resilience.

## Design tree and first interview frontier

1. Product scope: replace common application use cases through a native API, or provide protocol/command compatibility with existing clients?
   - Unlocks: exact Redis data types, RabbitMQ routing/protocol scope, document query language, SDK surface.
2. Execution boundary: handlers run in existing application processes, or arbitrary user code runs inside SubstrateDB?
   - Unlocks: worker protocol versus embedded runtime, language support, isolation and resource controls.
3. Initial proving workload: use an order-processing SaaS flow to exercise storage, jobs, live updates, cache, and schedules?
   - Unlocks: concrete data volumes, payload sizes, throughput and latency objectives, durability and transaction requirements.
4. Client trust boundary: direct scoped browser/mobile subscriptions, or application-server-only access?
   - Unlocks: authentication integration, authorization, token revocation, fanout and connection ownership.

Later rounds must cover consistency/partition behavior; replay retention and reconnect; ordering; retry and duplicate handling; cron time zones/missed runs; indexes/query scope; tenant boundaries; backup and restore; resource contention; operating environments; storage engine reuse; language/runtime; and measured performance targets. They depend on the initial scope and use cases.

## Performance engineering direction

Provisional preference: evaluate Rust first, with C++ as a serious alternative when engine/library reuse or team expertise favors it. No claim that language choice alone establishes performance.

Use workload benchmarks with fixed durability guarantees, dataset size relative to RAM, concurrency, hardware, and latency distributions. Include mixed workloads, long-running compaction/retention activity, slow consumers, crash recovery, and failover. Do not compare an in-memory acknowledgement against a quorum-durable acknowledgement as equivalent operations.

Useful references: [Rust Performance Book](https://nnethercote.github.io/perf-book/profiling.html), [Criterion profiling](https://bheisler.github.io/criterion.rs/book/user_guide/profiling.html), [Clang AddressSanitizer](https://clang.llvm.org/docs/AddressSanitizer.html), [Clang ThreadSanitizer](https://clang.llvm.org/docs/ThreadSanitizer.html).

The installed gstack benchmark skill targets page load times, Core Web Vitals, and resource sizes; it is not a database-engine benchmarking workflow. Specialist skill candidates are being evaluated separately. Glossary terms and ADRs will be created as terminology and consequential decisions are actually resolved in the interview.

## Inspected agent skill candidates

These are content-reviewed candidates, not independently proven best-in-class tools. None was installed.

- [po4yka rust-performance](https://github.com/po4yka/rust-skills/blob/main/skills/rust-performance/SKILL.md): operational profiling, optimized builds with symbols, Criterion caveats, heap profiling, and platform tools. Best hands-on profiling fit among inspected candidates, but very limited adoption (2 repository stars observed); validate commands against the chosen toolchain. Candidate install: `npx skills add po4yka/rust-skills --skill rust-performance`.
- [shenli designing-distributed-system-tests](https://github.com/shenli/distributed-system-testing/blob/main/skills/designing-distributed-system-tests/SKILL.md): explicit guarantees, fault hypotheses, operation histories and correctness checkers. Good complement for durability and replication; a workflow rather than a ready-made test harness. 231 repository stars observed. Candidate install: `npx skills add shenli/distributed-system-testing --skill designing-distributed-system-tests`.
- [actionbook m10-performance](https://github.com/actionbook/rust-skills/blob/main/skills/m10-performance/SKILL.md): useful profile-first and data-layout reminders, but high-level and not a complete DB performance methodology. CLI reported 3.5K skill installs; about 1.5K repository stars observed. Candidate install: `npx skills add actionbook/rust-skills --skill m10-performance`.
- [Jeffallan rust-engineer](https://github.com/Jeffallan/claude-skills/blob/main/skills/rust-engineer/SKILL.md): CLI reported 6.2K installs, but broad language guidance rather than deep DB engineering. Inspected example slices a string at byte 50 without length or UTF-8 boundary checks, so popularity alone is insufficient reason to choose it.

Recommended future project-specific skills: reproducible workload benchmarking, storage crash recovery, and replication/linearizability validation. Build them around actual project commands and invariants once those exist, rather than pretending a generic prompt establishes correctness or speed. Adoption counts are observations from this research session and can change.

## Implementation handoff — 2026-10-21

The approved Rust/Apache-2.0 build now lives in this repository. See README.md for execution, docs/IMPLEMENTATION.md for completed mechanisms and remaining production gates, docs/PROTOCOL.md for native client contracts, and docs/OPERATIONS.md for persistence/retention/backup semantics. This supersedes earlier statements that no implementation exists without changing the original product requirements. Small measured vector results are recorded separately and do not establish the agreed 1M/10M target envelope.
