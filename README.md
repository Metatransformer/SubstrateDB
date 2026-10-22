# SubstrateDB

A Rust runtime for JSON documents, vector and keyword search, durable jobs, cron schedules, cache primitives, and browser WebSockets. Application workers execute your code; Substrate stores and coordinates their work. One process is enough for development. Multiple copies of the same binary form a sharded Raft cluster without Redis, RabbitMQ, or an external coordinator.

**Status: development implementation, not a production-qualified database.** Functional and failure tests are included. The approximate vector index is experimental; million-vector capacity, sustained mixed-load behavior, upgrades, and fleet-wide resource isolation have not been qualified. See [implementation status](docs/IMPLEMENTATION.md).

## Run locally

Requires Rust 1.87+. Node 22+ is needed for the TypeScript SDK, examples, and dashboard source development, but not for a normal Rust build: the compiled dashboard assets are checked in and embedded in the binary.

```sh
cargo build --release --bin substrate
./target/release/substrate --dev
```

The development listener is restricted to loopback. Its token is `substrate-local-development-token`. State lives in `data/substrate.redb`. For a non-development deployment, set a random `SUBSTRATE_ADMIN_TOKEN` of at least 24 bytes and optionally a separate `SUBSTRATE_SIGNING_KEY`. Keep credentials consistent across nodes. Terminate public TLS at your ingress; internal Raft HTTP must be on a private trusted network or behind TLS.

Open [http://127.0.0.1:7432/dashboard](http://127.0.0.1:7432/dashboard) after startup. The administration dashboard is served by the same Rust process; it does not need a Node server. In development, select **Use local dev token** and then **Connect**. Outside `--dev`, enter the configured administrator token. The token is kept only in the page's memory, so a refresh requires reconnecting. The initial dashboard scope is namespace `app`, partition `default`.

The dashboard is a trusted, local administrator surface, not an application UI and not a hosted control plane. Keep it on loopback (or reach it through a controlled tunnel), and do not expose the administrator token to application browsers. Application frontends must continue to use short-lived scoped browser capabilities. See the [quickstart](docs/QUICKSTART.md) for an end-to-end local session and the [dashboard guide](docs/DASHBOARD.md) for every screen and the frontend build workflow.

```sh
export SUBSTRATE_ADMIN_TOKEN="$(openssl rand -hex 32)"
./target/release/substrate --data ./data/substrate.redb --listen 127.0.0.1:7432
```

Optional local IPC uses the same HTTP protocol and credentials over a Unix socket, created with mode `0600`:

```sh
./target/release/substrate --dev --unix-socket /tmp/substrate.sock
curl --unix-socket /tmp/substrate.sock http://localhost/health
```

A crash may leave a socket pathname. Remove that specific stale socket after confirming its process has stopped; startup never silently replaces an existing pathname.

## Use the native SDK

```sh
npm --prefix sdk ci
npm --prefix sdk run build
```

```ts
import { Substrate } from './sdk/dist/index.js';

const db = new Substrate({
  url: 'http://127.0.0.1:7432',
  token: process.env.SUBSTRATE_ADMIN_TOKEN!,
  namespace: 'shop',
  partition: 'customer-42',
});

await db.collection('orders').saveAndEnqueue(
  'order-123',
  { customer: '42', status: 'pending' },
  { id: 'fulfill-123', queue: 'fulfillment', payload: { order: 'order-123' } },
  { requestId: 'checkout-123' },
);

const controller = new AbortController();
await db.work('fulfillment', async (job, signal) => {
  // Perform an idempotent application operation; honor signal on lease loss.
  console.log(job.payload);
}, { signal: controller.signal });
```

The document and job commit atomically within one namespace/partition. `transaction` also batches up to 1,000 operations within that boundary. There are no cross-partition write transactions. Retry uncertain writes with the same `requestId` and identical payload. Deduplication defaults to 24 hours, not permanent exactly-once execution. Empty worker polls and unchanged maintenance operations do not allocate deduplication records.

See the [SDK reference](sdk/README.md), [server example](examples/server.mjs), [external worker](examples/worker.mjs), and [browser example](examples/browser.html).

## Capabilities

| Area | Implemented behavior |
|---|---|
| Documents | Nested JSON, CRUD, revision CAS, equality filters, secondary/compound/unique indexes, sorting, projection, pagination, atomic batches |
| Jobs | Pull workers, signed HTTP delivery, explicit ack, expiring receipts, heartbeat, retry/backoff, delay/expiry, ordering keys, dead letters, pause/resume |
| Scheduling | Five-field cron, UTC/IANA zones, DST policy, bounded catch-up, durable occurrence IDs, cooperative overlap avoidance |
| Cache | TTL values, counters, hashes, lists, sets, sorted sets, fixed-window rate limits, leases with monotonic fencing tokens |
| Search | Named dense/sparse fields, cosine/dot/Euclidean, BM25, hybrid RRF, metadata equality filters, exact/experimental graph ANN, batch submission, visibility barriers |
| Messaging | Durable exact/wildcard topic routing to queues; ephemeral WebSocket channels, presence, correlated request/reply |
| Realtime | Individual documents and indexed filtered sets, atomic initial snapshot/cursor, bounded result sets, reconnect/resnapshot, scoped credentials and revocation |
| Cluster | Independent fixed shard groups, explicit replica placement, quorum writes/reads, forwarding, leader failover, learner catch-up, operator-controlled membership movement |
| Operations | Checksummed standalone backups, paused restore, logical partition storage limits, retention, metrics, Docker, CI |

Cache operations are durable and replicated with other commands. They are not a wire-compatible Redis implementation or an in-memory Redis performance claim. No user code runs inside Substrate. Browser tokens cannot administer jobs or write documents.

## Browser and service access

Use the admin credential only in trusted server processes. Mint browser capabilities through `db.capability(...)`; grant exact resources and `subscribe`, `publish`, or `presence`. A capability's equality filter is enforced on both initial snapshots and later updates. Filtered subscriptions require a covering index and at most 1,000 results; individual-document watches use the primary key. Tokens expire within one hour and can be revoked explicitly.

The bundled dashboard is the deliberate browser exception: an operator may enter the admin token into that same-origin UI while using the server as a trusted local administration endpoint. The dashboard keeps the token in memory rather than browser storage. This exception does not apply to your product's browser code, copied dashboard builds on another origin, or a dashboard exposed to untrusted users.

Mint narrower server credentials with `db.serviceToken(...)`. These bind one namespace/partition, `read`/`write`, and resources such as `collection:orders`, `queue:fulfillment`, `cache:visits`, or `events`. `*` grants all supported resources within that scope, not cluster administration. Service tokens expire within 24 hours and are revocable. They are not accepted as browser credentials. Administrative maintenance, scheduling administration, routing administration, and cross-partition search currently require the administrator.

Ephemeral messages relay between configured cluster peers. Delivery is best effort; they are not a durable queue. Presence heartbeats run every five seconds and remote entries expire after fifteen seconds. Slow consumers receive a gap signal or disconnect. Use retained events or jobs when loss is unacceptable.

## Search and clusters

See [search semantics and measured benchmarks](docs/SEARCH.md) and [cluster configuration and membership](docs/CLUSTER.md). Exact search is the default. A successful vector submission is not yet searchable: await its visibility sequence. Updating a source document invalidates embeddings from older revisions.

`search.scatter` merges comparable vector scores from an explicit list of up to 64 partitions. Every partition must have the same vector field schema; any shard failure fails the query. It does not provide a global snapshot. Lexical/hybrid search currently stays within a partition.

## Operations

[Verification results](docs/VERIFICATION.md) record the executed checks. [Operations](docs/OPERATIONS.md) describes persistence, backups, defaults, retry semantics and known limits. [Protocol](docs/PROTOCOL.md) documents the command envelope and WebSocket messages.

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo build --locked --bin substrate
npm --prefix sdk ci
npm --prefix sdk test
node scripts/e2e.mjs
node scripts/cluster-e2e.mjs
```

```sh
docker build -t substratedb:dev .
docker run --rm -p 127.0.0.1:7432:7432 \
  -e SUBSTRATE_ADMIN_TOKEN -v substrate-data:/data substratedb:dev
```

## Project

[Architecture](ARCHITECTURE.md), [research](RESEARCH.md), [decisions](docs/adr/), and [build status](docs/IMPLEMENTATION.md) are kept in the repository. Aggregations, joins, a hosted control plane and the Vercel marketplace integration are later work. Apache-2.0; see [LICENSE](LICENSE).
