# Embedded administration dashboard

SubstrateDB includes a same-origin administration dashboard at `/dashboard`. A release build serves its static HTML, JavaScript, CSS, and documentation from the Rust binary at the same address as the HTTP API—for the default local listener, [http://127.0.0.1:7432/dashboard](http://127.0.0.1:7432/dashboard). Running or deploying the compiled binary does not require a separate Node/Next.js server.

The dashboard is an operator console for the current development implementation. It is not a hosted control plane, does not make the database production-qualified, does not provide complete Redis/RabbitMQ parity, and does not run job handler code inside SubstrateDB.

## Start and connect

For a persistent local development session:

```sh
cargo build --bin substrate
./target/debug/substrate --dev --listen 127.0.0.1:7432
```

Open `/dashboard`, select **Use local dev token**, then select **Connect**. That shortcut uses the fixed `substrate-local-development-token` enabled by `--dev`.

Outside development, start the loopback listener with a configured administrator token:

```sh
export SUBSTRATE_ADMIN_TOKEN="$(openssl rand -hex 32)"
./target/debug/substrate --listen 127.0.0.1:7432 --data ./data/substrate.redb
```

Enter the same token in the connection panel and select **Connect**. The token is kept in JavaScript memory only: it is not written to local storage, session storage, a cookie, or the URL. Refreshing or closing the page clears it and requires another connection.

Connection tests and all later commands use the same origin that served the page. The dashboard is intentionally a trusted local administrator surface. `--dev` refuses a non-loopback listener; for non-development operation, keep this admin interface on loopback or reach it through a controlled local tunnel. The public listener has no built-in TLS termination, so do not expose the dashboard or an administrator token to an untrusted network.

### Dashboard administrator versus application browser

An operator entering an admin token into the bundled, same-origin local dashboard is a narrow, deliberate exception to the normal browser rule. Product frontends must never copy this pattern. They receive short-lived capabilities minted by trusted application code through `/v1/capabilities`, limited to exact namespace, partition, resources, permissions, and expiry. Application browser capabilities can subscribe, publish, or use presence; they cannot administer documents, jobs, schedules, routing, or the cluster. See [Protocol: Credentials](PROTOCOL.md#credentials).

## Scope and names

The scope bar begins with namespace `app` and partition `default`. Almost every dashboard command includes those two values. To change scope, disconnect, edit namespace or partition, and connect again; the fields are locked while connected so an operation cannot silently move between scopes. Changing either value selects different logical data—it does not copy or move anything. Transactions cannot cross a namespace/partition boundary.

SubstrateDB does not maintain a global catalog of every collection, queue, cache key, vector field, or topic. Enter names explicitly in the relevant screen. An empty screen in a new scope is normal: neither the runtime nor the dashboard seeds example data. Schedules, retained events, and indexes can be listed within the selected scope where the native protocol provides a list operation, but that is not global discovery.

All dedicated operational controls act inside the connected namespace/partition. Operational metrics and the cluster shard information they may contain are the exception: they describe the connected node/runtime or its local cluster view rather than only the selected scope. The dashboard does not provide Raft membership controls.

## Forms, JSON, and results

Feature screens provide focused forms for common commands. Names and scalar options use form fields; document values, job payloads, filters, vectors, and other structured values use JSON editors. JSON must be syntactically valid and match the native payload shape. Numbers, booleans, arrays, objects, and `null` remain JSON types rather than strings.

Each submission shows the returned JSON result. Server failures remain structured as:

```json
{
  "error": {
    "code": "conflict",
    "message": "..."
  }
}
```

Dashboard submissions generate a fresh request ID for each click. For an ordinary interactive operation this keeps retries independent, but it does not provide a stable ID for resolving an ambiguous timeout. When a write must be safely retryable, use the SDK or curl and reuse an explicit request ID with the same action and payload. Reusing an ID for different content returns `conflict`. Read [Protocol](PROTOCOL.md) before using the low-level console for application logic.

## Screens

### Overview

Overview checks the connection and summarizes the selected scope with `scope.stats`, including logical stored bytes and configured limits. Runtime health, metrics, and cluster information describe the connected server, not only the selected scope. Logical bytes are accounting data and do not equal the physical redb file size.

Use Overview to confirm all three independent pieces of state before changing data:

1. the server origin and authenticated connection;
2. the namespace and partition in the scope bar;
3. the runtime/node information returned by the server.

### Documents

Documents operates on an explicitly named collection. It supports document reads, equality-filtered queries, puts, deletes, revision compare-and-set, index administration, and a live document watch.

- Put accepts an ID and a JSON value. Use `expected_revision: 0` for create-only, or the current positive revision for a guarded update.
- Get reads one explicit collection/ID.
- Query accepts an equality filter (including dotted nested paths) and a limit. Use API Console for protocol options that do not have dedicated fields, such as sort, projection, and offset. It is not an aggregation or join interface.
- Delete can include an expected revision.
- Index definitions are named and immutable. Creating an already used index name returns `conflict`; choose a new name for a different definition.

The result contains the authoritative revision. Preserve it when the next operation—particularly a vector submission—must refer to that exact document version.

**Live watch** mints a one-hour subscribe-only browser capability for the entered collection, then opens `/v1/ws`; the administrator token is not sent over the WebSocket. With a Document ID entered, the watch follows that single document. Otherwise the current equality filter is used for the subscription. Filtered watches require a covering index and are bounded to 1,000 documents. Stop the watch when finished; switching scope requires disconnecting first.

### Queues

Queues works with an explicitly entered queue or job ID. **Refresh queues** lists queue names already recorded in the selected scope; it is not cross-scope or cluster-wide discovery. The screen can enqueue, list/filter, get, retry a dead letter, cancel, pause, and resume. Lists are explicitly refreshed and capped at 100 jobs.

Claim, ack, nack, and heartbeat are intentionally left to real workers or API Console. A claim result contains both `job` and `receipt`; ack, nack, and heartbeat require that current receipt. A stale, expired, or superseded receipt returns `conflict`.

SubstrateDB supplies durable, at-least-once coordination. It does not execute the job payload. Normal processing belongs in an external worker such as the SDK `db.work(...)` loop in `examples/worker.mjs`. Use the job ID as an idempotency key for application side effects.

### Cache

Cache accepts an explicit key, JSON value, and optional TTL. Its focused controls set, get, read TTL, and delete. Advanced native operations—counters, hashes, sets, lists, sorted sets, leases, and fixed-window rate limits—are available through API Console.

TTL values are milliseconds. Expired values are invisible immediately even if physical cleanup has not yet reclaimed them. A key has one native value type at a time; using a hash/list/set operation against another type is rejected. Lease tokens are monotonically increasing fencing values, but an external protected resource must enforce the fence itself.

These primitives are durably stored and replicated like other commands. The screen is not a Redis client, and the runtime does not claim Redis wire compatibility or in-memory latency.

### Search

Search requires an explicit collection and, for vectors, an explicit named field.

1. Put the source document and note its revision.
2. Configure a dense vector field with dimensions and metric. A three-dimensional example uses dimensions `3` and metric `cosine`; use API Console for sparse fields, which require the `dot` metric.
3. Upsert a vector with the source document ID and exact `source_revision`.
4. Note the returned sequence. Submission is durable, but `searchable` is initially false.
5. Select **Check visibility** to call `vector.status`, then compare its resolved `sequence` with the sequence returned by the upsert. For a strict one-sequence barrier, run `search.wait` with `{ "collection": "...", "sequence": 42 }` in API Console. `not_ready` means indexing has not crossed that sequence yet.
6. Run a vector, text, or hybrid query.

The runtime generates no embeddings. If the document changes or is deleted before a queued vector is indexed, the stale submission is rejected. Updating a document later also prevents its older-revision vector from appearing in results. Exact vector search is the default; the persisted approximate graph is experimental. See [Search](SEARCH.md) for schema, visibility, and ranking details.

### Schedules

Schedules lists and administers cron records inside the selected scope. The focused create form accepts a five-field cron expression, queue, JSON job payload, timezone, and missed-run policy. It uses a catch-up bound of 100, disables overlap, and selects the first repeated DST occurrence. Use API Console when another supported catch-up, overlap, or DST policy is required. The job template has no job ID or absolute availability/expiry; the scheduler derives occurrence IDs and times.

List, pause/resume, and delete have dedicated controls; `schedule.get` is available through API Console. When due, the runtime enqueues a job. An external worker must claim and execute it; the scheduler does not embed your handler. Restored schedules are paused until an operator reconciles external effects and resumes them.

### Events

Events reads the selected partition's retained ordered event stream. Start with an `after` sequence and bounded limit; the response reports the latest and oldest retained positions. Refresh never advances the entered cursor automatically. After inspecting a page, **Advance cursor to latest** copies the returned latest value into the field for the next read. A `history_unavailable` error means the requested cursor predates retained history and requires a new snapshot/reconciliation—it is not an empty successful replay.

Named `consumer.read`/`consumer.ack` cursors are available through API Console or the SDK. Retained events are the tool for processing every durable change. Realtime WebSocket channels are best effort and may coalesce or signal gaps, so they are not a replacement when loss is unacceptable.

### API Console

API Console can send every native command, including operations that do not have a dedicated form. Choose an editable preset or enter the exact action name and its JSON payload; the console adds the currently selected namespace and partition, creates a fresh request ID, and displays the result or structured error. The 1 MiB HTTP command-body limit still applies.

The console is powerful enough to mutate or delete data. Consult the embedded [Protocol](PROTOCOL.md) and feature guides first. Routine application clients should not call internal maintenance commands such as `vector.apply`, `vector.flush`, `schedule.tick`, or `scope.maintain`; the runtime owns those background operations. Every console request still carries the connected namespace and partition. Cluster membership uses separate authenticated Raft HTTP routes and is not a dashboard control.

### Documentation

Documentation contains a concise in-console operator guide and links to the Markdown sources compiled into the running binary. The available source routes are:

| Route | Source |
|---|---|
| `/dashboard/docs/README.md` | Project overview |
| `/dashboard/docs/QUICKSTART.md` | End-to-end local walkthrough |
| `/dashboard/docs/DASHBOARD.md` | This dashboard guide |
| `/dashboard/docs/PROTOCOL.md` | HTTP/JSON and WebSocket protocol |
| `/dashboard/docs/OPERATIONS.md` | Durability, limits, backup, and security |
| `/dashboard/docs/SEARCH.md` | Vector, lexical, and hybrid search |
| `/dashboard/docs/CLUSTER.md` | Shards, replication, and membership |
| `/dashboard/docs/IMPLEMENTATION.md` | Implementation and qualification status |
| `/dashboard/docs/SDK.md` | TypeScript SDK reference |

Those paths return the embedded Markdown; `/dashboard/docs/` opens the embedded README. Because docs are binary inputs, editing a source Markdown file requires rebuilding the Rust binary before the running dashboard can serve the new text.

## Frontend development and embedding

The dashboard source is a Next.js static-export application under `dashboard/`. To reproduce the checked-in bundle:

```sh
npm --prefix dashboard ci
npm --prefix dashboard run build
cargo build --bin substrate
```

The dashboard build performs the static export and bundles the deployable files under `dashboard/dist`. The Rust build embeds that bundle plus the documentation listed above. Bundled assets are checked into the repository, so users building only the Rust binary do not need Node or npm. Node is required only when changing/rebuilding the frontend sources.

Use the Rust-served `/dashboard` for final verification because it exercises the production base path, same-origin API requests, embedded asset lookup, security headers, and embedded docs. After frontend or documentation changes, rerun `cargo build --bin substrate` and restart the binary; an already running process cannot acquire newly compiled assets.

## Troubleshooting

### 401 Unauthorized

- In `--dev`, select **Use local dev token** before **Connect**.
- Outside `--dev`, enter the exact `SUBSTRATE_ADMIN_TOKEN` used to start the process.
- A refresh clears the in-memory token, so reconnect.
- A browser capability or service token is not an administrator token and cannot power the admin dashboard.
- Confirm that the page and API are the same server origin; do not serve a copied dashboard from another web host.

### 409 Conflict

Read the error message. Common causes are a stale document `expected_revision`, a stale/expired queue receipt, reuse of a request ID with different content, a duplicate job ID with different input, or an attempt to change an immutable named vector/index configuration. Reload the current record and resolve the semantic conflict rather than blindly retrying different content under the same request ID.

### 425 / `not_ready`

Vector submission and search visibility are separate. Keep the returned submission sequence and retry `search.wait` after a short delay. If wait returns a different error, the indexed item may have been rejected—for example, because its `source_revision` became stale—and changing the delay will not fix it.

### Busy or overloaded database

The runtime bounds concurrent work and may return HTTP 429 with `overloaded`; back off before retrying. Large scans or graph updates can delay other commands. This development implementation has not completed fleet-wide resource-isolation or sustained mixed-load qualification.

If the process itself will not start because the redb file is locked/already open, another process owns that exact `--data` path. Stop the owning process cleanly. Do not delete a live database or lock artifact. A different path is an independent database and will appear empty.

### Dashboard or documentation is stale/missing

The server does not read dashboard assets or docs dynamically from the working tree. Rebuild the frontend if its source changed, rebuild the Rust binary for frontend or docs changes, then restart that binary. A Rust build uses the checked-in bundle and needs no Node unless you changed frontend source.

### Data appears missing

Verify namespace, partition, and `--data` path. `app/default` and `app/other` are separate logical scopes; `./data/substrate.redb` and `./data/quickstart.redb` are separate physical databases. The dashboard does not auto-discover or seed names, so also verify the explicit collection, queue, cache key, vector field, or consumer name.

### Remote access

Do not solve remote access by binding a development server publicly: `--dev` rejects it, and the listener does not terminate TLS. Keep the administrator dashboard local. If remote operations are unavoidable, use a controlled tunnel to a loopback listener and follow the network and credential guidance in [Operations](OPERATIONS.md#security-and-networking).

## Verification

After building the frontend and Rust binary, run the browser regression against an isolated temporary database:

```sh
npm --prefix dashboard exec -- playwright install chromium
npm --prefix dashboard test
```

The test covers authentication, document edits and live watches, queues, cache, vector search, schedules, events, the console, offline documentation, scope switching, mobile layout, and clearing credentials on reload. It closes its own runtime and removes its temporary data.
