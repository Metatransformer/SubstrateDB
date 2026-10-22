# Local quickstart

This guide starts one development runtime, exercises its major primitives through the native HTTP protocol, opens the embedded dashboard, runs an external queue worker, and verifies persistence across a restart. It uses an isolated data file so it does not mutate another local SubstrateDB database.

SubstrateDB is a development implementation, not a production-qualified database. It is its own protocol and does not promise full Redis, RabbitMQ, or MongoDB wire/API parity. Jobs and scheduled work run in your application workers, never inside the database process.

## 1. Build and start an isolated runtime

From the repository root:

```sh
cargo build --bin substrate
./target/debug/substrate \
  --dev \
  --data ./data/quickstart.redb \
  --listen 127.0.0.1:7432
```

Leave that terminal running. `--dev` is restricted to a loopback listener and enables the fixed local token `substrate-local-development-token`.

Open [http://127.0.0.1:7432/dashboard](http://127.0.0.1:7432/dashboard), select **Use local dev token**, and select **Connect**. The dashboard is compiled into the Rust binary; no Node server is running. Use namespace `app` and partition `default` throughout this guide.

If `./data/quickstart.redb` did not already exist, it starts empty; an existing path reopens its existing state. SubstrateDB does not create sample collections, queues, cache keys, vectors, schedules, or events. Names are created or used by the commands below; there is no automatic global catalog.

## 2. Prepare a curl helper

Open a second terminal in the repository root. The examples use `jq` to read values such as document revisions and job receipts.

```sh
export SUBSTRATE_URL='http://127.0.0.1:7432'
export SUBSTRATE_ADMIN_TOKEN='substrate-local-development-token'

substrate_command() {
  curl --fail-with-body --silent --show-error \
    -H "authorization: Bearer $SUBSTRATE_ADMIN_TOKEN" \
    -H 'content-type: application/json' \
    --data "$1" \
    "$SUBSTRATE_URL/v1/command"
}

curl --fail-with-body --silent --show-error "$SUBSTRATE_URL/health" | jq
```

Every command envelope selects a namespace and partition. A success is `{"result":...}`; a failure is `{"error":{"code":"...","message":"..."}}`. Supplying a stable `request_id` lets you safely retry an uncertain write with the same action and payload. Reusing that ID for different content returns `conflict`.

## 3. Put and read a document

Create a source document and retain its revision for the vector example:

```sh
DOCUMENT_RESPONSE="$(substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "document.put",
  "request_id": "quickstart-document-put-v1",
  "payload": {
    "collection": "articles",
    "id": "article-1",
    "value": {
      "title": "SubstrateDB quickstart",
      "category": "guide"
    },
    "expected_revision": 0
  }
}')"
printf '%s\n' "$DOCUMENT_RESPONSE" | jq
export DOCUMENT_REVISION="$(printf '%s\n' "$DOCUMENT_RESPONSE" | jq -er '.result.revision')"

substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "document.get",
  "payload": {
    "collection": "articles",
    "id": "article-1"
  }
}' | jq
```

`expected_revision: 0` means create only. Later updates can pass the revision just read to prevent overwriting a concurrent change. A mismatched revision returns HTTP 409 with error code `conflict`.

## 4. Enqueue, claim, and acknowledge a job

SubstrateDB stores and leases jobs. Your worker performs the application side effect. Enqueue one job:

```sh
substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "job.enqueue",
  "request_id": "quickstart-job-enqueue-v1",
  "payload": {
    "id": "welcome-1",
    "queue": "welcome-email",
    "payload": {
      "recipient": "reader@example.test"
    }
  }
}' | jq
```

Claim it and keep both the ID and the receipt. The receipt proves ownership of the current lease; an expired or superseded receipt cannot acknowledge the job.

```sh
CLAIM_RESPONSE="$(substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "job.claim",
  "request_id": "quickstart-job-claim-v1",
  "payload": {
    "queue": "welcome-email",
    "worker": "quickstart-shell",
    "lease_ms": 30000
  }
}')"
printf '%s\n' "$CLAIM_RESPONSE" | jq

export CLAIMED_JOB_ID="$(printf '%s\n' "$CLAIM_RESPONSE" | jq -er '.result.job.id')"
export CLAIM_RECEIPT="$(printf '%s\n' "$CLAIM_RESPONSE" | jq -er '.result.receipt')"

ACK_COMMAND="$(jq -cn \
  --arg id "$CLAIMED_JOB_ID" \
  --arg receipt "$CLAIM_RECEIPT" \
  '{
    namespace:"app",
    partition:"default",
    action:"job.ack",
    request_id:"quickstart-job-ack-v1",
    payload:{id:$id,receipt:$receipt}
  }')"
substrate_command "$ACK_COMMAND" | jq
```

Real workers should heartbeat long-running leases, make external side effects idempotent (the job ID is a useful key), and acknowledge only after durable completion. The TypeScript SDK's `db.work(...)` loop handles claim, heartbeat, ack/nack, and temporary claim retries; see `examples/worker.mjs`.

## 5. Store a cache value with a TTL

Cache commands are durable native operations, not a Redis wire protocol or an in-memory Redis performance claim.

```sh
substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "cache.set",
  "request_id": "quickstart-cache-set-v1",
  "payload": {
    "key": "session:demo",
    "value": {"user":"reader"},
    "ttl_ms": 60000
  }
}' | jq

substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "cache.get",
  "payload": {"key":"session:demo"}
}' | jq

substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "cache.ttl",
  "payload": {"key":"session:demo"}
}' | jq
```

Expired keys become invisible immediately and are reclaimed later by bounded maintenance.

## 6. Configure, submit, and wait for a vector

SubstrateDB stores caller-supplied embeddings; it does not generate them. Configure an immutable three-dimensional dense field:

```sh
substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "vector.configure",
  "request_id": "quickstart-vector-configure-v1",
  "payload": {
    "collection": "articles",
    "name": "embedding-3d",
    "dimensions": 3,
    "metric": "cosine",
    "kind": "dense"
  }
}' | jq
```

Submit a vector tied to the exact source document revision:

```sh
VECTOR_COMMAND="$(jq -cn \
  --argjson revision "$DOCUMENT_REVISION" \
  '{
    namespace:"app",
    partition:"default",
    action:"vector.upsert",
    request_id:"quickstart-vector-upsert-v1",
    payload:{
      collection:"articles",
      name:"embedding-3d",
      id:"article-1",
      vector:[0.1,0.2,0.3],
      source_revision:$revision
    }
  }')"
VECTOR_RESPONSE="$(substrate_command "$VECTOR_COMMAND")"
printf '%s\n' "$VECTOR_RESPONSE" | jq
export VECTOR_SEQUENCE="$(printf '%s\n' "$VECTOR_RESPONSE" | jq -er '.result.sequence')"
```

Submission is durable but indexing is asynchronous. Ask whether that sequence is visible:

```sh
WAIT_COMMAND="$(jq -cn \
  --argjson sequence "$VECTOR_SEQUENCE" \
  '{
    namespace:"app",
    partition:"default",
    action:"search.wait",
    payload:{collection:"articles",sequence:$sequence}
  }')"
substrate_command "$WAIT_COMMAND" | jq
```

If this first attempt returns HTTP 425 with `not_ready`, repeat it after a short delay. A successful result contains `searchable: true` and a resolved sequence at least as large as the submitted sequence. If the source document changes before indexing, the submission is rejected rather than making a stale vector visible.

After visibility succeeds:

```sh
substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "search.query",
  "payload": {
    "collection": "articles",
    "name": "embedding-3d",
    "vector": [0.1,0.2,0.3],
    "limit": 5
  }
}' | jq
```

Exact vector search is the default. Approximate graph search is experimental; see [Search](SEARCH.md).

## 7. Create a schedule and run an external worker

A five-field cron schedule creates durable jobs. It does not execute application code inside SubstrateDB.

```sh
substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "schedule.put",
  "request_id": "quickstart-schedule-put-v1",
  "payload": {
    "id": "quickstart-minute",
    "cron": "* * * * *",
    "timezone": "UTC",
    "missed": "once",
    "allow_overlap": false,
    "job": {
      "queue": "quickstart-cron",
      "payload": {"task":"scheduled-demo"}
    }
  }
}' | jq
```

Build the SDK once, then run the included external worker in another terminal:

```sh
npm --prefix sdk ci
npm --prefix sdk run build

SUBSTRATE_URL='http://127.0.0.1:7432' \
SUBSTRATE_ADMIN_TOKEN='substrate-local-development-token' \
SUBSTRATE_NAMESPACE='app' \
SUBSTRATE_PARTITION='default' \
SUBSTRATE_QUEUE='quickstart-cron' \
node examples/worker.mjs
```

The next occurrence is enqueued on the `quickstart-cron` queue. Press Ctrl-C to stop the example worker. For actual application code, treat scheduled delivery as at least once and make side effects idempotent.

## 8. Mint a realtime browser capability safely

Do not put the administrator token in your product's frontend. A trusted application server uses it to mint a short-lived capability for an authenticated user, then returns only the scoped capability to that browser.

The following command represents that trusted-server step; do not move it into browser JavaScript:

```sh
export CAPABILITY_EXPIRES_AT_MS="$(( $(date +%s) * 1000 + 600000 ))"
CAPABILITY_REQUEST="$(jq -cn \
  --argjson expires "$CAPABILITY_EXPIRES_AT_MS" \
  '{
    id:"quickstart-browser-session",
    namespace:"app",
    partition:"default",
    resources:["articles"],
    permissions:["subscribe"],
    expires_at_ms:$expires
  }')"

curl --fail-with-body --silent --show-error \
  -H "authorization: Bearer $SUBSTRATE_ADMIN_TOKEN" \
  -H 'content-type: application/json' \
  --data "$CAPABILITY_REQUEST" \
  "$SUBSTRATE_URL/v1/capabilities" | jq
```

Capabilities expire within one hour, can be revoked by ID, and grant only their exact resources and realtime permissions. See the [WebSocket protocol](PROTOCOL.md#websockets), the [credential rules](PROTOCOL.md#credentials), and `examples/server.mjs` for an application-server token endpoint. The dashboard's same-origin, local administrator login is a separate trusted-operator exception; it is not the pattern for an application browser.

## 9. Stop, restart, and verify persistence

Stop the runtime cleanly with Ctrl-C in its terminal. Start the same binary with the same data path:

```sh
./target/debug/substrate \
  --dev \
  --data ./data/quickstart.redb \
  --listen 127.0.0.1:7432
```

Then read the document again from the second terminal:

```sh
substrate_command '{
  "namespace": "app",
  "partition": "default",
  "action": "document.get",
  "payload": {
    "collection": "articles",
    "id": "article-1"
  }
}' | jq
```

The document, vector state, jobs, schedule, unexpired cache state, and retained events live in `./data/quickstart.redb`. A cache item whose TTL elapsed while the process was stopped is expired when read.

Only one process can open a standalone database path at a time. If startup reports that the database is locked or already open, stop the process using that exact file; do not delete a live database or lock file. A different `--data` path creates an independent empty database, so starting with `./data/other.redb` will not show the quickstart data.

## Next steps

- [Dashboard](DASHBOARD.md) explains every administration screen and its security model.
- [Protocol](PROTOCOL.md) lists command envelopes and response semantics.
- [SDK reference](../sdk/README.md) covers typed application access and external workers.
- [Operations](OPERATIONS.md) covers backups, limits, shutdown, and production qualification gaps.
