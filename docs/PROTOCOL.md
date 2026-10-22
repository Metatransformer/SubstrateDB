# Native protocol v0.1

This is Substrate's own HTTP/JSON protocol; it does not emulate MongoDB, AMQP, RESP or ZeroMQ clients. Prefer the typed SDK for application code. APIs may change before a stable release.

## Commands

`POST /v1/command`, `Authorization: Bearer <admin-or-service-token>`:

```json
{
  "namespace": "shop",
  "partition": "customer-42",
  "action": "document.put",
  "request_id": "stable-retry-id",
  "payload": {
    "collection": "orders",
    "id": "order-123",
    "value": {"status": "pending"},
    "expected_revision": 0
  }
}
```

The server supplies operation time and supplies a UUID if `request_id` is absent. To resolve an ambiguous write, the client must supply and reuse its own request ID. Success is `{"result": ...}`; failure is `{"error":{"code":"conflict","message":"..."}}`. A request ID reused with a different action/payload conflicts. Scope is part of the deduplication boundary. Successful replicated writes mean quorum acceptance; timeouts do not prove non-commit.

| Commands | Payload keys |
|---|---|
| `document.put/get/delete` | `collection`, `id`; put: `value`; put/delete: optional `expected_revision` |
| `document.query` | `collection`, optional `filter`, `sort`, `projection`, `offset`, `limit` |
| `document.commit` | `document` put payload and `job` enqueue payload |
| `transaction` | `commands`: up to 1,000 `{action,payload}` entries; one scope, atomic rollback |
| `index.create/list` | `collection`; create: `name`, `fields`, optional `unique` |
| `job.enqueue` | `id`, `queue`, `payload`, optional delay/lease/attempt/expiry/ordering/delivery settings |
| `job.claim` | `queue`, `worker`, optional `lease_ms`; result has `job` or null and `receipt` |
| `job.ack/nack/heartbeat` | `id`, `receipt`; nack: `retryable`, `delay_ms`; heartbeat: `lease_ms` |
| `job.get/list/cancel/retry` | `id`, or list filters `queue`, `status`, `limit` |
| `job.pause/resume` | `queue` |
| `route.bind/unbind/list` | bind: `id`, `topic`, `queue`, optional `delivery`; unbind: `id` |
| `message.publish` | `id`, `topic`, `payload`, optional delivery timing/ordering settings |
| `event.read` | `after`, `limit`; reports `latest` and `oldest` |
| `consumer.read/ack` | `name`, optional read `limit`; ack `sequence` |
| `cache.*` | `key` plus operation-specific arguments; see SDK |
| `schedule.put` | `id`, `cron`, `job`; optional `timezone`, `missed`, `max_catch_up`, `allow_overlap`, `dst_repeat` |
| `schedule.get/pause/resume/delete/list` | `id` except list |
| `vector.configure/describe` | `collection`, `name`; configure: `dimensions`, `metric`, `kind` |
| `vector.upsert/upsert_batch/delete/status` | See [search](SEARCH.md) |
| `search.query/wait` | See [search](SEARCH.md) |
| `search.scatter` | `partitions` and vector-only `query`; admin only, merges top results |
| `scope.configure/stats` | Configurable `max_bytes`, retention durations in milliseconds |
| `auth.revoke` | capability/service `id`, within its namespace/partition |

Routine application clients should not invoke `vector.apply/flush`, `schedule.tick`, or `scope.maintain`; those are runtime maintenance operations. Index definitions are immutable by name. Filters currently use equality, including nested dotted paths; range predicates, aggregations, joins and sorted top-N live subscriptions are not implemented.

Durable topic matching uses dot-separated segments: `*` matches one segment, terminal `#` matches zero or more. A message routes once per matched queue even if multiple bindings match. No match returns `unroutable`. Retained events are per-partition ordered history; `history_unavailable` means the cursor predates retention, never an empty successful replay. Event trimming does not discard pending jobs.

## WebSockets

Connect to `/v1/ws`. Within ten seconds send:

```json
{"type":"auth","token":"application-issued-browser-capability"}
```

The response is `authenticated`. Then choose a live query or channel:

```json
{"type":"subscribe","collection":"orders","filter":{"customer":"42"}}
{"type":"subscribe","collection":"orders","id":"order-123"}
{"type":"channel","topic":"orders.*"}
```

A socket has one document subscription and one channel subscription. Additional messages replace the corresponding subscription. The document stream begins with `snapshot` containing `documents` and `sequence`, followed by `changes` containing `added`, `updated`, `removed` IDs and a sequence. Intermediate mutations may coalesce into current state; use retained events when every event matters. Reconnect takes a new snapshot. History gaps signal `resnapshot_required` rather than silently dropping durable updates.

Channel commands are `publish`, `request`, or `reply`, with `topic`, `payload` and optional `correlation_id`. A request does not imply durable delivery or automatic server execution; another connected application responds. The SDK applies request timeouts and rejects pending requests on disconnect. Channel `presence` messages carry `topic` and `state`; subscribers receive membership updates and leaves. `ping` returns `pong`.

`refresh` with a new token revalidates identity and clears subscriptions/presence; the SDK resubscribes after `authenticated`. `gap` identifies ephemeral buffer loss. Invalid/expired/revoked credentials disconnect. Token resources are independently checked; publishing a wildcard-looking string does not grant access to other topics.

## Credentials

Admin-only `POST /v1/capabilities` accepts browser claims: `id`, `namespace`, `partition`, `resources`, `permissions`, `expires_at_ms`, optional equality `filter`. Permissions are `subscribe`, `publish`, `presence`; maximum lifetime is one hour.

Admin-only `POST /v1/service-tokens` accepts the same scope/identity fields, `read`/`write` permissions and typed resource strings, with no filter. Maximum lifetime is 24 hours. Returned service tokens begin `svc.` and grant no browser operations. A service credential cannot administer other credentials, retention/limits, schedules or routing. The token issuer remains application code; no separate auth service is required by Substrate.

## Signed HTTP jobs

Delivery sends the JSON job payload, `Content-Type: application/json`, and:

- `Substrate-Job-Id`
- `Substrate-Timestamp` (Unix milliseconds)
- `Substrate-Signature` (unpadded base64url HMAC-SHA256)

Sign the exact bytes `timestamp + "." + job_id + "." + raw_body` using the configured signing key. `verifyJobSignature` in the SDK verifies bytes and timestamp freshness. If `SUBSTRATE_SIGNING_KEY` is omitted, the key is `substrate-signing-v1:` followed by the admin token. Use a separate secret for production endpoints. Verify before parsing/acting, and deduplicate by job ID. Return a completed 2xx response only after durable handler completion; 202 causes retry.
