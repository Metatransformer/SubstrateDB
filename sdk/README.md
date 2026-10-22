# @substratedb/client

Typed TypeScript client for SubstrateDB commands, pull workers, signed HTTP delivery, and scoped realtime connections.

```js
import { Substrate } from '@substratedb/client';

const db = new Substrate({
  url: process.env.SUBSTRATE_URL ?? 'http://127.0.0.1:7432',
  token: process.env.SUBSTRATE_ADMIN_TOKEN,
  namespace: 'demo',
  partition: 'default',
});

const document = await db.collection('items').put('one', { title: 'Example' });
const indexed = await db.vectors.upsert('items', document.id, 'embedding', [0.2, 0.8], document.revision);
await db.vectors.wait('items', indexed.sequence);
```

`vector.upsert` is asynchronous: its sequence is submitted to the indexer, and `vectors.wait` polls `search.wait` until that sequence is searchable or the timeout is reached.

## Cache and messages

The cache client follows the server's native structures: `hset`/`hget`/`hdel`, `sadd`/`srem`/`members`, `lpush`/`rpush`/`lpop`/`rpop`/`range`, and `zadd`/`zrem`/`zrange`. Route helpers map directly to `route.bind`, `route.unbind`, `route.list`, and `message.publish`.

```js
await db.messaging.bind({ id: 'orders', topic: 'orders.*', queue: 'dispatch' });
await db.messaging.publish({ id: crypto.randomUUID(), topic: 'orders.created', payload: { orderId: '42' } });
```

## External workers

Substrate stores and leases jobs; it does not embed or run application code. `work` claims a job, renews its lease while your handler runs, and acknowledges only after the handler succeeds while the lease remains valid.

```js
const stop = new AbortController();
process.once('SIGINT', () => stop.abort());

await db.work('dispatch', async (job, signal) => {
  await deliver(job.payload, { signal });
}, {
  signal: stop.signal,
  onError: console.error,
});
```

Claim transport failures retry with capped exponential backoff. A heartbeat failure aborts the handler signal and suppresses both acknowledgement and stale-receipt nack. Delivery is at least once: handlers must make externally visible side effects idempotent, normally by using the job ID as an idempotency key.

## Signed HTTP delivery

Verify the signature against the exact raw request body before parsing it. The helper uses Web Crypto, matches `HMAC-SHA256(timestamp + "." + jobId + "." + body)`, compares signature bytes without an early exit, defaults to a five-minute maximum age, and rejects timestamps over 30 seconds in the future.

```js
import { verifyJobSignature } from '@substratedb/client';

const valid = await verifyJobSignature({
  secret: process.env.SUBSTRATE_SIGNING_KEY,
  signature: request.headers['substrate-signature'],
  timestamp: request.headers['substrate-timestamp'],
  jobId: request.headers['substrate-job-id'],
  body: rawBody,
  futureToleranceMs: 30_000,
});
```

Keep the signing secret on the server. It is unrelated to the scoped capability returned to a browser.

## Realtime

Create capabilities on a trusted application server using the admin credential, then return only the short-lived scoped token to the browser. `Realtime.watch` supplies typed document watch options. The server supports one active live-document subscription or channel per connection.

```js
import { Realtime } from '@substratedb/client';

const live = Realtime.watch({
  url: 'http://127.0.0.1:7432',
  token: async () => (await fetch('/token')).json().then(value => value.token),
  onMessage: console.log,
}, 'items', { filter: { tenant: 'demo' } });
```

Filtered document watches require a matching equality index on the filtered fields.

For channels, `request(topic, payload, { timeoutMs })` publishes a correlation ID and resolves when a matching `reply` arrives. Replies still reach `onMessage`. Pending requests reject on timeout or connection close, and reconnect/auth refresh re-sends the original subscription. A responder can call `reply(topic, correlationId, payload)`.

See `examples/server.mjs`, `examples/worker.mjs`, and `examples/browser.html` for a local end-to-end setup. Build first with `npm --prefix sdk run build`, start Substrate with `--dev`, and then run the examples from the repository root.

## Scoped services, batches and coordination

`serviceToken` mints a server credential for one namespace/partition, exact typed resources (`collection:orders`, `queue:email`, `cache:counter`, `events`) and `read`/`write` permissions. It requires the administrator and is distinct from browser capabilities. Pass its returned token into a new `Substrate` client. Use `revoke(id)` from the administrator to invalidate it.

`transaction([{action,payload}, ...], requestId)` batches up to 1,000 operations atomically within the client's scope. `vectors.upsertBatch(collection,name,items)` atomically submits derived vectors; wait on returned sequences before relying on visibility. `stats()` returns the scope's logical storage usage and configured limits.

`cache.acquireLease(key,owner,ttlMs)` returns an acquisition flag and monotonic fencing token. Renewal/release require that owner and token; old receipts cannot release a new owner's lease. External protected resources must enforce fencing tokens themselves. `cache.rateLimit(key,limit,windowMs)` implements an atomic fixed-window limit. These operations use durable storage rather than claiming Redis-equivalent latency.

`Realtime.watch(options,collection,{id})` watches an individual document without a secondary index. A connection can hold one document watch and one channel; subscriptions of the same type replace the previous one. Local examples are demonstrations: authenticate the application user and derive their scope on the server before exposing `/token` outside local development.
