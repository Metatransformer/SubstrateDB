import { createHmac } from 'node:crypto';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Realtime, Substrate, SubstrateError, verifyJobSignature } from '../dist/index.js';

test('scope and stable request IDs are transmitted without losing JSON values', async () => {
  let request;
  const db = new Substrate({ url: 'http://localhost:7432', token: 'test', namespace: 'one', fetch: async (_url, init) => {
    request = JSON.parse(init.body);
    return Response.json({ result: { ok: true } });
  } });
  await db.scope('two', 'partition').command('cache.set', { key: 'a', value: false }, { requestId: 'stable' });
  assert.equal(request.namespace, 'two');
  assert.equal(request.partition, 'partition');
  assert.equal(request.request_id, 'stable');
  assert.equal(request.payload.value, false);
});

test('structured errors retain their codes', async () => {
  const db = new Substrate({ url: 'http://localhost', token: 'x', namespace: 'n', fetch: async () => Response.json({ error: { code: 'conflict', message: 'revision changed' } }, { status: 409 }) });
  await assert.rejects(db.collection('c').get('x'), error => error instanceof SubstrateError && error.code === 'conflict' && error.status === 409);
});

test('response envelopes require an own result property while preserving null and false', async () => {
  const responses = [{ wrong: true }, { result: null }, { result: false }];
  const db = new Substrate({ url: 'http://localhost', token: 'x', namespace: 'n', fetch: async () => Response.json(responses.shift()) });
  await assert.rejects(db.command('document.get', {}), error => error.code === 'invalid_response');
  assert.equal(await db.command('cache.get', {}), null);
  assert.equal(await db.command('cache.get', {}), false);
});

test('cache, messaging, and vector helpers match command payload contracts', async () => {
  const requests = [];
  const results = {
    'cache.hset': { a: false },
    'cache.range': [1, 2],
    'cache.zrange': [{ member: 'a', score: 2 }],
    'route.bind': { id: 'orders', topic: 'orders.*', queue: 'dispatch', delivery: null },
    'route.unbind': { deleted: true },
    'route.list': { bindings: [] },
    'message.publish': { id: 'm1', jobs: ['message:m1:orders'] },
    'vector.delete': { deleted: true },
    'vector.status': { sequence: 4, submitted: 6, searchable: true },
  };
  const db = new Substrate({ url: 'http://localhost', token: 'x', namespace: 'n', fetch: async (_url, init) => {
    const request = JSON.parse(init.body);
    requests.push(request);
    return Response.json({ result: results[request.action] });
  } });

  assert.deepEqual(await db.cache.hset('h', 'a', false), { a: false });
  assert.deepEqual(await db.cache.range('l', { start: 2, limit: 5 }), [1, 2]);
  assert.deepEqual(await db.cache.zrange('z', { limit: 3, descending: true }), [{ member: 'a', score: 2 }]);
  await db.messaging.bind({ id: 'orders', topic: 'orders.*', queue: 'dispatch' });
  await db.messaging.unbind('orders');
  await db.messaging.list();
  await db.messaging.publish({ id: 'm1', topic: 'orders.created', payload: { order: 1 }, max_attempts: 4 });
  await db.vectors.delete('items', 'a', 'embedding');
  assert.deepEqual(await db.vectors.status('items', 4), { sequence: 4, submitted: 6, searchable: true });

  assert.deepEqual(requests.map(({ action, payload }) => ({ action, payload })), [
    { action: 'cache.hset', payload: { key: 'h', field: 'a', value: false } },
    { action: 'cache.range', payload: { key: 'l', start: 2, limit: 5 } },
    { action: 'cache.zrange', payload: { key: 'z', limit: 3, descending: true } },
    { action: 'route.bind', payload: { id: 'orders', topic: 'orders.*', queue: 'dispatch' } },
    { action: 'route.unbind', payload: { id: 'orders' } },
    { action: 'route.list', payload: {} },
    { action: 'message.publish', payload: { id: 'm1', topic: 'orders.created', payload: { order: 1 }, max_attempts: 4 } },
    { action: 'vector.delete', payload: { collection: 'items', id: 'a', name: 'embedding' } },
    { action: 'vector.status', payload: { collection: 'items', sequence: 4 } },
  ]);
});

test('signed HTTP helper matches server HMAC and rejects tampering, stale, and future requests', async () => {
  const secret = 'signing-secret';
  const timestamp = 1_700_000_000_000;
  const jobId = 'job-1';
  const body = '{"hello":"world"}';
  const signature = createHmac('sha256', secret).update(`${timestamp}.${jobId}.${body}`).digest('base64url');
  const input = { secret, signature, timestamp, jobId, body, nowMs: timestamp + 1_000 };

  assert.equal(await verifyJobSignature(input), true);
  assert.equal(await verifyJobSignature({ ...input, body: `${body} ` }), false);
  assert.equal(await verifyJobSignature({ ...input, signature: `${signature.slice(0, -1)}A` }), false);
  assert.equal(await verifyJobSignature({ ...input, nowMs: timestamp + 300_001 }), false);
  assert.equal(await verifyJobSignature({ ...input, timestamp: timestamp + 30_001, nowMs: timestamp }), false);
  const futureTimestamp = timestamp + 30_001;
  const futureSignature = createHmac('sha256', secret).update(`${futureTimestamp}.${jobId}.${body}`).digest('base64url');
  assert.equal(await verifyJobSignature({ ...input, timestamp: futureTimestamp, signature: futureSignature, nowMs: timestamp, futureToleranceMs: 31_000 }), true);
});

test('worker retries temporary claim failures with capped abortable backoff', async () => {
  const controller = new AbortController();
  let claims = 0;
  const db = new Substrate({ url: 'http://localhost', token: 'x', namespace: 'n', fetch: async (_url, init) => {
    const { action } = JSON.parse(init.body);
    assert.equal(action, 'job.claim');
    claims += 1;
    if (claims === 1) return Response.json({ error: { code: 'overloaded', message: 'busy' } }, { status: 503 });
    controller.abort();
    return Response.json({ result: { job: null } });
  } });

  await db.work('q', async () => {}, { signal: controller.signal, claimRetryInitialMs: 1, claimRetryMaxMs: 2 });
  assert.equal(claims, 2);
});

test('worker never acknowledges or nacks after losing its lease', async () => {
  const controller = new AbortController();
  const actions = [];
  let claims = 0;
  const db = new Substrate({ url: 'http://localhost', token: 'x', namespace: 'n', fetch: async (_url, init) => {
    const { action } = JSON.parse(init.body);
    actions.push(action);
    if (action === 'job.claim') {
      claims += 1;
      if (claims > 1) {
        controller.abort();
        return Response.json({ result: { job: null } });
      }
      return Response.json({ result: { job: { id: 'j1', queue: 'q', payload: null, status: 'leased', attempt: 1 }, receipt: 'r1' } });
    }
    if (action === 'job.heartbeat') return Response.json({ error: { code: 'conflict', message: 'lease lost' } }, { status: 409 });
    return Response.json({ result: {} });
  } });

  await db.work('q', (_job, signal) => new Promise(resolve => signal.addEventListener('abort', resolve, { once: true })), { signal: controller.signal, leaseMs: 100, idleMs: 1 });
  assert.ok(actions.includes('job.heartbeat'));
  assert.equal(actions.includes('job.ack'), false);
  assert.equal(actions.includes('job.nack'), false);
});

test('realtime request/reply uses correlation IDs, preserves callbacks, times out, and rejects on close', async () => {
  const originalWebSocket = globalThis.WebSocket;
  class FakeWebSocket {
    static OPEN = 1;
    static instances = [];
    readyState = 0;
    sent = [];
    onopen = null;
    onmessage = null;
    onerror = null;
    onclose = null;
    constructor(url) { this.url = String(url); FakeWebSocket.instances.push(this); }
    send(value) { this.sent.push(JSON.parse(value)); }
    open() { this.readyState = FakeWebSocket.OPEN; this.onopen?.({}); }
    message(value) { this.onmessage?.({ data: JSON.stringify(value) }); }
    close() { this.readyState = 3; this.onclose?.({}); }
  }
  globalThis.WebSocket = FakeWebSocket;
  const observed = [];
  const realtime = Realtime.watch({ url: 'http://localhost:7432', token: async () => 'scoped', onMessage: message => observed.push(message) }, 'items', { filter: { tenant: 'a' } });
  try {
    await new Promise(resolve => setImmediate(resolve));
    const socket = FakeWebSocket.instances[0];
    socket.open();
    socket.message({ type: 'authenticated' });
    assert.deepEqual(socket.sent.slice(0, 2), [
      { type: 'auth', token: 'scoped' },
      { type: 'subscribe', collection: 'items', filter: { tenant: 'a' } },
    ]);

    const response = realtime.request('rpc.echo', { value: false }, { timeoutMs: 100 });
    const request = socket.sent.at(-1);
    assert.equal(request.type, 'request');
    assert.equal(request.topic, 'rpc.echo');
    assert.deepEqual(request.payload, { value: false });
    assert.equal(typeof request.correlation_id, 'string');
    socket.message({ type: 'reply', topic: 'rpc.echo', correlation_id: request.correlation_id, payload: { echoed: false } });
    assert.deepEqual(await response, { echoed: false });
    assert.equal(observed.at(-1).type, 'reply');

    await assert.rejects(realtime.request('rpc.slow', null, { timeoutMs: 5 }), error => error.code === 'timeout');
    const pending = realtime.request('rpc.close', null, { timeoutMs: 1_000 });
    socket.close();
    await assert.rejects(pending, error => error.code === 'connection_closed');
  } finally {
    realtime.close();
    globalThis.WebSocket = originalWebSocket;
  }
});
