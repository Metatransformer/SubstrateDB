import { Substrate } from '../sdk/dist/index.js';

const url = process.env.SUBSTRATE_URL ?? 'http://127.0.0.1:7432';
const token = process.env.SUBSTRATE_ADMIN_TOKEN ?? 'substrate-local-development-token';
const namespace = process.env.SUBSTRATE_NAMESPACE ?? 'demo';
const partition = process.env.SUBSTRATE_PARTITION ?? 'default';
const queue = process.env.SUBSTRATE_QUEUE ?? 'demo-jobs';

if (new URL(url).hostname !== '127.0.0.1' && new URL(url).hostname !== 'localhost' && !process.env.SUBSTRATE_ADMIN_TOKEN) {
  throw new Error('Set SUBSTRATE_ADMIN_TOKEN when connecting beyond localhost');
}

const db = new Substrate({ url, token, namespace, partition });
const stop = new AbortController();
process.once('SIGINT', () => stop.abort());
process.once('SIGTERM', () => stop.abort());

console.log(`Waiting for ${queue} jobs at ${url}; press Ctrl-C to stop`);
await db.work(queue, async (job, signal) => {
  if (signal.aborted) return;
  // Use job.id as an idempotency key for real external side effects.
  console.log('received', job.id, job.payload);
}, {
  signal: stop.signal,
  worker: process.env.SUBSTRATE_WORKER_ID ?? `example-${process.pid}`,
  onError: error => console.error('worker error', error),
});
