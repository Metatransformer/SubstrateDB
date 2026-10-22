import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { Substrate, verifyJobSignature } from '../sdk/dist/index.js';

const substrateUrl = process.env.SUBSTRATE_URL ?? 'http://127.0.0.1:7432';
const adminToken = process.env.SUBSTRATE_ADMIN_TOKEN ?? 'substrate-local-development-token';
const signingKey = process.env.SUBSTRATE_SIGNING_KEY ?? `substrate-signing-v1:${adminToken}`;
const host = process.env.EXAMPLE_HOST ?? '127.0.0.1';
const port = Number(process.env.EXAMPLE_PORT ?? 8080);
const namespace = process.env.SUBSTRATE_NAMESPACE ?? 'demo';
const partition = process.env.SUBSTRATE_PARTITION ?? 'default';
const browserHtml = await readFile(new URL('./browser.html', import.meta.url));

if (host !== '127.0.0.1' && host !== 'localhost' && !process.env.SUBSTRATE_ADMIN_TOKEN) {
  throw new Error('Set SUBSTRATE_ADMIN_TOKEN before exposing the example server');
}

const db = new Substrate({ url: substrateUrl, token: adminToken, namespace, partition });

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url ?? '/', `http://${request.headers.host ?? `${host}:${port}`}`);
    if (request.method === 'GET' && url.pathname === '/') {
      response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      response.end(browserHtml);
      return;
    }
    if (request.method === 'GET' && url.pathname === '/token') {
      const expiresAt = Date.now() + 10 * 60_000;
      const capability = await db.capability({
        id: `browser-${crypto.randomUUID()}`,
        namespace,
        partition,
        resources: ['demo.events'],
        permissions: ['subscribe', 'publish'],
        expires_at_ms: expiresAt,
      });
      response.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      response.end(JSON.stringify({ ...capability, url: substrateUrl }));
      return;
    }
    if (request.method === 'POST' && url.pathname === '/jobs') {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = Buffer.concat(chunks);
      const valid = await verifyJobSignature({
        secret: signingKey,
        signature: String(request.headers['substrate-signature'] ?? ''),
        timestamp: String(request.headers['substrate-timestamp'] ?? ''),
        jobId: String(request.headers['substrate-job-id'] ?? ''),
        body,
      });
      if (!valid) {
        response.writeHead(401).end('invalid signature');
        return;
      }
      console.log('signed job', request.headers['substrate-job-id'], JSON.parse(body.toString('utf8')));
      response.writeHead(204).end();
      return;
    }
    response.writeHead(404).end('not found');
  } catch (error) {
    console.error(error);
    response.writeHead(500).end('request failed');
  }
});

server.listen(port, host, () => {
  console.log(`Example app: http://${host}:${port}`);
  console.log(`Substrate: ${substrateUrl}`);
  console.log(`HTTP job target: http://${host}:${port}/jobs`);
});
