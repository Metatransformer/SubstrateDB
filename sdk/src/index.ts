import { z } from "zod";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type DocumentValue = Record<string, Json>;
export interface Document<T = DocumentValue> { id: string; revision: number; value: T }
export interface JobInput { id: string; queue: string; payload: Json; available_at_ms?: number; max_attempts?: number; lease_ms?: number; ordering_key?: string; delivery?: { url: string }; expires_at_ms?: number }
export interface Job extends JobInput { status: string; attempt: number; receipt?: string; lease_until_ms?: number }
export interface Capability { id: string; namespace: string; partition: string; resources: string[]; permissions: ("subscribe" | "publish" | "presence")[]; expires_at_ms: number; filter?: Record<string, Json> }
export interface ClientOptions { url: string; token: string; namespace: string; partition?: string; timeoutMs?: number; fetch?: typeof fetch }
export interface CommandOptions { requestId?: string; signal?: AbortSignal }
export interface RouteDelivery { url: string }
export interface RouteBinding { id: string; topic: string; queue: string; delivery?: RouteDelivery | null }
export interface MessageInput { id: string; topic: string; payload: Json; available_at_ms?: number; expires_at_ms?: number; max_attempts?: number; ordering_key?: string }
export interface VectorStatus { sequence: number; submitted: number; searchable: boolean }
export interface VectorConfiguration { name: string; dimensions: number; metric: "cosine" | "dot" | "euclidean"; kind: "dense" | "sparse" }
export interface VectorMutation { id: string; name: string; source_revision: number; sequence?: number; searchable: boolean }

export class SubstrateError extends Error {
  constructor(readonly code: string, message: string, readonly status?: number) { super(message); this.name = "SubstrateError"; }
}

const resultEnvelope = z.object({}).passthrough().refine(
  value => Object.prototype.hasOwnProperty.call(value, "result"),
);
const errorEnvelope = z.object({ error: z.object({ code: z.string(), message: z.string() }) }).passthrough();
const envelope = z.union([resultEnvelope, errorEnvelope]);

export class Substrate {
  private readonly base: string;
  private readonly fetcher: typeof fetch;

  constructor(private readonly options: ClientOptions) {
    const u = new URL(options.url);
    if (!["http:", "https:"].includes(u.protocol)) throw new Error("Substrate URL must use HTTP(S)");
    this.base = options.url.replace(/\/$/, "");
    this.fetcher = options.fetch ?? fetch;
  }

  scope(namespace: string, partition = "default") { return new Substrate({ ...this.options, namespace, partition }); }

  async command<T = Json>(action: string, payload: unknown, options: CommandOptions = {}): Promise<T> {
    const signal = options.signal ?? AbortSignal.timeout(this.options.timeoutMs ?? 30_000);
    const response = await this.fetcher(`${this.base}/v1/command`, {
      method: "POST",
      headers: { "Authorization": `Bearer ${this.options.token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ namespace: this.options.namespace, partition: this.options.partition ?? "default", action, payload, request_id: options.requestId ?? crypto.randomUUID() }),
      signal,
    });
    const parsed = envelope.safeParse(await response.json());
    if (!parsed.success) throw new SubstrateError("invalid_response", "Server returned an invalid response", response.status);
    if ("error" in parsed.data) {
      const error = errorEnvelope.parse(parsed.data).error;
      throw new SubstrateError(error.code, error.message, response.status);
    }
    if (!response.ok) throw new SubstrateError("http_error", response.statusText, response.status);
    return parsed.data.result as T;
  }

  collection<T extends DocumentValue = DocumentValue>(collection: string) {
    return {
      put: (id: string, value: T, expectedRevision?: number) => this.command<Document<T>>("document.put", { collection, id, value, expected_revision: expectedRevision }),
      get: (id: string) => this.command<Document<T>>("document.get", { collection, id }),
      delete: (id: string, expectedRevision?: number) => this.command<{ deleted: boolean; revision: number }>("document.delete", { collection, id, expected_revision: expectedRevision }),
      query: (query: { filter?: Record<string, Json>; sort?: { field: string; direction: "asc" | "desc" }; limit?: number; offset?: number; projection?: string[] } = {}) => this.command<{ documents: Document<T>[]; total: number }>("document.query", { collection, ...query }),
      createIndex: (name: string, fields: string[], unique = false) => this.command("index.create", { collection, name, fields, unique }),
      saveAndEnqueue: (id: string, value: T, job: JobInput, options: { expectedRevision?: number; requestId?: string } = {}) => this.command<{ document: Document<T>; job: Job }>("document.commit", { document: { collection, id, value, expected_revision: options.expectedRevision }, job }, { requestId: options.requestId }),
    };
  }

  transaction(commands: { action: string; payload: Json }[], requestId?: string) { return this.command<{ results: Json[] }>("transaction", { commands }, { requestId }); }
  stats() { return this.command("scope.stats", {}); }
  async serviceToken(claims: Omit<Capability, "permissions" | "filter"> & { permissions: ("read" | "write")[] }): Promise<{ token: string; expires_at_ms: number }> {
    const response = await this.fetcher(`${this.base}/v1/service-tokens`, { method: "POST", headers: { "Authorization": `Bearer ${this.options.token}`, "Content-Type": "application/json" }, body: JSON.stringify(claims), signal: AbortSignal.timeout(this.options.timeoutMs ?? 30_000) });
    const value: unknown = await response.json();
    if (!response.ok) throw new SubstrateError("service_token_failed", JSON.stringify(value), response.status);
    return z.object({ token: z.string(), expires_at_ms: z.number() }).parse(value);
  }
  readonly jobs = {
    enqueue: (job: JobInput) => this.command<Job>("job.enqueue", job),
    get: (id: string) => this.command<Job>("job.get", { id }),
    list: (filter: { queue?: string; status?: string; limit?: number } = {}) => this.command<{ jobs: Job[] }>("job.list", filter),
    claim: (queue: string, worker: string, leaseMs = 30_000, options: CommandOptions = {}) => this.command<{ job: Job | null; receipt?: string }>("job.claim", { queue, worker, lease_ms: leaseMs }, options),
    ack: (id: string, receipt: string) => this.command<Job>("job.ack", { id, receipt }),
    nack: (id: string, receipt: string, options: { retryable?: boolean; delay_ms?: number } = {}) => this.command<Job>("job.nack", { id, receipt, ...options }),
    heartbeat: (id: string, receipt: string, leaseMs = 30_000) => this.command<Job>("job.heartbeat", { id, receipt, lease_ms: leaseMs }),
    pause: (queue: string) => this.command<{ queue: string; paused: boolean }>("job.pause", { queue }),
    resume: (queue: string) => this.command<{ queue: string; paused: boolean }>("job.resume", { queue }),
    retry: (id: string) => this.command<Job>("job.retry", { id }),
    cancel: (id: string) => this.command<Job>("job.cancel", { id }),
  };

  async work(
    queue: string,
    handler: (job: Job, signal: AbortSignal) => Promise<void>,
    options: { signal: AbortSignal; worker?: string; leaseMs?: number; idleMs?: number; claimRetryInitialMs?: number; claimRetryMaxMs?: number; onError?: (error: unknown) => void },
  ): Promise<void> {
    const worker = options.worker ?? crypto.randomUUID();
    const lease = options.leaseMs ?? 30_000;
    const initialRetry = Math.max(1, options.claimRetryInitialMs ?? 100);
    const maxRetry = Math.max(initialRetry, options.claimRetryMaxMs ?? 5_000);
    let retryMs = initialRetry;

    while (!options.signal.aborted) {
      let claim: { job: Job | null; receipt?: string };
      try {
        claim = await this.jobs.claim(queue, worker, lease, { signal: options.signal });
        retryMs = initialRetry;
      } catch (error) {
        if (options.signal.aborted) break;
        options.onError?.(error);
        if (!isTemporaryClaimError(error)) throw error;
        await sleep(retryMs, options.signal);
        retryMs = Math.min(maxRetry, retryMs * 2);
        continue;
      }
      if (!claim.job || !claim.receipt) {
        await sleep(options.idleMs ?? 250, options.signal);
        continue;
      }

      const job = claim.job;
      const receipt = claim.receipt;
      const handlerController = new AbortController();
      const heartbeatStop = new AbortController();
      let active = true;
      let leaseFailure: unknown;
      const forwardAbort = () => handlerController.abort(options.signal.reason);
      options.signal.addEventListener("abort", forwardAbort, { once: true });
      const heartbeats = (async () => {
        while (active && !handlerController.signal.aborted) {
          await sleep(Math.max(50, lease / 3), heartbeatStop.signal);
          if (!active || handlerController.signal.aborted) break;
          try {
            await this.jobs.heartbeat(job.id, receipt, lease);
          } catch (error) {
            leaseFailure = error;
            handlerController.abort(error);
            break;
          }
        }
      })();

      try {
        await handler(job, handlerController.signal);
        active = false;
        heartbeatStop.abort();
        await heartbeats;
        if (leaseFailure) throw leaseFailure;
        if (handlerController.signal.aborted || options.signal.aborted) throw handlerController.signal.reason ?? new Error("worker cancelled");
        await this.jobs.ack(job.id, receipt);
      } catch (error) {
        options.onError?.(error);
        if (!leaseFailure && !options.signal.aborted) {
          try { await this.jobs.nack(job.id, receipt); }
          catch (nackError) { options.onError?.(nackError); }
        }
      } finally {
        active = false;
        heartbeatStop.abort();
        handlerController.abort();
        options.signal.removeEventListener("abort", forwardAbort);
        await heartbeats;
      }
    }
  }

  readonly cache = {
    acquireLease: (key: string, owner: string, ttl_ms: number) => this.command<{ acquired: boolean; token?: number; expires_at_ms?: number }>("cache.lease_acquire", { key, owner, ttl_ms }),
    renewLease: (key: string, owner: string, token: number, ttl_ms: number) => this.command("cache.lease_renew", { key, owner, token, ttl_ms }),
    releaseLease: (key: string, owner: string, token: number) => this.command("cache.lease_release", { key, owner, token }),
    rateLimit: (key: string, limit: number, window_ms: number) => this.command<{ allowed: boolean; remaining: number; reset_at_ms: number }>("cache.rate_limit", { key, limit, window_ms }),
    get: <T extends Json = Json>(key: string) => this.command<T | null>("cache.get", { key }),
    set: (key: string, value: Json, options: { ttl_ms?: number; nx?: boolean; xx?: boolean } = {}) => this.command<{ stored: boolean }>("cache.set", { key, value, ...options }),
    delete: (key: string) => this.command<{ deleted: boolean }>("cache.delete", { key }),
    increment: (key: string, by = 1) => this.command<number>("cache.increment", { key, by }),
    ttl: (key: string) => this.command<number>("cache.ttl", { key }),
    expire: (key: string, ttl_ms: number) => this.command<boolean>("cache.expire", { key, ttl_ms }),
    hset: <T extends Json>(key: string, field: string, value: T) => this.command<Record<string, Json>>("cache.hset", { key, field, value }),
    hget: <T extends Json = Json>(key: string, field: string) => this.command<T | null>("cache.hget", { key, field }),
    hdel: (key: string, field: string) => this.command<Record<string, Json>>("cache.hdel", { key, field }),
    sadd: <T extends Json>(key: string, value: T) => this.command<T[]>("cache.sadd", { key, value }),
    srem: <T extends Json>(key: string, value: T) => this.command<T[]>("cache.srem", { key, value }),
    members: <T extends Json = Json>(key: string) => this.command<T[]>("cache.members", { key }),
    lpush: <T extends Json>(key: string, value: T) => this.command<number>("cache.lpush", { key, value }),
    rpush: <T extends Json>(key: string, value: T) => this.command<number>("cache.rpush", { key, value }),
    lpop: <T extends Json = Json>(key: string) => this.command<T | null>("cache.lpop", { key }),
    rpop: <T extends Json = Json>(key: string) => this.command<T | null>("cache.rpop", { key }),
    range: <T extends Json = Json>(key: string, options: { start?: number; limit?: number } = {}) => this.command<T[]>("cache.range", { key, ...options }),
    zadd: (key: string, member: string, score: number) => this.command<Record<string, number>>("cache.zadd", { key, member, score }),
    zrem: (key: string, member: string) => this.command<Record<string, number>>("cache.zrem", { key, member }),
    zrange: (key: string, options: { limit?: number; descending?: boolean } = {}) => this.command<{ member: string; score: number }[]>("cache.zrange", { key, ...options }),
  };

  readonly messaging = {
    bind: (binding: RouteBinding) => this.command<RouteBinding>("route.bind", binding),
    unbind: (id: string) => this.command<{ deleted: boolean }>("route.unbind", { id }),
    list: () => this.command<{ bindings: RouteBinding[] }>("route.list", {}),
    publish: (message: MessageInput) => this.command<{ id: string; jobs: string[] }>("message.publish", message),
  };

  readonly vectors = {
    upsertBatch: (collection: string, name: string, items: { id: string; vector: number[] | { indices: number[]; values: number[] }; source_revision: number }[]) => this.command<{ items: { sequence: number; searchable: boolean }[] }>("vector.upsert_batch", { collection, name, items }),
    configure: (collection: string, name: string, dimensions: number, metric: "cosine" | "dot" | "euclidean" = "cosine", kind: "dense" | "sparse" = "dense") => this.command<VectorConfiguration>("vector.configure", { collection, name, dimensions, metric, kind }),
    upsert: (collection: string, id: string, name: string, vector: number[] | { indices: number[]; values: number[] }, source_revision: number) => this.command<VectorMutation & { sequence: number }>("vector.upsert", { collection, id, name, vector, source_revision }),
    delete: (collection: string, id: string, name: string) => this.command<{ deleted: boolean }>("vector.delete", { collection, id, name }),
    status: (collection: string, sequence?: number) => this.command<VectorStatus>("vector.status", { collection, sequence }),
    search: (query: { collection: string; name?: string; vector?: number[] | { indices: number[]; values: number[] }; text?: string; filter?: Record<string, Json>; limit?: number; exact?: boolean; ef?: number }) => this.command<{ hits: (Document & { score: number })[]; mode: string }>("search.query", query),
    wait: async (collection: string, sequence: number, timeoutMs = 30_000): Promise<VectorStatus> => {
      const end = Date.now() + timeoutMs;
      for (;;) {
        try { return await this.command<VectorStatus>("search.wait", { collection, sequence }); }
        catch (error) {
          if (!(error instanceof SubstrateError) || error.code !== "not_ready" || Date.now() >= end) throw error;
          await sleep(50);
        }
      }
    },
  };

  readonly events = {
    read: (after = 0, limit = 100) => this.command<{ events: Json[]; latest: number; oldest: number }>("event.read", { after, limit }),
    consume: (name: string, limit = 100) => this.command<{ events: Json[]; latest: number; oldest: number }>("consumer.read", { name, limit }),
    ack: (name: string, sequence: number) => this.command("consumer.ack", { name, sequence }),
  };

  readonly schedules = {
    put: (schedule: { id: string; cron: string; job: Omit<JobInput, "id">; timezone?: string; missed?: "once" | "skip" | "replay"; max_catch_up?: number; allow_overlap?: boolean; dst_repeat?: "first" | "second" }) => this.command("schedule.put", schedule),
    pause: (id: string) => this.command("schedule.pause", { id }),
    resume: (id: string) => this.command("schedule.resume", { id }),
    list: () => this.command("schedule.list", {}),
    delete: (id: string) => this.command("schedule.delete", { id }),
  };

  async capability(claims: Capability): Promise<{ token: string; expires_at_ms: number }> {
    const response = await this.fetcher(`${this.base}/v1/capabilities`, {
      method: "POST",
      headers: { "Authorization": `Bearer ${this.options.token}`, "Content-Type": "application/json" },
      body: JSON.stringify(claims),
      signal: AbortSignal.timeout(this.options.timeoutMs ?? 30_000),
    });
    const value: unknown = await response.json();
    if (!response.ok) throw new SubstrateError("capability_failed", JSON.stringify(value), response.status);
    return z.object({ token: z.string(), expires_at_ms: z.number() }).parse(value);
  }

  revoke(id: string) { return this.command("auth.revoke", { id }); }
}

function isTemporaryClaimError(error: unknown): boolean {
  if (!(error instanceof SubstrateError)) return error instanceof TypeError;
  return error.status === 408 || error.status === 429 || (error.status !== undefined && error.status >= 500)
    || ["overloaded", "internal", "storage_error", "http_error", "not_ready"].includes(error.code);
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise(resolve => {
    if (signal?.aborted) { resolve(); return; }
    const finish = () => { clearTimeout(timer); signal?.removeEventListener("abort", finish); resolve(); };
    const timer = setTimeout(finish, ms);
    signal?.addEventListener("abort", finish, { once: true });
  });
}

export interface VerifyJobSignatureOptions {
  secret: string | BufferSource;
  signature: string;
  timestamp: string | number;
  jobId: string;
  body: string | BufferSource;
  maxAgeMs?: number;
  futureToleranceMs?: number;
  nowMs?: number;
}

export async function verifyJobSignature(options: VerifyJobSignatureOptions): Promise<boolean> {
  const timestamp = String(options.timestamp);
  if (!/^\d+$/.test(timestamp)) return false;
  const timestampMs = Number(timestamp);
  const now = options.nowMs ?? Date.now();
  const maxAge = options.maxAgeMs ?? 5 * 60_000;
  const futureTolerance = options.futureToleranceMs ?? 30_000;
  if (!Number.isSafeInteger(timestampMs) || maxAge < 0 || futureTolerance < 0) return false;
  if (timestampMs < now - maxAge || timestampMs > now + futureTolerance) return false;
  const key = typeof options.secret === "string" ? new TextEncoder().encode(options.secret) : copyBytes(options.secret);
  const body = typeof options.body === "string" ? new TextEncoder().encode(options.body) : copyBytes(options.body);
  const prefix = new TextEncoder().encode(`${timestamp}.${options.jobId}.`);
  const message = new Uint8Array(prefix.length + body.length);
  message.set(prefix);
  message.set(body, prefix.length);
  const cryptoKey = await crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const expected = new Uint8Array(await crypto.subtle.sign("HMAC", cryptoKey, message));
  const supplied = decodeBase64Url(options.signature);
  return supplied !== undefined && constantTimeEqual(expected, supplied);
}

function copyBytes(value: BufferSource): Uint8Array<ArrayBuffer> {
  if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0));
  return Uint8Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
}

function decodeBase64Url(value: string): Uint8Array | undefined {
  if (!/^[A-Za-z0-9_-]*$/.test(value)) return undefined;
  try {
    const standard = value.replace(/-/g, "+").replace(/_/g, "/");
    const decoded = atob(standard + "=".repeat((4 - standard.length % 4) % 4));
    return Uint8Array.from(decoded, character => character.charCodeAt(0));
  } catch { return undefined; }
}

function constantTimeEqual(left: Uint8Array, right: Uint8Array): boolean {
  const length = Math.max(left.length, right.length);
  let mismatch = left.length ^ right.length;
  for (let index = 0; index < length; index++) mismatch |= (left[index] ?? 0) ^ (right[index] ?? 0);
  return mismatch === 0;
}

export interface RealtimeOptions { url: string; token: () => Promise<string>; onMessage: (message: Record<string, unknown>) => void; onError?: (error: unknown) => void }
export interface DocumentWatchOptions { id?: string; filter?: Record<string, Json> }
export type RealtimeSubscription =
  | { type: "subscribe"; collection: string; id?: string; filter?: Record<string, Json> }
  | { type: "channel"; topic: string };

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
}

export class Realtime {
  private socket?: WebSocket;
  private closed = false;
  private retries = 0;
  private timer?: ReturnType<typeof setTimeout>;
  private refresh?: ReturnType<typeof setInterval>;
  private readonly pending = new Map<string, PendingRequest>();

  constructor(private readonly options: RealtimeOptions, private readonly subscription: RealtimeSubscription | Record<string, unknown>) { void this.connect(); }

  static watch(options: RealtimeOptions, collection: string, watch: DocumentWatchOptions = {}): Realtime {
    return new Realtime(options, { type: "subscribe", collection, ...watch });
  }

  private async connect() {
    if (this.closed) return;
    try {
      const token = await this.options.token();
      if (this.closed) return;
      const url = new URL("/v1/ws", this.options.url);
      url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
      const socket = new WebSocket(url);
      this.socket = socket;
      socket.onopen = () => socket.send(JSON.stringify({ type: "auth", token }));
      socket.onmessage = event => {
        try {
          const message = JSON.parse(String(event.data)) as Record<string, unknown>;
          if (message.type === "authenticated") {
            this.retries = 0;
            socket.send(JSON.stringify(this.subscription));
          }
          if (message.type === "reply" && typeof message.correlation_id === "string") {
            const pending = this.pending.get(message.correlation_id);
            if (pending) {
              clearTimeout(pending.timer);
              this.pending.delete(message.correlation_id);
              pending.resolve(message.payload);
            }
          }
          this.options.onMessage(message);
        } catch (error) { this.options.onError?.(error); }
      };
      socket.onerror = error => this.options.onError?.(error);
      socket.onclose = () => {
        if (this.socket === socket) this.socket = undefined;
        if (this.refresh) { clearInterval(this.refresh); this.refresh = undefined; }
        this.rejectPending(new SubstrateError("connection_closed", "Realtime connection closed"));
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        if (!this.closed) this.timer = setTimeout(() => { this.timer = undefined; void this.connect(); }, Math.min(30_000, 250 * 2 ** Math.min(this.retries++, 7)));
      };
      this.refresh = setInterval(() => {
        void this.options.token().then(next => {
          if (!this.closed && this.socket === socket && socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: "refresh", token: next }));
        }).catch(error => { if (!this.closed) this.options.onError?.(error); });
      }, 30_000);
    } catch (error) {
      this.options.onError?.(error);
      if (!this.closed) this.timer = setTimeout(() => { this.timer = undefined; void this.connect(); }, 1000);
    }
  }

  send(message: Record<string, unknown>) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) throw new SubstrateError("not_connected", "Realtime connection is not open");
    this.socket.send(JSON.stringify(message));
  }

  request<T = Json>(topic: string, payload: Json, options: { timeoutMs?: number } = {}): Promise<T> {
    const correlationId = crypto.randomUUID();
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(correlationId);
        reject(new SubstrateError("timeout", "Realtime request timed out"));
      }, options.timeoutMs ?? 30_000);
      this.pending.set(correlationId, { resolve: value => resolve(value as T), reject, timer });
      try { this.send({ type: "request", topic, payload, correlation_id: correlationId }); }
      catch (error) {
        clearTimeout(timer);
        this.pending.delete(correlationId);
        reject(error);
      }
    });
  }

  reply(topic: string, correlationId: string, payload: Json) {
    this.send({ type: "reply", topic, payload, correlation_id: correlationId });
  }

  close() {
    this.closed = true;
    if (this.timer) { clearTimeout(this.timer); this.timer = undefined; }
    if (this.refresh) { clearInterval(this.refresh); this.refresh = undefined; }
    this.rejectPending(new SubstrateError("connection_closed", "Realtime connection closed"));
    const socket = this.socket;
    this.socket = undefined;
    if (socket) {
      socket.onopen = null;
      socket.onmessage = null;
      socket.onerror = null;
      socket.onclose = null;
      socket.close();
    }
  }

  private rejectPending(error: SubstrateError) {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }
}
