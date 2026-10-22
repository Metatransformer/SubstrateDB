"use client";

import { Children, cloneElement, isValidElement, useId, FormEvent, ReactElement, ReactNode, useCallback, useEffect, useRef, useState } from "react";

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
type JsonObject = { [key: string]: Json };
type Screen =
  | "Overview"
  | "Documents"
  | "Queues"
  | "Cache"
  | "Search"
  | "Schedules"
  | "Events"
  | "API Console"
  | "Documentation";

type ApiError = { code: string; message: string; status?: number };
type OperationState = { pending: boolean; data?: Json; error?: ApiError };

const EMPTY_OPERATION: OperationState = { pending: false };
const RESULT_LIMIT = 60_000;

const navigation: { label: string; items: Screen[] }[] = [
  { label: "Workspace", items: ["Overview", "Documents", "Queues", "Cache", "Search"] },
  { label: "Automation", items: ["Schedules", "Events"] },
  { label: "Developer", items: ["API Console", "Documentation"] },
];

const iconPaths: Record<Screen | "Menu" | "Database" | "Copy" | "Refresh" | "Arrow", ReactNode> = {
  Overview: <><path d="M4 13h6V4H4v9Z"/><path d="M14 20h6v-9h-6v9Z"/><path d="M4 20h6v-3H4v3Z"/><path d="M14 7h6V4h-6v3Z"/></>,
  Documents: <><path d="M6 3h9l4 4v14H6V3Z"/><path d="M14 3v5h5"/><path d="M9 13h6M9 17h6"/></>,
  Queues: <><path d="M5 6h14M5 12h14M5 18h9"/><path d="m17 16 3 2-3 2"/></>,
  Cache: <><path d="M4 7c0-2 3.6-3.5 8-3.5S20 5 20 7s-3.6 3.5-8 3.5S4 9 4 7Z"/><path d="M4 7v5c0 2 3.6 3.5 8 3.5s8-1.5 8-3.5V7M4 12v5c0 2 3.6 3.5 8 3.5s8-1.5 8-3.5v-5"/></>,
  Search: <><circle cx="11" cy="11" r="6"/><path d="m16 16 4 4M11 8v6M8 11h6"/></>,
  Schedules: <><rect x="4" y="5" width="16" height="15" rx="2"/><path d="M8 3v4M16 3v4M4 10h16M12 13v4l3 1"/></>,
  Events: <><path d="M4 12h3l2-6 4 12 2-6h5"/><path d="M4 4v16h16"/></>,
  "API Console": <><rect x="3" y="4" width="18" height="16" rx="2"/><path d="m7 9 3 3-3 3M13 15h4"/></>,
  Documentation: <><path d="M4 5a3 3 0 0 1 3-2h5v17H7a3 3 0 0 0-3 2V5ZM20 5a3 3 0 0 0-3-2h-5v17h5a3 3 0 0 1 3 2V5Z"/></>,
  Menu: <path d="M4 7h16M4 12h16M4 17h16"/>,
  Database: <><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></>,
  Copy: <><rect x="8" y="8" width="11" height="11" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/></>,
  Refresh: <><path d="M20 7v5h-5"/><path d="M4 17v-5h5M6.1 9a7 7 0 0 1 11.5-2L20 9M4 15l2.4 2a7 7 0 0 0 11.5-2"/></>,
  Arrow: <path d="m9 18 6-6-6-6"/>,
};

function Icon({ name, size = 18 }: { name: keyof typeof iconPaths; size?: number }) {
  return (
    <svg className="icon" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      {iconPaths[name]}
    </svg>
  );
}

function parseJsonObject(value: string, label: string): JsonObject {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error(`${label} must be valid JSON.`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object.`);
  }
  return parsed as JsonObject;
}

function parseJson(value: string, label: string): Json {
  try {
    return JSON.parse(value) as Json;
  } catch {
    throw new Error(`${label} must be valid JSON.`);
  }
}

function toApiError(error: unknown): ApiError {
  if (error && typeof error === "object" && "code" in error && "message" in error) {
    return error as ApiError;
  }
  return { code: "client_error", message: error instanceof Error ? error.message : "Unexpected error" };
}

function formatJson(value: unknown): string {
  const text = JSON.stringify(value, null, 2) ?? String(value);
  return text.length > RESULT_LIMIT ? `${text.slice(0, RESULT_LIMIT)}\n… output truncated` : text;
}

function optionalNumber(value: string): number | undefined {
  if (!value.trim()) return undefined;
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0 || !Number.isInteger(number)) throw new Error("Expected a non-negative integer.");
  return number;
}

function Field({ label, hint, children, className = "" }: { label: string; hint?: string; children: ReactNode; className?: string }) {
  const labelId = useId();
  return (
    <label className={`field ${className}`}>
      <span id={labelId} className="field-label">{label}</span>
      {Children.map(children, child => isValidElement(child) ? cloneElement(child as ReactElement<{ "aria-labelledby"?: string }>, { "aria-labelledby": labelId }) : child)}
      {hint && <span className="field-hint">{hint}</span>}
    </label>
  );
}

function Button({ children, variant = "secondary", busy = false, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "secondary" | "danger" | "ghost"; busy?: boolean }) {
  return (
    <button className={`button button-${variant}`} {...props}>
      {busy && <span className="spinner" aria-hidden="true" />}
      {children}
    </button>
  );
}

function PageHeader({ eyebrow, title, description, actions }: { eyebrow: string; title: string; description: string; actions?: ReactNode }) {
  return (
    <header className="page-header">
      <div>
        <div className="eyebrow">{eyebrow}</div>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </header>
  );
}

function Panel({ title, description, actions, children, className = "" }: { title: string; description?: string; actions?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`panel ${className}`}>
      <div className="panel-header">
        <div>
          <h2>{title}</h2>
          {description && <p>{description}</p>}
        </div>
        {actions && <div className="panel-actions">{actions}</div>}
      </div>
      {children}
    </section>
  );
}

function EmptyState({ children }: { children: ReactNode }) {
  return <div className="empty-state">{children}</div>;
}

function ResultView({ state, empty = "Run an operation to inspect its response." }: { state: OperationState; empty?: string }) {
  if (state.pending) return <div className="result-state"><span className="spinner" /> Waiting for SubstrateDB…</div>;
  if (state.error) {
    return (
      <div className="error-box" role="alert">
        <strong>{state.error.code}</strong>
        <span>{state.error.message}</span>
        {state.error.status && <small>HTTP {state.error.status}</small>}
      </div>
    );
  }
  if (state.data === undefined) return <EmptyState>{empty}</EmptyState>;
  return <pre className="json-result" tabIndex={0}>{formatJson(state.data)}</pre>;
}

function StatusPill({ connected }: { connected: boolean }) {
  return <span className={`status-pill ${connected ? "is-connected" : ""}`}><span className="status-dot" />{connected ? "Connected" : "Offline"}</span>;
}

function DataTable({ columns, rows, empty }: { columns: { key: string; label: string; render?: (row: JsonObject) => ReactNode }[]; rows: JsonObject[]; empty: string }) {
  if (!rows.length) return <EmptyState>{empty}</EmptyState>;
  return (
    <div className="table-scroll">
      <table>
        <thead><tr>{columns.map(column => <th key={column.key}>{column.label}</th>)}</tr></thead>
        <tbody>
          {rows.slice(0, 100).map((row, index) => (
            <tr key={String(row.id ?? row.sequence ?? index)}>
              {columns.map(column => <td key={column.key}>{column.render ? column.render(row) : String(row[column.key] ?? "—")}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 100 && <div className="table-note">Showing the first 100 results.</div>}
    </div>
  );
}

export default function Dashboard() {
  const [screen, setScreen] = useState<Screen>("Overview");
  const [mobileNav, setMobileNav] = useState(false);
  const [namespace, setNamespace] = useState("app");
  const [partition, setPartition] = useState("default");
  const [token, setToken] = useState("");
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [connectionError, setConnectionError] = useState<ApiError>();
  const [health, setHealth] = useState<Json>();
  const [stats, setStats] = useState<Json>();

  const command = useCallback(async (action: string, payload: JsonObject, tokenOverride?: string): Promise<Json> => {
    const credential = tokenOverride ?? token;
    if (!credential) throw { code: "not_connected", message: "Enter a credential and connect first." } satisfies ApiError;
    const response = await fetch("/v1/command", {
      signal: AbortSignal.timeout(30_000),
      method: "POST",
      headers: { Authorization: `Bearer ${credential}`, "Content-Type": "application/json" },
      body: JSON.stringify({ namespace, partition, action, payload, request_id: crypto.randomUUID() }),
    });
    let envelope: unknown;
    try {
      envelope = await response.json();
    } catch {
      throw { code: "invalid_response", message: "Server returned a non-JSON response.", status: response.status } satisfies ApiError;
    }
    if (envelope && typeof envelope === "object" && "error" in envelope) {
      const value = (envelope as { error?: Partial<ApiError> }).error;
      throw { code: value?.code ?? "request_failed", message: value?.message ?? "Request failed.", status: response.status } satisfies ApiError;
    }
    if (!response.ok || !envelope || typeof envelope !== "object" || !("result" in envelope)) {
      throw { code: "invalid_response", message: `Unexpected server response (${response.status}).`, status: response.status } satisfies ApiError;
    }
    return (envelope as { result: Json }).result;
  }, [namespace, partition, token]);

  const connect = async (event?: FormEvent) => {
    event?.preventDefault();
    if (!namespace.trim() || !partition.trim() || !token) {
      setConnectionError({ code: "missing_connection", message: "Namespace, partition, and credential are required." });
      return;
    }
    setConnecting(true);
    setConnectionError(undefined);
    try {
      const healthResponse = await fetch("/health", { cache: "no-store" });
      if (!healthResponse.ok) throw { code: "health_failed", message: `Health check returned HTTP ${healthResponse.status}.`, status: healthResponse.status };
      const healthValue = await healthResponse.json() as Json;
      const statsValue = await command("scope.stats", {}, token);
      setHealth(healthValue);
      setStats(statsValue);
      setConnected(true);
    } catch (error) {
      setConnected(false);
      setConnectionError(toApiError(error));
    } finally {
      setConnecting(false);
    }
  };

  const disconnect = () => {
    setConnected(false);
    setToken("");
    setHealth(undefined);
    setStats(undefined);
    setConnectionError(undefined);
  };

  const navigate = (next: Screen) => {
    setScreen(next);
    setMobileNav(false);
  };

  return (
    <div className="app-shell">
      {mobileNav && <button className="nav-scrim" aria-label="Close navigation" onClick={() => setMobileNav(false)} />}
      <aside className={`sidebar ${mobileNav ? "is-open" : ""}`}>
        <div className="brand">
          <span className="brand-mark"><Icon name="Database" size={22} /></span>
          <span><strong>Substrate</strong><small>DB Console</small></span>
        </div>
        <nav aria-label="Dashboard navigation">
          {navigation.map(group => (
            <div className="nav-group" key={group.label}>
              <div className="nav-label">{group.label}</div>
              {group.items.map(item => (
                <button key={item} className={`nav-item ${screen === item ? "is-selected" : ""}`} aria-current={screen === item ? "page" : undefined} onClick={() => navigate(item)}>
                  <Icon name={item} />
                  <span>{item}</span>
                </button>
              ))}
            </div>
          ))}
        </nav>
        <div className="sidebar-footer">
          <StatusPill connected={connected} />
          <div className="sidebar-scope"><span>Active scope</span><strong>{namespace} / {partition}</strong></div>
        </div>
      </aside>

      <div className="workspace">
        <header className="connection-bar">
          <button className="mobile-menu" onClick={() => setMobileNav(true)} aria-label="Open navigation"><Icon name="Menu" size={21} /></button>
          <div className="scope-badge" title="Selected namespace and partition">
            <span className="scope-label">Scope</span>
            <strong>{namespace}</strong><span>/</span><strong>{partition}</strong>
          </div>
          <form className="connection-form" onSubmit={connect}>
            <label><span>Namespace</span><input aria-label="Namespace" value={namespace} disabled={connected || connecting} onChange={event => setNamespace(event.target.value)} autoComplete="off" /></label>
            <label><span>Partition</span><input aria-label="Partition" value={partition} disabled={connected || connecting} onChange={event => setPartition(event.target.value)} autoComplete="off" /></label>
            <label className="token-field"><span>Admin credential</span><input aria-label="Admin credential" type="password" value={token} disabled={connected || connecting} onChange={event => setToken(event.target.value)} autoComplete="off" placeholder="Bearer token" /></label>
            {!connected ? (
              <>
                <Button type="button" variant="ghost" disabled={connecting} onClick={() => setToken("substrate-local-development-token")}>Use local dev token</Button>
                <Button type="submit" variant="primary" busy={connecting} disabled={connecting}>Connect</Button>
              </>
            ) : <Button type="button" onClick={disconnect}>Disconnect</Button>}
          </form>
        </header>
        {connectionError && <div className="connection-error" role="alert"><strong>{connectionError.code}</strong><span>{connectionError.message}</span></div>}

        <main className="main-content" key={`${namespace}:${partition}:${connected}`}>
          {screen === "Overview" && <Overview connected={connected} command={command} token={token} health={health} stats={stats} setHealth={setHealth} setStats={setStats} />}
          {screen === "Documents" && <Documents connected={connected} command={command} token={token} namespace={namespace} partition={partition} />}
          {screen === "Queues" && <Queues connected={connected} command={command} />}
          {screen === "Cache" && <Cache connected={connected} command={command} onNavigate={navigate} />}
          {screen === "Search" && <Search connected={connected} command={command} />}
          {screen === "Schedules" && <Schedules connected={connected} command={command} />}
          {screen === "Events" && <Events connected={connected} command={command} />}
          {screen === "API Console" && <ApiConsole connected={connected} command={command} />}
          {screen === "Documentation" && <Documentation onNavigate={navigate} />}
        </main>
      </div>
    </div>
  );
}

type CommandFn = (action: string, payload: JsonObject) => Promise<Json>;
type ScreenProps = { connected: boolean; command: CommandFn };

function Overview({ connected, command, token, health, stats, setHealth, setStats }: ScreenProps & { token: string; health?: Json; stats?: Json; setHealth: (value: Json) => void; setStats: (value: Json) => void }) {
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<ApiError>();
  const [metrics, setMetrics] = useState<string>();
  const [metricsPending, setMetricsPending] = useState(false);
  const statsObject = stats && typeof stats === "object" && !Array.isArray(stats) ? stats as JsonObject : {};
  const limits = statsObject.limits && typeof statsObject.limits === "object" && !Array.isArray(statsObject.limits) ? statsObject.limits as JsonObject : {};
  const healthObject = health && typeof health === "object" && !Array.isArray(health) ? health as JsonObject : {};

  const refresh = async () => {
    setRefreshing(true); setError(undefined);
    try {
      const response = await fetch("/health", { cache: "no-store" });
      if (!response.ok) throw { code: "health_failed", message: `Health check returned HTTP ${response.status}.` };
      const [healthValue, statsValue] = await Promise.all([response.json() as Promise<Json>, command("scope.stats", {})]);
      setHealth(healthValue); setStats(statsValue);
    } catch (value) { setError(toApiError(value)); }
    finally { setRefreshing(false); }
  };

  const loadMetrics = async () => {
    setMetricsPending(true); setError(undefined);
    try {
      const response = await fetch("/metrics", { headers: { Authorization: `Bearer ${token}` }, cache: "no-store" });
      if (!response.ok) throw { code: "metrics_failed", message: `Metrics returned HTTP ${response.status}.`, status: response.status };
      const text = await response.text();
      setMetrics(text.length > RESULT_LIMIT ? `${text.slice(0, RESULT_LIMIT)}\n… output truncated` : text);
    } catch (value) { setError(toApiError(value)); }
    finally { setMetricsPending(false); }
  };

  return (
    <>
      <PageHeader eyebrow="Local runtime" title="Overview" description="Inspect runtime health, scoped storage usage, and operator metrics without changing data." actions={<Button onClick={refresh} disabled={!connected || refreshing} busy={refreshing}><Icon name="Refresh" size={16} /> Refresh</Button>} />
      {!connected && <DisconnectedNotice />}
      {error && <div className="error-box inline-error" role="alert"><strong>{error.code}</strong><span>{error.message}</span></div>}
      <div className="metric-grid">
        <article className="metric-card"><span>Runtime</span><strong>{connected ? String(healthObject.status ?? "Unknown") : "Offline"}</strong><small>{healthObject.mode ? `${healthObject.mode} mode` : "Connect to inspect"}</small></article>
        <article className="metric-card"><span>Logical data</span><strong>{formatBytes(Number(statsObject.logical_bytes ?? 0))}</strong><small>Scoped serialized bytes</small></article>
        <article className="metric-card"><span>Event cursor</span><strong>{String(statsObject.event_sequence ?? "—")}</strong><small>Latest retained sequence</small></article>
        <article className="metric-card"><span>Storage limit</span><strong>{limits.max_bytes ? formatBytes(Number(limits.max_bytes)) : "—"}</strong><small>{healthObject.version ? `SubstrateDB ${healthObject.version}` : "Current scope"}</small></article>
      </div>
      <div className="two-column">
        <Panel title="Scope statistics" description="Authoritative usage and retention settings for the selected namespace and partition.">
          {stats ? <pre className="json-result compact" tabIndex={0}>{formatJson(stats)}</pre> : <EmptyState>Connect to load scope statistics.</EmptyState>}
        </Panel>
        <Panel title="Prometheus metrics" description="Admin-only runtime counters. Metrics load only when requested." actions={<Button onClick={loadMetrics} disabled={!connected || metricsPending} busy={metricsPending}>Load metrics</Button>}>
          {metrics !== undefined ? <pre className="json-result compact metrics-result" tabIndex={0}>{metrics || "No metrics returned."}</pre> : <EmptyState>Metrics have not been requested.</EmptyState>}
        </Panel>
      </div>
    </>
  );
}

function Documents({ connected, command, token, namespace, partition }: ScreenProps & { token: string; namespace: string; partition: string }) {
  const [collection, setCollection] = useState("items");
  const [id, setId] = useState("");
  const [value, setValue] = useState('{\n  "status": "active"\n}');
  const [expectedRevision, setExpectedRevision] = useState("");
  const [filter, setFilter] = useState("{}");
  const [limit, setLimit] = useState("50");
  const [operation, setOperation] = useState<OperationState>(EMPTY_OPERATION);
  const [documents, setDocuments] = useState<JsonObject[]>([]);
  const [indexName, setIndexName] = useState("");
  const [indexFields, setIndexFields] = useState("");
  const [unique, setUnique] = useState(false);
  const [indexOperation, setIndexOperation] = useState<OperationState>(EMPTY_OPERATION);
  const [watching, setWatching] = useState(false);
  const [watchMessages, setWatchMessages] = useState<JsonObject[]>([]);
  const [watchError, setWatchError] = useState<ApiError>();
  const socketRef = useRef<WebSocket | null>(null);
  const watchGeneration = useRef(0);

  const run = async (action: string, payload: JsonObject, target = setOperation) => {
    target({ pending: true });
    try {
      const result = await command(action, payload);
      target({ pending: false, data: result });
      if (action === "document.get" && result && typeof result === "object" && !Array.isArray(result)) {
        const document = result as JsonObject;
        setValue(JSON.stringify(document.value, null, 2));
        setExpectedRevision(String(document.revision));
      }
      if (action === "document.query" && result && typeof result === "object" && !Array.isArray(result)) {
        const rows = (result as JsonObject).documents;
        setDocuments(Array.isArray(rows) ? rows.filter(row => row && typeof row === "object" && !Array.isArray(row)) as JsonObject[] : []);
      }
    } catch (error) { target({ pending: false, error: toApiError(error) }); }
  };

  const queryDocuments = () => {
    try {
      run("document.query", { collection, filter: parseJsonObject(filter, "Filter"), limit: Number(limit) });
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };

  const putDocument = () => {
    try {
      if (!id) throw new Error("Document ID is required.");
      const payload: JsonObject = { collection, id, value: parseJsonObject(value, "Document value") };
      const revision = optionalNumber(expectedRevision);
      if (revision !== undefined) payload.expected_revision = revision;
      run("document.put", payload);
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };

  const getDocument = () => id ? run("document.get", { collection, id }) : setOperation({ pending: false, error: { code: "client_error", message: "Document ID is required." } });
  const deleteDocument = () => {
    if (!id) { setOperation({ pending: false, error: { code: "client_error", message: "Document ID is required." } }); return; }
    if (!window.confirm(`Delete ${collection}/${id}? This cannot be undone.`)) return;
    try {
      const payload: JsonObject = { collection, id };
      const revision = optionalNumber(expectedRevision);
      if (revision !== undefined) payload.expected_revision = revision;
      run("document.delete", payload);
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };

  const createIndex = () => {
    const fields = indexFields.split(",").map(field => field.trim()).filter(Boolean);
    if (!indexName || !fields.length) { setIndexOperation({ pending: false, error: { code: "client_error", message: "Index name and at least one field are required." } }); return; }
    run("index.create", { collection, name: indexName, fields, unique }, setIndexOperation);
  };

  const stopWatch = useCallback(() => {
    watchGeneration.current += 1;
    socketRef.current?.close();
    socketRef.current = null;
    setWatching(false);
  }, []);

  useEffect(() => stopWatch, [stopWatch]);
  useEffect(() => { if (!connected) stopWatch(); }, [connected, stopWatch]);

  const startWatch = async () => {
    stopWatch(); setWatchError(undefined); setWatchMessages([]);
    const generation = watchGeneration.current;
    try {
      if (!collection) throw new Error("Collection is required.");
      const watchFilter = parseJsonObject(filter, "Filter");
      const capabilityResponse = await fetch("/v1/capabilities", {
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ id: `dashboard-watch-${crypto.randomUUID()}`, namespace, partition, resources: [collection], permissions: ["subscribe"], expires_at_ms: Date.now() + 60 * 60_000, filter: null }),
      });
      const capability = await capabilityResponse.json() as { token?: string; error?: ApiError };
      if (!capabilityResponse.ok || !capability.token) throw { ...(capability.error ?? { code: "capability_failed", message: "Could not mint a browser capability." }), status: capabilityResponse.status };
      if (generation !== watchGeneration.current) return;
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      const socket = new WebSocket(`${protocol}//${window.location.host}/v1/ws`);
      socketRef.current = socket;
      socket.onopen = () => socket.send(JSON.stringify({ type: "auth", token: capability.token }));
      socket.onmessage = event => {
        let message: JsonObject;
        try { message = JSON.parse(String(event.data)) as JsonObject; }
        catch { return; }
        if (message.type === "authenticated") {
          socket.send(JSON.stringify({ type: "subscribe", collection, ...(id ? { id } : { filter: watchFilter }) }));
          setWatching(true);
        }
        if (message.type === "error") setWatchError(toApiError(message.error));
        setWatchMessages(current => [...current, message].slice(-100));
      };
      socket.onerror = () => setWatchError({ code: "websocket_error", message: "Live watch connection failed." });
      socket.onclose = () => { if (socketRef.current === socket) { socketRef.current = null; setWatching(false); } };
    } catch (error) { if (generation === watchGeneration.current) setWatchError(toApiError(error)); }
  };

  return (
    <>
      <PageHeader eyebrow="Document store" title="Documents" description="Query, inspect, and mutate one explicit collection. Equality filters support dotted field paths." />
      {!connected && <DisconnectedNotice />}
      <div className="document-toolbar">
        <Field label="Collection"><input value={collection} onChange={event => setCollection(event.target.value)} disabled={!connected} /></Field>
        <Field label="Document ID" hint="Required for get, put, and delete"><input value={id} onChange={event => setId(event.target.value)} disabled={!connected} placeholder="item-123" /></Field>
        <Field label="Expected revision" hint="0 creates only; blank is an upsert"><input inputMode="numeric" value={expectedRevision} onChange={event => setExpectedRevision(event.target.value)} disabled={!connected} placeholder="Optional" /></Field>
      </div>
      <div className="two-column wide-left">
        <div className="stack">
          <Panel title="Query collection" description="Results are bounded to 1,000 by the server; this console defaults to 50.">
            <div className="form-grid compact-grid">
              <Field label="Equality filter"><textarea rows={5} value={filter} onChange={event => setFilter(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
              <Field label="Limit"><input type="number" min="1" max="1000" value={limit} onChange={event => setLimit(event.target.value)} disabled={!connected} /></Field>
            </div>
            <div className="button-row"><Button variant="primary" onClick={queryDocuments} disabled={!connected || operation.pending} busy={operation.pending}>Run query</Button><Button onClick={getDocument} disabled={!connected || operation.pending}>Get by ID</Button></div>
          </Panel>
          <Panel title="Document value" description="Values must be JSON objects. Writes happen only when you click Save.">
            <Field label="JSON value"><textarea className="code-input" rows={11} value={value} onChange={event => setValue(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
            <div className="button-row"><Button variant="primary" onClick={putDocument} disabled={!connected || operation.pending}>Save document</Button><Button variant="danger" onClick={deleteDocument} disabled={!connected || operation.pending}>Delete…</Button></div>
          </Panel>
        </div>
        <Panel title="Operation response" description="The latest explicit document operation appears here."><ResultView state={operation} /></Panel>
      </div>
      <Panel title="Query results" description={`${documents.length} document${documents.length === 1 ? "" : "s"} loaded from the latest query.`}>
        <DataTable rows={documents} empty="Run a collection query to populate this table." columns={[
          { key: "id", label: "ID", render: row => <button className="table-link" onClick={() => { setId(String(row.id)); setValue(JSON.stringify(row.value, null, 2)); setExpectedRevision(String(row.revision)); }}>{String(row.id ?? "")}</button> },
          { key: "revision", label: "Revision" },
          { key: "value", label: "Value", render: row => <code className="table-json">{JSON.stringify(row.value)}</code> },
        ]} />
      </Panel>
      <div className="two-column">
        <Panel title="Indexes" description="Definitions are immutable by name. Field paths may be comma-separated.">
          <div className="form-grid">
            <Field label="Index name"><input value={indexName} onChange={event => setIndexName(event.target.value)} disabled={!connected} placeholder="by_status" /></Field>
            <Field label="Fields"><input value={indexFields} onChange={event => setIndexFields(event.target.value)} disabled={!connected} placeholder="status, account.id" /></Field>
            <label className="check-field"><input type="checkbox" checked={unique} onChange={event => setUnique(event.target.checked)} disabled={!connected} /><span>Unique index</span></label>
          </div>
          <div className="button-row"><Button variant="primary" onClick={createIndex} disabled={!connected || indexOperation.pending}>Create index</Button><Button onClick={() => run("index.list", { collection }, setIndexOperation)} disabled={!connected || indexOperation.pending}>List indexes</Button></div>
          <ResultView state={indexOperation} empty="Create or list indexes for this collection." />
        </Panel>
        <Panel title="Live watch" description="Mints a one-hour, subscribe-only browser capability. Enter a document ID for a single-document watch; otherwise use an indexed equality filter." actions={watching ? <Button onClick={stopWatch}>Stop watch</Button> : <Button variant="primary" onClick={startWatch} disabled={!connected}>Start watch</Button>}>
          <div className="watch-status"><StatusPill connected={watching} /><span>{watching ? "Receiving snapshots and changes" : "Not watching"}</span></div>
          {watchError && <div className="error-box" role="alert"><strong>{watchError.code}</strong><span>{watchError.message}</span></div>}
          {watchMessages.length ? <pre className="json-result compact" tabIndex={0}>{formatJson(watchMessages)}</pre> : <EmptyState>Live messages will appear here. Watches are bounded to 1,000 documents.</EmptyState>}
        </Panel>
      </div>
    </>
  );
}

function Queues({ connected, command }: ScreenProps) {
  const [queue, setQueue] = useState("default");
  const [jobId, setJobId] = useState("");
  const [payload, setPayload] = useState('{\n  "task": "process"\n}');
  const [status, setStatus] = useState("");
  const [operation, setOperation] = useState<OperationState>(EMPTY_OPERATION);
  const [jobs, setJobs] = useState<JsonObject[]>([]);
  const [queues, setQueues] = useState<string[]>([]);

  const run = async (action: string, body: JsonObject) => {
    setOperation({ pending: true });
    try {
      const result = await command(action, body);
      setOperation({ pending: false, data: result });
      if (action === "job.list" && result && typeof result === "object" && !Array.isArray(result)) {
        const list = (result as JsonObject).jobs;
        setJobs(Array.isArray(list) ? list.filter(item => item && typeof item === "object" && !Array.isArray(item)) as JsonObject[] : []);
      }
      if (action === "job.queues" && result && typeof result === "object" && !Array.isArray(result)) {
        const list = (result as JsonObject).queues;
        setQueues(Array.isArray(list) ? list.filter((item): item is string => typeof item === "string") : []);
      }
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };

  const enqueue = () => {
    try {
      if (!jobId || !queue) throw new Error("Job ID and queue are required.");
      run("job.enqueue", { id: jobId, queue, payload: parseJson(payload, "Job payload") });
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  const requireJob = (action: string, destructive = false) => {
    if (!jobId) { setOperation({ pending: false, error: { code: "client_error", message: "Job ID is required." } }); return; }
    if (destructive && !window.confirm(`Cancel job ${jobId}?`)) return;
    run(action, { id: jobId });
  };

  return (
    <>
      <PageHeader eyebrow="Durable work" title="Queues" description="Inspect queues and job state, enqueue work, and control retries without running worker code in the database." actions={<Button onClick={() => run("job.queues", {})} disabled={!connected || operation.pending}><Icon name="Refresh" size={16} /> Refresh queues</Button>} />
      {!connected && <DisconnectedNotice />}
      <div className="callout"><strong>Execution stays external.</strong><span>SubstrateDB stores, leases, and delivers jobs. Your worker process performs side effects and acknowledges completion. Jobs are at-least-once.</span></div>
      <div className="document-toolbar">
        <Field label="Queue"><input list="known-queues" value={queue} onChange={event => setQueue(event.target.value)} disabled={!connected} /><datalist id="known-queues">{queues.map(name => <option key={name} value={name} />)}</datalist></Field>
        <Field label="Job ID"><input value={jobId} onChange={event => setJobId(event.target.value)} disabled={!connected} placeholder="job-123" /></Field>
        <Field label="Status filter"><select value={status} onChange={event => setStatus(event.target.value)} disabled={!connected}><option value="">All statuses</option><option value="ready">Ready</option><option value="leased">Leased</option><option value="completed">Completed</option><option value="dead_letter">Dead letter</option><option value="cancelled">Cancelled</option><option value="expired">Expired</option></select></Field>
      </div>
      <div className="two-column wide-left">
        <Panel title="Jobs" description="List is explicitly refreshed and capped at 100 jobs." actions={<Button variant="primary" onClick={() => run("job.list", { queue, ...(status ? { status } : {}), limit: 100 })} disabled={!connected || operation.pending}>List jobs</Button>}>
          <DataTable rows={jobs} empty="Choose a queue and list its jobs." columns={[
            { key: "id", label: "ID", render: row => <button className="table-link" onClick={() => setJobId(String(row.id ?? ""))}>{String(row.id ?? "")}</button> },
            { key: "status", label: "Status", render: row => <span className={`tag tag-${String(row.status)}`}>{String(row.status ?? "")}</span> },
            { key: "attempt", label: "Attempt" },
            { key: "available_at_ms", label: "Available", render: row => formatTime(row.available_at_ms) },
          ]} />
        </Panel>
        <Panel title="Job operation" description="Use the selected ID for inspection or state changes.">
          <div className="button-row wrap"><Button onClick={() => requireJob("job.get")} disabled={!connected || operation.pending}>Get</Button><Button onClick={() => requireJob("job.retry")} disabled={!connected || operation.pending}>Retry dead letter</Button><Button variant="danger" onClick={() => requireJob("job.cancel", true)} disabled={!connected || operation.pending}>Cancel…</Button></div>
          <div className="button-row wrap"><Button onClick={() => run("job.pause", { queue })} disabled={!connected || operation.pending}>Pause queue</Button><Button onClick={() => run("job.resume", { queue })} disabled={!connected || operation.pending}>Resume queue</Button></div>
          <ResultView state={operation} />
        </Panel>
      </div>
      <Panel title="Enqueue job" description="No job is created until you click Enqueue. Reusing an ID with different input returns a conflict.">
        <Field label="Payload"><textarea className="code-input" rows={7} value={payload} onChange={event => setPayload(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
        <div className="button-row"><Button variant="primary" onClick={enqueue} disabled={!connected || operation.pending}>Enqueue</Button></div>
      </Panel>
    </>
  );
}

function Cache({ connected, command, onNavigate }: ScreenProps & { onNavigate: (screen: Screen) => void }) {
  const [key, setKey] = useState("");
  const [value, setValue] = useState('{"enabled": true}');
  const [ttl, setTtl] = useState("");
  const [operation, setOperation] = useState<OperationState>(EMPTY_OPERATION);
  const run = async (action: string, payload: JsonObject) => {
    setOperation({ pending: true });
    try { setOperation({ pending: false, data: await command(action, payload) }); }
    catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  const requireKey = (action: string, payload: JsonObject = {}) => {
    if (!key) { setOperation({ pending: false, error: { code: "client_error", message: "Cache key is required." } }); return; }
    run(action, { key, ...payload });
  };
  const setValueOperation = () => {
    try {
      const payload: JsonObject = { value: parseJson(value, "Cache value") };
      const ttlValue = optionalNumber(ttl);
      if (ttlValue !== undefined) payload.ttl_ms = ttlValue;
      requireKey("cache.set", payload);
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  const remove = () => {
    if (!key || !window.confirm(`Delete cache key “${key}”?`)) return;
    requireKey("cache.delete");
  };
  return (
    <>
      <PageHeader eyebrow="Durable cache primitives" title="Cache" description="Read and write one explicit key. Expiry is enforced on reads even before maintenance reclaims storage." />
      {!connected && <DisconnectedNotice />}
      <div className="two-column">
        <Panel title="Key operation" description="Values accept any JSON type. TTL is optional and measured in milliseconds.">
          <div className="form-grid">
            <Field label="Key"><input value={key} onChange={event => setKey(event.target.value)} disabled={!connected} placeholder="feature:flags" /></Field>
            <Field label="TTL (ms)" hint="Leave blank for no expiry"><input inputMode="numeric" value={ttl} onChange={event => setTtl(event.target.value)} disabled={!connected} placeholder="60000" /></Field>
          </div>
          <Field label="JSON value"><textarea className="code-input" rows={8} value={value} onChange={event => setValue(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
          <div className="button-row wrap"><Button variant="primary" onClick={setValueOperation} disabled={!connected || operation.pending}>Set value</Button><Button onClick={() => requireKey("cache.get")} disabled={!connected || operation.pending}>Get</Button><Button onClick={() => requireKey("cache.ttl")} disabled={!connected || operation.pending}>Read TTL</Button><Button variant="danger" onClick={remove} disabled={!connected || operation.pending}>Delete…</Button></div>
        </Panel>
        <Panel title="Response" description="False, null, zero, and empty collections are preserved exactly."><ResultView state={operation} /></Panel>
      </div>
      <div className="callout"><strong>Need hashes, sets, lists, sorted sets, leases, or rate limits?</strong><span>Use the API Console for explicit <code>cache.h*</code>, <code>cache.s*</code>, <code>cache.l*</code>, <code>cache.z*</code>, lease, and rate-limit commands.</span><Button variant="ghost" onClick={() => onNavigate("API Console")}>Open console <Icon name="Arrow" size={14} /></Button></div>
    </>
  );
}

function Search({ connected, command }: ScreenProps) {
  const [collection, setCollection] = useState("articles");
  const [name, setName] = useState("embedding");
  const [dimensions, setDimensions] = useState("3");
  const [metric, setMetric] = useState("cosine");
  const [documentId, setDocumentId] = useState("");
  const [revision, setRevision] = useState("");
  const [vector, setVector] = useState("[0.1, 0.2, 0.3]");
  const [text, setText] = useState("");
  const [filter, setFilter] = useState("{}");
  const [limit, setLimit] = useState("10");
  const [exact, setExact] = useState(true);
  const [operation, setOperation] = useState<OperationState>(EMPTY_OPERATION);
  const [hits, setHits] = useState<JsonObject[]>([]);
  const run = async (action: string, payload: JsonObject) => {
    setOperation({ pending: true });
    try {
      const result = await command(action, payload);
      setOperation({ pending: false, data: result });
      if (action === "search.query" && result && typeof result === "object" && !Array.isArray(result)) {
        const rows = (result as JsonObject).hits;
        setHits(Array.isArray(rows) ? rows.filter(row => row && typeof row === "object" && !Array.isArray(row)) as JsonObject[] : []);
      }
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  const configure = () => run("vector.configure", { collection, name, dimensions: Number(dimensions), metric, kind: "dense" });
  const upsert = () => {
    try {
      if (!documentId || !revision) throw new Error("Document ID and source revision are required.");
      run("vector.upsert", { collection, name, id: documentId, vector: parseJson(vector, "Vector"), source_revision: Number(revision) });
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  const search = () => {
    try {
      const payload: JsonObject = { collection, name, filter: parseJsonObject(filter, "Filter"), limit: Number(limit), exact };
      if (vector.trim()) payload.vector = parseJson(vector, "Vector");
      if (text.trim()) payload.text = text.trim();
      if (!payload.vector && !payload.text) throw new Error("Provide a vector, search text, or both.");
      run("search.query", payload);
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  return (
    <>
      <PageHeader eyebrow="Hybrid retrieval" title="Search" description="Configure caller-supplied vector fields, submit document revisions, and run dense, text, or hybrid queries." actions={<Button onClick={() => run("vector.status", { collection })} disabled={!connected || operation.pending}>Check visibility</Button>} />
      {!connected && <DisconnectedNotice />}
      <div className="callout"><strong>Indexing is asynchronous.</strong><span>Upsert returns a sequence with <code>searchable: false</code>. Check visibility before expecting the vector in results. SubstrateDB does not generate embeddings.</span></div>
      <div className="three-column">
        <Panel title="1. Configure field" description="A named field’s dimensions, metric, and kind are immutable.">
          <Field label="Collection"><input value={collection} onChange={event => setCollection(event.target.value)} disabled={!connected} /></Field>
          <Field label="Vector field"><input value={name} onChange={event => setName(event.target.value)} disabled={!connected} /></Field>
          <div className="form-grid">
            <Field label="Dimensions"><input type="number" min="1" max="65536" value={dimensions} onChange={event => setDimensions(event.target.value)} disabled={!connected} /></Field>
            <Field label="Metric"><select value={metric} onChange={event => setMetric(event.target.value)} disabled={!connected}><option value="cosine">Cosine</option><option value="dot">Dot product</option><option value="euclidean">Euclidean</option></select></Field>
          </div>
          <Button variant="primary" onClick={configure} disabled={!connected || operation.pending}>Configure</Button>
        </Panel>
        <Panel title="2. Submit vector" description="The source document must exist at exactly this revision.">
          <div className="form-grid">
            <Field label="Document ID"><input value={documentId} onChange={event => setDocumentId(event.target.value)} disabled={!connected} /></Field>
            <Field label="Source revision"><input type="number" min="1" value={revision} onChange={event => setRevision(event.target.value)} disabled={!connected} /></Field>
          </div>
          <Field label="Dense vector"><textarea className="code-input" rows={6} value={vector} onChange={event => setVector(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
          <Button variant="primary" onClick={upsert} disabled={!connected || operation.pending}>Submit vector</Button>
        </Panel>
        <Panel title="Latest response" description="Configuration, submission, query, and status responses share this panel."><ResultView state={operation} /></Panel>
      </div>
      <Panel title="Search collection" description="A vector plus text runs hybrid reciprocal-rank fusion. Equality filters force exact vector search.">
        <div className="search-form-grid">
          <Field label="Query vector"><textarea className="code-input" rows={5} value={vector} onChange={event => setVector(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
          <Field label="Text query"><textarea rows={5} value={text} onChange={event => setText(event.target.value)} disabled={!connected} placeholder="Optional BM25 text" /></Field>
          <Field label="Equality filter"><textarea className="code-input" rows={5} value={filter} onChange={event => setFilter(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
        </div>
        <div className="inline-controls">
          <Field label="Limit"><input type="number" min="1" max="1000" value={limit} onChange={event => setLimit(event.target.value)} disabled={!connected} /></Field>
          <label className="check-field"><input type="checkbox" checked={exact} onChange={event => setExact(event.target.checked)} disabled={!connected} /><span>Exact vector search</span></label>
          <Button variant="primary" onClick={search} disabled={!connected || operation.pending}>Run search</Button>
        </div>
      </Panel>
      <Panel title="Search hits" description={`${hits.length} hit${hits.length === 1 ? "" : "s"} from the latest query.`}>
        <DataTable rows={hits} empty="Run a query to populate search results." columns={[
          { key: "id", label: "Document" }, { key: "score", label: "Score", render: row => typeof row.score === "number" ? row.score.toFixed(6) : "—" }, { key: "revision", label: "Revision" }, { key: "value", label: "Value", render: row => <code className="table-json">{JSON.stringify(row.value)}</code> },
        ]} />
      </Panel>
    </>
  );
}

function Schedules({ connected, command }: ScreenProps) {
  const [id, setId] = useState("");
  const [cron, setCron] = useState("*/5 * * * *");
  const [timezone, setTimezone] = useState("UTC");
  const [missed, setMissed] = useState("once");
  const [queue, setQueue] = useState("default");
  const [payload, setPayload] = useState('{\n  "task": "scheduled"\n}');
  const [operation, setOperation] = useState<OperationState>(EMPTY_OPERATION);
  const [schedules, setSchedules] = useState<JsonObject[]>([]);
  const [selected, setSelected] = useState<JsonObject>();
  const selectSchedule = (row: JsonObject) => {
    setSelected(row); setId(String(row.id)); setCron(String(row.cron));
    setTimezone(String(row.timezone)); setMissed(String(row.missed));
    const job = row.job as JsonObject;
    setQueue(String(job.queue)); setPayload(JSON.stringify(job.payload, null, 2));
  };
  const run = async (action: string, body: JsonObject) => {
    setOperation({ pending: true });
    try {
      const result = await command(action, body);
      setOperation({ pending: false, data: result });
      if (action === "schedule.list" && result && typeof result === "object" && !Array.isArray(result)) {
        const rows = (result as JsonObject).schedules;
        setSchedules(Array.isArray(rows) ? rows.filter(row => row && typeof row === "object" && !Array.isArray(row)) as JsonObject[] : []);
      }
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  const create = () => {
    try {
      if (!id || !queue) throw new Error("Schedule ID and queue are required.");
      const previous = selected?.id === id ? selected : undefined;
      run("schedule.put", { id, cron, timezone, missed, max_catch_up: previous?.max_catch_up ?? 100, allow_overlap: previous?.allow_overlap ?? false, dst_repeat: previous?.dst_repeat ?? "first", job: { ...(previous?.job as JsonObject ?? {}), queue, payload: parseJson(payload, "Job payload") } });
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  const act = (action: string, scheduleId: string) => {
    if (action === "schedule.delete" && !window.confirm(`Delete schedule ${scheduleId}?`)) return;
    setId(scheduleId); run(action, { id: scheduleId });
  };
  return (
    <>
      <PageHeader eyebrow="Cron orchestration" title="Schedules" description="Create five-field cron schedules that enqueue durable jobs. Time zones use IANA names." actions={<Button variant="primary" onClick={() => run("schedule.list", {})} disabled={!connected || operation.pending}><Icon name="Refresh" size={16} /> List schedules</Button>} />
      {!connected && <DisconnectedNotice />}
      <div className="two-column wide-left">
        <Panel title="Schedules" description="Select a schedule ID from the table to edit or control it.">
          <DataTable rows={schedules} empty="List schedules to inspect this scope." columns={[
            { key: "id", label: "ID", render: row => <button className="table-link" onClick={() => selectSchedule(row)}>{String(row.id ?? "")}</button> },
            { key: "cron", label: "Cron", render: row => <code>{String(row.cron ?? "")}</code> },
            { key: "timezone", label: "Time zone" },
            { key: "next_at_ms", label: "Next run", render: row => formatTime(row.next_at_ms) },
            { key: "paused", label: "State", render: row => <span className="tag">{row.paused ? "Paused" : "Active"}</span> },
            { key: "actions", label: "Actions", render: row => <span className="table-actions"><button disabled={!connected || operation.pending} onClick={() => act(row.paused ? "schedule.resume" : "schedule.pause", String(row.id))}>{row.paused ? "Resume" : "Pause"}</button><button disabled={!connected || operation.pending} className="danger-link" onClick={() => act("schedule.delete", String(row.id))}>Delete</button></span> },
          ]} />
        </Panel>
        <Panel title="Latest response"><ResultView state={operation} /></Panel>
      </div>
      <Panel title="Create or replace schedule" description="Templates cannot contain a job ID or absolute availability/expiry. The scheduler assigns those per occurrence.">
        <div className="schedule-grid">
          <Field label="Schedule ID"><input value={id} onChange={event => setId(event.target.value)} disabled={!connected} placeholder="hourly-rollup" /></Field>
          <Field label="Cron (5 fields)"><input value={cron} onChange={event => setCron(event.target.value)} disabled={!connected} /></Field>
          <Field label="Time zone"><input value={timezone} onChange={event => setTimezone(event.target.value)} disabled={!connected} placeholder="America/Denver" /></Field>
          <Field label="Missed run policy"><select value={missed} onChange={event => setMissed(event.target.value)} disabled={!connected}><option value="once">Run once</option><option value="skip">Skip</option><option value="replay">Replay up to 100</option></select></Field>
          <Field label="Queue"><input value={queue} onChange={event => setQueue(event.target.value)} disabled={!connected} /></Field>
        </div>
        <Field label="Job payload"><textarea className="code-input" rows={7} value={payload} onChange={event => setPayload(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
        <Button variant="primary" onClick={create} disabled={!connected || operation.pending}>Save schedule</Button>
      </Panel>
    </>
  );
}

function Events({ connected, command }: ScreenProps) {
  const [after, setAfter] = useState("0");
  const [limit, setLimit] = useState("100");
  const [operation, setOperation] = useState<OperationState>(EMPTY_OPERATION);
  const [events, setEvents] = useState<JsonObject[]>([]);
  const [latest, setLatest] = useState<number>();
  const [oldest, setOldest] = useState<number>();
  const refresh = async () => {
    setOperation({ pending: true });
    try {
      const result = await command("event.read", { after: Number(after), limit: Number(limit) });
      setOperation({ pending: false, data: result });
      if (result && typeof result === "object" && !Array.isArray(result)) {
        const object = result as JsonObject;
        setEvents(Array.isArray(object.events) ? object.events.filter(row => row && typeof row === "object" && !Array.isArray(row)) as JsonObject[] : []);
        setLatest(typeof object.latest === "number" ? object.latest : undefined);
        setOldest(typeof object.oldest === "number" ? object.oldest : undefined);
      }
    } catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  return (
    <>
      <PageHeader eyebrow="Retained changefeed" title="Events" description="Read ordered, partition-scoped history from an explicit cursor. Refresh never advances the cursor automatically." actions={<Button variant="primary" onClick={refresh} disabled={!connected || operation.pending} busy={operation.pending}><Icon name="Refresh" size={16} /> Read events</Button>} />
      {!connected && <DisconnectedNotice />}
      <div className="document-toolbar event-toolbar">
        <Field label="After sequence"><input type="number" min="0" value={after} onChange={event => setAfter(event.target.value)} disabled={!connected} /></Field>
        <Field label="Limit"><input type="number" min="1" max="1000" value={limit} onChange={event => setLimit(event.target.value)} disabled={!connected} /></Field>
        <div className="cursor-summary"><span>Retained range</span><strong>{oldest ?? "—"} → {latest ?? "—"}</strong></div>
        <Button onClick={() => setAfter(String(latest ?? after))} disabled={!connected || latest === undefined}>Advance cursor to latest</Button>
      </div>
      {operation.error && <div className="error-box inline-error" role="alert"><strong>{operation.error.code}</strong><span>{operation.error.message}</span></div>}
      <Panel title="Event stream" description={`${events.length} retained event${events.length === 1 ? "" : "s"} after sequence ${after}.`}>
        <DataTable rows={events} empty={operation.pending ? "Loading events…" : "No retained events follow this cursor."} columns={[
          { key: "sequence", label: "Sequence" },
          { key: "timestamp_ms", label: "Time", render: row => formatTime(row.timestamp_ms) },
          { key: "kind", label: "Kind", render: row => <span className="tag">{String(row.kind ?? row.action ?? "event")}</span> },
          { key: "resource", label: "Resource", render: row => <code>{String(row.resource ?? row.key ?? "—")}</code> },
          { key: "data", label: "Payload", render: row => <code className="table-json">{JSON.stringify(row.data ?? row)}</code> },
        ]} />
      </Panel>
    </>
  );
}

const consolePresets: Record<string, { action: string; payload: JsonObject }> = {
  "Scope statistics": { action: "scope.stats", payload: {} },
  "Atomic transaction": { action: "transaction", payload: { commands: [{ action: "cache.set", payload: { key: "counter", value: 0 } }, { action: "cache.increment", payload: { key: "counter", by: 1 } }] } },
  "Publish message": { action: "message.publish", payload: { id: "message-1", topic: "orders.created", payload: { order_id: "order-123" } } },
  "List routes": { action: "route.list", payload: {} },
  "Increment cache": { action: "cache.increment", payload: { key: "counter", by: 1 } },
  "Hash field": { action: "cache.hset", payload: { key: "profile:1", field: "name", value: "Ada" } },
  "Set member": { action: "cache.sadd", payload: { key: "tags", value: "new" } },
  "List push": { action: "cache.rpush", payload: { key: "activity", value: { type: "created" } } },
  "Sorted-set add": { action: "cache.zadd", payload: { key: "scores", member: "ada", score: 42 } },
  "Query documents": { action: "document.query", payload: { collection: "items", filter: {}, limit: 25 } },
};

function ApiConsole({ connected, command }: ScreenProps) {
  const [preset, setPreset] = useState("Scope statistics");
  const [action, setAction] = useState(consolePresets[preset].action);
  const [payload, setPayload] = useState(formatJson(consolePresets[preset].payload));
  const [operation, setOperation] = useState<OperationState>(EMPTY_OPERATION);
  const applyPreset = (name: string) => {
    setPreset(name); setAction(consolePresets[name].action); setPayload(formatJson(consolePresets[name].payload)); setOperation(EMPTY_OPERATION);
  };
  const run = async () => {
    setOperation({ pending: true });
    try { setOperation({ pending: false, data: await command(action.trim(), parseJsonObject(payload, "Payload")) }); }
    catch (error) { setOperation({ pending: false, error: toApiError(error) }); }
  };
  return (
    <>
      <PageHeader eyebrow="Raw protocol" title="API Console" description="Send one explicit command to the selected scope. Each run gets a fresh request ID." />
      {!connected && <DisconnectedNotice />}
      <div className="two-column wide-left console-layout">
        <Panel title="Command" description="Presets are editable examples. Nothing runs until you click Run command.">
          <div className="form-grid">
            <Field label="Preset"><select value={preset} onChange={event => applyPreset(event.target.value)}>{Object.keys(consolePresets).map(name => <option key={name}>{name}</option>)}</select></Field>
            <Field label="Action"><input value={action} onChange={event => setAction(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
          </div>
          <Field label="Payload"><textarea className="code-input console-input" rows={20} value={payload} onChange={event => setPayload(event.target.value)} disabled={!connected} spellCheck={false} /></Field>
          <div className="button-row"><Button variant="primary" onClick={run} disabled={!connected || operation.pending || !action.trim()} busy={operation.pending}>Run command</Button><span className="muted-note">POST /v1/command · 1 MiB request limit</span></div>
        </Panel>
        <Panel title="Response" description="Server errors preserve their code and message."><ResultView state={operation} /></Panel>
      </div>
    </>
  );
}

function Documentation({ onNavigate }: { onNavigate: (screen: Screen) => void }) {
  const curl = `curl http://127.0.0.1:7432/v1/command \\\n  -H 'Authorization: Bearer substrate-local-development-token' \\\n  -H 'Content-Type: application/json' \\\n  -d '{"namespace":"app","partition":"default","action":"scope.stats","payload":{},"request_id":"quick-start-1"}'`;
  return (
    <>
      <PageHeader eyebrow="Operator guide" title="Documentation" description="Protocol essentials for local development, scoped data, workers, search, and realtime clients." actions={<a className="button button-secondary" href="/dashboard/docs/DASHBOARD.md">Dashboard guide <Icon name="Arrow" size={14} /></a>} />
      <div className="callout doc-links"><strong>Offline reference library</strong>{["QUICKSTART", "DASHBOARD", "PROTOCOL", "SDK", "OPERATIONS", "SEARCH", "CLUSTER", "IMPLEMENTATION"].map(name => <a key={name} href={`/dashboard/docs/${name}.md`}>{name.toLowerCase().replaceAll("_", " ")}</a>)}</div>
      <div className="docs-layout">
        <aside className="docs-toc" aria-label="On this page"><strong>On this page</strong><a href="#quick-start">Quick start</a><a href="#auth">Authentication</a><a href="#namespaces">Namespaces</a><a href="#documents">Documents</a><a href="#workers">Workers & cron</a><a href="#vectors">Vectors</a><a href="#changefeeds">Changefeeds</a><a href="#limitations">Limitations</a></aside>
        <article className="docs-content">
          <section id="quick-start"><span className="docs-number">01</span><h2>Quick start</h2><p>The dashboard and API share one origin. Start the server, open <code>/dashboard/</code>, use the local development token, and connect to <code>app / default</code>.</p><pre className="code-block"><code>{curl}</code></pre><Button onClick={() => navigator.clipboard?.writeText(curl)}><Icon name="Copy" size={15} /> Copy command</Button></section>
          <section id="auth"><span className="docs-number">02</span><h2>Authentication</h2><p>Admin and service credentials authorize HTTP commands. Short-lived browser capabilities authorize WebSocket subscribe, publish, and presence operations. This dashboard keeps the entered credential in React memory only: no local storage, cookies, or query strings.</p><a href="/dashboard/docs/PROTOCOL.md#credentials">Credential protocol <Icon name="Arrow" size={13} /></a></section>
          <section id="namespaces"><span className="docs-number">03</span><h2>Namespaces and partitions</h2><p>Every command is scoped by namespace and partition. Request deduplication, documents, queues, cache keys, schedules, and retained events all live inside that boundary. Disconnect before switching scope so an operation cannot silently move between partitions.</p></section>
          <section id="documents"><span className="docs-number">04</span><h2>Documents</h2><p>Documents are JSON objects with monotonic revisions. <code>expected_revision: 0</code> means create-only; a current revision enables optimistic concurrency. Queries support bounded equality filters, dotted paths, sorting, projection, offset, and limit.</p><div className="doc-actions"><Button variant="primary" onClick={() => onNavigate("Documents")}>Open Documents</Button><a href="/dashboard/docs/PROTOCOL.md#commands">Command reference</a></div></section>
          <section id="workers"><span className="docs-number">05</span><h2>Workers and cron</h2><p>Jobs are durably queued but executed by external workers. Claims use expiring receipts, side effects are at-least-once, and handlers must deduplicate. Schedules use five-field cron expressions and enqueue job templates; maintenance operations stay with the runtime.</p><div className="doc-links"><a href="/dashboard/docs/PROTOCOL.md#signed-http-jobs">Signed HTTP delivery</a><a href="/dashboard/docs/OPERATIONS.md">Operations guide</a></div></section>
          <section id="vectors"><span className="docs-number">06</span><h2>Vectors and search</h2><p>SubstrateDB stores caller-supplied dense or sparse vectors; it does not create embeddings. Vector submission is durable and asynchronous. Exact search is the correctness path, while the graph path is experimental and should be recall-tested against your data.</p><a href="/dashboard/docs/SEARCH.md">Search reference <Icon name="Arrow" size={13} /></a></section>
          <section id="changefeeds"><span className="docs-number">07</span><h2>Changefeeds</h2><p>Retained events provide ordered, partition-scoped history. WebSocket document subscriptions start with a snapshot and then send coalesced changes. Use events when every mutation matters; use live watch when current state matters.</p><a href="/dashboard/docs/PROTOCOL.md#websockets">WebSocket protocol <Icon name="Arrow" size={13} /></a></section>
          <section id="limitations"><span className="docs-number">08</span><h2>Current boundaries</h2><ul><li>No global resource catalog exists yet. Enter collection and cache names explicitly; queues can be listed within the selected scope.</li><li>Filters are equality-only. Joins, range predicates, and general aggregation are not implemented.</li><li>Collection watches require an indexed equality filter and are bounded to 1,000 documents. Single-document watches use the document ID.</li><li>Approximate vector search has no established large-scale recall or latency guarantee.</li><li>This console never creates demo data or starts mutations automatically.</li></ul></section>
        </article>
      </div>
    </>
  );
}

function DisconnectedNotice() {
  return <div className="disconnected-notice"><span className="status-dot" /><span>Connect with a credential to enable controls. Documentation remains available offline.</span></div>;
}

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function formatTime(value: Json | undefined) {
  if (typeof value !== "number") return "—";
  try { return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(value); }
  catch { return String(value); }
}
