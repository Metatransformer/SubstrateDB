import { existsSync, readFileSync, statSync } from "node:fs";
import path from "node:path";

export const SITE_ORIGIN = "https://substratedb-docs.vercel.app";
export const GITHUB_REPOSITORY = "https://github.com/Metatransformer/SubstrateDB";

export type DocEntry = {
  slug: string;
  title: string;
  description: string;
  source: string;
  group: "Get started" | "Reference" | "Engineering";
};

export const DOCS: readonly DocEntry[] = [
  {
    slug: "overview",
    title: "Overview",
    description: "What SubstrateDB implements, how to run it, and its current qualification boundary.",
    source: "README.md",
    group: "Get started",
  },
  {
    slug: "quickstart",
    title: "Local quickstart",
    description: "Build a local runtime and exercise documents, jobs, search, and persistence.",
    source: "docs/QUICKSTART.md",
    group: "Get started",
  },
  {
    slug: "dashboard",
    title: "Dashboard",
    description: "Operate the embedded, same-origin administration dashboard safely.",
    source: "docs/DASHBOARD.md",
    group: "Get started",
  },
  {
    slug: "protocol",
    title: "Native protocol",
    description: "The HTTP, WebSocket, command, credential, and error contracts.",
    source: "docs/PROTOCOL.md",
    group: "Reference",
  },
  {
    slug: "sdk",
    title: "TypeScript SDK",
    description: "Typed application APIs for documents, work queues, search, and realtime.",
    source: "sdk/README.md",
    group: "Reference",
  },
  {
    slug: "operations",
    title: "Operations",
    description: "Configuration, persistence, backup, restore, metrics, and deployment guidance.",
    source: "docs/OPERATIONS.md",
    group: "Reference",
  },
  {
    slug: "search",
    title: "Search",
    description: "Vector search: dense, sparse, lexical, hybrid, filtering, and visibility semantics.",
    source: "docs/SEARCH.md",
    group: "Reference",
  },
  {
    slug: "cluster",
    title: "Cluster",
    description: "Shard placement, Raft replication, failover, and membership operations.",
    source: "docs/CLUSTER.md",
    group: "Reference",
  },
  {
    slug: "implementation",
    title: "Implementation status",
    description: "Implemented behavior, known limits, and work that is not yet qualified.",
    source: "docs/IMPLEMENTATION.md",
    group: "Engineering",
  },
  {
    slug: "verification",
    title: "Verification",
    description: "The functional, failure, and operational checks included in the repository.",
    source: "docs/VERIFICATION.md",
    group: "Engineering",
  },
  {
    slug: "architecture",
    title: "Architecture",
    description: "Runtime boundaries, replicated state, storage, execution, and isolation.",
    source: "ARCHITECTURE.md",
    group: "Engineering",
  },
  {
    slug: "research",
    title: "Research notebook",
    description: "Design context, evaluated systems, and the decisions that shaped SubstrateDB.",
    source: "RESEARCH.md",
    group: "Engineering",
  },
] as const;

const docsBySlug = new Map(DOCS.map((doc) => [doc.slug, doc]));
const docsBySource = new Map(DOCS.map((doc) => [doc.source, doc]));

function repositoryRoot(): string {
  return path.resolve(process.cwd(), "..");
}

export function getDoc(slug: string): DocEntry | undefined {
  return docsBySlug.get(slug);
}

export function readDoc(doc: DocEntry): string {
  return readFileSync(path.join(repositoryRoot(), doc.source), "utf8");
}

function splitTarget(target: string): { pathname: string; suffix: string } {
  const queryIndex = target.indexOf("?");
  const hashIndex = target.indexOf("#");
  const indexes = [queryIndex, hashIndex].filter((index) => index >= 0);
  const boundary = indexes.length > 0 ? Math.min(...indexes) : target.length;
  return { pathname: target.slice(0, boundary), suffix: target.slice(boundary) };
}

function encodeRepositoryPath(value: string): string {
  return value.split("/").map(encodeURIComponent).join("/");
}

function resolvedRepositoryPath(source: string, target: string): string | undefined {
  let decoded: string;
  try {
    decoded = decodeURIComponent(target);
  } catch {
    decoded = target;
  }

  const resolved = path.posix.normalize(path.posix.join(path.posix.dirname(source), decoded));
  if (resolved === ".." || resolved.startsWith("../") || path.posix.isAbsolute(resolved)) {
    return undefined;
  }
  return resolved.replace(/^\.\//, "");
}

export function rewriteMarkdownLink(source: string, href: string): string {
  if (!href || href.startsWith("#") || /^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(href)) {
    return href;
  }

  const { pathname, suffix } = splitTarget(href);
  const resolved = resolvedRepositoryPath(source, pathname);
  if (!resolved) return href;

  const knownDoc = docsBySource.get(resolved);
  if (knownDoc) return `/${knownDoc.slug}/${suffix}`;

  const absolutePath = path.join(repositoryRoot(), resolved);
  const isDirectory = existsSync(absolutePath) && statSync(absolutePath).isDirectory();
  const view = isDirectory ? "tree" : "blob";
  return `${GITHUB_REPOSITORY}/${view}/main/${encodeRepositoryPath(resolved)}${suffix}`;
}

export function rewriteMarkdownImage(source: string, src: string): string | undefined {
  if (!src || /^(?:data:|https?:\/\/)/i.test(src)) return src;

  const { pathname, suffix } = splitTarget(src);
  const resolved = resolvedRepositoryPath(source, pathname);
  if (!resolved) return undefined;

  const absolutePath = path.join(repositoryRoot(), resolved);
  if (!existsSync(absolutePath) || !statSync(absolutePath).isFile()) return undefined;

  return `https://raw.githubusercontent.com/Metatransformer/SubstrateDB/main/${encodeRepositoryPath(resolved)}${suffix}`;
}

export function isExternalLink(href: string): boolean {
  return /^(?:https?:)?\/\//i.test(href);
}

export function isLocalRuntimeLink(href: string): boolean {
  try {
    const url = new URL(href);
    return url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "[::1]";
  } catch {
    return false;
  }
}
