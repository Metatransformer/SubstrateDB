import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import type { ReactNode } from "react";
import ReactMarkdown, { type Components } from "react-markdown";
import remarkGfm from "remark-gfm";
import { DocNavigation } from "../../components/doc-navigation";
import {
  DOCS,
  GITHUB_REPOSITORY,
  SITE_ORIGIN,
  getDoc,
  isExternalLink,
  isLocalRuntimeLink,
  readDoc,
  rewriteMarkdownImage,
  rewriteMarkdownLink,
} from "../../lib/docs";

export const dynamicParams = false;

type DocPageProps = { params: Promise<{ slug: string }> };

export function generateStaticParams() {
  return DOCS.map((doc) => ({ slug: doc.slug }));
}

export async function generateMetadata({ params }: DocPageProps): Promise<Metadata> {
  const { slug } = await params;
  const doc = getDoc(slug);
  if (!doc) return {};

  return {
    title: doc.title,
    description: doc.description,
    alternates: { canonical: `/${doc.slug}/` },
    openGraph: {
      type: "article",
      title: `${doc.title} · SubstrateDB`,
      description: doc.description,
      url: `${SITE_ORIGIN}/${doc.slug}/`,
    },
  };
}

function textFromNode(node: ReactNode): string {
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (Array.isArray(node)) return node.map(textFromNode).join("");
  if (node && typeof node === "object" && "props" in node) {
    return textFromNode((node as { props: { children?: ReactNode } }).props.children);
  }
  return "";
}

function githubHeadingId(children: ReactNode): string {
  return textFromNode(children)
    .toLocaleLowerCase()
    .trim()
    .replace(/<[^>]*>/g, "")
    .replace(/[^\p{L}\p{N}\s_-]/gu, "")
    .replace(/\s+/g, "-");
}

function MarkdownArticle({ markdown, source }: { markdown: string; source: string }) {
  const headingCounts = new Map<string, number>();

  const heading = (level: 1 | 2 | 3 | 4 | 5 | 6) => {
    const Heading = `h${level}` as const;
    return function MarkdownHeading({ children }: { children?: ReactNode }) {
      const baseId = githubHeadingId(children);
      const count = headingCounts.get(baseId) ?? 0;
      headingCounts.set(baseId, count + 1);
      const id = count === 0 ? baseId : `${baseId}-${count}`;
      return <Heading id={id}>{children}</Heading>;
    };
  };

  const components: Components = {
    h1: heading(1),
    h2: heading(2),
    h3: heading(3),
    h4: heading(4),
    h5: heading(5),
    h6: heading(6),
    a({ href = "", children }) {
      const rewrittenHref = rewriteMarkdownLink(source, href);
      const external = isExternalLink(rewrittenHref);
      const localRuntime = isLocalRuntimeLink(rewrittenHref);
      const anchor = (
        <a
          href={rewrittenHref}
          target={external ? "_blank" : undefined}
          rel={external ? "noopener noreferrer" : undefined}
          title={localRuntime ? "Available only while your local SubstrateDB runtime is running" : undefined}
        >
          {children}
          {external && !localRuntime && <span className="external-mark" aria-hidden="true">↗</span>}
        </a>
      );

      if (!localRuntime) return anchor;
      return <span className="local-runtime-link">{anchor}<span>requires local runtime</span></span>;
    },
    img({ src = "", alt = "" }) {
      const sourceValue = typeof src === "string" ? src : "";
      const rewrittenSrc = rewriteMarkdownImage(source, sourceValue);
      if (!rewrittenSrc) return <span className="missing-image">Image unavailable: {alt || sourceValue}</span>;
      // Markdown images are repository-authored content and are intentionally not optimized in the static export.
      // eslint-disable-next-line @next/next/no-img-element
      return <img src={rewrittenSrc} alt={alt} loading="lazy" />;
    },
    table({ children }) {
      return <div className="markdown-table"><table>{children}</table></div>;
    },
  };

  return (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={components} skipHtml>
      {markdown}
    </ReactMarkdown>
  );
}

export default async function DocPage({ params }: DocPageProps) {
  const { slug } = await params;
  const doc = getDoc(slug);
  if (!doc) notFound();

  const markdown = readDoc(doc);
  const currentIndex = DOCS.findIndex((entry) => entry.slug === doc.slug);
  const previous = currentIndex > 0 ? DOCS[currentIndex - 1] : undefined;
  const next = currentIndex < DOCS.length - 1 ? DOCS[currentIndex + 1] : undefined;
  const sourceUrl = `${GITHUB_REPOSITORY}/blob/main/${doc.source}`;

  return (
    <div className="docs-page-shell">
      <DocNavigation docs={DOCS} currentSlug={doc.slug} />
      <article className="doc-article">
        <div className="doc-breadcrumbs">
          <Link href="/">Docs</Link><span aria-hidden="true">/</span><span>{doc.group}</span>
        </div>
        <div className="doc-intro">
          <p>{doc.description}</p>
          <a href={sourceUrl} target="_blank" rel="noopener noreferrer">View source <span aria-hidden="true">↗</span></a>
        </div>
        <div className="development-callout">
          <span aria-hidden="true">i</span>
          <p><strong>Development implementation.</strong> SubstrateDB is not yet a production-qualified database. This page is rendered from the repository’s current documentation.</p>
        </div>
        <div className="markdown-body">
          <MarkdownArticle markdown={markdown} source={doc.source} />
        </div>
        <nav className="doc-pagination" aria-label="Adjacent documentation">
          {previous ? (
            <Link href={`/${previous.slug}/`} className="previous-doc"><span>Previous</span><strong>← {previous.title}</strong></Link>
          ) : <span />}
          {next && (
            <Link href={`/${next.slug}/`} className="next-doc"><span>Next</span><strong>{next.title} →</strong></Link>
          )}
        </nav>
      </article>
    </div>
  );
}
