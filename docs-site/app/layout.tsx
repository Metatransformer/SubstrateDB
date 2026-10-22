import type { Metadata } from "next";
import Link from "next/link";
import type { ReactNode } from "react";
import { GITHUB_REPOSITORY, SITE_ORIGIN } from "../lib/docs";
import "./globals.css";

export const metadata: Metadata = {
  metadataBase: new URL(SITE_ORIGIN),
  title: {
    default: "SubstrateDB Documentation",
    template: "%s · SubstrateDB",
  },
  description: "Documentation for SubstrateDB, a self-hosted Rust runtime for documents, jobs, search, scheduling, cache primitives, and realtime.",
  applicationName: "SubstrateDB Documentation",
  alternates: { canonical: "/" },
  openGraph: {
    type: "website",
    siteName: "SubstrateDB Documentation",
    title: "SubstrateDB Documentation",
    description: "Build with SubstrateDB's integrated data, work, search, and realtime primitives.",
    url: SITE_ORIGIN,
  },
};

function DatabaseMark() {
  return (
    <span className="brand-mark" aria-hidden="true">
      <svg viewBox="0 0 24 24">
        <ellipse cx="12" cy="6" rx="7.5" ry="3" />
        <path d="M4.5 6v6c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3V6M4.5 12v6c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-6" />
      </svg>
    </span>
  );
}

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <header className="site-header">
          <div className="header-inner">
            <Link className="site-brand" href="/" aria-label="SubstrateDB documentation home">
              <DatabaseMark />
              <span className="brand-type"><strong>Substrate</strong><span>DB</span></span>
              <span className="brand-section">Docs</span>
            </Link>
            <nav className="header-nav" aria-label="Primary navigation">
              <Link href="/quickstart/">Quickstart</Link>
              <Link href="/protocol/">Protocol</Link>
              <a href={GITHUB_REPOSITORY} target="_blank" rel="noopener noreferrer">
                GitHub
                <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 5h5v5M19 5l-8 8M18 13v5a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5" /></svg>
              </a>
            </nav>
          </div>
        </header>
        <main>{children}</main>
        <footer className="site-footer">
          <div>
            <span>SubstrateDB is open-source infrastructure under active development.</span>
            <a href="https://metatransformer.com/blog/substratedb" target="_blank" rel="noopener noreferrer">Project portfolio <span aria-hidden="true">↗</span></a>
          </div>
        </footer>
      </body>
    </html>
  );
}
