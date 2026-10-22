"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

type NavigationDoc = {
  slug: string;
  title: string;
  description: string;
  group: string;
};

export function DocNavigation({ docs, currentSlug }: { docs: readonly NavigationDoc[]; currentSlug: string }) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const normalizedQuery = query.trim().toLocaleLowerCase();

  const groups = useMemo(() => {
    const groupNames = Array.from(new Set(docs.map((doc) => doc.group)));
    return groupNames.map((group) => ({
      group,
      docs: docs.filter((doc) => {
        if (doc.group !== group) return false;
        if (!normalizedQuery) return true;
        return `${doc.title} ${doc.description}`.toLocaleLowerCase().includes(normalizedQuery);
      }),
    }));
  }, [docs, normalizedQuery]);

  const resultCount = groups.reduce((total, group) => total + group.docs.length, 0);

  return (
    <>
      <button
        className="docs-menu-button"
        type="button"
        aria-expanded={open}
        aria-controls="docs-navigation"
        onClick={() => setOpen((value) => !value)}
      >
        <span aria-hidden="true">{open ? "×" : "☰"}</span>
        {open ? "Close navigation" : "Browse documentation"}
      </button>
      {open && <button className="docs-nav-scrim" aria-label="Close documentation navigation" onClick={() => setOpen(false)} />}
      <aside id="docs-navigation" className={`docs-navigation ${open ? "is-open" : ""}`}>
        <div className="docs-search">
          <label htmlFor="docs-search-input">Filter documentation</label>
          <div className="search-field">
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <circle cx="11" cy="11" r="6" />
              <path d="m16 16 4 4" />
            </svg>
            <input
              id="docs-search-input"
              type="search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search titles"
              autoComplete="off"
            />
          </div>
        </div>

        <nav aria-label="Documentation">
          {groups.map(({ group, docs: groupDocs }) => groupDocs.length > 0 && (
            <div className="docs-nav-group" key={group}>
              <div className="docs-nav-label">{group}</div>
              {groupDocs.map((doc) => (
                <Link
                  key={doc.slug}
                  href={`/${doc.slug}/`}
                  className={doc.slug === currentSlug ? "is-active" : undefined}
                  aria-current={doc.slug === currentSlug ? "page" : undefined}
                  onClick={() => setOpen(false)}
                >
                  <span>{doc.title}</span>
                </Link>
              ))}
            </div>
          ))}
          {resultCount === 0 && <p className="docs-no-results">No documentation titles match “{query}”.</p>}
        </nav>

        <div className="docs-nav-note">
          <span className="status-dot" />
          Development implementation
        </div>
      </aside>
    </>
  );
}
