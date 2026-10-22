import { readdir, readFile, access } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';
const root = fileURLToPath(new URL('../docs-site/out/', import.meta.url));
const pages = [];
async function walk(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) await walk(path);
    else if (entry.name.endsWith('.html')) pages.push(path);
  }
}
await walk(root);
assert.ok(pages.length >= 13, 'Expected the landing page and curated documentation pages');
let checked = 0;
for (const page of pages) {
  const html = await readFile(page, 'utf8');
  for (const [, raw] of html.matchAll(/(?:href|src)="([^"]+)"/g)) {
    if (!raw.startsWith('/') || raw.startsWith('//')) continue;
    const pathname = decodeURIComponent(raw.split(/[?#]/)[0]);
    const target = resolve(root, `.${pathname}`);
    assert.ok(target.startsWith(root.replace(/\/$/, '')), 'Asset escaped output directory');
    const possibilities = [target, `${target}.html`, join(target, 'index.html')];
    let exists = false;
    for (const path of possibilities) {
      try { await access(path); exists = true; break; } catch {}
    }
    assert.ok(exists, `Missing local link or asset ${raw} in ${page}`);
    checked++;
  }
}
console.log(`Docs export passed: ${pages.length} pages and ${checked} internal links/assets checked.`);
