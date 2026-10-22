# Publishing the documentation

The documentation frontend lives in `docs-site/`. Next.js exports static HTML from a curated list of the repository's Markdown files. There is no deployed database, secret, API proxy, or administrator dashboard on this site.

```sh
npm --prefix docs-site ci
npm --prefix docs-site run build
npm --prefix docs-site run typecheck
node scripts/docs-smoke.mjs
```

The root `vercel.json` builds only this documentation site and publishes `docs-site/out`. `.vercelignore` excludes database files, local environment files, runtime build output, and frontend dependency directories.

Production project: `metatransformer/substratedb-docs`.

```sh
vercel link --scope metatransformer --project substratedb-docs
vercel deploy --prod --scope metatransformer
```

Public documentation: https://substratedb-docs.vercel.app

The Rust dashboard keeps its separate build workflow and remains embedded in the local runtime. Changing the public documentation does not redeploy a database or expose local data.
