# Search

SubstrateDB indexes caller-supplied dense or sparse vectors alongside documents. It does not generate embeddings. Vector submission is durable and asynchronous: `vector.upsert` and `vector.upsert_batch` return sequence numbers, while `vector.flush` applies queued work. Exact vector search remains the default.

## Vector fields

Configure a named field before submitting vectors:

```json
{
  "action": "vector.configure",
  "payload": {
    "collection": "articles",
    "name": "embedding",
    "dimensions": 768,
    "metric": "cosine",
    "kind": "dense"
  }
}
```

Dense fields support `cosine`, `dot`, and `euclidean`. Sparse fields use `kind: "sparse"` and require `metric: "dot"`. Dimensions are limited to `1..=65536`; dense values must match the configured dimension and all values must fit the finite f32 range without nonzero underflow, and sparse indices must be unique and in range.

A field configuration is immutable. Repeating the identical configuration is idempotent, but changing its dimensions, metric, or kind returns `conflict`, even if work is still pending. Use a new field name for a new schema. This prevents already indexed or queued vectors from being reinterpreted under a different schema.

## Submission and visibility

A single submission identifies the source document revision it represents:

```json
{
  "action": "vector.upsert",
  "payload": {
    "collection": "articles",
    "name": "embedding",
    "id": "article-1",
    "vector": [0.1, 0.2],
    "source_revision": 3
  }
}
```

The document must exist and its current revision must equal `source_revision`. The response includes a monotonically increasing collection sequence and `"searchable": false`.

Submit up to 1,000 vectors atomically with:

```json
{
  "action": "vector.upsert_batch",
  "payload": {
    "collection": "articles",
    "name": "embedding",
    "items": [
      {"id": "article-1", "vector": [0.1, 0.2], "source_revision": 3},
      {"id": "article-2", "vector": [0.3, 0.4], "source_revision": 1}
    ]
  }
}
```

Every item uses the same validation as a single upsert. If any item is invalid, no item is queued and no sequence is consumed. A successful response has an `items` array containing each item's assigned `sequence`.

`vector.flush` processes up to 100 queued submissions per call. If a document changes or disappears between submission and indexing, the item is rejected. Rejected items still advance visibility once all earlier sequences are resolved. `search.wait` with that sequence returns the retained rejection code and message rather than reporting a generic indexing failure.

```json
{"action":"search.wait","payload":{"collection":"articles","sequence":42}}
```

`vector.status` returns the highest contiguous resolved `sequence` and the latest `submitted` sequence. `search.wait` returns `not_ready` while its requested sequence is above the resolved sequence.

Deleting a vector also rejects every already queued upsert for the same collection, field, and document. Therefore a later `vector.flush` cannot resurrect it. A new upsert submitted after the delete is allowed and receives a new sequence.

## Query behavior

Vector queries default to the exact brute-force path:

```json
{
  "action": "search.query",
  "payload": {
    "collection": "articles",
    "name": "embedding",
    "vector": [0.1, 0.2],
    "filter": {"tenant": "acme"},
    "limit": 10
  }
}
```

Filters are equality matches against document values and may use dotted field paths. Filtered vector queries always use the exact path. At result time, SubstrateDB checks that the source document still exists and that its current revision equals the indexed vector revision, so deleted documents and stale vectors are not returned.

Text-only queries use BM25 over tokens from string values in the document. A query containing both `text` and `vector` independently ranks the BM25 and vector lists, then combines them with reciprocal-rank fusion using `1 / (60 + rank)`, where rank starts at 1. The response modes are `bm25`, `exact`, `graph_approximate`, and `hybrid_rrf`.

Set `"exact": false` to use the persisted neighbor graph. `ef` controls traversal breadth, must be at least `limit`, and is capped at 100,000. Construction and traversal are deterministic for the same indexed state. Replacement and deletion remove stale backlinks, and deletion reconnects the removed vector's live adjacent nodes before choosing a new entry point.

The custom graph is experimental. It is a deterministic, single-layer approximate index, not a production-proven HNSW implementation. It has no established large-scale recall or latency SLA. Use exact search when correctness is required, and validate approximate recall against your own data before deployment.

## Benchmark harness

`search_bench` uses only public `Engine` commands. It generates deterministic normalized cosine vectors, durably writes documents and queued vector submissions, flushes the index, uses exact search as brute-force ground truth, and compares graph top-10 results. Its JSON output includes seed and parameters, OS/architecture/logical CPU count, recall@10, insertion submission time, indexing time, and exact/graph p50, p95, and p99 query latency.

Run a small check:

```sh
cargo run --release --bin search_bench -- --count 10000 --dims 32 --queries 100 --ef 128 --seed 42
```

Run the one-million-vector single-node experiment separately, with enough local disk and time for durable per-command writes:

```sh
cargo run --release --bin search_bench -- --count 1000000 --dims 128 --queries 1000 --ef 256 --seed 42
```

This is a single-process, single-node harness. It does not exercise cluster routing, replication, failover, concurrent clients, or network latency.

For a 10-million-vector cluster experiment, do not use a larger local harness run and label it clustered. Provision the intended cluster topology, load 10 million deterministic vectors through the cluster's public command endpoint with an external parallel loader, wait for every partition's submitted sequence, and query through the cluster coordinator. Compute recall against a separately retained exact top-10 ground-truth set and report end-to-end latency together with shard count, replicas, hardware, loader concurrency, dimensions, `ef`, seed, and failure state. A distributed loader/evaluator is not included in this local harness.

### Recorded smoke result

On 2026-10-21, the following debug-build smoke command was run on macOS/aarch64 with 12 logical CPUs:

```sh
cargo run --quiet --bin search_bench -- --count 200 --dims 8 --queries 20 --ef 64 --seed 42
```

It measured recall@10 `1.0`; insertion submission time `2592.02 ms`; indexing time `2064.33 ms`; exact search p50/p95/p99 `13.22/13.46/13.68 ms`; and graph search p50/p95/p99 `11.97/12.11/12.24 ms`.

This result only verifies that the harness runs and that exact/graph comparison is wired correctly on a tiny deterministic dataset. It is a debug build, a single run, and far too small for capacity, production latency, or million-vector claims.

### Release-build development measurement

The repository also contains [the raw 2,000-vector, 32-dimensional result](benchmarks/search-2000-32.json), with 100 queries, `ef=128`, seed 42, on a 12-logical-CPU macOS/aarch64 machine. It measured recall@10 1.0, graph p50/p95/p99 8.99/15.16/20.46 ms, exact p50/p95/p99 21.61/33.80/44.31 ms, submission 21.07 seconds and indexing 13.77 seconds. Other build activity was running on the machine; this is an integration measurement, not an isolated capacity benchmark or service-level promise. It also identifies substantial remaining storage/JSON/index overhead to address before large-scale qualification.

Cross-partition `search.scatter` accepts `{partitions:[...],query:{collection,name,vector,...}}` through the HTTP command endpoint. It compares field schemas, runs bounded concurrent searches, includes the source partition in each hit and merges scores. Exact local top-k supports exact merged top-k for comparable vector metrics; approximate local results preserve no unmeasured recall guarantee. Any partition error fails the whole query. There is no global read snapshot, and lexical/hybrid scatter is rejected because partition-local BM25 statistics do not establish a valid global ranking.
