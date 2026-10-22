# ADR 0020: Initial runtime mechanisms

Status: implemented development mechanisms; production qualification pending.

Rust and Apache-2.0 are confirmed by the project owner. The initial implementation uses Tokio/Axum, redb 2.6.3 immediate transactions, OpenRaft 0.9, a native HTTP/WebSocket JSON protocol, and a TypeScript SDK. Raft logs use incremental redb tables rather than rewriting a full JSON log. Each fixed physical shard runs its own consensus group; explicit voter placement and safe membership movement operate within configured discovery peers.

Cache operations share durable transaction semantics. Optional leases expose fencing generations, and external handlers remain responsible for idempotency and cancellation. Queue claims use due-time and ordering indexes. Indexed live queries use atomic snapshots/cursors and coalesced state deltas; they do not promise delivery of every intermediate mutation. Browser capability and scoped service credential permissions are separate.

Lexical postings update with documents. Vector submissions are versioned and asynchronously applied; exact search is the baseline. The persisted single-layer neighbor graph is an experimental implementation, not a production-proven HNSW choice. Numeric values are restricted to a finite f32-compatible range to prevent invalid similarity arithmetic. BM25 plus RRF supplies partition-local hybrid search; vector scatter checks compatible schemas across partitions.

Explicit limitations: per-node admission/per-partition logical storage budgets, fixed shard counts, no global search snapshot, no global hybrid statistics, no automated fleet backup control plane, no production ANN scale or sustained-load qualification. These are release gates, not silently removed product requirements. See IMPLEMENTATION.md and OPERATIONS.md.
