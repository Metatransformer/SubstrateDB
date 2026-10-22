# Contributing

Keep command application deterministic: operation time, IDs and other nondeterministic inputs are chosen before replication. A mutation's document/index/event/job effects and request deduplication belong in one engine transaction. Never acknowledge a durable write before the persistence/quorum boundary. An external job is at least once; do not promise exactly-once side effects.

Preserve namespace/partition boundaries in every key and authorize before exposing data. Derived vectors must validate source revisions; deletes must not be undone by delayed indexing. New queue transitions must maintain due-time and ordering indexes. Snapshot installation must remain recoverable at every durable boundary.

Add regression tests for changed semantics, including rollback or restart when relevant. Run formatting, Clippy with warnings denied, the Rust tests, SDK tests and appropriate end-to-end scripts from README.md. Do not weaken quorum, durability or authorization to improve a benchmark. Record hardware, parameters, recall and latency distributions together; small synthetic results do not establish large-scale readiness.

Wire changes are currently allowed in development but require matching SDK, examples and protocol documentation. Persistent format changes require migration/rejection behavior and recovery tests before supporting existing installations. See docs/IMPLEMENTATION.md for remaining production gates.
