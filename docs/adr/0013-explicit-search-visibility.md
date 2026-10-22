---
status: accepted
---

# Explicit search visibility

Vector indexing may proceed asynchronously after a durable document write. Callers can explicitly wait until their write is searchable, and indexing progress is observable. This separates durable commit from search visibility to support write throughput while giving applications control over freshness. Search must still enforce deletion and authorization rules. Visibility tokens, timeout behavior, replica selection, and the precise cross-shard visibility contract remain undecided.
