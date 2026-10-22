---
status: accepted
---

# Atomic document and job commit

Saving a document and submitting its processing job will be atomic so one cannot commit without the other. This eliminates the application-level dual-write gap and requires coordinating the document and queue commit. The transaction and shard boundary remains undecided; this decision does not promise arbitrary cross-shard transactions.
