---
status: accepted
---

# Partition-scoped transactions

Initial atomic document-plus-job operations require related records and jobs to share an explicit logical partition key; unsupported cross-partition atomic operations are rejected. This trades application data-placement constraints for simpler distributed correctness and performance. Cross-partition transactions are deferred. The logical partition key does not yet determine physical shard layout, and no universal order or tenant key is prescribed.
