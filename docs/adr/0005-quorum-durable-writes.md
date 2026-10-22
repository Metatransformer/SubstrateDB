---
status: accepted
---

# Quorum-durable writes

Durable documents and jobs will be acknowledged only under a protocol that preserves acknowledged writes through one node failure in a properly configured three-node cluster. Without majority, writes pause or fail instead of accepting divergent durable state. This trades latency and availability for durability and safety. The specific protocol and storage engine remain open; ephemeral and cache guarantees are separate and undecided.
