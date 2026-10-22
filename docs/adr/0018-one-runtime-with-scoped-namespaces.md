---
status: accepted
---

# One runtime with scoped namespaces

The same binary supports standalone and replicated/sharded operation without a mandatory external coordinator. Clusters host isolated namespaces with scoped credentials and resource limits; application tenancy remains a separate concept, and neither namespace nor tenant dictates a single physical shard. This preserves a small deployment footprint while supporting multiple applications, at the cost of implementing coordination and isolation inside Substrate. A standalone node does not claim replicated machine-failure tolerance.
