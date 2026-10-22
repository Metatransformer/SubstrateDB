---
status: accepted
---

# Indexed live-query boundary

Initial live subscriptions cover individual documents and indexed filtered document sets, supplying an initial snapshot followed by additions, updates, and removals. Index support is an explicit subscription requirement, trading arbitrary-query flexibility for a tractable update path. Sorted top-N, aggregation, and continuously maintained vector-result subscriptions are deferred; ordinary sorted queries and vector searches remain available. Index eligibility and snapshot-to-update handoff still require detailed design.
