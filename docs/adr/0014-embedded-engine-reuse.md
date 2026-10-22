---
status: accepted
---

# Reuse embedded engine components

Substrate may embed existing storage and indexing libraries while remaining one independently deployable service with no required external database. This trades dependency, build, and integration constraints for avoiding unnecessary reinvention of mature storage machinery. No specific storage, vector-index, or consensus library has been selected.
