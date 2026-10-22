---
status: accepted
---

# Initial query scope with a path to aggregation

The initial document API supports nested documents, CRUD, conditional updates, secondary/compound indexes, filtering, sorting, projection, and pagination. Joins and a general aggregation pipeline are deferred to keep the initial engine tractable; aggregation remains an explicit future capability. Preserve an extensible query representation without implementing speculative aggregation machinery now. The supported live-query subset remains to be specified.
